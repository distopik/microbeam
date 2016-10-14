'use strict'
const _     = require('lodash'),
      zmq   = require('zmq'),
      sid   = require('hyperid')(),
      Etcd  = require('node-etcd'),
      Bloom = require('bloomrun'),
      sonic = require('jsonic'),
      pack  = require('msgpack'),
      proto = 'tcp',
      eroot = '/services',
      keydx =  /\/services\/([a-zA-Z0-9\{\}\:\-\_\s\.\,\+]+)\/([a-zA-Z0-9\{\}\:\-\_\s\.\,\+]+)/

function fixupUrl(url) {
  /* any overrides set? if so, use them */
  const extIp = process.env.MICRO_EXT_IP
  if (extIp) {
    /* replace */
    url = url.replace(/\:\/\/[a-z0-9\.\-\:]+\:/, `://${extIp}:`)
  }
  return url
}

function genId () { return sid().replace(/\//g, ':') }

function create($, instance) {
  return new Promise(resolve => {
    const sock = zmq.socket('pull')
    sock.bindSync(`${proto}://*:${process.env.MICRO_EXT_PORT || 0}`)

    const url = fixupUrl(sock.getsockopt(zmq.options.last_endpoint)),
          id  = genId()

    console.log(`micro: ${id} pulling from ${url}`)
    resolve({id, url, sock})
  })
}

function main({id, url, sock}, instance) {
  const etcd     = new Etcd(),
        bloom    = Bloom({indexing: 'depth'}),
        gloBloom = Bloom({indexing: 'depth'}),
        patterns = {},
        requests = {},
        sockets  = {}

  _.each(instance.handlers, ({pattern, handler}) => {
    bloom.add(sonic(pattern), handler)
  })

  instance.send = msg => {
    /* register requests */
    const requestId  = genId(),
          pattern    = gloBloom.lookup(msg),
          patternStr = sonic.stringify(pattern)

    return new Promise((resolve, reject) => {
      if (!pattern || !patternStr || !patterns[patternStr]) {
        return reject(new Error(`Message pattern is unroutable: ${sonic.stringify(msg)}, potential: ${patternStr}`))
      }

      let remote = null
      for (const [host, address] of patterns[patternStr]) {
        if (!sockets[address]) {
          sockets[address] = zmq.socket('push')
          sockets[address].connect(address)
        }
        remote = sockets[address]
        break
      }

      if (remote != null) {
        const expiresOn = (new Date()).valueOf() + 5000 /* ms */
        requests[requestId] = {resolve, reject, expiresOn}
        remote.send(pack.pack([{requestId, returnUrl: url}, msg]))
      } else {
        return reject(new Error(`Message is not routable to any host ${patternStr}`))
      }
    })
  }

  sock.on('message', raw => {
    const payload     = pack.unpack(raw),
          [meta, msg] = payload


    console.log('message', {meta, msg})

    if (meta.mode === 'reply') {
      /* reply to a request we did earlier */
      const {requestId} = meta
      if (requestId && requests[requestId]) {
        const {resolve, reject} = requests[requestId]
        delete requests[requestId]

        if (meta.error) {
          reject(meta.error)
        } else {
          resolve(msg)
        }
      }
    } else {
      function reply(err, msg) {
        console.log('reply', {err, msg})
        if (!sockets[meta.returnUrl]) {
          sockets[meta.returnUrl] = zmq.socket('push')
          sockets[meta.returnUrl].bindSync(meta.returnUrl)
        }
        if (err) {
          sockets[meta.returnUrl].send(pack.pack([{
            requestId: meta.requestId,
            mode: 'reply',
            returnUrl: url,
            error: err}, {}]))
        } else {
          sockets[meta.returnUrl].send(pack.pack([{
            requestId: meta.requestId,
            returnUrl: url,
            mode: 'reply'
          }, msg]))
        }
      }
      const handler = bloom.lookup(msg)

      /* match msg */
      if (handler) {
        /* invoke */
        Promise
          .resolve(handler.call(instance, msg, meta))
          .then   ($ => reply(null, $))
          .catch  ($ => reply($))
      } else {
        /* error - no handler - should not happen much */
        reply(new Error('no handler'))
      }
    }
  })

  let calledUpdateYet = false
  /* update etcd every now and then */
  function updateServiceDirectory() {
    Promise.all(_.map(instance.handlers, ({pattern}) => (
      new Promise((resolve, reject) => {
        etcd.set(`${eroot}/${pattern}/${id}`, url, {ttl: 60}, (err) => {
          if (err) {
            reject(err)
          } else {
            resolve()
          }
        })}))))
        .then ($ => {
          if (!calledUpdateYet) {
            calledUpdateYet = true
            instance.postInit &&
              setTimeout($ => instance.postInit.call(instance, instance), 1500)
          }
        })
        .then ($ => setTimeout(updateServiceDirectory, 5000))
        .catch($ => {
          console.error($)
          setTimeout(updateServiceDirectory, 5000)
        })
  }

  /* grill down timeouts */
  function fireRequestTimeouts() {
    const now = (new Date()).valueOf()
    _.each(requests, ({expiresOn, reject}, requestId) => {
      if (expiresOn < now) {
        /* evict */
        console.warn(`request ${requestId} timed out`)
        delete requests[requestId]
        reject(new Error('timeout'))
      }
    })

    setTimeout(fireRequestTimeouts, 25)
  }

  /* watch for service patterns */
  etcd.watcher(eroot, null, {recursive: true})
      .on('change', ({action, node: {key, value}}) => {
        const [match, pattern, host] = keydx.exec(key)
        if (!match || !pattern || !host)
          return false

        if (action === 'expire' || action === 'delete') {
          if (patterns[pattern] && patterns[pattern].has(host)) {
            patterns[pattern].delete(host)
            console.log(`host ${host} down ${pattern}`)

            if (patterns[pattern].size == 0) {
              gloBloom.remove(sonic(pattern))
              patterns.delete(pattern)
            }
          }
        }
        if (action === 'set') {
          if (!patterns[pattern])
            patterns[pattern] = new Map()

          if (!patterns[pattern].has(host)) {
            patterns[pattern].set(host, value)
            gloBloom.add(sonic(pattern))

            console.log(`host ${host} up ${pattern}`)
          }
        }
      })

  fireRequestTimeouts()
  updateServiceDirectory()
}

module.exports = function (initializer) {
  const instance = {
    handlers: [],
    prefix:   {},
    genId
  }

  const service = function (prefix) {
    instance.prefix = prefix
    return instance
  }

  instance.on = function (pattern, handler) {
    pattern = _.assign({}, instance.prefix, _.isString(pattern) ? sonic(pattern) : pattern)
    pattern = sonic.stringify(pattern)

    instance.handlers.push({pattern, handler})
    return instance
  }

  instance.init = function (handler) {
    instance.postInit = handler
  }

  return Promise
    .resolve(initializer.call(instance, service))
    .then   ($ => create ($, instance))
    .then   ($ => main   ($, instance))
}
