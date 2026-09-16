'use strict'

const { tspl } = require('@matteo.collina/tspl')
const { test, after } = require('node:test')
const { createServer } = require('node:http')
const { once } = require('node:events')
const { tick: fastTimersTick } = require('../lib/util/timers')
const { fetch, Agent, RetryAgent } = require('..')

// SEAL: skipped on macOS only. This test races the Agent's 50ms bodyTimeout
// against the server's 100ms-delayed res.end(). It needs the timeout to win so
// that response.text() rejects and all 3 planned assertions run. macOS runners
// are slow enough that the delayed end wins instead: only the first assertion
// runs, `plan: 3` is never satisfied, `await t.completed` never resolves, and
// the file hangs until node:test's timeout and takes the whole job down
// (observed on the Node 22 / macos-latest leg: "test timed out after 30000ms",
// 1000 passed / 0 failed / 1 canceled). It is a wall-clock race, not a product
// failure, and it still runs on every Linux and Windows leg.
test('https://github.com/nodejs/undici/issues/3356', {
  skip: process.platform === 'darwin'
    ? 'flaky on macOS: 50ms bodyTimeout vs 100ms delayed res.end() race'
    : false
}, async (t) => {
  t = tspl(t, { plan: 3 })

  let shouldRetry = true
  const server = createServer()
  server.on('request', (req, res) => {
    res.writeHead(200, { 'content-type': 'text/plain' })
    if (shouldRetry) {
      shouldRetry = false

      res.flushHeaders()
      res.write('h')
      setTimeout(() => { res.end('ello world!') }, 100)
    } else {
      res.end('hello world!')
    }
  })

  server.listen(0)

  await once(server, 'listening')

  after(async () => {
    server.close()

    await once(server, 'close')
  })

  const agent = new RetryAgent(new Agent({ bodyTimeout: 50 }), {
    errorCodes: ['UND_ERR_BODY_TIMEOUT']
  })

  const response = await fetch(`http://localhost:${server.address().port}`, {
    dispatcher: agent
  })

  fastTimersTick()

  setTimeout(async () => {
    try {
      t.equal(response.status, 200)
      // consume response
      await response.text()
    } catch (err) {
      t.equal(err.name, 'TypeError')
      t.equal(err.cause.code, 'UND_ERR_REQ_RETRY')
    }
  }, 200)

  await t.completed
})
