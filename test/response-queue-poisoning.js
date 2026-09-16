'use strict'

const assert = require('node:assert')
const { once } = require('node:events')
const { createServer } = require('node:net')
const { test } = require('node:test')
const { Client } = require('..')

function readBody (body) {
  return new Promise((resolve, reject) => {
    let data = ''
    body.setEncoding('latin1')
    body.on('data', chunk => { data += chunk })
    body.on('end', () => resolve(data))
    body.on('error', reject)
  })
}

test('should not reuse an idle socket with buffered unsolicited response bytes', async (t) => {
  let responses = 0

  const server = createServer((socket) => {
    socket.on('data', () => {
      if (responses++ === 0) {
        socket.write(
          'HTTP/1.1 200 OK\r\n' +
          'Connection: keep-alive\r\n' +
          'Keep-Alive: timeout=300\r\n' +
          'Content-Length: 9\r\n' +
          '\r\n' +
          '/request1' +
          'HTTP/1.1 200 OK\r\n' +
          'Poison-Free-Socket: true\r\n' +
          'Connection: keep-alive\r\n' +
          'Keep-Alive: timeout=300\r\n' +
          'Content-Length: 0\r\n' +
          '\r\n'
        )
      } else {
        socket.end(
          'HTTP/1.1 200 OK\r\n' +
          'Connection: close\r\n' +
          'Content-Length: 9\r\n' +
          '\r\n' +
          '/request2'
        )
      }
    })
  })
  t.after(() => server.close())

  server.listen(0, '127.0.0.1')
  await once(server, 'listening')

  const client = new Client(`http://127.0.0.1:${server.address().port}`, {
    keepAliveTimeout: 300e3
  })
  t.after(() => client.close())

  // The unsolicited response rides the same write as the response to
  // /request1, so the socket teardown it triggers races with dispatching
  // /request2. Wait for the disconnect to make the assertion deterministic.
  const disconnected = once(client, 'disconnect')

  const response1 = await client.request({ path: '/request1', method: 'GET' })
  assert.strictEqual(await readBody(response1.body), '/request1')

  await disconnected

  const response2 = await client.request({ path: '/request2', method: 'GET' })
  assert.strictEqual(response2.headers['poison-free-socket'], undefined)
  assert.strictEqual(await readBody(response2.body), '/request2')
})

test('an unsolicited response must not be matched to a queued request', async (t) => {
  let connections = 0

  const server = createServer((socket) => {
    const connection = ++connections

    socket.on('error', () => {})
    socket.once('data', () => {
      if (connection === 1) {
        // The response to /request1 immediately followed by an unsolicited
        // one. Both ride a single write, so the poisoned bytes are buffered
        // on the socket at the exact moment it goes idle while /request2 is
        // still sitting in the queue, unwritten.
        socket.write(
          'HTTP/1.1 200 OK\r\n' +
          'Connection: keep-alive\r\n' +
          'Keep-Alive: timeout=300\r\n' +
          'Content-Length: 9\r\n' +
          '\r\n' +
          '/request1' +
          'HTTP/1.1 200 OK\r\n' +
          'Poison-Free-Socket: true\r\n' +
          'Connection: keep-alive\r\n' +
          'Keep-Alive: timeout=300\r\n' +
          'Content-Length: 8\r\n' +
          '\r\n' +
          'poisoned'
        )
      } else {
        socket.end(
          'HTTP/1.1 200 OK\r\n' +
          'Connection: close\r\n' +
          'Content-Length: 9\r\n' +
          '\r\n' +
          '/request2'
        )
      }
    })
  })
  t.after(() => server.close())

  server.listen(0, '127.0.0.1')
  await once(server, 'listening')

  const client = new Client(`http://127.0.0.1:${server.address().port}`, {
    pipelining: 1,
    keepAliveTimeout: 300e3
  })
  t.after(() => client.close())

  // Both requests are queued up front: /request2 waits in the queue while
  // /request1 is in flight, which is the state the poisoned bytes target.
  const request1 = client.request({ path: '/request1', method: 'GET' })
  const request2 = client.request({ path: '/request2', method: 'GET' })
  request1.catch(() => {})
  request2.catch(() => {})

  const response1 = await request1
  assert.strictEqual(await readBody(response1.body), '/request1')

  const response2 = await request2
  assert.strictEqual(response2.headers['poison-free-socket'], undefined)
  assert.strictEqual(await readBody(response2.body), '/request2')
})
