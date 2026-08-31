import assert from 'node:assert/strict'
import test from 'node:test'

import { reloadPlugin } from '../src/reload-server-plugin.mjs'

class FakeSocket {
   constructor(id, sockets, disconnectingListener) {
      this.id = id
      this.data = {}
      this.rooms = new Set([id])
      this.handlers = new Map()
      this.emitted = []
      this.sockets = sockets
      this.disconnectingListener = disconnectingListener
      this.joinDelay = Promise.resolve()
   }

   on(event, handler) { this.handlers.set(event, handler) }
   emit(event, ...args) { this.emitted.push([event, ...args]) }
   async join(room) {
      await this.joinDelay
      this.rooms.add(room)
   }
   disconnect() {
      this.disconnectingListener(this, 'server namespace disconnect')
      this.sockets.delete(this.id)
   }
}

test('transfers state when the replacement connects before the old socket disconnects', async () => {
   const sockets = new Map()
   let connectListener
   let disconnectingListener
   const app = {
      get(key) {
         if (key === 'io') return { sockets: { sockets } }
         if (key === 'config') return {}
      },
      addConnectListener(listener) { connectListener = listener },
      addDisconnectingListener(listener) { disconnectingListener = listener },
      log() {},
   }
   await reloadPlugin(app, { authorizeRoomRestore: async () => true })

   const oldSocket = new FakeSocket('old', sockets, (...args) => disconnectingListener(...args))
   oldSocket.data.user = 'Ada'
   oldSocket.rooms.add('project:1')
   sockets.set(oldSocket.id, oldSocket)
   connectListener(oldSocket)
   const transferToken = oldSocket.emitted.find(([event]) => event === 'cnx-transfer-token')[1]

   const newSocket = new FakeSocket('new', sockets, (...args) => disconnectingListener(...args))
   let finishJoin
   newSocket.joinDelay = new Promise(resolve => { finishJoin = resolve })
   sockets.set(newSocket.id, newSocket)
   connectListener(newSocket)
   const transfer = newSocket.handlers.get('cnx-transfer')('old', 'new', transferToken)

   await Promise.resolve()
   assert.equal(newSocket.emitted.some(([event]) => event === 'cnx-transfer-ack'), false)
   finishJoin()
   await transfer

   assert.equal(newSocket.data.user, 'Ada')
   assert.equal(newSocket.rooms.has('project:1'), true)
   assert.equal(sockets.has('old'), false)
   assert.deepEqual(
      newSocket.emitted.find(([event]) => event === 'cnx-transfer-ack').slice(1),
      ['old', 'new'],
   )
})
