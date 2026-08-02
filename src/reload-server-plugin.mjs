import { randomUUID } from 'node:crypto'

  /**
 * Register Express-X reload plugin
 *
 * @param {object} app Express-X/Express application
 * @param {object} options
 */

export const roomCache = new WeakMap()
export const dataCache = new WeakMap()


export async function reloadPlugin(app, options = {}) {

   const io = app.get('io')
   const transferTtlMs = app.get('config')?.reloadTransferTtlMs ?? 2 * 60 * 1000
   const rooms = Object.create(null)
   const data = Object.create(null)
   const transferTokens = Object.create(null)
   const transferExpiryTimers = Object.create(null)
   const consumedSocketIds = new Set()
   roomCache.set(app, rooms)
   dataCache.set(app, data)

   app.addDisconnectingListener((socket, reason) => {
      console.log('onSocketDisconnecting', socket.id, reason)
      if (consumedSocketIds.delete(socket.id)) return
      // save socket data & rooms in caches
      const alreadySavedData = data[socket.id]
      const alreadySavedRooms = rooms[socket.id]

      // Current socket.data takes precedence over stale cached data so that any
      // updates made between disconnections are not overwritten.
      data[socket.id] = Object.assign({}, alreadySavedData, socket.data)
      rooms[socket.id] = new Set(socket.rooms)
      if (alreadySavedRooms) for (const room of alreadySavedRooms) rooms[socket.id].add(room)
      transferTokens[socket.id] = socket.data.__cnxTransferToken
      clearTimeout(transferExpiryTimers[socket.id])
      transferExpiryTimers[socket.id] = setTimeout(() => {
         delete rooms[socket.id]
         delete data[socket.id]
         delete transferTokens[socket.id]
         delete transferExpiryTimers[socket.id]
      }, transferTtlMs)
      transferExpiryTimers[socket.id].unref?.()
   })

   app.addConnectListener((socket) => {
      console.log('onSocketConnect', socket.id)
      const transferToken = randomUUID()
      socket.data.__cnxTransferToken = transferToken
      transferTokens[socket.id] = transferToken
      socket.emit('cnx-transfer-token', transferToken)
   
      // when client ask for transfer from fromSocketId to toSocketId
      socket.on('cnx-transfer', async (fromSocketId, toSocketId, claimedToken) => {
         app.log('verbose', `cnx-transfer from ${fromSocketId} to ${toSocketId}`)
         // A socket may only claim its own ID as the destination — prevent session hijacking
         if (toSocketId !== socket.id || fromSocketId === socket.id || typeof claimedToken !== 'string'
            || transferTokens[fromSocketId] !== claimedToken) {
            app.log('verbose', `cnx-transfer rejected: toSocketId ${toSocketId} !== socket.id ${socket.id}`)
            socket.emit('cnx-transfer-error', fromSocketId, toSocketId)
            return
         }
         // copy connection room & data from 'fromSocketId' to 'toSocketId'
         const toSocket = io.sockets.sockets.get(toSocketId)
         const fromSocket = io.sockets.sockets.get(fromSocketId)
         // Usually the old socket has disconnected and its state is cached. During a
         // fast reload it may still be live, so snapshot it directly instead.
         const fromSocketRooms = rooms[fromSocketId] ?? fromSocket?.rooms
         const fromSocketData = data[fromSocketId] ?? fromSocket?.data
         if (toSocket && fromSocketRooms) {
            // copy rooms
            for (const room of fromSocketRooms) {
               if (room === fromSocketId) continue // do not include room associated to socket#id
               await toSocket.join(room)
            }
            // copy data
            toSocket.data = {
               ...fromSocketData,
               ...toSocket.data,
               __cnxTransferToken: transferToken,
            }
            // console.log('cnx-transfer data', toSocket.data)
            // console.log('cnx-transfer rooms', toSocket.rooms)
            // remove 'from' cache data
            delete rooms[fromSocketId]
            delete data[fromSocketId]
            delete transferTokens[fromSocketId]
            clearTimeout(transferExpiryTimers[fromSocketId])
            delete transferExpiryTimers[fromSocketId]
            if (fromSocket) {
               consumedSocketIds.add(fromSocketId)
               fromSocket.disconnect(true)
            }
            // send acknowlegment to toSocket
            toSocket.emit('cnx-transfer-ack', fromSocketId, toSocketId)
         } else {
            console.log(`*** CNX TRANSFER ERROR, ${fromSocketId} -> ${toSocketId}`)
            if (toSocket) toSocket.emit('cnx-transfer-error', fromSocketId, toSocketId)
         }
      })
   })
}
