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
   roomCache.set(app, rooms)
   dataCache.set(app, data)

   app.addDisconnectingListener((socket, reason) => {
      console.log('onSocketDisconnecting', socket.id, reason)
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
      socket.emit('cnx-transfer-token', transferToken)
   
      // when client ask for transfer from fromSocketId to toSocketId
      socket.on('cnx-transfer', async (fromSocketId, toSocketId, claimedToken) => {
         app.log('verbose', `cnx-transfer from ${fromSocketId} to ${toSocketId}`)
         // A socket may only claim its own ID as the destination — prevent session hijacking
         if (toSocketId !== socket.id || typeof claimedToken !== 'string'
            || transferTokens[fromSocketId] !== claimedToken) {
            app.log('verbose', `cnx-transfer rejected: toSocketId ${toSocketId} !== socket.id ${socket.id}`)
            socket.emit('cnx-transfer-error', fromSocketId, toSocketId)
            return
         }
         // copy connection room & data from 'fromSocketId' to 'toSocketId'
         const toSocket = io.sockets.sockets.get(toSocketId)
         // data & rooms of fromSocketId are taken from dataCache and roomCache, since socket no longer exists
         const fromSocketRooms = rooms[fromSocketId]
         if (toSocket && fromSocketRooms) {
            // copy rooms
            for (const room of fromSocketRooms) {
               if (room === fromSocketId) continue // do not include room associated to socket#id
               toSocket.join(room)
            }
            // copy data
            toSocket.data = {
               ...data[fromSocketId],
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
            // send acknowlegment to toSocket
            toSocket.emit('cnx-transfer-ack', fromSocketId, toSocketId)
         } else {
            console.log(`*** CNX TRANSFER ERROR, ${fromSocketId} -> ${toSocketId}`)
            if (toSocket) toSocket.emit('cnx-transfer-error', fromSocketId, toSocketId)
         }
      })
   })
}
