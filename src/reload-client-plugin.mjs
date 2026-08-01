import { useSessionStorage } from '@vueuse/core'

/**
 * Enrich `app` with listeners handling socket data transfer on page reload
 *
 * Usage:
 *   reloadPlugin(app)
 */
export async function reloadPlugin(app) {

   const cnxid = useSessionStorage('cnxid', '')
   const cnxtoken = useSessionStorage('cnxtoken', '')
   const handleTransferToken = token => {
      if (typeof token === 'string') cnxtoken.value = token
   }

   app.addConnectListener(async (socket) => {
      const socketId = socket.id
      console.log('connect', socketId)
      const prevSocketId = cnxid.value
      const prevTransferToken = cnxtoken.value
      socket.off('cnx-transfer-token', handleTransferToken)
      socket.on('cnx-transfer-token', handleTransferToken)
      cnxid.value = socketId
      if (prevSocketId && prevTransferToken) {
         console.log('cnx-transfer', prevSocketId, 'to', socketId)
         let timeout
         const cleanup = () => {
            clearTimeout(timeout)
            socket.off('cnx-transfer-ack', handleAck)
            socket.off('cnx-transfer-error', handleError)
         }
         const handleAck = (fromSocketId, toSocketId) => {
            console.log('ACK ACK!!!', fromSocketId, toSocketId)
            cleanup()
         }
         const handleError = (fromSocketId, toSocketId) => {
            console.log('ERR ERR!!!', fromSocketId, toSocketId)
            cleanup()
         }
         socket.once('cnx-transfer-ack', handleAck)
         socket.once('cnx-transfer-error', handleError)
         timeout = setTimeout(cleanup, 5000)
         socket.emit('cnx-transfer', prevSocketId, socketId, prevTransferToken)
      }
   })
}
