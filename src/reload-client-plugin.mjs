/**
 * Enrich `app` with listeners handling socket data transfer on page reload
 *
 * Usage:
 *   reloadPlugin(app)
 */
export async function reloadPlugin(app) {

   const handleTransferToken = token => {
      if (typeof token === 'string') sessionStorage.setItem('cnxtoken', token)
   }

   app.addConnectListener(async (socket) => {
      const socketId = socket.id
      console.log('connect', socketId)
      const prevSocketId = sessionStorage.getItem('cnxid') ?? ''
      const prevTransferToken = sessionStorage.getItem('cnxtoken') ?? ''
      socket.off('cnx-transfer-token', handleTransferToken)
      socket.on('cnx-transfer-token', handleTransferToken)
      sessionStorage.setItem('cnxid', socketId)
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
