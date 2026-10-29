// Own each stream's requests while honoring an optional caller cancellation.
export function streamAbort(externalSignal) {
   const controller = new AbortController()
   const forward = () => controller.abort(externalSignal.reason)
   externalSignal?.addEventListener('abort', forward, { once: true })
   if (externalSignal?.aborted) forward()
   return {
      signal: controller.signal,
      abort() {
         externalSignal?.removeEventListener('abort', forward)
         controller.abort()
      },
   }
}
