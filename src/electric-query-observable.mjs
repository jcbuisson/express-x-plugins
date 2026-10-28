import { defer, finalize, shareReplay } from 'rxjs'

// Cache only subscribed queries. Unused query objects retain no model resources.
export function shareQueryObservables(createObservable) {
   const active = new Map()
   return (where = {}) => {
      const query = structuredClone(where)
      const key = JSON.stringify(canonical(query))
      return defer(() => {
         let shared = active.get(key)
         if (!shared) {
            shared = createObservable(query).pipe(
               finalize(() => { if (active.get(key) === shared) active.delete(key) }),
               shareReplay({ bufferSize: 1, refCount: true }),
            )
            active.set(key, shared)
         }
         return shared
      })
   }
}

function canonical(value) {
   if (value instanceof Date) return ['date', value.toISOString()]
   if (value === null) return ['null']
   if (Array.isArray(value)) return ['array', value.map(canonical)]
   if (typeof value === 'object') return ['object', Object.keys(value).sort()
      .filter(key => value[key] !== undefined).map(key => [key, canonical(value[key])])]
   return [typeof value, String(value)]
}
