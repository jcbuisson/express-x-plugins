import { defer, finalize, shareReplay, startWith } from 'rxjs'

// Cache only subscribed queries. Unused query objects retain no model resources.
export function shareQueryObservables(createObservable) {
   const active = new Map()
   return (where = {}, options = {}) => {
      const query = structuredClone(where)
      const key = JSON.stringify(canonical(query))
      return withInitialValue(defer(() => {
         let shared = active.get(key)
         if (!shared) {
            shared = createObservable(query).pipe(
               finalize(() => { if (active.get(key) === shared) active.delete(key) }),
               shareReplay({ bufferSize: 1, refCount: true }),
            )
            active.set(key, shared)
         }
         return shared
      }), options)
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

// Seed UI subscriptions without changing one-shot reads or the shared source.
export function withInitialValue(observable, options = {}) {
   return Object.hasOwn(options, 'initialValue')
      ? observable.pipe(startWith(options.initialValue), shareReplay({ bufferSize: 1, refCount: true }))
      : observable
}
