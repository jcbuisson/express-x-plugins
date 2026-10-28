import assert from 'node:assert/strict'
import test from 'node:test'
import { Observable, Subject } from 'rxjs'
import { shareQueryObservables } from '../src/electric-query-observable.mjs'

test('equivalent queries share work, replay, and release their last subscription', () => {
   let starts = 0, stops = 0
   const source = new Subject()
   const observe = shareQueryObservables(() => new Observable(subscriber => {
      starts++
      const subscription = source.subscribe(subscriber)
      return () => { stops++; subscription.unsubscribe() }
   }))
   const a = [], b = []
   const first = observe({ done: false, age: { gte: 1, lt: 5 } })
   const one = first.subscribe(value => a.push(value))
   source.next([1])
   const two = observe({ age: { lt: 5, gte: 1 }, done: false }).subscribe(value => b.push(value))
   assert.equal(starts, 1)
   assert.deepEqual(b, [[1]])
   one.unsubscribe()
   source.next([2])
   assert.deepEqual(a, [[1]])
   assert.deepEqual(b, [[1], [2]])
   assert.equal(stops, 0)
   two.unsubscribe()
   assert.equal(stops, 1)
   const three = first.subscribe(value => a.push(value))
   assert.equal(starts, 2)
   assert.deepEqual(a, [[1]], 'old replay must be discarded')
   three.unsubscribe()
})

test('query keys preserve types and snapshot filters; errors allow retry', () => {
   const queries = []
   let fail = true
   const observe = shareQueryObservables(query => new Observable(subscriber => {
      queries.push(query)
      if (fail) subscriber.error(new Error('read failed'))
      else subscriber.next(query)
   }))
   const query = { id: 1n }
   const first = observe(query)
   query.id = 2n
   first.subscribe({ error() {} })
   fail = false
   const a = first.subscribe()
   const b = observe({ id: '1' }).subscribe()
   assert.deepEqual(queries, [{ id: 1n }, { id: 1n }, { id: '1' }])
   a.unsubscribe()
   b.unsubscribe()
})


test('offline models share database reads and refresh after the last unsubscribe', async () => {
   const { createOfflineElectricModel } = await import('../src/electric-offline-model.mjs')
   let reads = 0
   let rows = [{ data: { uid: 'one', done: false } }]
   const model = createOfflineElectricModel({
      db: { async query() { reads++; return { rows } } }, primaryKey: 'uid', modelName: 'todos',
   })
   const a = [], b = []
   const one = model.getObservable({ done: false }).subscribe(value => a.push(value))
   const two = model.getObservable({ done: false }).subscribe(value => b.push(value))
   await new Promise(resolve => setImmediate(resolve))
   assert.equal(reads, 1)
   assert.deepEqual(a, b)
   one.unsubscribe()
   two.unsubscribe()
   rows = []
   const three = model.getObservable({ done: false }).subscribe(value => a.push(value))
   await new Promise(resolve => setImmediate(resolve))
   assert.equal(reads, 2)
   assert.deepEqual(a.at(-1), [])
   three.unsubscribe()
})
