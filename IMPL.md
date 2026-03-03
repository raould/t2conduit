# 1. Minimal SDK in t2-lang

We'll define a debug SDK that wraps JS operations and deep clones input for safe pipelines.

;; t2-conduit-sdk-debug

(program
  ;; deep clone helper
  (defmacro deep-clone [x]
    `(structuredClone ,x))

  ;; map
  (defmacro map [arr fn]
    `(Array.prototype.map.call (deep-clone ,arr) ,fn))

  ;; filter
  (defmacro filter [arr fn]
    `(Array.prototype.filter.call (deep-clone ,arr) ,fn))

  ;; reduce
  (defmacro reduce [arr fn init]
    `(Array.prototype.reduce.call (deep-clone ,arr) ,fn ,init))

  ;; concat
  (defmacro concat [a b]
    `(Array.prototype.concat.call (deep-clone ,a) (deep-clone ,b)))

  ;; assign
  (defmacro assign [obj updates]
    `(Object.assign (deep-clone ,obj) ,updates))
)

Macros ensure the deep clone is applied before any operation.

These are thin wrappers, easy to step into for debugging.

# 2. Minimal Pipeline Runner
(program
  ;; runPipeline: executes stages sequentially, everything async
  (defmacro runPipeline [source stages sink]
    `(async (let [data ,source]
       (loop [d ,data
              ss ,stages]
         (if (empty? ss)
           (,sink d)
           (recur (async (let [next []]
                    (for [item d]
                      (push next (await ((first ss) item))))
                    next))
                  (rest ss))))))
)

Async-first: all stages return Promises.

Sequential execution, no fusion.

Uses await to simplify async handling.

deep-clone happens inside SDK macros.

# 3. Minimal Pipeline Example
(program
  (let users
    '[{"name":"Alice","active":true}
      {"name":"Bob","active":false}
      {"name":"Carol","active":true}])

  ;; define stage functions
  (defmacro normalize-user [u]
    `(assign ,u {:name (String.prototype.toUpperCase.call (:name ,u))})))

  (defmacro active? [u]
    `(:active ,u))

  ;; run the pipeline
  (runPipeline
    users
    [(map normalize-user)
     (filter active?)]
    (fn [result]
      (console.log result))))

Everything uses SDK macros (map, filter, assign).

Deep clones ensure upstream users remains unmodified.

Works entirely async by default — even though this first MVP doesn’t require network calls.

# 4. Failing Acceptance Test (Example)

We can write a minimal test that deliberately fails to ensure the pipeline wiring works:

(program
  (let testData '[1 2 3])

  ;; deliberately wrong filter to trigger failure
  (runPipeline
    testData
    [(filter (fn [x] (throw "Intentional failure")))]
    (fn [result]
      (console.log "Should not reach here"))))

Ensures error propagation works through async stages.
