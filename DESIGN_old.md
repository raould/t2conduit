# t2conduit

Design Document · Version 2.1

*A deterministic, algebraic, macro-optimized streaming pipeline library for t2lang*

License: public domain CC0

---

# Table of Contents

1. Overview
2. Motivation
3. Design Philosophy
4. Core Concepts
5. Purity Model
6. Pure-Enough Stdlib
7. Mode Polymorphism & Sync/Async Annotations
8. Pipeline Operations
9. Macro-Time Optimization
10. Code Generation
11. Type System Integration
12. Examples
13. Performance Characteristics
14. Migration Guide
15. API Reference

---

# 1) Overview

t2conduit is a streaming pipeline library written in t2lang — an s-expression frontend for TypeScript. It provides a principled, algebraically-optimizable approach to data transformation pipelines, where all optimization happens at macro-expansion time rather than at runtime.

t2conduit is implemented entirely as t2lang macros and annotated runtime functions. It requires no special support from the t2lang compiler beyond what any well-designed s-expression language should provide: a macro system with AST access, var metadata that survives to macro-expansion time, generator primitives (`function*`, `yield`, `async function*`, `await`), and `resolve` at macro-expansion time. None of these are t2conduit-specific — they are general t2lang features that t2conduit consumes like any other library macro would.

This means t2conduit is optional, replaceable, and composable with other pipeline designs. If you don't require it, the compiler knows nothing about it.

## Key Features

- Deterministic linear pipelines: Source → Conduit chain → Sink
- Purity as macro-expansion-time metadata — zero runtime overhead
- Fusion happens in the `pipeline` macro, not in a runtime `pipeline()` call
- Fused output is a native JS/TS generator — no custom iterator machinery
- Sync/async determined by annotation; pipeline color inferred automatically by the macro
- Pure-enough stdlib with clearly-documented preconditions
- All-pure-sync pipelines over array sources can lower to a plain for loop

---

# 2) Motivation

## The Problem

JavaScript/TypeScript applications process streams of data constantly. Existing approaches each have real costs:

### Traditional imperative code

```
const results = [];
for (const user of users) {
  const normalized = user.email.toLowerCase().trim();
  if (normalized.length > 0) {
    results.push({ ...user, email: normalized });
  }
}
```

- Not composable — each pipeline is unique imperative code
- Impossible to optimize mechanically
- Mutation-heavy; separation of concerns is manual discipline

### Existing stream libraries (RxJS, Highland, etc.)

```
from(users).pipe(
  map(user => ({ ...user, email: user.email.toLowerCase().trim() })),
  filter(user => user.email.length > 0),
  toArray()
)
```

- No fusion — each operator creates intermediate iterables at runtime
- Unclear purity semantics; optimizer cannot reason about operations
- Observable/stream object overhead for conceptually simple transformations

### The t2conduit approach

```clojure
(pipeline
  (source/array users)
  (conduit/map normalize-email)   ; ^:pure
  (conduit/filter has-email?)     ; ^:pure
  (sink/to-array))
```

With `^:pure` annotations, the `pipeline` macro fuses these into a single generator at macro-expansion time. No runtime IR, no runtime optimizer, no construction overhead. The output is idiomatic JavaScript that any JS engine understands directly.

---

# 3) Design Philosophy

## Macro-Time Over Runtime

The v1 design performed optimization inside the `pipeline()` constructor at runtime. This meant every pipeline construction paid an optimization tax, and the optimizer operated on runtime values rather than static structure.

The v2 design moves all of this into the `pipeline` macro. The programmer writes t2lang; the macro resolves `^:pure` and `^:async` annotations at expansion time, applies fusion rules, and emits optimized TypeScript/JavaScript. The macro is a small, self-contained compiler for pipeline forms. It lives in the t2conduit library — not in the t2lang compiler.

## t2conduit Is a Library, Not a Compiler Feature

The t2lang compiler has no knowledge of Source, Conduit, or Sink. It has no fusion pass, no pipeline IR, no special handling of `conduit/map`. All of that lives in the `pipeline` macro, which is part of the t2conduit library.

What the compiler provides — and must provide for its own reasons, independent of t2conduit:

1. **A macro system with AST access.** The `pipeline` macro walks the pipeline form at expansion time and rewrites it before any code is emitted.
2. **Var metadata surviving to macro-expansion time.** `^:pure` and `^:async` on a `defn` must be readable by macros that encounter that var later. This is standard in Lisp macro systems. *Not yet implemented in t2lang — see §15.*
3. **`resolve` at macro-expansion time.** The macro calls `resolve` to look up what `normalize-email` actually is and reads its metadata. Standard. *Not yet implemented in t2lang — see §15.*
4. **Generator primitives.** `function*`, `yield`, `async function*`, and `await` as t2lang primitives the macro can emit.

That is all. Anyone who doesn't want t2conduit just doesn't require it.

## How the Macro Does Fusion

Fusion is AST rewriting. When the `pipeline` macro sees a sequence of `(conduit/map f)` and `(conduit/filter p)` forms where `f` and `p` carry `^:pure` metadata, it rewrites them into a single fused generator form before any code is emitted. The compiler never sees the original `conduit/map` / `conduit/filter` structure — it sees the already-fused output.

This is exactly what Clojure macros do with `->` and what `core.async` does with `go` — the macro transforms the entire body into something the compiler never sees in its original form.

```clojure
; What the programmer writes:
(pipeline
  (source/array users)
  (conduit/map normalize-email)   ; ^:pure
  (conduit/filter adult?)         ; ^:pure
  (conduit/tap log-user)          ; no ^:pure — effect barrier
  (conduit/map extract-email)     ; ^:pure
  (sink/to-array))

; What the pipeline macro expands to — fusion already done:
(let [seg1 (fn* [iter]
              (generator [u iter]
                (let [e (normalize-email (:email u))]
                  (when (adult? (:age u))
                    (yield (assoc u :email e))))))
      seg2 (fn* [iter]
              (generator [u iter]
                (log-user u)
                (yield u)))
      seg3 (fn* [iter]
              (generator [u iter]
                (yield (extract-email u))))]
  (-> users seg1 seg2 seg3 vec))
```

The compiler sees only functions and function application.

## Trust-Based Purity — The Full Contract

t2conduit does not infer purity. It does not verify purity. It does not provide a development-mode checker. The programmer asserts purity via `^:pure` metadata, and the `pipeline` macro trusts that assertion unconditionally at expansion time.

*If you mark a function `^:pure` and it is not actually pure, the macro will produce wrong results. There is no mechanism to catch this. This is intentional.*

This is the same contract as `restrict` in C, `unsafe` in Rust, and `unsafePerformIO` in Haskell. The rationale:

1. Purity inference in a language that compiles to JavaScript is intractable. JS's dynamism makes static effect analysis unreliable.
2. A runtime checker with known large blind spots gives false confidence, which is worse than no checker.
3. Zero overhead in both production and development.
4. The community may build testing tools — for example, a library that checks for non-determinism by running functions multiple times with cloned inputs and comparing outputs. That is a good idea. It is not t2conduit's job.

## Pipelines Are Native Generators

A conduit is a function from iterator to iterator. Pipeline composition is function composition. The fused result is a single generator function body. There is no custom Source/Conduit/Sink object model at runtime — those are macro-expansion-time concepts only.

---

# 4) Core Concepts

## Pipeline Structure

All pipelines are linear:

```
Source → Conduit → Conduit → ... → Conduit → Sink
```

This is not a graph. Branching, merging, and multiple sources are out of scope.

The linear constraint is a deliberate tradeoff, not an oversight. A macro walking a linear sequence of forms can identify adjacent pure segments and apply rewrite rules mechanically — the structure is fully visible in the syntax. A dataflow graph requires the macro to reason about which branches share state, whether reordering across a join point is safe, and how to handle differing async colors on parallel branches. None of that is intractable in principle, but it adds substantial complexity for uncertain gain: most graph patterns can be expressed as composed independent linear pipelines with minimal overhead.

### Handling Common Graph Patterns

**Fan-out** (one source, multiple consumers): run two pipelines over the same source. If the source is an array this is free. If it is a stream that can only be consumed once, collect it to an array first with `sink/to-array`, then fan out.

```clojure
; Fan-out: same users processed two ways
(def active-emails
  (pipeline (source/array users)
            (conduit/filter (fn ^:pure [u] (.-active u)))
            (conduit/map    (fn ^:pure [u] (.-email u)))
            (sink/to-array)))

(def admin-ids
  (pipeline (source/array users)
            (conduit/filter (fn ^:pure [u] (.-admin u)))
            (conduit/map    (fn ^:pure [u] (.-id u)))
            (sink/to-array)))
```

**Zip** (pair elements from two sources): construct the paired source before the pipeline starts, then process linearly.

```clojure
; Zip: pair users with their scores before the pipeline
(def paired (map vector users scores))   ; or use a zip utility

(def result
  (pipeline (source/array paired)
            (conduit/map (fn ^:pure [[user score]]
                           (assoc user :score score)))
            (sink/to-array)))
```

**Merge** (interleave two streams into one): use `source/merge` to produce a single interleaved source, then process linearly. Note that merge order is not guaranteed for async sources.

```clojure
; Merge: two async streams into one pipeline
; source/merge collapses two streams into one — the pipeline remains linear
(def result
  (pipeline (source/merge stream-a stream-b)
            (conduit/map parse-event)
            (sink/to-array)))
```

*`source/merge` is a planned addition to the stdlib. It is not in the current release.*

## Macro-Time Entities

Source, Conduit, and Sink are macro-expansion-time concepts. They exist in t2lang syntax and in the `pipeline` macro's expansion logic. They do not exist as runtime objects in the emitted JavaScript.

| Entity | Macro-time meaning | Runtime reality |
|---|---|---|
| `Source a` | Produces a stream of values of type `a` | A generator function or array |
| `Conduit a b` | Transforms stream of `a` into stream of `b` | Composed into fused generator body |
| `Sink a r` | Consumes stream of `a`, produces result `r` | A consuming loop or fold |
| `Pipeline r` | Complete linear composition | A single generator function + consumer |

## What Runtime Pipeline Composition Looks Like

After macro expansion, the emitted runtime code is trivial function composition:

```clojure
; The macro expands to something like this — just functions composed:
(-> source seg1 seg2 seg3 sink)
```

No IR, no parse pass, no normalization, no lowering at runtime. The macro has already done all of that.

---

# 5) Purity Model

## Purity Levels

Every function used in a pipeline carries one of two purity levels, expressed as t2lang metadata. The `pipeline` macro reads this metadata at expansion time via `resolve`.

### `^:pure`

The programmer's promise that this function is:

- Deterministic — same inputs always produce same outputs
- Free of observable side effects
- Free of I/O — no network, no file system, no database
- Free of external mutable state — reads no globals that can change

The macro may fuse, reorder, or eliminate `^:pure` operations freely.

```clojure
(defn ^:pure normalize-email [email]
  (-> email str/trim str/lower-case))

(defn ^:pure adult? [age]
  (>= age 18))
```

### No annotation (effect)

Functions without `^:pure` are treated as having arbitrary effects. The macro preserves them exactly as written and does not fuse across them. This is the conservative default.

```clojure
; No ^:pure — treated as an effect barrier
(defn fetch-user-details [user-id]
  (await (fetch (str "/api/users/" user-id))))
```

*There is no `^:local` annotation. A logging call is either pure (it is not) or it is an effect. Mark logging functions with no annotation; they become effect barriers.*

## Effect Barriers

An unannotated function in the middle of a pipeline creates a fusion barrier. The macro fuses everything it can on each side of the barrier independently:

```clojure
(pipeline
  (source/array users)
  (conduit/map normalize-email)  ; ^:pure  ─┐ fused
  (conduit/filter adult?)        ; ^:pure  ─┘
  (conduit/tap log-user)         ;  no annotation — BARRIER
  (conduit/map extract-email)    ; ^:pure  ── fused independently
  (sink/to-set))
```

The macro emits two generator segments separated by the tap, not one fused pass. Order of side effects is always preserved.

---

# 6) Pure-Enough Stdlib

The stdlib provides operations annotated `^:pure` where the promise can be justified by construction. Each tier documents exactly why the promise holds and what preconditions are required.

## Tier 1 — Primitive Operations (Genuinely Pure)

Operations on JavaScript primitives (number, string, boolean, symbol) are genuinely deterministic. Primitives have no identity, no mutable state, and no `valueOf()` surprises when typed correctly.

```clojure
; Purely pure. No caveats.
(Pure/Number.add 1 2)        ;=> 3
(Pure/String.trim "  hi ")   ;=> "hi"
(Pure/Boolean.not false)     ;=> true
```

**Precondition:** pass only primitive values. Passing objects with custom `valueOf()` or `toString()` is undefined behavior.

```
Pure.Number  — add, sub, mul, div, mod, pow, gt, lt, gte, lte, eq, neq,
               abs, floor, ceil, round, min, max, clamp
Pure.String  — concat, trim, trimStart, trimEnd, toUpperCase, toLowerCase,
               length, slice, split, startsWith, endsWith, includes, indexOf,
               padStart, padEnd, repeat, replace (literal, non-regex)
Pure.Boolean — and, or, not, xor
Pure.Util    — identity, constant, isNullish, isNull, isDefined
```

## Tier 2 — Structural Sharing (Pure-Enough via Externally Delegated Trust)

Operations on objects and collections using Ramda. This tier is categorically different from Tiers 1, 3, and 4. There is no structural invariant the macro can enforce, no type-system guarantee, and no expansion-time check. The purity promise here is entirely delegated to Ramda's published contract: Ramda commits to never mutating inputs and always returning new values via structural sharing or copying. Your `^:pure` annotation is only as valid as that external commitment.

This means:

- **Pin Ramda to a specific version.** A Ramda update could, in principle, introduce a mutation — unlikely, but outside your control. Pinning makes the trust relationship explicit and auditable.
- **Treat any input with proxy traps or custom property descriptors as outside this tier.** Ramda's non-mutation guarantee applies to plain objects. An object that mutates on read via a proxy trap can violate purity regardless of what Ramda does. If your inputs might carry these, do not annotate the operation `^:pure`.
- **Understand that you cannot verify this at macro-expansion time.** The macro trusts your annotation. If Ramda's behavior changes or your inputs are non-plain, you will get wrong results silently.

```clojure
(Ramda/assoc :email new-email user)
(Ramda/assocPath [:address :city] "Portland" user)
(Ramda/merge defaults overrides)
(Ramda/omit [:password :token] user)
```

**Precondition:** inputs are plain objects with no proxy traps or mutating property descriptors. Ramda is pinned to a known-good version. You have read Ramda's documented behavior for the specific functions you use and accept responsibility for the trust chain.

## Tier 3 — Frozen Types (Pure-Enough Under Structural Guarantee)

Operations on deeply frozen objects are safe because no getter can mutate through a frozen structure.

```clojure
; Requires deeply frozen input — enforceable via DeepReadonly<T>
(FrozenPure/get frozen-state :current-user)
(FrozenPure/getPath frozen-state [:user :email])
```

## Choosing a Tier

| Situation | Recommended tier |
|---|---|
| Working with numbers, strings, booleans | Pure (Tier 1) |
| Reading/transforming plain objects | Ramda (Tier 2) |
| Working with deeply frozen data structures | FrozenPure (Tier 3) |
| Complex nested object updates | Immer — see escape hatch below |
| Untrusted input with unknown shape | No pure annotation — treat as effect |

*If you need an operation not covered here, define it with `^:pure` and own the promise. The stdlib is just a set of common operations where we have already reasoned through the preconditions for you.*

## Escape Hatch: Immer for Complex Updates

Immer is available in t2conduit for deeply nested object updates that are genuinely awkward to express with Ramda. It does not belong in the purity tier model and should not be treated as a tier you choose when you want purity.

Immer's containment guarantee — that mutations are confined to the producer function and the original state is never touched — is a runtime behavioral invariant enforced by Immer's own proxy machinery. It is not structural, not type-enforced, and not visible to the macro. This is categorically different from every tier above.

The specific failure mode to understand is the **draft escape**: a reference to the draft leaks outside the producer function, either intentionally or accidentally. When this happens, `^:pure` is silently wrong and the macro will produce incorrect results with no diagnostic signal. Draft escapes in practice are uncommon but the failure when they occur is uniquely hard to diagnose — the cause is temporally separated from the effect, potentially by many pipeline executions.

The most common accidental escape patterns are async producers (where an `await` inside the producer races with Immer's finalization), passing the draft to a helper function that stores a reference, and callbacks created inside the producer that close over the draft and fire after it returns.

```clojure
; Use Immer when the update is too complex for Ramda.
; Only annotate ^:pure if you have carefully audited
; that no draft reference escapes the producer.
(Immer/update state
  (fn [draft]
    (set! (.-user.email draft) new-email)
    (set! (.-user.updatedAt draft) timestamp)))
```

**Default posture:** do not annotate Immer producers `^:pure` unless you have audited the producer for escapes. An un-annotated Immer producer is an effect barrier — the macro preserves it exactly as written, fusion stops at it, and correctness is maintained. This is the safe default. Annotating `^:pure` is an opt-in that buys fusion at the cost of taking full responsibility for containment.

---

# 7) Mode Polymorphism & Sync/Async Annotations

Pipelines can execute synchronously or asynchronously. The mode is determined by annotation on individual functions — not on the pipeline call itself — and inferred by the `pipeline` macro at expansion time.

## The Annotation

```clojure
; Sync — macro emits regular function*
(defn ^:pure ^:sync normalize-email [email]
  (-> email str/trim str/lower-case))

; Async — macro emits async function*
(defn ^:async fetch-details [user-id]
  (await (fetch (str "/api/users/" user-id))))
```

## Color Inference

The macro's rule: if any stage in a pipeline resolves to a var with `^:async` metadata, the entire pipeline lowers to `async function*`. Sync stages inside an async pipeline are trivially valid.

Critically, the user never writes `conduit/asyncMap` versus `conduit/map`. There is one `conduit/map`. The macro emits the appropriate generator flavor based on what it finds in the resolved vars' metadata.

```clojure
; Automatically async because fetch-details is ^:async.
; Nothing special written on the pipeline call.
(pipeline
  (source/array user-ids)
  (conduit/map normalize-id)    ; ^:pure ^:sync
  (conduit/map fetch-details)   ; ^:async — makes whole pipeline async
  (conduit/map parse-response)  ; ^:pure ^:sync, fine in async context
  (sink/to-array))
```

## Inline Lambdas

For inline lambdas written directly inside a `pipeline` form, the macro reads the annotation from the AST node — no `resolve` needed. The same rule applies: `^:async` present → async generator; absent → sync generator.

```clojure
; Inline lambda — must be annotated explicitly.
(conduit/map (fn ^:async [batch]
               (await (Promise.all ((. batch map) fetch-user-details)))))
```

If `^:async` is omitted from an inline lambda that contains `await`, the macro emits a sync generator and TypeScript flags `await` in a non-async context at compile time. The macro does not inspect the lambda body for `await` — annotation is the programmer's responsibility.

## Solving "What Color Is Your Pipeline?"

The classic problem: adding one async operation forces rewriting every function call to its async variant, propagating async color up through all callers.

t2conduit avoids this because the annotation lives on the function's var metadata, not on the pipeline operator. The macro reads it at expansion time. Adding an async stage to a sync pipeline requires changing nothing else — the macro sees `^:async` and emits the right generator type automatically.

---

# 8) Pipeline Operations

## Sources

| Operation | Description |
|---|---|
| `(source/array coll)` | Emit elements of a JS array or ArrayLike |
| `(source/range start end)` | Emit integers from start (inclusive) to end (exclusive) |
| `(source/read-lines path)` | Emit lines of a file (async) |
| `(source/channel ch)` | *(v2)* Emit values from a channel — see note below |
| `(source/fetch url)` | Emit response body as a byte stream (async) |
| `(source/of & vals)` | Emit a fixed sequence of values |
| `(source/empty)` | Emit nothing |

**v1 source model is pull-only.** All v1 sources (`array`, `range`, `read-lines`, `fetch`, `of`, `empty`) are pull-based: the pipeline drives iteration. `source/channel` is a push-based primitive — a producer/consumer coordination point where an external producer puts values onto the channel and the pipeline consumes them. Designing this correctly (backpressure, closing semantics, error propagation) requires its own dedicated treatment and is deferred to v2.

## Conduits

| Operation | Description |
|---|---|
| `(conduit/map f)` | Apply f to each element |
| `(conduit/filter pred)` | Keep elements where pred returns truthy |
| `(conduit/mapMaybe f)` | Apply f; keep elements where result is not nil/none |
| `(conduit/flatMap f)` | Apply f to each element; flatten one level of results |
| `(conduit/tap f)` | Apply f for side effects; pass element through unchanged |
| `(conduit/take n)` | Keep first n elements |
| `(conduit/drop n)` | Skip first n elements |
| `(conduit/takeWhile pred)` | Keep elements while pred is true |
| `(conduit/dropWhile pred)` | Skip elements while pred is true |
| `(conduit/distinct)` | Remove **consecutive** duplicates (streaming, like Unix `uniq`) |
| `(conduit/buffer n)` | Batch into arrays of size n (async: run n concurrent ops) |
| `(conduit/partition n)` | Emit non-overlapping windows of size n |
| `(conduit/scan f init)` | Emit running accumulation (like reduce but streaming) |

*Note: `conduit/distinct` removes consecutive duplicates only. To remove all duplicates globally, use `sink/to-set` or filter against an accumulated set in a stateful conduit.*

## Sinks

| Operation | Description |
|---|---|
| `(sink/to-array)` | Collect all elements into a JS array |
| `(sink/to-set)` | Collect unique elements into a JS Set |
| `(sink/to-map key-fn)` | Collect into a Map keyed by key-fn(element) |
| `(sink/reduce f init)` | Fold elements into a single value |
| `(sink/sum)` | Sum numeric elements |
| `(sink/count)` | Count elements |
| `(sink/first)` | Return first element or undefined |
| `(sink/last)` | Return last element or undefined |
| `(sink/for-each f)` | Apply f to each element; return undefined |
| `(sink/into coll)` | Append elements to an existing collection |

## Error Handling (v1)

t2conduit v1 has no special error semantics. Exceptions thrown inside a stage — whether a sync throw or an async rejection — propagate out of the emitted generator or function using normal JavaScript exception mechanics. The pipeline's caller handles them with `try/catch` or `.catch()` on the returned `Promise`, exactly as they would with any other JS code.

Per-element error recovery (e.g. skipping bad elements, collecting errors alongside results, `conduit/mapResult`) is deferred to v2.

---

# 9) Macro-Time Optimization

All optimization in t2conduit happens inside the `pipeline` macro at expansion time, before any JavaScript is emitted.

## What the Macro Does

```
t2lang source
  ↓
pipeline macro invoked
  ↓
resolve each stage var, read ^:pure / ^:sync / ^:async metadata
  ↓
identify adjacent pure segments and effect barriers
  ↓
apply algebraic rewrite rules to pure segments
  ↓
infer sync/async mode from resolved metadata
  ↓
emit fused generator forms (or flat loop if eligible)
  ↓
compiler sees only the emitted forms — no pipeline IR, no Source/Conduit/Sink
```

## Fusion Rules (Applied Only to `^:pure` Segments)

### Map fusion

```
map f ∘ map g  →  map (x => f(g(x)))

; Macro emits a single let-binding chain:
(generator [x iter]
  (let [a (g x)
        b (f a)]
    (yield b)))
```

### Filter fusion

```
filter p ∘ filter q  →  filter (x => p(x) && q(x))
```

### Map-filter fusion (via mapMaybe)

```
filter p ∘ map f  →  mapMaybe (x => { const y = f(x); return p(y) ? y : none; })

; Better than applying f twice via naive filter+map reorder.
```

### Identity elimination

```
map identity      →  (remove stage entirely)
filter (_ => true) →  (remove stage entirely)
```

### Effect barriers

Fusion stops at any unannotated stage. The macro fuses on each side of the barrier independently; order of side effects is always preserved.

```
; Input: map f → map g → tap log → map h → filter p
; f, g are ^:pure. log has no annotation. h, p are ^:pure.
; Macro fuses:
;   mapMaybe(x => { let a=f(x); let b=g(a); return b; })
;   → tap(log)
;   → mapMaybe(x => { let a=h(x); return p(a) ? a : none; })
```

## Flat Loop Optimization

When all of these conditions hold, the macro lowers the entire pipeline to a plain for loop with no generator overhead:

- All stages are `^:pure`
- All stages are `^:sync`
- The source is a finite array (`source/array` or `source/of`)

**How the macro checks eligibility.** For each stage, the macro calls `resolve` on the stage form and reads the `^:pure` and `^:sync` metadata off the resolved var. All stdlib conduit forms (`conduit/map`, `conduit/filter`, `conduit/take`, etc.) are defined with `^:pure ^:sync` in their source, so they pass the check automatically — no special whitelist is needed. Inline lambdas annotated `^:pure` also pass. The eligibility check is uniform: stdlib and user-defined functions are treated identically; the only question is whether the resolved var carries both annotations. This requires `(resolve sym)` at macro-expansion time. *Not yet implemented in t2lang — see §15.*

**`conduit/take` in flat loops.** Although `conduit/take` maintains a count internally, it is structurally pure: for a given input sequence and count `n`, the output is always the first `n` elements. Its `^:pure` annotation is accurate. The macro handles it as an early-exit: instead of a generator `yield`, it emits a `break` when the count is reached (see Example 5).

```clojure
; t2lang:
(pipeline
  (source/array [1 2 3 4 5])
  (conduit/map (fn ^:pure [x] (+ x 1)))
  (conduit/filter (fn ^:pure [x] (> x 3)))
  (sink/to-array))

; Emitted JavaScript (no generators, no iterators):
function runPipeline(input) {
  const result = [];
  for (let i = 0; i < input.length; i++) {
    const a = input[i] + 1;
    if (a > 3) result.push(a);
  }
  return result;
}
```

This is faster than even a fused generator. The macro chooses it automatically.

---

# 10) Code Generation

## Fused Generator Output

For the general case, the macro emits fused generator functions:

```clojure
; t2lang input:
(pipeline
  (source/array users)
  (conduit/filter (fn ^:pure [u] (.-active u)))
  (conduit/map normalize-email)   ; ^:pure
  (conduit/filter adult?)         ; ^:pure
  (conduit/tap log-user)          ; no annotation — barrier
  (conduit/map extract-email)     ; ^:pure
  (sink/to-set))
```

```typescript
; Emitted TypeScript:
function runPipeline(users: User[]): Set<string> {
  // Segment 1: fused (filter + map + filter → mapMaybe)
  function* seg1(iter: Iterable<User>) {
    for (const u of iter) {
      if (!u.active) continue;
      const email = normalizeEmail(u.email);
      if (!adult(u.age)) continue;
      yield { ...u, email };
    }
  }
  // Barrier: tap (log-user)
  function* seg2(iter: Iterable<User>) {
    for (const u of iter) {
      logUser(u);
      yield u;
    }
  }
  // Segment 3: fused (single pure map)
  function* seg3(iter: Iterable<User>) {
    for (const u of iter) {
      yield extractEmail(u);
    }
  }
  // Sink
  return new Set(seg3(seg2(seg1(users))));
}
```

## Async Pipeline Output

When any stage resolves to a `^:async` var, the macro emits `async function*` throughout:

```typescript
// Downstream sync stages become async generators transparently:
async function* seg1(iter: AsyncIterable<string>) {
  for await (const id of iter) {
    yield await fetchDetails(id);  // ^:async
  }
}
async function* seg2(iter: AsyncIterable<User>) {
  for await (const u of iter) {
    const email = normalizeEmail(u.email);  // sync, no await needed
    if (adult(u.age)) yield { ...u, email };
  }
}
```

---

# 11) Type System Integration

## Purity in the Type System

Purity (`^:pure`) is a macro-expansion-time concept only. The macro reads `^:pure` annotations when deciding whether to fuse stages; the emitted TypeScript carries no trace of purity. There is no `Pure<F>` wrapper type, no runtime purity property, and no TypeScript type that encodes fusion eligibility — all of that reasoning is done and discarded at expansion time.

## Pipeline Type Propagation

The macro tracks element types through the pipeline, catching type mismatches between conduit stages:

```typescript
// Type error caught at compile time:
pipeline(
  Source.array<User>(users),
  Conduit.map((u: User): string => u.email),  // User → string
  Conduit.filter((x: number) => x > 0),       // ERROR: expects number, got string
  Sink.toArray()
);
```

## Sync/Async Type Safety

The sync/async mode of a pipeline is reflected in the return type of the emitted code. Because the macro emits concrete TypeScript — not a runtime object — TypeScript infers the type directly from the expansion:

```typescript
// All-sync pipeline: macro emits a plain for loop or sync generator.
// TypeScript infers string[] from the for loop body.
const result: string[] = runPipeline(users);

// Any-async pipeline: macro emits async function* + for-await consumer.
// TypeScript infers Promise<string[]> from the async wrapper.
const result: Promise<string[]> = runPipeline(userIds);
```

The `pipeline` macro is responsible for emitting the correct generator flavor (`function*` vs `async function*`) and the correct sink wrapper. There is no runtime `.run()` call — the return type is a property of the emitted code, not of any pipeline object.

---

# 12) Examples

## Example 1: User Data Processing (All Sync)

```clojure
(defn ^:pure normalize-email [email]
  (-> email str/trim str/lower-case))

(defn ^:pure adult? [age]
  (>= age 18))

(def adult-emails
  (pipeline
    (source/array users)
    (conduit/filter (fn ^:pure [u] (.-active u)))
    (conduit/map    (fn ^:pure [u]
                      (assoc u :email (normalize-email (.-email u)))))
    (conduit/filter (fn ^:pure [u] (adult? (.-age u))))
    (conduit/map    (fn ^:pure [u] (.-email u)))
    (sink/to-set)))

; Macro fuses all four conduit stages into a single loop.
; adult-emails is a Set<string>.
```

## Example 2: Async Enrichment

```clojure
(defn ^:async fetch-user-details [user-id]
  (let [resp (await (fetch (str "/api/users/" user-id)))]
    (await (.json resp))))

(def enriched-users
  (pipeline
    (source/array ["u1" "u2" "u3"])
    (conduit/map fetch-user-details)         ; ^:async — makes whole pipeline async
    (conduit/filter (fn ^:pure [u]
                      (not (nil? (.-email u)))))
    (conduit/map (fn ^:pure [u]
                   (assoc u :email (str/lower-case (.-email u)))))
    (sink/to-array)))

; Call site: (def result (await (enriched-users)))
```

## Example 3: File Processing

```clojure
(def processed-csv
  (pipeline
    (source/read-lines "data.csv")  ; ^:async
    (conduit/drop 1)                ; skip header
    (conduit/map str/trim)          ; ^:pure
    (conduit/filter (fn ^:pure [line]
                      (> (count line) 0)))
    (conduit/map (fn ^:pure [line]
                   (mapv str/trim (str/split line ","))))
    (sink/to-array)))
```

## Example 4: Mixed Purity — Effect Barriers

```clojure
(def save-to-db
  (fn [x] (await (db/save! x))))  ; no ^:pure — effect

(def result
  (pipeline
    (source/range 1 101)
    (conduit/map (fn ^:pure [x] (* x 2)))  ; ^:pure ─┐ fused
    (conduit/map (fn ^:pure [x] (* x 2)))  ; ^:pure ─┘
    (conduit/tap save-to-db)               ; BARRIER
    (conduit/filter (fn ^:pure [x]         ; ^:pure — fused independently
                      (> x 100)))
    (sink/to-array)))

; Macro emits:
;   Segment 1: fused map+map → single generator
;   Barrier:   save-to-db tap (async, preserves all calls)
;   Segment 2: filter in its own generator
```

## Example 5: Flat Loop Optimization

```clojure
; All ^:pure, all ^:sync, finite array source.
; Macro emits a plain for loop — no generators at all.
(def result
  (pipeline
    (source/array [1 2 3 4 5 6 7 8 9 10])
    (conduit/map    (fn ^:pure [x] (+ x 5)))
    (conduit/filter (fn ^:pure [x] (> x 7)))
    (conduit/map    (fn ^:pure [x] (* x 2)))
    (conduit/take 5)
    (sink/sum)))

; Emitted JS:
; let acc = 0, count = 0;
; for (const x of source) {
;   const a = x + 5; if (a <= 7) continue;
;   const b = a * 2; if (count >= 5) break;
;   acc += b; count++;
; }
; return acc;
```

---

# 13) Performance Characteristics

## The Cost Model

Because optimization happens at macro-expansion time, there is no runtime cost to pipeline construction. The emitted code is just function composition — effectively free to set up.

| Pipeline type | Lowering | Relative cost |
|---|---|---|
| All `^:pure`, `^:sync`, array source | Plain for loop | Fastest — no generator overhead |
| All `^:pure`, `^:sync`, non-array source | Fused sync generator* | Fast — one generator, no intermediates |
| `^:pure` with effect barriers | Multiple generators, composed | Good — minimal intermediates |
| Any `^:async` stage | Fused async generator* | I/O-bound; generator overhead is noise |
| Unfused (all effect) | One generator per stage | Same as naive chaining |

## Async Concurrency

`conduit/buffer n` batches incoming elements into arrays of size `n`. For async I/O, pair it with a `conduit/map` that fans the batch out concurrently via `Promise.all`:

```clojure
; Without buffering: API calls execute one at a time.
; Total time ≈ n * per-call latency.
; With buffer + Promise.all: 100 calls run concurrently per batch.
; Total time ≈ (n / 100) * per-call latency — proportionally faster
; by roughly the concurrency factor, assuming the upstream can keep up.
(pipeline
  (source/array user-ids)
  (conduit/buffer 100)                                          ; batch into arrays of 100
  (conduit/map (fn ^:async [batch]
                 (await (Promise.all ((. batch map) fetch-user-details)))))
  (conduit/flatMap identity)                                    ; flatten batches back to elements
  (conduit/map parse-response)
  (sink/to-array))
```

The actual speedup depends on per-call latency, upstream throughput, and server-side rate limiting. The principle is structural: sequential I/O is bounded by latency per call; concurrent I/O amortizes that latency across the batch.

---

# 14) Migration Guide

## From RxJS

```typescript
; RxJS
from(users).pipe(
  map(u => ({ ...u, email: u.email.toLowerCase() })),
  filter(u => u.age >= 18),
  toArray()
).subscribe(result => console.log(result));
```

```clojure
; t2conduit
(def result
  (pipeline
    (source/array users)
    (conduit/map (fn ^:pure [u]
                   (assoc u :email (str/lower-case (.-email u)))))
    (conduit/filter (fn ^:pure [u] (>= (.-age u) 18)))
    (sink/to-array)))
```

## From Array Method Chaining

```typescript
; Array methods — three passes, two intermediate arrays
const result = users
  .map(u => normalizeEmail(u.email))
  .filter(e => e.length > 0)
  .map(e => e.toUpperCase());
```

```clojure1
; t2conduit — fused to one pass, zero intermediate arrays
(def result
  (pipeline
    (source/array users)
    (conduit/map    (fn ^:pure [u] (normalize-email (.-email u))))
    (conduit/filter (fn ^:pure [e] (> (count e) 0)))
    (conduit/map    Pure/String.toUpperCase)
    (sink/to-array)))
```

## From Lodash Chain

```typescript
; Lodash
const result = _.chain(users)
  .map(u => u.email.toLowerCase())
  .filter(e => e.length > 0)
  .uniq()     ; removes ALL duplicates globally
  .value();
```

```clojure
; t2conduit
; Note: conduit/distinct removes consecutive duplicates only (streaming).
; For global deduplication, use sink/to-set:
(def result
  (pipeline
    (source/array users)
    (conduit/map    (fn ^:pure [u] (str/lower-case (.-email u))))
    (conduit/filter (fn ^:pure [e] (> (count e) 0)))
    (sink/to-set)))   ; Set eliminates all duplicates globally
```

---

# 15) API Reference

## Purity Annotations (t2lang Metadata)

```clojure
^:pure   ; Function is deterministic, no side effects.
         ; pipeline macro may fuse, reorder, or eliminate.
         ; No annotation = effect = macro preserves as-is.

^:sync   ; Function is synchronous (macro emits regular generator).
^:async  ; Function is asynchronous (macro emits async generator).
         ; If omitted, macro infers from function body.
```

## Stdlib Namespaces

- `Pure.Number` — add, sub, mul, div, mod, pow, gt, lt, gte, lte, eq, neq, abs, floor, ceil, round, min, max, clamp
- `Pure.String` — concat, trim, trimStart, trimEnd, toUpperCase, toLowerCase, length, slice, split, startsWith, endsWith, includes, indexOf, padStart, padEnd, repeat, replace
- `Pure.Boolean` — and, or, not, xor
- `Pure.Util` — identity, constant, isNullish, isNull, isDefined
- `Ramda` — Array.map/filter/reduce/sort/uniq; Object.assoc/assocPath/merge/omit/pick/dissoc *(Tier 2: library trust)*
- `Immer` — update (escape hatch for complex updates; not a purity tier — see Section 6)
- `FrozenPure` — get, getPath *(Tier 3: requires DeepReadonly input)*

## Pipeline Construction

```clojure
(pipeline source & conduits sink)

; Macro. Expands at macro-expansion time.
; Resolves all stage vars, reads metadata, applies fusion rules,
; infers sync/async mode, emits optimized generator or flat loop.
; Returns a Pipeline<R> where R is the sink's result type.
; Sync if all stages are sync; async if any stage is async.
; No runtime optimization — all of that happened at expansion time.
```

The `& conduits` rest parameter requires variadic macro support in t2lang. *Not yet implemented — see §15.*

## t2lang Prerequisites

t2conduit requires the following from the t2lang compiler — all general-purpose features, none t2conduit-specific:

| Prerequisite | Status |
|---|---|
| Macro system with full AST access | ✓ Implemented (stage5) |
| `function*`, `yield`, `async function*`, `await` as emittable primitives | ✓ Implemented (stage5) |
| Variadic macro parameters (`& rest`) | ✗ Not yet implemented |
| Var metadata (`^:pure`, `^:async`, etc.) readable by macros at expansion time | ✗ Not yet implemented |
| `(resolve sym)` available at macro-expansion time | ✗ Not yet implemented |

**t2conduit cannot be fully implemented until variadic macros, var metadata, and `resolve` are added to t2lang.** Without them, the `pipeline` macro can only read annotations on inline lambdas defined directly inside the pipeline form — it cannot look up metadata on named vars defined elsewhere. Cross-file use and any non-trivial library are blocked on these two features.

## Development & Tooling

t2conduit intentionally provides no runtime purity checker. Available as separate community projects:

- `t2conduit-test` — runs functions multiple times with deep-cloned inputs, probes for non-determinism
- `t2conduit-inspect` — macro plugin that emits a human-readable fusion report alongside expanded output
- `t2conduit-bench` — benchmarking harness comparing fused vs unfused pipeline performance

*These are not part of t2conduit itself.*

---

# Summary

t2conduit v2 is a library macro, not a compiler feature. The core design:

1. **Purity is a macro-expansion-time promise** — `^:pure` metadata on t2lang functions, trusted unconditionally, with no runtime verification machinery.
2. **Fusion happens in the `pipeline` macro** — the macro resolves vars, reads metadata, rewrites the pipeline AST, and emits optimized generators before the compiler ever sees the pipeline form.
3. **The compiler knows nothing about t2conduit** — it sees only the expanded output: functions, generators, and function application.
4. **The fused output is a native JS generator** — no custom iterator objects, no intermediate IR at runtime.
5. **Sync/async is annotated per-function and inferred per-pipeline** — you never write `conduit/asyncMap`; the macro reads `^:async` off the var and emits the right generator type.
6. **The pure-enough stdlib provides a curated set of operations** with documented preconditions and clearly differentiated trust models: three purity tiers (genuine, delegated, structural) plus an Immer escape hatch that sits deliberately outside the tier model.
7. **All-pure, all-sync, array-source pipelines lower to plain for loops** — the macro emits this automatically when eligible.
8. **`^:pure` is an assertion the programmer makes and the macro trusts unconditionally** — the correctness of the optimized output depends entirely on that assertion being true.

What v2 removes from v1: the runtime IR, the runtime optimizer, the runtime lowering pass, the dev-mode Proxy checker, the `^:local` annotation tier, the `conduit/asyncMap` vs `conduit/map` distinction, and the pipeline construction overhead.

What v2 removes from the v2.0 draft: the framing of t2conduit as a compiler subsystem. t2conduit is a library. The compiler provides general-purpose primitives; the `pipeline` macro does the work.
