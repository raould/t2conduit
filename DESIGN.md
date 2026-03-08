
# t2-conduit Design Document (MVP)
## 1. Overview

**t2-conduit** is a **typed, linear, pull-based pipeline framework** for TypeScript.
It is designed to allow **simple, composable data transformations** while supporting:

* Pull-based execution via async generators (consumer-driven).
* Async-by-default stages with constant-memory guarantees for linear pipelines.
* Capability injection — stages declare typed context requirements; the runner provides scoped context per named slot.
* Exception-safe resource management via `await using` on the pipeline context.
* Instrumented logging for debugging and observability.
* Compile-time type checking between stages and across capability requirements.
* Future extensibility (Transformer fusion, batch-aware sinks, parallel stages).

### Goals

* MVP that is **usable in real projects** without requiring AST-level macro work.
* Simple stage API that is **typed and composable**.
* Instrumentation hooks without over-complicating stage implementation.
* Allow multiple runner implementations in the future (e.g., debug vs. performant).

### Non-Goals

* **Push/reactive pipelines** — use rxjs or similar for producer-driven, event-reactive streams.
* **Parallel or concurrent stage execution** — the pull chain is single-threaded and linear in the MVP.
* **Fan-out / multi-sink pipelines** — one source, one sink per pipeline; branching is out of scope.
* **Built-in serialization, schema validation, or data coercion** — stages handle their own data shapes.
* **Distributed or cross-process execution** — t2-conduit runs in a single process.
* **Automatic performance tuning or backpressure negotiation** — the consumer drives at its own pace; no adaptive rate control.

---

## 2. Core Concepts

### 2.1 Pull-Based Execution

Pull semantics are implemented via **async generators**. Each stage wraps the upstream generator and suspends at each `yield` until downstream requests the next item. The sink is the actual driver — it calls `next()` (directly or via `for await...of`), which propagates upstream through the generator chain.

```
sink.next() → stageC.next() → stageB.next() → stageA.next() → source
```

* **Stages only compute when requested** — upstream suspends until downstream pulls.
* **Constant memory** for linear pipelines — no intermediate buffering between stages.
* **Natural backpressure** — the consumer controls the rate; no explicit debouncing needed.
* **Batch vending** is a sink-level concern: the sink calls `next()` N times to collect N items. Individual stages may also pull from upstream in batches by calling `upstream.next()` multiple times before yielding.

**Comparison vs Push (rxjs-style)**

| Aspect       | Pull (async generators)             | Push (rxjs-style)              |
| ------------ | ----------------------------------- | ------------------------------ |
| Who drives   | Consumer (sink)                     | Producer                       |
| Backpressure | Inherent — upstream suspends        | Must debounce/drop explicitly  |
| Memory       | Constant for 1:1 pipelines          | Depends on operator buffering  |
| Use cases    | Transform pipelines, dataflow       | UI updates, reactive streams   |

---

### 2.2 Async Execution Model

* **Stages are always async** — the `Stage<Ctx, I, O>` type requires `AsyncGenerator` at both input and output boundaries. There is no sync stage; this is enforced by the type, not a default.
* **Transformers may be sync or async** — `Transformer<I, O>` returns `O`, `Promise<O>`, or `AsyncGenerator<O>`. `lift` promotes all three cases to a `Stage` uniformly; `await` on a sync return is a no-op.
* Flattening of 1-to-many transforms handled internally via `yield*` (see §2.3.3).
* **Debugging simplicity**: per-stage log context isolated by the runner; stack traces may be async-fragmented but instrumentation aids tracing (see §2.5).

---

### 2.3 Stage Design

#### 2.3.1 Core Type

A stage is a **context-parameterised async generator factory**:

```ts
type Stage<Ctx, I, O> = (ctx: Ctx) => (upstream: AsyncGenerator<I>) => AsyncGenerator<O>;
```

* `Ctx` — the capabilities this stage requires, declared as a plain structural interface (see §2.3.2).
* `I`, `O` — input and output item types.
* The stage receives a **scoped context** from the runner — never the full pipeline context.
* Pull semantics are inherent: the generator suspends at each `yield` until downstream pulls.

**Example — a stage that reads files:**
```ts
interface FileReader {
  readFile(path: string): Promise<string>;
}

const parseStage: Stage<FileReader, string, ParsedRow> =
  (ctx) => async function*(upstream) {
    for await (const path of upstream) {
      const content = await ctx.readFile(path);
      yield parse(content);
    }
  };
```

#### 2.3.2 Capability Declaration

Each stage declares its own plain structural interface for its context requirements. No branding or registration is needed in the stage itself — capability isolation between stages is enforced at the **pipeline level** via named slots (see §2.4). Two stages may declare identical-looking interfaces for the same underlying need; the pipeline author explicitly wires each one.

#### 2.3.3 Transformer Layer (optional)

For pure, resource-free transforms, a lighter `Transformer` type avoids generator boilerplate:

```ts
type Transformer<I, O> = (input: I) => O | AsyncGenerator<O> | Promise<O | AsyncGenerator<O>>;
```

The framework provides `lift` to promote a `Transformer` to a `Stage`:

```ts
const lift = <I, O>(t: Transformer<I, O>): Stage<{}, I, O> =>
  (_ctx) => async function*(upstream) {
    for await (const item of upstream) {
      const result = await t(item);
      if (isAsyncGenerator(result)) yield* result;
      else yield result;
    }
  };
```

Multiple `Transformer`s can be composed via function composition before lifting, reducing the number of generator coroutine boundaries in the pipeline.

**Example:**
```ts
const double = lift((n: number) => n * 2);
```

#### 2.3.4 PureStage — Reusable Stage Instances

By default, the pipeline builder asserts that no stage instance is registered more than once (`===` check), since stages may be stateful generators. Stateless factory functions can opt out of this check using `pure()`:

```ts
export const PURE = Symbol('t2-conduit/pure');
export type PureStage<Ctx, I, O> = Stage<Ctx, I, O> & { readonly [typeof PURE]: true };

export function pure<Ctx, I, O>(stage: Stage<Ctx, I, O>): PureStage<Ctx, I, O> {
  return Object.assign(stage, { [PURE]: true as const });
}
```

By calling `pure()`, the developer asserts that each call to `stage(ctx)(upstream)` produces an **independent generator with no shared mutable state**. The same instance may then be registered under multiple named slots.

```ts
const double = pure(lift((n: number) => n * 2));

createPipeline([
  { name: 'pre-scale',  stage: double },   // ✅ same instance — ok, it's pure
  { name: 'post-scale', stage: double },
]);
```

---

### 2.4 Pipeline Assembly & Runner

#### 2.4.1 Named Slots

Stages are registered with the pipeline builder under **named slots** chosen by the pipeline author:

```ts
const pipeline = createPipeline([
  { name: 'fetch',  stage: fetchStage  },
  { name: 'parse',  stage: parseStage  },
  { name: 'enrich', stage: enrichStage },
]);
```

The pipeline's required context type is **inferred** from the slot names and stage capability types:

```ts
// TypeScript infers:
// { fetch: FetchCap, parse: ParseCap, enrich: EnrichCap }
```

Each slot name is a string literal type — distinct even if two stages declare structurally identical capability interfaces. The pipeline author explicitly provides a value for each slot, making sharing vs. isolation a visible, intentional choice.

The pipeline builder enforces at construction time:
1. **No duplicate slot names** — caught at the type level via `NoDuplicateNames<T>` and at runtime.
2. **No duplicate stage instances** — runtime `===` check, skipped for `PureStage` instances.

#### 2.4.2 Context & Resource Lifecycle

The runner owns the **pipeline context** and its lifetime. The context is created before the pipeline runs and disposed after it completes (normally or via exception), using `await using`:

```ts
async function run<Ctx extends AsyncDisposable, O>(
  ctxFactory: () => Ctx,
  pipeline: ComposedPipeline<Ctx, O>
): Promise<O[]> {
  await using ctx = ctxFactory();
  return collect(pipeline(ctx));
}
```

Stages that need resources acquire them via the context (injected by the runner). Resources registered on the context are disposed when the context is disposed — stages do not manage resource lifetimes directly.

#### 2.4.3 Execution

The runner:
* Composes the generator chain: `stageC(ctx.enrich)(stageB(ctx.parse)(stageA(ctx.fetch)(source())))`.
* Projects the scoped context slice to each stage by slot name.
* Wraps each stage's generator with **instrumentation hooks** (see §2.5).
* Propagates `generator.return()` to all upstream generators on early termination or error, ensuring `finally` blocks and resource cleanup run.

---

### 2.5 Instrumentation & Logging

Instrumentation is owned and configured by the runner. The runner creates an isolated log context per named slot before execution — no cross-stage interference.

Hooks wrap each stage's generator at the `next()` call boundary:

```ts
async function* instrument<O>(
  gen: AsyncGenerator<O>,
  log: StageLog,
  hooks: InstrumentationHooks
): AsyncGenerator<O> {
  hooks.onStart(log);
  try {
    for await (const item of gen) {
      hooks.onItem(log, item);
      yield item;
    }
    hooks.onComplete(log);
  } catch (err) {
    hooks.onError(log, err);
    throw err;
  }
}
```

* **`onStart`** fires when the stage's generator is first iterated.
* **`onItem`** fires for each yielded item.
* **`onError`** fires before re-throwing; receives the error and current log context.
* **`onComplete`** fires when the upstream generator is exhausted normally.
* Accumulated log contexts are returned at pipeline completion alongside the result.
* Instrumentation is extensible — future plugins (metrics, tracing) use the same hooks.

---

### 2.6 Errors

Three element-level error modes are supported, configured per pipeline:

| Mode | Behaviour |
| ----------- | --------- |
| **Fail-fast** (default) | Pipeline stops on first error; all upstream generators closed via `generator.return()`. |
| **Drop** | Erroring item is discarded; pipeline continues with the next item. |
| **Propagate** | Error is wrapped as `Err<E>` and passed through remaining stages. |

Errors carry:
* Stage name (slot name)
* Stage context
* Optional instrumentation log snapshot

---

### 2.7 Types & Compile-Time Checking

* Stage inputs and outputs checked at **compile time** in TypeScript.
* Pipeline capability requirements are inferred as the intersection of all named slot types — missing capabilities surface as type errors at the `run()` call site.
* No implicit coercions or automatic batching; developer handles data shapes.

---

### 2.8 Future Improvements

* Transformer-level fusion: compose multiple `Transformer`s before lifting to reduce generator boundary count.
* Batch-aware sinks and pull-batching primitives.
* Parallel stage execution within a pipeline.
* Determinism annotations (`Det<T>` / `Nondet<T>`) as stage combinators for memoization and caching, without framework-level support.
* AST inspection / macros for automatic metadata derivation.
* Instrumentation plugin system (metrics, distributed tracing).
* Mermaid interaction diagrams for request-response flow (appendix).

---

## 3. End-to-End Example

A pipeline that reads log files, parses JSON lines, and collects only error-level entries.

```ts
import { Stage, Transformer, lift, pure, createPipeline, run, fromArray } from 't2-conduit';
import * as fs from 'node:fs/promises';

// ── Capability interface (declared by the stage, not the framework) ───────────

interface FileReader {
  readLines(path: string): AsyncGenerator<string>;
}

// ── Stage definitions ─────────────────────────────────────────────────────────

// Full stage — needs FileReader; yields each line from each file path
const readLines: Stage<FileReader, string, string> =
  (ctx) => async function*(paths) {
    for await (const path of paths) {
      yield* ctx.readLines(path);
    }
  };

// Pure transformer — parses a JSON line into a LogEntry; throws on malformed input
interface LogEntry { level: string; message: string; ts: number; }

const parseJson = pure(lift(
  (line: string): LogEntry => JSON.parse(line)
));

// Pure stage — filters to error-level entries only
const errorsOnly = pure(
  (_ctx: {}) => async function*(upstream: AsyncGenerator<LogEntry>) {
    for await (const entry of upstream) {
      if (entry.level === 'error') yield entry;
    }
  }
);

// ── Pipeline assembly ─────────────────────────────────────────────────────────

const pipeline = createPipeline([
  { name: 'read',   stage: readLines  },  // requires FileReader
  { name: 'parse',  stage: parseJson  },  // requires {} (nothing)
  { name: 'filter', stage: errorsOnly },  // requires {} (nothing)
]);

// TypeScript infers the required context type:
//   { read: FileReader, parse: {}, filter: {} }
// Missing or mistyped capabilities surface as type errors here, not at runtime.

// ── Context ───────────────────────────────────────────────────────────────────

class AppContext implements AsyncDisposable {
  // Satisfies { read: FileReader }
  read: FileReader = {
    async *readLines(path) {
      const fh = await fs.open(path);
      try {
        for await (const line of fh.readLines()) yield line;
      } finally {
        await fh.close();  // runs even if the consumer stops early
      }
    }
  };

  // parse and filter require {} — any object satisfies this
  parse  = {};
  filter = {};

  async [Symbol.asyncDispose]() { /* top-level cleanup if needed */ }
}

// ── Run ───────────────────────────────────────────────────────────────────────

const errors: LogEntry[] = await run(
  () => new AppContext(),
  pipeline,
  fromArray(['app.log', 'worker.log']),  // source of file paths
  { errorMode: 'drop' }                  // malformed JSON lines are dropped, not fatal
);

console.log(`Found ${errors.length} error entries`);
```

**What this demonstrates:**
* `readLines` — a full `Stage` with a capability interface; uses `try/finally` for per-file resource safety.
* `parseJson` — a sync `Transformer` wrapped with `lift` and marked `pure`; no boilerplate generator code.
* `errorsOnly` — a pure `Stage` for filtering; registered twice if needed without violating the `===` check.
* `createPipeline` — assembles named slots; TypeScript infers the required context type from all three.
* `AppContext` — satisfies the inferred type; implements `AsyncDisposable` so `await using` in the runner guarantees cleanup.
* `run()` — owns the context lifetime, composes the generator chain, and applies the `drop` error policy.

---

## 4. Summary

The MVP design focuses on:

* **Linear, pull-based pipelines** via async generators.
* **Capability-injected stages** with compile-time type safety across the pipeline.
* **Exception-safe resource management** via `await using` on the runner-owned context.
* **Instrumentation** integrated at the generator boundary, not the stage invocation.
* Minimal complexity in stages and runner to allow **iterative future improvements**.

---

## 5. Comparison with Other Approaches

### 5.1 Standard JS/TS Approaches

| Approach | Model | Backpressure | Type Safety | Resource Mgmt | Notes |
|---|---|---|---|---|---|
| **Node.js Streams** | Push + paused pull | `highWaterMark` / `drain` | Weak | Manual `.destroy()` | Complex legacy API (streams1/2/3); widespread but error-prone |
| **Web Streams** (`ReadableStream`) | Pull | Built-in via BYOB | Reasonable | `cancel()` / `abort()` | Better designed; available Node 16+; no stage composition API |
| **Raw async generators** | Pull | Inherent | Strong | `try/finally` only | t2-conduit's own foundation — no capabilities, instrumentation, or pipeline assembly |
| **`Promise.all` + arrays** | Batch | None | Strong | N/A | Materialises everything; fine for small data, not streaming |

### 5.2 Libraries

| Library | Model | Foundation | Type Safety | Capability Injection | Resource Mgmt | Error Handling |
|---|---|---|---|---|---|---|
| **RxJS** | Push (Observable) | Custom | Good | None | `finalize()` operator | `catchError`, retry operators |
| **IxJS** | Pull | Async iterables | Good | None | `finally()` operator | Limited |
| **Highland.js** | Pull | Node streams | Poor (unmaintained) | None | Via streams | Limited |
| **Scramjet** | Push/pull | Node streams | Reasonable | None | Via streams | Promise-based |
| **Effect-TS** | Pull + effects | Custom (fibres) | Excellent | Effect environment (`R`) | `acquireRelease` | Typed errors (`E`) |

### 5.3 Narrative

**Node.js Streams** are the most widely deployed but arguably the worst designed. Backpressure requires manually checking return values of `.write()` and listening for `'drain'`. Error handling is event-based (`'error'` event) — uncaught errors crash the process. Resource cleanup is manual and fragile across stream chains. `stream.pipeline()` helps but doesn't fix the underlying model.

**Web Streams** are better designed but thin — `pipeThrough` and `pipeTo` compose streams, but there's no concept of named stages, capability injection, typed sequencing, or instrumentation. It's a browser primitive, not a framework.

**RxJS** is the dominant reactive library in JS. It's excellent for event-driven/UI work (the original use case) but a poor fit for data pipelines: push semantics mean backpressure requires explicit operators, operators like `switchMap` silently drop in-flight work, and there's no structured resource lifecycle. The operator library is enormous but often misused for pipeline work.

**IxJS** (Microsoft's Interactive Extensions) is the closest foundation to t2-conduit — it's built on async iterables, pull-based, and has reasonable TypeScript support. It provides a rich operator set (`map`, `filter`, `flatMap`, `take`, etc.). What it lacks: no capability injection, no named slots, no structured error modes, no `await using` resource lifecycle, no instrumentation hooks. It's a good utility library for working with async iterables, not a pipeline framework.

**Effect-TS** is the most architecturally comparable to t2-conduit's goals. It has typed errors (`E`), a typed environment (`R`, analogous to `Ctx`), and `acquireRelease` for `ResourceT`-style cleanup. Its `Stream` module is pull-based. The key difference: Effect-TS is a **full effect system** — you're opting into a comprehensive programming model (fibres, scheduling, dependency injection, tracing). t2-conduit aims to be a narrow, focused pipeline framework that uses TypeScript and standard JS primitives (`async function*`, `await using`) rather than a parallel runtime.

### 5.4 Where t2-conduit Sits

```
                   Narrow ◄────────────────────────────► Comprehensive

Raw generators → IxJS → t2-conduit → Effect-TS Stream
                              ↑
                    Pipeline framework:
                    typed stages, capabilities,
                    named slots, await using,
                    error modes, instrumentation

Node Streams / Web Streams → orthogonal (push-first, built-in primitives)
RxJS → orthogonal (reactive/event-driven)
```

**t2-conduit's distinguishing features** that no other JS/TS library has together:

1. **Per-stage capability injection via named slots** — typed, structural, compile-time checked.
2. **`await using` context lifecycle** — leverages TS 5.2 `AsyncDisposable` rather than a custom effect system.
3. **`PureStage` marker** — explicit opt-in for safe instance reuse.
4. **Explicit error modes** (fail-fast / drop / propagate) configured per pipeline.
5. **Generator-boundary instrumentation** rather than stage-invocation hooks.

The closest prior art is Effect-TS's `Stream + Layer` system, but t2-conduit deliberately avoids the full effect system overhead — you bring your own `AppContext` class; the framework does not own your dependency injection container.

