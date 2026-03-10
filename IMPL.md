
# t2-conduit Implementation Guide

This document covers how to use t2-conduit to build typed, pull-based data pipelines. Four core tasks:

1. [Making a Stage](#1-making-a-stage)
2. [Making a Pipeline](#2-making-a-pipeline)
3. [Making the Context](#3-making-the-context)
4. [Running the Pipeline](#4-running-the-pipeline)

---

## 1. Making a Stage

### 1.1 What a Stage Is

A stage is a **context-parameterised async generator factory**. It takes a scoped context (its capabilities), and returns a function that wraps an upstream generator to produce a downstream generator.

```ts
type Stage<Ctx, I, O> = (ctx: Ctx) => (upstream: AsyncGenerator<I>) => AsyncGenerator<O>;
```

The runner calls `stage(ctx)` to bind capabilities, then calls the result with the upstream generator. Stages never call each other directly — the runner composes them.

**Internal lifecycle:**
```
stage(ctx)                        → bound generator factory
bound(upstream: AsyncGenerator)   → this stage's generator
runner iterates this generator    → pulls items through the chain
```

---

### 1.2 Defining Capabilities

Declare a plain TypeScript interface for what the stage needs. No base classes, no registration, no framework imports:

```ts
interface DbReader {
  query(sql: string): Promise<Row[]>;
}
```

**Conventions:**
- Keep interfaces narrow — only what this stage actually uses.
- Do not reuse capability interfaces across unrelated stages. The pipeline author decides sharing via named slots.
- Interface names are local to the stage's module; no global registry.

---

### 1.3 Writing a Full Stage

```ts
// Capability
interface DbReader {
  query(sql: string): Promise<Row[]>;
}

// Stage: takes SQL queries upstream, yields individual rows downstream
const fetchRows: Stage<DbReader, string, Row> =
  (ctx) => async function*(upstream) {
    for await (const sql of upstream) {
      const rows = await ctx.query(sql);
      for (const row of rows) yield row;   // 1-to-many: one query → many rows
    }
  };
```

**Pseudocode:**
```
fetchRows(ctx):
  return async-generator(upstream):
    for sql in upstream:
      rows = await ctx.query(sql)
      for row in rows:
        emit row
```

**Resource cleanup inside a stage:** use `try/finally`. The runner guarantees `generator.return()` is called on upstream generators when the pipeline stops — `finally` blocks will run:

```ts
const streamFiles: Stage<FileSystem, string, string> =
  (ctx) => async function*(paths) {
    for await (const path of paths) {
      const fh = await ctx.open(path);
      try {
        for await (const line of fh.lines()) yield line;
      } finally {
        await fh.close();   // runs even if consumer stops early
      }
    }
  };
```

---

### 1.4 Using the Transformer Layer

For pure, resource-free 1:1 or 1:many transforms, use `Transformer<I, O>` + `lift` to avoid generator boilerplate.

```ts
type Transformer<I, O> = (input: I) => O | AsyncGenerator<O> | Promise<O | AsyncGenerator<O>>;
```

`lift` promotes a `Transformer` to a `Stage<{}, I, O>` (no capabilities required):

```ts
// Sync 1:1
const trim = lift((s: string) => s.trim());

// Async 1:1
const enrich = lift(async (row: Row): Promise<EnrichedRow> => ({
  ...row,
  score: await computeScore(row),
}));

// 1:many (flatMap style) — return an AsyncGenerator
const splitCsv = lift(async function*(line: string) {
  for (const cell of line.split(',')) yield cell.trim();
});
```

**How `lift` works internally:**
```
lift(transformer):
  return (_ctx) => async-generator(upstream):
    for item in upstream:
      result = await transformer(item)
      if result is AsyncGenerator:
        yield* result          // forward all emitted values
      else:
        yield result           // single value
```

Multiple `Transformer`s can be composed before lifting, reducing the number of generator coroutine boundaries:

```ts
const process = lift(
  (line: string) => line.trim().toLowerCase()   // composed inline
);

// Or compose separately
const pipeline = (line: string) => normalise(tokenise(line));
const processStage = lift(pipeline);
```

---

### 1.5 Marking a Stage as Pure

By default, the pipeline builder rejects the same stage instance registered twice (`===` check) — stages may be stateful. If a stage is a stateless factory (each call produces an independent generator), opt in with `pure()`:

```ts
export const PURE = Symbol('t2-conduit/pure');

export function pure<Ctx, I, O>(stage: Stage<Ctx, I, O>): PureStage<Ctx, I, O> {
  return Object.assign(stage, { [PURE]: true as const });
}
```

```ts
// Stateless — safe to reuse
const double = pure(lift((n: number) => n * 2));

createPipeline([
  { name: 'pre-scale',  stage: double },   // ✅
  { name: 'post-scale', stage: double },   // ✅ same instance, but pure
]);
```

**Contract of `pure()`:** each invocation of `stage(ctx)(upstream)` produces a generator with no shared mutable state between invocations. The developer asserts this; it is not verified by the framework.

---

## 2. Making a Pipeline

### 2.1 Assembling Stages with Named Slots

```ts
const pipeline = createPipeline([
  { name: 'fetch',  stage: fetchRows  },
  { name: 'filter', stage: filterRows },
  { name: 'format', stage: formatRows },
]);
```

Each entry is a **named slot**: a stage paired with a name chosen by the pipeline author. The name becomes the key by which the runner looks up the stage's capabilities in the context.

**What `createPipeline` does:**
```
createPipeline(slots):
  assert: no two slots share the same name         → type-level + runtime
  assert: no two slots share the same stage ref    → runtime (skip PureStage)
  return ComposedPipeline:
    slots: ordered slot list
    ctxType: inferred as { [name]: StageCap, ... } for each slot
```

---

### 2.2 Type Inference

TypeScript infers the required context type from the slot list automatically. No manual type annotation needed:

```ts
// Given:
//   fetchRows:  Stage<DbReader,  string, Row>
//   filterRows: Stage<{},        Row,    Row>
//   formatRows: Stage<Formatter, Row,    string>

const pipeline = createPipeline([
  { name: 'fetch',  stage: fetchRows  },
  { name: 'filter', stage: filterRows },
  { name: 'format', stage: formatRows },
]);

// TypeScript infers pipeline requires:
// { fetch: DbReader, filter: {}, format: Formatter }

// Missing capabilities surface as compile-time errors at run():
await run(() => new AppContext(), pipeline, source);
//                 ^^^^^^^^^^^^ TS checks AppContext satisfies the inferred type
```

---

### 2.3 Duplicate Detection

**Duplicate slot names** — caught at the type level via `NoDuplicateNames<T>` and also at runtime:

```ts
createPipeline([
  { name: 'read', stage: stageA },
  { name: 'read', stage: stageB },   // TS error: duplicate slot name 'read'
]);
```

**Duplicate stage instances** — runtime only (`===` check), skipped for `PureStage`:

```ts
const myStage: Stage<..> = ...;

createPipeline([
  { name: 'step1', stage: myStage },
  { name: 'step2', stage: myStage },  // throws at runtime — same ref, not pure
]);
```

---

### 2.4 How the Generator Chain Is Composed

For slots `[A, B, C]` with a source, the runner builds:

```
C(ctx.c)( B(ctx.b)( A(ctx.a)( source() ) ) )
```

Each stage wraps the previous stage's generator. The outermost generator is iterated by the runner (the sink drives everything).

**Pseudocode:**
```
compose(slots, ctx, source):
  gen = source
  for slot in slots (left to right):
    gen = slot.stage(ctx[slot.name])(gen)
  return gen    // final generator; runner iterates this
```

---

## 3. Making the Context

### 3.1 The Required Context Type

After `createPipeline`, the required context type is inferred. For the pipeline above:

```ts
// Inferred (you do not write this — TypeScript figures it out):
type RequiredCtx = {
  fetch:  DbReader;
  filter: {};
  format: Formatter;
};
```

You implement this as a class (or plain object) that satisfies the type and implements `AsyncDisposable` for the runner's `await using` lifecycle.

---

### 3.2 Implementing the Context

```ts
import { fs } from 'node:fs/promises';

class AppContext implements AsyncDisposable {

  // Satisfies { fetch: DbReader }
  fetch: DbReader = {
    async query(sql) {
      return db.execute(sql);
    }
  };

  // Satisfies { filter: {} } — any object works
  filter = {};

  // Satisfies { format: Formatter }
  format: Formatter = {
    formatRow: (row) => JSON.stringify(row),
  };

  // Called by the runner when the pipeline exits (normally or via exception)
  async [Symbol.asyncDispose]() {
    await db.close();
  }
}
```

**Rules:**
- Each slot named `'foo'` must correspond to a property `foo` on the context satisfying the stage's capability interface.
- Slots with `Stage<{}, I, O>` (no capabilities) require a property that satisfies `{}` — any non-null object works; use `{}` as a placeholder.
- `[Symbol.asyncDispose]` is mandatory for `run()`. It is called after the pipeline finishes, regardless of success or error.

---

### 3.3 Resource Scoping

Resources split into two categories:

**Pipeline-scoped** (live for the full `run()` call): use a static async factory to open resources, close in `[Symbol.asyncDispose]`:

```ts
class AppContext implements AsyncDisposable {
  private constructor(private conn: DbConnection) {}

  static async create(): Promise<AppContext> {
    return new AppContext(await db.connect());   // opened once
  }

  fetch: DbReader = {
    query: (sql) => this.conn.execute(sql),
  };

  async [Symbol.asyncDispose]() {
    await this.conn.close();           // closed when pipeline ends
  }
}

// Pass the factory to run():
await run(() => AppContext.create(), pipeline, source);
```

**Item-scoped** (live for one item's processing): managed with `try/finally` inside the generator:

```ts
fetch: FileReader = {
  async *readLines(path) {
    const fh = await fs.open(path);
    try {
      for await (const line of fh.readLines()) yield line;
    } finally {
      await fh.close();   // closed per file, even on early termination
    }
  }
};
```

---

### 3.4 Sharing vs. Isolating Implementations

Named slots make sharing an explicit, visible decision:

```ts
class AppContext implements AsyncDisposable {
  private fs = new FileSystem();

  // Explicit sharing — both slots use the same FileSystem instance
  'read-raw'      = this.fs;
  'read-enriched' = this.fs;

  // Or explicit isolation — separate instances, separate config
  // 'read-raw'      = new FileSystem({ root: '/raw' });
  // 'read-enriched' = new FileSystem({ root: '/enriched' });

  async [Symbol.asyncDispose]() {
    await this.fs.close();
  }
}
```

Two stages with structurally identical capability interfaces are not automatically shared — the pipeline author must make the sharing decision here.

---

## 4. Running the Pipeline

### 4.1 The `run()` Function

```ts
const { results, logs } = await run(
  () => new AppContext(),            // context factory — called once per run
  pipeline,                         // ComposedPipeline from createPipeline
  fromArray(['query-a', 'query-b']),// source: AsyncGenerator<I>
  { errorMode: 'drop' }             // options
);
```

**What `run()` does internally:**
```
run(ctxFactory, pipeline, source, options):
  await using ctx = ctxFactory()         // context created; dispose scheduled
  gen = compose(pipeline.slots, ctx, source)
  logContexts = pipeline.slots.map(slot => new StageLog(slot.name))
  gen = instrument(gen, logContexts, options.hooks)   // wrap with instrumentation per slot
  results = []
  try:
    for item in gen:
      results.push(item)
  finally:
    ctx[Symbol.asyncDispose]()           // always runs — await using guarantees it
  return { results, logs: logContexts }
```

The `await using` statement ensures `ctx[Symbol.asyncDispose]()` runs on every exit path — normal completion, early `break`, or thrown exception.

---

### 4.2 Source Creation

A source is any `AsyncGenerator<I>`. The framework provides helpers:

```ts
// From a fixed array — useful for tests and simple cases
const src = fromArray(['query-a', 'query-b', 'query-c']);

// From any sync or async iterable
const src = fromIterable(process.stdin);
const src = fromIterable(fs.readdir('/data'));

// Custom source — any async generator function
async function* pollQueue(): AsyncGenerator<Job> {
  while (true) {
    const job = await queue.dequeue();
    if (!job) break;
    yield job;
  }
}
```

---

### 4.3 Error Modes

The error mode is set per `run()` call and governs what happens when a stage throws processing an item.

**Fail-fast (default):** pipeline stops immediately; all upstream generators are closed via `generator.return()`.

```ts
await run(factory, pipeline, source, { errorMode: 'fail-fast' });
```

```
fail-fast internally:
  error thrown in stage → re-throw immediately
  runner catches → calls generator.return() on all upstream generators
  pipeline exits with the error
```

**Drop:** erroring item is silently discarded; pipeline continues with the next item.

```ts
await run(factory, pipeline, source, { errorMode: 'drop' });
```

```
drop internally:
  for item in upstream:
    try:
      yield transform(item)
    catch err:
      hooks.onError(log, err)
      continue    // item skipped
```

**Propagate:** error is wrapped as `Err<E>` and passed through remaining stages as a value. Downstream stages can inspect or pass it through.

```ts
await run(factory, pipeline, source, { errorMode: 'propagate' });
```

```
propagate internally:
  for item in upstream:
    if item is Err<E>:
      yield item          // pass error through without invoking this stage
    else:
      try:
        yield Ok(transform(item))
      catch err:
        yield Err(err)    // wrap error as a value
```

---

### 4.4 Collecting Results

`run()` collects all output into an array by default. For large outputs or streaming sinks, use `runWith()` to consume without materialising:

```ts
// Default: collect to array
const results: string[] = await run(factory, pipeline, source);

// Streaming sink: consume without accumulation
// runWith returns only the `logs`, since result accumulation is handled by the caller.
await runWith(factory, pipeline, source, async (gen) => {
  for await (const item of gen) {
    await db.insert(item);
  }
});
```

---

### 4.5 Instrumentation Hooks

Pass hooks to `run()` to observe each stage's generator boundary:

```ts
const results = await run(factory, pipeline, source, {
  errorMode: 'drop',
  hooks: {
    onStart(log) {
      console.log(`[${log.slot}] started`);
    },
    onItem(log, item) {
      log.count = (log.count ?? 0) + 1;
    },
    onError(log, err) {
      console.error(`[${log.slot}] error:`, err.message);
    },
    onComplete(log) {
      console.log(`[${log.slot}] done — ${log.count} items`);
    },
  },
});
```

**Hook call points:**
```
instrument(gen, log, hooks):
  hooks.onStart(log)
  try:
    for item in gen:
      hooks.onItem(log, item)
      yield item
    hooks.onComplete(log)
  catch err:
    hooks.onError(log, err)
    re-throw
```

Each named slot gets its own isolated `StageLog` object — hooks for one stage cannot interfere with another's log context. `run()` now returns both the collected `results` array and the accumulated `logs` so consumers can inspect or emit them once the pipeline finishes.

---

## 5. t2-lang Implementation

This section shows how to implement t2-conduit usage in **t2-lang**, the host language. For each concept, we show the raw verbose form, then any macro that makes it cleaner, then the macro usage.

### 5.1 Notes

t2-conduit uses two t2-lang constructs worth noting:

- **`try/finally`** — `(try stmt... (finally cleanup...))`. Used for per-item resource cleanup inside generator stages.
- **Computed method names** — `(method [(. Symbol asyncDispose)] () body...)` inside a `class-body`. Used for `[Symbol.asyncDispose]` on context classes.

---

### 5.2 Macro Overview

| Macro | Purpose | Replaces |
|---|---|---|
| `defcap` | Declare a capability interface | `(interface ...)` boilerplate |
| `defstage` | Declare a typed stage with capability binding | `(const ... (async-lambda ...))` boilerplate |
| `deflift` | Declare a pure transformer stage | `(const ... (pure (lift ...)))` boilerplate |
| `defpipeline` | Assemble named slots into a pipeline | `(createPipeline (array ...))` boilerplate |
| `defcontext` | Build an `AsyncDisposable` context class | `(class ...)` with `[Symbol.asyncDispose]` |

---

### 5.3 Core Type Declarations

These are defined once in the framework source. Shown here for reference.

```t2-lang
;; Stage<Ctx, I, O> = (ctx: Ctx) => (upstream: AsyncGenerator<I>) => AsyncGenerator<O>
(type Stage
  (type-params Ctx I O)
  (tfn ((ctx Ctx))
    (tfn ((upstream (AsyncGenerator I)))
      (AsyncGenerator O))))

;; Transformer<I, O> = (input: I) => O | AsyncGenerator<O> | Promise<O | AsyncGenerator<O>>
(type Transformer
  (type-params I O)
  (tfn ((input I))
    (union O (AsyncGenerator O) (Promise (union O (AsyncGenerator O))))))
```

**Emitted TypeScript:**
```ts
type Stage<Ctx, I, O> = (ctx: Ctx) => (upstream: AsyncGenerator<I>) => AsyncGenerator<O>;
type Transformer<I, O> = (input: I) => O | AsyncGenerator<O> | Promise<O | AsyncGenerator<O>>;
```

---

### 5.4 `defcap` — Capability Interface

**Raw t2-lang:**
```t2-lang
(interface FileReader
  (FileReader
    (readLines (tfn ((path string)) (AsyncGenerator string)))))
```

**Macro definition:**
```t2-lang
;; (defcap Name (method-name (param-type ...) return-type) ...)
(defmacro defcap ((name) (rest methods))
  (return
    (quasi (interface (unquote name)
             ((unquote name)
               (unquote-splicing methods))))))
```

**With macro:**
```t2-lang
(defcap FileReader
  (readLines (tfn ((path string)) (AsyncGenerator string))))

(defcap DbReader
  (query (tfn ((sql string)) (Promise (Array Row)))))
```

**Emitted TypeScript:**
```ts
interface FileReader {
  readLines(path: string): AsyncGenerator<string>;
}
interface DbReader {
  query(sql: string): Promise<Row[]>;
}
```

---

### 5.5 `defstage` — Stage Definition

**Raw t2-lang** (verbose — this is what we want to avoid writing by hand):
```t2-lang
(const readLines
  (async-lambda ((ctx : FileReader))
    (async-generator-fn ((upstream : (AsyncGenerator string)) : (AsyncGenerator string))
      (for-await path upstream
        (yield* (method-call ctx readLines path))))))
```

**Macro definition:**
```t2-lang
;; (defstage name cap-type in-type out-type (ctx-param) body...)
;; ctx-param becomes the name bound to the scoped context inside the stage body.
(defmacro defstage ((name) (cap) (in-t) (out-t) (ctx) (rest body))
  (return
    (quasi
      (const (unquote name)
        (async-lambda (((unquote ctx) : (unquote cap)))
          (async-generator-fn
            ((upstream : (AsyncGenerator (unquote in-t))) : (AsyncGenerator (unquote out-t)))
            (unquote-splicing body)))))))
```

**With macro:**
```t2-lang
(defstage readLines FileReader string string ctx
  (for-await path upstream
    (yield* (method-call ctx readLines path))))

(defstage fetchRows DbReader string Row ctx
  (for-await sql upstream
    (const rows (await (method-call ctx query sql)))
    (for-of row rows
      (yield row))))
```

**Emitted TypeScript:**
```ts
const readLines: Stage<FileReader, string, string> =
  (ctx) => async function*(upstream) {
    for await (const path of upstream) {
      yield* ctx.readLines(path);
    }
  };

const fetchRows: Stage<DbReader, string, Row> =
  (ctx) => async function*(upstream) {
    for await (const sql of upstream) {
      const rows = await ctx.query(sql);
      for (const row of rows) yield row;
    }
  };
```

**Stage with per-item resource cleanup** uses `try`/`finally` natively:

```t2-lang
(defstage streamFiles FileSystem string string ctx
  (for-await path upstream
    (const fh (await (method-call ctx open path)))
    (try
      (for-await line (method-call fh lines)
        (yield line))
      (finally
        (await (method-call fh close))))))
```

**Emitted TypeScript:**
```ts
const streamFiles: Stage<FileSystem, string, string> =
  (ctx) => async function*(upstream) {
    for await (const path of upstream) {
      const fh = await ctx.open(path);
      try {
        for await (const line of fh.lines()) yield line;
      } finally {
        await fh.close();
      }
    }
  };
```

---

### 5.6 `deflift` — Pure Transformer Stage

**Raw t2-lang:**
```t2-lang
(const parseJson
  (pure (lift (lambda ((line : string) : LogEntry)
    (JSON.parse line)))))
```

**Macro definition:**
```t2-lang
;; (deflift name fn-expr)
;; Always marks the result pure — deflift is only for stateless transforms.
(defmacro deflift ((name) (fn-expr))
  (return
    (quasi
      (const (unquote name)
        (pure (lift (unquote fn-expr)))))))
```

**With macro:**
```t2-lang
(deflift parseJson (lambda ((line : string) : LogEntry)
  (method-call JSON parse line)))

(deflift trimLine (lambda ((s : string) : string)
  (method-call s trim)))

;; 1-to-many: return an async generator
(deflift splitCsv (lambda ((line : string) : (AsyncGenerator string))
  (async-generator-fn ()
    (for-of cell (method-call line split ',')
      (yield (method-call cell trim))))))
```

**Emitted TypeScript:**
```ts
const parseJson = pure(lift((line: string): LogEntry => JSON.parse(line)));
const trimLine  = pure(lift((s: string): string => s.trim()));
const splitCsv  = pure(lift((line: string): AsyncGenerator<string> => async function*() {
  for (const cell of line.split(',')) yield cell.trim();
}()));
```

---

### 5.7 `defpipeline` — Pipeline Assembly

**Raw t2-lang:**
```t2-lang
(const logPipeline
  (createPipeline
    (array
      (object (name 'read)   (stage readLines))
      (object (name 'parse)  (stage parseJson))
      (object (name 'filter) (stage errorsOnly)))))
```

**Macro definition:**
```t2-lang
;; (defpipeline name (slot-name stage-expr) ...)
(defmacro defpipeline ((name) (rest slots))
  (return
    (quasi
      (const (unquote name)
        (createPipeline
          (array
            (unquote-splicing
              (map slots (lambda ((slot))
                (quasi (object
                  (name (unquote (index slot 0)))
                  (stage (unquote (index slot 1))))))))))))))
```

**With macro:**
```t2-lang
(defpipeline logPipeline
  ('read   readLines)
  ('parse  parseJson)
  ('filter errorsOnly))
```

**Emitted TypeScript:**
```ts
const logPipeline = createPipeline([
  { name: 'read',   stage: readLines  },
  { name: 'parse',  stage: parseJson  },
  { name: 'filter', stage: errorsOnly },
]);
```

---

### 5.8 `defcontext` — Context Class

**Raw t2-lang:**
```t2-lang
(class AppContext
  (implements AsyncDisposable)
  (class-body
    (field read : FileReader
      (object
        (readLines (async-generator-fn ((path : string) : (AsyncGenerator string))
          (const fh (await (method-call fs open path)))
          (try
            (for-await line (method-call fh readLines)
              (yield line))
            (finally
              (await (method-call fh close))))))))
    (field parse  (object))
    (field filter (object))
    (method [(. Symbol asyncDispose)] ()
      (await (method-call db close)))))
```

**Macro definition:**
```t2-lang
;; (defcontext Name
;;   (slot slot-name cap-impl-expr)...
;;   (dispose dispose-body...))
;;
;; Emits a class with one field per slot and [Symbol.asyncDispose].
(defmacro defcontext ((name) (rest clauses))
  (const slot-clauses   (filter clauses (lambda ((c)) (= (index c 0) :slot))))
  (const dispose-clause (find   clauses (lambda ((c)) (= (index c 0) :dispose))))
  (return
    (quasi
      (class (unquote name)
        (implements AsyncDisposable)
        (class-body
          (unquote-splicing
            (map slot-clauses (lambda ((s))
              (quasi (field (unquote (index s 1)) (unquote (index s 2)))))))
          (method [(. Symbol asyncDispose)] ()
            (unquote-splicing (index dispose-clause 1))))))))
```

**With macro:**
```t2-lang
(defcontext AppContext
  (:slot read
    (object
      (readLines (async-generator-fn ((path : string) : (AsyncGenerator string))
        (const fh (await (method-call fs open path)))
        (try
          (for-await line (method-call fh readLines)
            (yield line))
          (finally
            (await (method-call fh close))))))))
  (:slot parse  (object))
  (:slot filter (object))
  (:dispose
    (await (method-call db close))))
```

**Emitted TypeScript:**
```ts
class AppContext implements AsyncDisposable {
  read: FileReader = {
    async *readLines(path: string): AsyncGenerator<string> {
      const fh = await fs.open(path);
      try {
        for await (const line of fh.readLines()) yield line;
      } finally {
        await fh.close();
      }
    }
  };
  parse  = {};
  filter = {};
  async [Symbol.asyncDispose]() {
    await db.close();
  }
}
```

---

### 5.9 Running the Pipeline

No macro needed — `run()` is a plain function call. The t2-lang form maps directly:

```t2-lang
(import (object (Stage) (lift) (pure) (createPipeline) (run) (fromArray)) 't2-conduit')
(import (object (fs)) 'node:fs/promises')

(const errors
  (await
    (run
      (lambda () (new AppContext))
      logPipeline
      (fromArray (array 'app.log' 'worker.log'))
      (object (errorMode :drop)))))

(method-call console log (template 'Found ' (. errors length) ' error entries'))
```

**Emitted TypeScript:**
```ts
import { Stage, lift, pure, createPipeline, run, fromArray } from 't2-conduit';
import { fs } from 'node:fs/promises';

const errors = await run(
  () => new AppContext(),
  logPipeline,
  fromArray(['app.log', 'worker.log']),
  { errorMode: 'drop' }
);
console.log(`Found ${errors.length} error entries`);
```

---

### 5.10 Complete Example in t2-lang

The full log-pipeline example from §3 of this document, in t2-lang:

```t2-lang
(program

  (import (object (run) (fromArray) (lift) (pure)) 't2-conduit')
  (import (object (fs)) 'node:fs/promises')

  ;; ── Capability ────────────────────────────────────────────────────────────

  (defcap FileReader
    (readLines (tfn ((path string)) (AsyncGenerator string))))

  ;; ── Stages ───────────────────────────────────────────────────────────────

  (defstage readLines FileReader string string ctx
    (for-await path upstream
      (yield* (method-call ctx readLines path))))

  (interface LogEntry
    (LogEntry
      (level string)
      (message string)
      (ts number)))

  (deflift parseJson (lambda ((line : string) : LogEntry)
    (method-call JSON parse line)))

  (const errorsOnly
    (pure
      (async-lambda ((_ctx : object))
        (async-generator-fn ((upstream : (AsyncGenerator LogEntry)) : (AsyncGenerator LogEntry))
          (for-await entry upstream
            (if (= (. entry level) 'error')
              (yield entry)))))))

  ;; ── Pipeline ─────────────────────────────────────────────────────────────

  (defpipeline logPipeline
    ('read   readLines)
    ('parse  parseJson)
    ('filter errorsOnly))

  ;; ── Context ──────────────────────────────────────────────────────────────

  (defcontext AppContext
    (:slot read
      (object
        (readLines (async-generator-fn ((path : string) : (AsyncGenerator string))
          (const fh (await (method-call fs open path)))
          (try
            (for-await line (method-call fh readLines)
              (yield line))
            (finally
              (await (method-call fh close))))))))
    (:slot parse  (object))
    (:slot filter (object))
    (:dispose))  ;; no top-level cleanup needed

  ;; ── Run ──────────────────────────────────────────────────────────────────

  (const errors
    (await
      (run
        (lambda () (new AppContext))
        logPipeline
        (fromArray (array 'app.log' 'worker.log'))
        (object (errorMode :drop)))))

  (method-call console log
    (template 'Found ' (. errors length) ' error entries'))
)
```
