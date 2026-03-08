Here’s a fully updated and expanded **t2-conduit design doc** integrating the latest decisions—including **reducers for stages**, **Instrumentation Layer for logging**, and the other MVP choices. I’ve structured it so it can serve as a baseline for iteration.

---

# t2-conduit Design Document (MVP)

## 1. Overview

**t2-conduit** is a **typed, linear, pull-based pipeline framework** for TypeScript.
It is designed to allow **simple, composable data transformations** while supporting:

* Pull-based execution (consumer-driven).
* Async-by-default stages.
* Minimal SDK wrapping.
* Reducer-style stage definitions.
* Instrumented logging for debugging and observability.
* Compile-time type checking between stages.
* Future extensibility (fusion, batch-aware sinks, metadata automation).

### Goals

* MVP that is **usable in real projects** without requiring AST-level macro work.
* Simple stage API that is **typed and composable**.
* Instrumentation hooks without over-complicating stage implementation.
* Allow multiple SDK implementations in the future (e.g., debug vs. performant).

---

## 2. Core Concepts

### 2.1 Pull-Based Execution

* **Consumer-driven (“vend”)**: downstream requests drive upstream data.
* **Stages only compute when requested**, avoiding unnecessary computation.
* **No hidden data drops** unless explicitly handled.
* MVP semantics: **linear, single-item flow** (batches and parallelism deferred to future improvements).

> **Todo:** Appendix with full mermaid interaction diagrams showing request → response flow.

**Comparison vs Push (rxjs-style)**

| Aspect       | Pull (“vend”)                       | Push (“stream”)                |
| ------------ | ----------------------------------- | ------------------------------ |
| Who drives   | Consumer                            | Producer                       |
| Backpressure | Natural; consumer controls rate     | Must debounce/drop explicitly  |
| Hidden drops | Possible if pre-source is unmanaged | More obvious; sink must handle |
| Use cases    | Transform pipelines, dataflow       | UI updates, reactive streams   |

**Escape hatch:** `poll()` to request data only if immediately available.

---

### 2.2 Minimal SDK Wrapping

* Wrap standard JS/TS APIs for **purity and immutability**.
* MVP: **deep-clone wrapper** (`t2-conduit-sdk-debug`) to simplify debugging.
* Future SDK implementations may optimize performance.
* API mirrors JS stdlib naming for familiarity.

---

### 2.3 Async by Default

* All stages are **async functions** by default.
* SDK provides common async operators: `debounce`, `filter`, `merge`.
* Flattening handled internally (async stage returning another async stage).
* Minimal visual illustration included; detailed interaction diagrams deferred to appendix.
* **Debugging simplicity**: per-stage log context; stack traces may still be async-fragmented but instrumentation aids tracing.

---

### 2.4 Metadata & Determinism

* MVP **does not automatically derive metadata from types**.
* Users must annotate **Det<T>** or **Nondet<T>** explicitly.
* Async behavior detected automatically from stage signature.
* Metadata only needed for advanced pipeline analysis (future improvement).

---

### 2.5 Stage Design

* Each stage is a **reducer-style function**:

```ts
type Stage<I, O> = (step: (acc: any, out: O) => any) =>
                   (acc: any, input: I) => any;
```

* Input type `I`, output type `O` are fully generic.
* **No assumption about batching**; `I` may be a single value or a collection.
* Stage **does not know about other stages**; the runner orchestrates sequencing.
* Reducers allow potential parallelism/fusion in future versions.

---

### 2.6 Runner

* The **runner** is the only intermediary between stages.
* Default MVP runner is **async**.
* Responsible for:

  * Executing stages sequentially.
  * Managing **instrumentation hooks**.
  * Maintaining per-stage logs (global per stage, not per item).
  * Handling errors according to fail-fast semantics.

---

### 2.7 Instrumentation Layer

* Replaces the previous logging-before/after-stage mechanism.
* Owned and configured by the runner.
* Provides hooks for:

  * Before stage execution.
  * After stage execution.
  * On stage error.
* Instrumentation can append arbitrary data to stage log context.
* Log context **isolated per stage**, preventing interference between stages.
* Supports extensibility for future AOP-like plugins (currently only logging).

**Example pseudocode:**

```ts
runner.instrumentation.before(stage, stageLog);
await stage(input);
runner.instrumentation.after(stage, stageLog);
```

---

### 2.8 Errors

* Fail-fast by default (pipeline stops on first stage error).
* Per-item error handling deferred to future enhancements.
* Errors carry:

  * Fixed error codes (`T2C-001`, `T2C-002`, etc.)
  * Source span (where available)
  * Stage context
  * Optional instrumentation log snapshot

---

### 2.9 Logging

* Logs are **threaded through the runner**, not stages directly.
* Stage log context is **created by the runner**, passed into stage execution.
* Accumulated logs returned at pipeline completion.
* Provides visibility into pipeline execution without polluting stage logic.

---

### 2.10 Types & Compile-Time Checking

* Stage inputs and outputs checked at **compile time** in TypeScript.
* MVP ensures **type-safe sequencing** of stages.
* No implicit coercions or automatic batching; developer handles data shapes.

---

### 2.11 Future Improvements

* AST inspection macros for automatic metadata derivation.
* Stream fusion for performance optimization.
* Batch-aware sinks / pull-batching.
* Instrumentation plugin system beyond logging.
* Parallel/reducer optimizations for JS environment.
* Append appendix with **mermaid diagrams** for request-response flow.

---

## 3. Summary

The MVP design focuses on:

* **Linear, pull-based pipelines**.
* **Async-by-default reducer stages**.
* **Instrumentation layer for logging/debugging**.
* **Compile-time type safety**.
* Minimal complexity in stages, runner, and SDK to allow **iterative future improvements**.
