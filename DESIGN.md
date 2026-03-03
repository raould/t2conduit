# t2-conduit MVP Design Document

## 1. Overview

**t2-conduit** is a lightweight pipeline execution framework intended for **TypeScript environments**. It enables developers to compose reusable **stages** into **typed pipelines** that process data sequentially.

The MVP focuses on:

* **Linear pipelines**
* **Async stage execution**
* **Compile-time type compatibility**
* **Runner-mediated orchestration**
* **Per-stage logging contexts**

The design intentionally excludes more complex features such as branching, routing graphs, and automatic metadata derivation from types. These may be added in future versions.

---

# 2. Core Architectural Principles

The architecture follows several strict invariants.

### 2.1 Stages Are Pipeline-Agnostic

Stages must not know:

* what pipeline they belong to
* which stages run before or after them
* how routing works

Stages only perform computation.

```
input → output
```

---

### 2.2 Runner Owns Orchestration

All pipeline execution is mediated by a **runner**.

The runner is responsible for:

* stage scheduling
* async execution
* error handling
* drop semantics
* stage log creation
* log aggregation

Stages never call other stages.

---

### 2.3 Pipelines Are Linear (MVP)

The MVP supports **strictly linear pipelines**:

```
StageA → StageB → StageC
```

No branching or routing exists in the MVP runtime.

Future versions may extend pipelines into DAG execution graphs.

---

### 2.4 Compile-Time Type Safety

Stage compatibility is enforced at **compile time**.

For adjacent stages:

```
Output(StageN) must be assignable to Input(StageN+1)
```

Type validation occurs during compilation using generated TypeScript type constraints.

---

# 3. Core Concepts

## 3.1 Stage

A **stage** is a reusable processing unit.

It has:

* an input type
* an output type
* a log type

### Stage Interface

```
Stage<I, O, L>
```

Where:

| Parameter | Meaning        |
| --------- | -------------- |
| I         | input type     |
| O         | output type    |
| L         | stage log type |

---

### Stage Definition

```
interface Stage<I, O, L> {

  name: string

  createLog(): L

  process(
    input: I,
    log: L
  ): Promise<Result<Option<O>, Error>>
}
```

---

### Responsibilities

A stage:

* receives input
* mutates its log
* returns a result

A stage **does not**:

* choose the next stage
* access other stage logs
* interact with the pipeline definition

---

# 4. Result Semantics

Stages return:

```
Result<Option<O>, Error>
```

This supports three outcomes.

| Return Value      | Meaning           |
| ----------------- | ----------------- |
| `Ok(Some(value))` | continue pipeline |
| `Ok(None)`        | drop item         |
| `Err(error)`      | pipeline failure  |

---

### Continue

```
Ok(Some(output))
```

The pipeline proceeds to the next stage.

---

### Drop

```
Ok(None)
```

The item stops processing and exits the pipeline.

No further stages run.

---

### Failure

```
Err(error)
```

The entire pipeline execution fails.

---

# 5. Pipeline Definition

Pipelines define an ordered sequence of stages.

Example:

```
pipeline UserSessionPipeline {

  ParseJson
  ValidateUser
  CreateSession

}
```

This expands into a linear stage list:

```
[
  ParseJson,
  ValidateUser,
  CreateSession
]
```

---

# 6. Compile-Time Type Validation

The pipeline macro generates compile-time checks between adjacent stages.

Conceptually:

```
AssertAssignable<
  OutputOf<ParseJson>,
  InputOf<ValidateUser>
>

AssertAssignable<
  OutputOf<ValidateUser>,
  InputOf<CreateSession>
>
```

If a mismatch exists, compilation fails.

Example error:

```
Pipeline type mismatch:

Stage ParseJson produces: Json
Stage SendEmail expects: Email
```

---

# 7. Pipeline Runner

The runner executes stages sequentially.

Execution is **asynchronous**.

### Runner Responsibilities

The runner:

* creates stage logs
* invokes stages
* aggregates logs
* enforces drop/failure semantics

---

### Runner Algorithm

```
async runPipeline(stages, input):

  value = input
  logs = []

  for stage in stages:

      log = stage.createLog()

      result = await stage.process(value, log)

      logs.push({
          stage: stage.name,
          data: log
      })

      match result:

          Ok(Some(v)):
              value = v
              continue

          Ok(None):
              return {
                  result: null,
                  logs
              }

          Err(e):
              throw PipelineError(e, logs)

  return {
      result: value,
      logs
  }
```

---

# 8. Stage Logging System

Each stage receives a **private log context**.

Properties:

* created once per stage execution
* mutable by the stage
* inaccessible to other stages

The runner aggregates all logs.

---

### Log Lifecycle

```
Runner
  ↓
createLog()
  ↓
stage.process(input, log)
  ↓
runner collects log
```

---

### Example Log Structure

```
{
  stage: "ParseJson",
  data: {
    parsed: 120,
    failures: 3
  }
}
```

The final pipeline result contains:

```
{
  result: T | null
  logs: StageLog[]
}
```

Where:

```
StageLog = {
  stage: string
  data: unknown
}
```

---

### Log Isolation

Stages can only access their own log.

This ensures:

* no cross-stage interference
* deterministic log aggregation
* clean stage instrumentation

---

# 9. Example Stage

Example stage implementation:

```
type ParseLog = {
  parsed: number
  failures: number
}

const ParseJson: Stage<string, Json, ParseLog> = {

  name: "ParseJson",

  createLog() {
    return {
      parsed: 0,
      failures: 0
    }
  },

  async process(input, log) {

    try {
      const obj = JSON.parse(input)

      log.parsed++

      return Ok(Some(obj))

    } catch {

      log.failures++

      return Ok(None)

    }

  }
}
```

---

# 10. Example Pipeline

```
pipeline UserSessionPipeline {

  ParseJson
  ValidateUser
  CreateSession

}
```

Type flow:

```
string → Json → User → Session
```

---

# 11. Final Pipeline Result

Example output:

```
{
  result: Session,
  logs: [

    {
      stage: "ParseJson",
      data: {
        parsed: 120,
        failures: 3
      }
    },

    {
      stage: "ValidateUser",
      data: { ... }
    },

    {
      stage: "CreateSession",
      data: { ... }
    }

  ]
}
```

---

# 12. Non-Goals for MVP

The following features are intentionally excluded:

### Pipeline Branching

```
StageA
 ├─ StageB
 └─ StageC
```

### Routing Logic

Dynamic routing between stages.

### Graph Pipelines

Directed acyclic graph execution.

### Metadata Auto-Derivation

Deriving pipeline metadata from type structures.

### Cross-Stage Communication

Stages cannot access other stage logs or data.

---

# 13. Future Extensions

Potential future features include:

### Branching Pipelines

Support for conditional routing.

### Parallel Stage Execution

Fork/join pipeline structures.

### Router Abstraction

Separate routing layer once branching exists.

### Pipeline Replay

Deterministic replay using captured inputs and logs.

### Streaming Pipelines

Continuous data processing.

### Enhanced Observability

Built-in metrics and structured tracing.

---

# 14. MVP Design Summary

The MVP architecture prioritizes:

* **simplicity**
* **strong typing**
* **predictable execution**
* **minimal runtime complexity**

Key properties:

* linear async pipelines
* compile-time type validation
* runner-mediated orchestration
* stage isolation
* per-stage logging

The result is a lightweight, type-safe pipeline system suitable for TypeScript-based applications.
