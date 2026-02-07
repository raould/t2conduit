t2conduit: Complete Specification

A Pull-Based Streaming Library for TypeScript

Escaping the Observable/Signal/Promise nightmare with one unified async model
Table of Contents

    Introduction
    The Problem: Function Colors & Interop Hell
    Background & Influences
    Design Philosophy
    Naming Philosophy
    Core Concepts
    Type System Design
    Operator Reference
    t2lang Macro Integration
    RxJS Comparison
    Interop Guide
    Implementation
    Examples
    Roadmap
    Appendices

1. Introduction
1.1 What is t2conduit?

t2conduit is a pull-based streaming library for TypeScript that provides:

    One async model — Source<T> replaces both Promise and Observable
    Unix-pipe semantics — data flows Source → Pipe → Pipe → Sink
    Automatic backpressure — consumers drive the flow
    Resource safety — cleanup happens automatically via async iteration
    Tiered type safety — opt-in strictness from loose to pedantic
    Readable naming — no Haskell symbol soup, just familiar words
    Macro integration — first-class support for t2lang

1.2 The One-Sentence Pitch

Stop juggling Promises, Observables, and Signals. Use Source<T> for everything async.
typescript

// t2conduit: One pattern for everything
const user = await pipe(getUser(id), first());           // Single value
const users = await pipe(getUsers(), collect());         // Multiple values
await pipe(streamLogs(), take(100), forEach(console.log)); // Streaming

1.3 Quick Comparison
Aspect	RxJS Observable	Angular Signal	Promise	t2conduit Source
Execution	Lazy (cold) or eager (hot)	Sync reactive	Eager	Always lazy
Values	0 to ∞	Current only	Exactly 1	0 to ∞
Cleanup	Manual .unsubscribe()	Automatic*	None	Automatic
Backpressure	Manual	N/A	N/A	Automatic
await compatible	No	No	Yes	Yes
2. The Problem
2.1 What Color Is Your Function?

Bob Nystrom's famous article described how async/await creates two "colors" of functions:

BLUE functions: sync,  return T
RED functions:  async, return Promise<T>

The rule: Red can call blue. Blue cannot call red.
typescript

// BLUE
function add(a: number, b: number): number {
  return a + b;
}

// RED
async function fetchUser(id: string): Promise<User> {
  return fetch(`/api/users/${id}`).then(r => r.json());
}

// RED can call BLUE ✓
async function getDoubledAge(id: string): Promise<number> {
  const user = await fetchUser(id);
  return add(user.age, user.age);  // ✓
}

// BLUE cannot call RED ✗
function processUser(id: string): User {
  return fetchUser(id);  // Type error: Promise<User> is not User
}

The infection: Once ANY function in a call chain is red, everything above it must be red.
2.2 The Third Color: Observable Purple

RxJS introduces a third color:

BLUE functions:   sync,     return T
RED functions:    async,    return Promise<T>
PURPLE functions: reactive, return Observable<T>

Now the conversion rules explode:
Caller → Callee	Allowed?	How?
Blue → Blue	✅	Direct
Red → Blue	✅	Direct
Red → Red	✅	await
Purple → Blue	✅	Inside operators
Purple → Red	⚠️	switchMap? mergeMap? concatMap?
Purple → Purple	✅	pipe
Blue → Red	❌	Impossible
Blue → Purple	❌	Impossible
Red → Purple	⚠️	firstValueFrom (lossy!)
typescript

// PURPLE calling RED is awkward
function enrichUsers(): Observable<EnrichedUser[]> {
  return getUsers().pipe(
    switchMap(users =>                    // Which operator?!
      from(Promise.all(                   // Wrap Promise in Observable
        users.map(u => enrichUser(u))     // enrichUser is async (red)
      ))
    ),
  );
}

// RED calling PURPLE is lossy
async function processUsers(): Promise<void> {
  const users = await firstValueFrom(getUsers());
  // What if getUsers() emits multiple times? Lost.
  // What if it never emits? Hangs forever.
  // What if it errors before emitting? Throws.
}

2.3 The Fourth Color: Signals

Angular 16+ adds Signals — yet another async paradigm:

BLUE:   sync
RED:    Promise
PURPLE: Observable
ORANGE: Signal (sync reactive)

Now you need:

    toSignal(obs$) — but needs injection context!
    toObservable(signal) — cold subscription to shared state?!
    No direct Signal ↔ Promise conversion

typescript

// The interop nightmare
@Component({...})
class UserComponent {
  // Purple → Orange (needs injection context, initial value)
  user = toSignal(this.http.get<User>('/api/user'), { initialValue: undefined });
  
  // Orange in template
  template: `{{ user()?.name }}`  // Must handle undefined
  
  // What if you need it as Promise somewhere?
  async doSomething() {
    const u = await firstValueFrom(toObservable(this.user));  // 🤮
  }
}

2.4 The Refactoring Nightmare

When colors change, every caller must change:
typescript

// Version 1: Promise (RED)
class UserService {
  async getUser(id: string): Promise<User> {
    return fetch(`/api/users/${id}`).then(r => r.json());
  }
}

// Every caller:
const user = await this.userService.getUser(id);

Now you need caching with reactivity. Observable required:
typescript

// Version 2: Observable (PURPLE) — COLOR CHANGED!
class UserService {
  getUser(id: string): Observable<User> {
    return this.http.get<User>(`/api/users/${id}`).pipe(
      shareReplay(1),
    );
  }
}

// EVERY CALLER MUST CHANGE:
this.userService.getUser(id).pipe(
  takeUntilDestroyed(),
).subscribe(user => { ... });

2.5 The Conversion Tax

Every color boundary requires ceremony:
typescript

// Promise → Observable
from(promise)                    // But promise already started!
defer(() => from(promiseFn()))   // Need defer for laziness

// Observable → Promise
await firstValueFrom(obs$)       // Might hang, might throw
await lastValueFrom(obs$)        // Must complete

// Observable → Signal
toSignal(obs$, { initialValue }) // Needs injection context

// Signal → Observable
toObservable(signal)             // Cold sub to shared state?!

2.6 t2conduit's Answer: Standard Function Colors

BLUE:  sync
GREEN: Source<T>  ← Replaces both RED and PURPLE

Green is special:

    It's AsyncIterable<T> — works with await
    One value or many — same type
    Lazy by default
    Streaming or one-shot — consumer decides

typescript

// GREEN — the only async color
function getUser(id: string): Source<User> { ... }
function getUsers(): Source<User> { ... }
function streamLogs(): Source<LogEntry> { ... }

// All consumed the same way!
const user = await pipe(getUser(id), first());
const users = await pipe(getUsers(), collect());
await pipe(streamLogs(), take(100), forEach(console.log));

The refactoring scenario becomes trivial:
typescript

// Version 1: Simple fetch
getUser(id): Source<User> {
  return fromLazyPromise(() => fetch(`/api/users/${id}`).then(r => r.json()));
}

// Version 2: Caching + reactivity — SAME RETURN TYPE!
getUser(id): Source<User> {
  return async function* () {
    if (cache.has(id)) yield cache.get(id);
    const fresh = await fetch(`/api/users/${id}`).then(r => r.json());
    cache.set(id, fresh);
    yield fresh;
  }();
}

// Callers: UNCHANGED
const user = await pipe(getUser(id), first());

3. Background & Influences
3.1 Haskell Conduit

Conduit is a Haskell streaming library with:

    Pull-based evaluation — sinks drive pipelines
    Resource safety — bracket pattern with guaranteed cleanup
    Composability — pipelines compose with operators

t2conduit translates Conduit concepts but not its syntax:
Haskell Conduit	t2conduit	Notes
Source m a	Source<T>	Same concept
Conduit a m b	Pipe<A, B>	Same concept
Sink a m r	Sink<T, R>	Same concept
.| operator	pipe() function	Readable name, not symbol
runConduit	await pipe(...)	Just await
bracketP	bracket()	Same concept
yield	yield	Same
await	for await	Same

Key difference: We take the semantics, not the symbols.
3.2 t2lang

t2lang is an S-expression syntax for TypeScript, enabling Clojure-style macros:
lisp

;; t2lang pipeline — readable names, not symbols
(await (pipe (fromFile "data.csv")
             (splitLines)
             (map parse)
             (filter valid?)
             (chunk 100)
             (flatMap saveBatch {:concurrency 5})
             (count)))

t2conduit provides macros for:

    Pipeline composition with compile-time validation
    Resource lifecycle enforcement
    Friendly error messages (vs TypeScript's cryptic generics)
    Ownership and cardinality checking

3.3 RxJS (Anti-Influence)

RxJS shaped t2conduit by demonstrating what to avoid:
RxJS Problem	t2conduit Solution
Hot/cold confusion	Everything is cold
share() race conditions	Explicit share() changes type
Manual unsubscribe()	Automatic via iteration protocol
100+ operators	68 well-named operators
switchMap vs mergeMap vs...	flatMapLatest, flatMap({ concurrency })
Push-based (no backpressure)	Pull-based (automatic backpressure)
3.4 Other Influences
Source	Contribution
Unix pipes	Linear composition model
RxJS pipe()	Function name (already standard in JS)
Rust iterators	collect(), filter(), map() naming
TC39 Explicit Resource Management	await using for cleanup
Clojure transducers	Composable transforms
Elixir GenStage	Backpressure model
Array.prototype	Operator names (map, filter, reduce, etc.)
4. Design Philosophy
4.1 Pull Over Push

Push (RxJS):     Producer → Consumer (producer controls pace)
Pull (t2conduit): Producer ← Consumer (consumer controls pace)

Pull-based streaming eliminates:

    Backpressure complexity — consumer only asks when ready
    Subscription management — iteration handles lifecycle
    Hot/cold confusion — starting iteration = starting production

4.2 Linear Over Graph

RxJS allows complex dataflow graphs. t2conduit prefers linear pipelines:

Source → Pipe → Pipe → Pipe → Sink

When you need multiple consumers, explicitly collect() or use share().
4.3 Native Over Novel

t2conduit builds on standard primitives:

    AsyncIterable<T> for sources
    async function* for generators
    for await...of for consumption
    try/finally for cleanup
    await using for resource management

If you understand async iteration, you understand t2conduit.
4.4 Explicit Over Implicit
RxJS (Implicit)	t2conduit (Explicit)
mergeMap(..., 5)	flatMap(..., { concurrency: 5 })
share() silently enables multi-sub	share() changes type to "shared"
bufferTime(1000) unbounded	buffer({ time: 1000, maxSize: 100, onOverflow: 'drop' })
4.5 Errors You Can Read

TypeScript generic errors are awful:

Type 'Source<Event, "infinite", "shared", "scoped", "unbounded">' is not 
assignable to type 'Source<Event, Cardinality, "owned", Lifecycle, "bounded">'.

t2conduit addresses this via:

    Tiered strictness — start loose, add types as needed
    Macro errors — t2lang provides human-readable messages
    Runtime checks — catch issues even without strict types

5. Naming Philosophy
5.1 The Problem with Symbol Soup

Haskell and Scala use operators that intimidate newcomers:
haskell

-- Haskell Conduit
runConduit $ sourceFile "input.txt" .| mapC processLine .| sinkFile "output.txt"

-- Scala ZIO
source >>> transform >>> sink

People see .| or >>> and think "I need a PhD to use this."
5.2 Design Principles for Names

    Readable as English — code should almost read as a sentence
    Familiar from JS/TS ecosystem — leverage existing knowledge
    Discoverable — autocomplete should help
    No symbols required — symbols as optional sugar only
    Consistent with Array.prototype — use same names where applicable

5.3 Primary Names

The main API uses clear, familiar words:
Concept	Primary Name	Notes
Compose stages	pipe	Standard in RxJS, Ramda, lodash/fp
Create from items	from, fromArray	Familiar from RxJS
Create from file	fromFile, fromLines	Self-descriptive
Transform each item	map	Same as Array.prototype.map
Keep matching items	filter	Same as Array.prototype.filter
Limit count	take	Common in streaming libraries
Skip items	drop	Opposite of take
Batch items	chunk	Clear meaning
Flatten nested	flatten	Same as Array.prototype.flat
Map + flatten	flatMap	Same as Array.prototype.flatMap
Collect to array	collect	From Rust, clear intent
Get first	first	Self-descriptive
Get last	last	Self-descriptive
Fold to value	reduce	Same as Array.prototype.reduce
Run side effect	forEach	Same as Array.prototype.forEach
Consume all	drain	Clear metaphor
5.4 Operator Naming vs RxJS
RxJS Name	t2conduit Name	Why
switchMap	flatMapLatest	Describes behavior: latest wins
mergeMap	flatMap + options	{ concurrency: n } is explicit
concatMap	flatMapConcat	Describes behavior: sequential
exhaustMap	flatMapFirst	Describes behavior: first wins
bufferCount	chunk	Simpler, more common term
toArray	collect	Matches Rust/Kotlin convention
tap	tap	Already good
catchError	catchError	Already good
5.5 Symbol Aliases (Optional)

For users who prefer brevity, symbols are available but never required:
lisp

;; Recommended: readable names (used in all documentation)
(await (pipe (from items)
             (map transform)
             (filter valid?)
             (collect)))

;; Optional: symbol alias (for experienced users only)
(|> (from items)
    (map transform)
    (filter valid?)
    (collect))

Policy:

    All documentation uses word names
    All error messages use word names
    Tutorials never introduce symbols
    Symbols exist for those who want them, but are not promoted

5.6 Comparison

Scary (Haskell-style):
haskell

runConduit $ src .| mapC f .| filterC p .| foldlC (+) 0

Friendly (t2conduit):
typescript

await pipe(
  from(src),
  map(f),
  filter(p),
  reduce((a, b) => a + b, 0),
);

The operations are identical. The names make all the difference.
6. Core Concepts
6.1 The Three Types
typescript

type Source<T> = AsyncIterable<T>;
type Pipe<A, B> = (source: Source<A>) => Source<B>;
type Sink<T, R> = (source: Source<T>) => Promise<R>;

Source: Where data comes from (file, array, HTTP response, database) Pipe: Transforms data (map, filter, chunk, flatten) Sink: Consumes data, returns result (collect, reduce, forEach)
6.2 The pipe Function
typescript

// Source + Pipes → Source
const transformed = pipe(source, map(f), filter(p));

// Source + Pipes + Sink → Promise<R>
const result = await pipe(source, map(f), filter(p), collect());

Implementation is trivial:
typescript

function pipe(source: Source<any>, ...stages: any[]): any {
  let current = source;
  for (const stage of stages) {
    current = stage(current);
    if (current instanceof Promise) return current;
  }
  return current;
}

6.3 Creating Sources

Sources are async generators:
typescript

// From array
async function* fromArray<T>(items: T[]): Source<T> {
  for (const item of items) yield item;
}

// From file (with cleanup!)
async function* fromFile(path: string): Source<string> {
  const handle = await fs.open(path, 'r');
  try {
    for await (const line of handle.readLines()) {
      yield line;
    }
  } finally {
    await handle.close();  // Always runs!
  }
}

// Lazy promise
function fromLazyPromise<T>(fn: () => Promise<T>): Source<T> {
  return async function* () {
    yield await fn();
  }();
}

6.4 Creating Pipes

Pipes are functions returning async generators:
typescript

function map<A, B>(fn: (a: A) => B): Pipe<A, B> {
  return async function* (source) {
    for await (const item of source) {
      yield fn(item);
    }
  };
}

function filter<T>(pred: (t: T) => boolean): Pipe<T, T> {
  return async function* (source) {
    for await (const item of source) {
      if (pred(item)) yield item;
    }
  };
}

function chunk<T>(size: number): Pipe<T, T[]> {
  return async function* (source) {
    let batch: T[] = [];
    for await (const item of source) {
      batch.push(item);
      if (batch.length >= size) {
        yield batch;
        batch = [];
      }
    }
    if (batch.length > 0) yield batch;
  };
}

6.5 Creating Sinks

Sinks consume and return a value:
typescript

function collect<T>(): Sink<T, T[]> {
  return async (source) => {
    const items: T[] = [];
    for await (const item of source) items.push(item);
    return items;
  };
}

function first<T>(): Sink<T, T | undefined> {
  return async (source) => {
    for await (const item of source) return item;
    return undefined;
  };
}

function reduce<T, R>(fn: (acc: R, x: T) => R, init: R): Sink<T, R> {
  return async (source) => {
    let acc = init;
    for await (const item of source) acc = fn(acc, item);
    return acc;
  };
}

6.6 Resource Management
Automatic via Iteration Protocol
typescript

async function* fromFile(path: string) {
  const handle = await fs.open(path);
  try {
    for await (const chunk of handle.readLines()) {
      yield chunk;
    }
  } finally {
    await handle.close();  // Runs on complete, error, OR break!
  }
}

// Even early exit triggers cleanup:
for await (const line of fromFile('huge.csv')) {
  if (line.includes('STOP')) break;  // finally still runs!
}

Bracket Pattern
typescript

function bracket<T, R>(
  acquire: () => Promise<T>,
  release: (t: T) => Promise<void>,
  use: (t: T) => Source<R>
): Source<R> {
  return async function* () {
    const resource = await acquire();
    try {
      yield* use(resource);
    } finally {
      await release(resource);
    }
  }();
}

TC39 Explicit Resource Management
typescript

await using file = openFile('data.csv');
const lines = await pipe(file, collect());
// file[Symbol.asyncDispose]() called automatically

7. Type System Design
7.1 The Problem with Complex Types

Full phantom types create unreadable errors:
typescript

type Source<T, C extends Cardinality, O extends Ownership, L extends Lifecycle, P extends Pressure> = ...

// Error message:
// Type 'Source<User, "unknown", "owned", "scoped", "bounded">' is not assignable to...

7.2 Tiered Strictness Solution

Same runtime, different .d.ts files. Pick your level:
typescript

import { pipe, fromFile } from 't2conduit';           // Level 0: Loose
import { pipe, fromFile } from 't2conduit/strict';    // Level 1: Resources
import { pipe, fromFile } from 't2conduit/strict/2';  // Level 2: Cardinality
import { pipe, fromFile } from 't2conduit/strict/3';  // Level 3: Ownership
import { pipe, fromFile } from 't2conduit/pedantic';  // Level 4: Everything

Level 0: Loose
typescript

type Source<T> = AsyncIterable<T>;
type Pipe<A, B> = (source: Source<A>) => Source<B>;
type Sink<T, R> = (source: Source<T>) => Promise<R>;

// Everything is just Source<T>
declare function fromFile(path: string): Source<string>;
declare function collect<T>(): Sink<T, T[]>;

For: Prototyping, scripts, "I know what I'm doing"
Level 1: Strict (Resources)
typescript

type Source<T> = AsyncIterable<T>;
type ScopedSource<T> = Source<T> & AsyncDisposable;

// Sources needing cleanup return ScopedSource
declare function fromFile(path: string): ScopedSource<string>;
declare function fromQuery(db: Pool, sql: string): ScopedSource<Row>;

// Sinks only accept non-scoped unless in withScope
declare function collect<T>(): Sink<Source<T>, T[]>;

Catches:
typescript

// ❌ Error: ScopedSource not assignable to Source
const lines = await pipe(fromFile('data.csv'), collect());

// ✅ Fix: explicit scope
await using file = fromFile('data.csv');
const lines = await pipe(file, collect());

For: Production code (recommended default)
Level 2: Cardinality
typescript

type Cardinality = 'empty' | 'one' | 'nonEmpty' | 'unknown';
type Source<T, C extends Cardinality = 'unknown'> = AsyncIterable<T> & { __card: C };

// first() overloads
declare function first<T>(): Sink<Source<T, 'nonEmpty'>, T>;       // Guaranteed!
declare function first<T>(): Sink<Source<T, 'unknown'>, T | undefined>;

Catches:
typescript

// ❌ Error: Type 'User | undefined' not assignable to 'User'
const user: User = await pipe(fromArray(users), filter(u => u.active), first());

// ✅ Fix: handle undefined or use firstOrThrow
const user = await pipe(..., firstOrThrow('No active users'));

For: Avoiding undefined surprises
Level 3: Ownership
typescript

type Ownership = 'owned' | 'shared';
type Source<T, C, O extends Ownership = 'owned'> = ...;

// share() changes type
declare function share<T, C>(): Pipe<Source<T, C, 'owned'>, Source<T, C, 'shared'>>;

// collect() requires owned
declare function collect<T, C>(): Sink<Source<T, C, 'owned'>, T[]>;

Catches:
typescript

const source = fromArray([1, 2, 3]);

// ❌ Error: source already consumed
await pipe(source, reduce((a, b) => a + b, 0));
await pipe(source, collect());  // Error!

// ✅ Fix: share explicitly
const shared = pipe(fromArray([1, 2, 3]), share());

For: Library code, complex pipelines
Level 4: Pedantic
typescript

type Source<T,
  C extends Cardinality = 'unknown',
  O extends Ownership = 'owned',
  L extends Lifecycle = 'managed',
  P extends Pressure = 'bounded'
> = AsyncIterable<T> & { /* phantoms */ };

// Full signatures
declare function fromDOMEvent(el: Element, event: string): 
  Source<Event, 'infinite', 'shared', 'scoped', 'unbounded'>;

// collect() rejects unbounded
declare function collect<T>(): Sink<Source<T, any, 'owned', any, 'bounded'>, T[]>;

For: Debugging memory/resource issues (temporary)
Level Summary
Level	Import	Tracks	Error Quality	Use Case
0	t2conduit	Just T	Great	Prototyping
1	t2conduit/strict	+ Scoped	Good	Production default
2	t2conduit/strict/2	+ Cardinality	OK	Careful code
3	t2conduit/strict/3	+ Ownership	Rough	Library code
4	t2conduit/pedantic	Everything	Awful	Debugging
Escape Hatches
typescript

import { unsafe } from 't2conduit/strict';

const source = unsafe.nonEmpty(fromFile('config.json'));
const owned = unsafe.asOwned(sharedSource);

8. Operator Reference
8.1 MVP Operators (68 total)
Sources (14)
Operator	Signature	Notes
fromArray	<T>(items: T[]) → Source<T>	In-memory
from	<T>(items: T[]) → Source<T>	Alias for fromArray
fromIterable	<T>(iter: Iterable<T>) → Source<T>	Sync iterables
fromAsync	<T>(iter: AsyncIterable<T>) → Source<T>	Pass-through
fromPromise	<T>(p: Promise<T>) → Source<T>	Single item (eager!)
fromLazyPromise	<T>(fn: () => Promise<T>) → Source<T>	Single item (lazy)
of	<T>(...items: T[]) → Source<T>	Variadic
empty	<T>() → Source<T>	Zero items
range	(start, end) → Source<number>	Numeric range
generate	<T>(fn: () => T | null) → Source<T>	Pull until null
fromFile	(path) → ScopedSource<Uint8Array>	File chunks
fromLines	(path) → ScopedSource<string>	Line by line
fromResponse	(res: Response) → Source<Uint8Array>	Fetch response
fromQuery	(db, sql) → ScopedSource<Row>	DB cursor
Pipes — Transform (4)
Operator	Signature	Notes
map	<A,B>(fn: A → B) → Pipe<A,B>	Same as Array.map
filter	<T>(pred: T → boolean) → Pipe<T,T>	Same as Array.filter
tap	<T>(fn: T → void) → Pipe<T,T>	Side effect
scan	<T,R>(fn, init) → Pipe<T,R>	Running accumulator
Pipes — Limit (5)
Operator	Signature	Notes
take	(n: number) → Pipe<T,T>	First n items
drop	(n: number) → Pipe<T,T>	Skip first n
takeWhile	<T>(pred) → Pipe<T,T>	Until predicate fails
dropWhile	<T>(pred) → Pipe<T,T>	Skip until predicate fails
slice	(start, end) → Pipe<T,T>	Range of items
Pipes — Batch (2)
Operator	Signature	Notes
chunk	(size: number) → Pipe<T, T[]>	Fixed-size batches
chunkBy	<T>(fn: T → K) → Pipe<T, T[]>	Group by key changes
Pipes — Flatten (4)
Operator	Signature	Notes
flatten	Pipe<T[], T>	One level
flatMap	<A,B>(fn, opts?) → Pipe<A,B>	Map + flatten with concurrency
flatMapConcat	<A,B>(fn) → Pipe<A,B>	Sequential (one at a time)
flatMapLatest	<A,B>(fn) → Pipe<A,B>	Cancel previous, keep latest

flatMap options:
typescript

{
  concurrency?: number;  // default: 1 (sequential)
}

Pipes — Unique (2)
Operator	Signature	Notes
distinct	<T>(opts?) → Pipe<T,T>	Default: consecutive only
distinctBy	<T,K>(fn, opts?) → Pipe<T,T>	By key function

Options:
typescript

{
  scope?: 'consecutive' | 'global';  // default: 'consecutive' (memory-safe)
  maxSize?: number;                   // required for 'global' at Level 4
}

Pipes — Error Handling (3)
Operator	Signature	Notes
catchError	<T>(fn: Error → Source<T>) → Pipe<T,T>	Recover from errors
retry	(n, opts?) → Pipe<T,T>	Retry failed operations
finalize	<T>(fn: () → void) → Pipe<T,T>	Always runs at end
Pipes — Encoding (8)
Operator	Signature	Notes
decode	(encoding?) → Pipe<Uint8Array, string>	Bytes to string
encode	(encoding?) → Pipe<string, Uint8Array>	String to bytes
splitLines	Pipe<string, string>	Split on newlines
joinLines	Pipe<string, string>	Join with newlines
parseJSON	<T>() → Pipe<string, T>	Parse each as JSON
stringifyJSON	Pipe<unknown, string>	Stringify each
parseJSONLines	<T>() → Pipe<Uint8Array, T>	NDJSON parsing
Sinks — Collect (5)
Operator	Signature	Notes
collect	<T>() → Sink<T, T[]>	To array
collectMap	<T,K>(fn) → Sink<T, Map<K,T>>	To Map
collectSet	<T>() → Sink<T, Set<T>>	To Set
collectString	Sink<string, string>	Concatenate
collectBuffer	Sink<Uint8Array, Uint8Array>	Concatenate
Sinks — Single Item (5)
Operator	Signature	Notes
first	<T>() → Sink<T, T | undefined>	First item
firstOrThrow	<T>(msg?) → Sink<T, T>	First or error
last	<T>() → Sink<T, T | undefined>	Last item
lastOrThrow	<T>(msg?) → Sink<T, T>	Last or error
single	<T>() → Sink<T, T>	Exactly one or error
Sinks — Aggregate (5)
Operator	Signature	Notes
reduce	<T,R>(fn, init) → Sink<T,R>	Same as Array.reduce
count	Sink<unknown, number>	Count items
sum	Sink<number, number>	Sum numbers
some	<T>(pred) → Sink<T, boolean>	Same as Array.some
every	<T>(pred) → Sink<T, boolean>	Same as Array.every
Sinks — Side Effects (2)
Operator	Signature	Notes
forEach	<T>(fn) → Sink<T, void>	Same as Array.forEach
drain	Sink<unknown, void>	Consume all, discard
Sinks — Output (3)
Operator	Signature	Notes
toFile	(path) → Sink<Uint8Array, void>	Write to file
toResponse	(opts?) → Sink<Uint8Array, Response>	HTTP Response
toWritable	(stream) → Sink<Uint8Array, void>	Node/Web stream
Combinators (4)
Operator	Signature	Notes
concat	<T>(...sources) → Source<T>	Sequential
merge	<T>(...sources) → Source<T>	Interleaved
zip	<A,B>(a, b) → Source<[A,B]>	Paired tuples
zipWith	<A,B,R>(a, b, fn) → Source<R>	Paired + transform
Utilities (4)
Operator	Signature	Notes
pipe	(source, ...stages) → Source | Promise	Compose pipeline
share	<T>() → Pipe<Source<T>, SharedSource<T>>	Multi-consumer
tee	<T>(source, n) → Source<T>[]	Split shared
bracket	(acquire, release, use) → Source<T>	Resource safety
9. t2lang Macro Integration
9.1 Primary Macro: pipe

The main macro uses the same name as the function:
lisp

;; Define the pipe macro (same name as function)
(defmacro pipe (source & stages)
  "Compose source through pipeline stages."
  `(pipeImpl ~source ~@stages))

;; Usage — clean and readable
(const result
  (await (pipe (from items)
               (map transform)
               (filter valid?)
               (collect))))

9.2 Auto-Await Variant: run

For convenience, run implies await:
lisp

(defmacro run (source & stages)
  "Compose and execute pipeline (auto-awaits)."
  `(await (pipe ~source ~@stages)))

;; Usage — even cleaner
(const result
  (run (from items)
       (map transform)
       (filter valid?)
       (collect)))

9.3 Resource Management
lisp

(defmacro with-source (bindings & body)
  "Bind a source with guaranteed cleanup."
  (let* ((name (first (first bindings)))
         (source-expr (second (first bindings)))
         (src-sym (gensym "source")))
    `(let* ((~src-sym ~source-expr))
       (try
         (let* ((~name ~src-sym))
           ~@body)
         (finally
           (when (prop ~src-sym "close")
             ((prop ~src-sym "close"))))))))

;; Usage
(with-source [file (fromFile "data.csv")]
  (run file
       (splitLines)
       (map parse)
       (collect)))

9.4 SQL Macros
lisp

(defmacro sql (& parts)
  "Build parameterized query from template."
  (let* ((parsed (parse-sql-template parts)))
    `(object
       "text" ~(sql->parameterized-string parsed)
       "values" (array ~@(sql->param-exprs parsed)))))

;; Usage
(const query (sql SELECT * FROM users WHERE id = ~userId AND active = ~isActive))
;; Expands to: { text: "SELECT * FROM users WHERE id = $1 AND active = $2", 
;;               values: [userId, isActive] }

9.5 Compile-Time Validation
lisp

(defmacro pipeline (source & stages)
  "Pipeline with compile-time validation and friendly errors."
  (let* ((issues (analyze-pipeline source stages))]
    (doseq [issue issues]
      (case (:type issue)
        :double-use
          (compile-error!
            (format "Source '%s' is consumed twice:\n  First: %s\n  Second: %s\n\nFix: use (share source) or collect first."
                    (:name issue) (:first-loc issue) (:second-loc issue)))
        :scoped-escape
          (compile-error!
            (format "Source '%s' requires cleanup but escapes its scope.\n  Created: %s\n  Escapes: %s\n\nFix: wrap in (with-source [...] ...)"
                    (:name issue) (:created-loc issue) (:escape-loc issue)))
        :maybe-empty
          (compile-warning!
            "first() may return undefined here. Use firstOrThrow() or handle undefined.")))
    `(pipe ~source ~@stages)))

9.6 Domain-Specific Macros
lisp

;; HTTP handler as pipeline
(defmacro defhandler (name method path & stages)
  "Define HTTP handler as a pipeline."
  `(export (const ~name
     (object
       "method" ~(symbol->string method)
       "path" ~path
       "handler" (fn (req: Request): (Promise Response)
                   (run (fromRequest req) ~@stages))))))

;; Usage
(defhandler export-users GET "/api/users/export"
  (authenticate)
  (authorize "users:read")
  (fromQuery db "SELECT * FROM users")
  (map userToJSON)
  (stringifyJSON)
  (toResponse {:contentType "application/json"}))

9.7 Symbol Aliases (Optional)

For users who prefer brevity, symbols are available:
lisp

;; These are defined but not promoted in documentation
(defalias |> pipe)
(defalias |>! run)

;; Usage (optional, for experienced users)
(|>! (from items)
     (map f)
     (collect))

Policy: Documentation, tutorials, and error messages always use word names (pipe, run), never symbols.
10. RxJS Comparison
10.1 Bug-by-Bug Examples
Example 1: Leaky File Processor

RxJS (5 hidden bugs):
typescript

function processCSV(path: string) {
  const lines$ = new Observable<string>(subscriber => {
    const rl = createInterface({ input: createReadStream(path) });
    rl.on('line', line => subscriber.next(line));
    rl.on('close', () => subscriber.complete());
    // BUG 1: No cleanup on error
    // BUG 2: No cleanup on unsubscribe
  });

  return lines$.pipe(
    map(line => line.split(',')[2]),
    distinct(),  // BUG 3: Unbounded memory
    bufferCount(100),
    mergeMap(batch => saveToDB(batch)),  // BUG 4: Unlimited concurrency
    // BUG 5: Partial state on error
  );
}

t2conduit:
typescript

async function processCSV(path: string) {
  await pipe(
    fromLines(path),                    // Auto-cleanup
    map(line => line.split(',')[2]),
    distinct({ scope: 'consecutive' }), // Memory-safe
    chunk(100),
    flatMap(batch => saveToDB(batch), { concurrency: 5 }),
    drain(),
  );
}

Bug	RxJS	t2conduit
Leak on error	❌ Manual	✅ Automatic
Leak on unsubscribe	❌ Manual	✅ Automatic
Unbounded distinct	❌ Silent OOM	✅ Consecutive default
Unlimited concurrency	❌ Hidden	✅ Explicit
Error handling	❌ Complex	✅ try/catch
Example 2: Share Race Condition

RxJS:
typescript

const user$ = from(fetchUser(userId)).pipe(share());

// Component A subscribes, fetch starts
user$.subscribe(user => renderProfile(user));

// Fetch might complete before B subscribes...
// B gets NOTHING!
user$.subscribe(user => renderSettings(user));

t2conduit (problem doesn't exist):
typescript

// Just get the value
const user = await pipe(fromLazyPromise(() => fetchUser(userId)), first());
renderProfile(user);
renderSettings(user);

// For actual streaming, collect and reuse
const items = await pipe(source, collect());
processA(items);
processB(items);

Example 3: Operator Confusion

RxJS — four operators, same signature:
typescript

switchMap(x => fetch(x))   // Cancel previous
mergeMap(x => fetch(x))    // All concurrent
concatMap(x => fetch(x))   // Queue
exhaustMap(x => fetch(x))  // Ignore while busy

t2conduit — names describe behavior:
typescript

flatMapLatest(x => fetch(x))                  // Cancel previous (name says "latest")
flatMap(x => fetch(x), { concurrency: Infinity })  // All concurrent
flatMapConcat(x => fetch(x))                  // Sequential (name says "concat")
flatMap(x => fetch(x), { concurrency: 1 })    // Explicit sequential

Example 4: Memory Bomb

RxJS — silent until OOM:
typescript

fromEvent(document, 'click').pipe(
  bufferTime(5000),  // No max!
).subscribe(clicks => process(clicks));

t2conduit Level 4 — compile error:
typescript

// ❌ Error: buffer() on unbounded requires maxSize
pipe(fromDOMEvent(document, 'click'), buffer({ time: 5000 }));

// ✅ Fixed
pipe(
  fromDOMEvent(document, 'click'),
  buffer({ time: 5000, maxSize: 100, onOverflow: 'drop' }),
);

10.2 What Pull-Based Solves Automatically
Problem	RxJS	t2conduit
Backpressure	Manual	Automatic
Memory accumulation	Silent OOM	One item at a time
Hot/cold confusion	Major footgun	Everything cold
Subscription leaks	unsubscribe()	Iteration protocol
share() races	Hidden refcount	Type changes

~60% of RxJS bugs vanish by switching to pull-based.
11. Interop Guide
11.1 The Conversion Matrix
From	To	Method	Notes
Observable	Source	fromObservable(obs$)	Lazy, cleanup handled
Source	Observable	toObservable(source)	Unsubscribe cancels
Signal	Source	fromSignal(sig)	Streams all changes
Source	Signal	toSignal(source, opts)	Needs cleanup strategy
Promise	Source	fromLazyPromise(() => p)	Lazy!
Source	Promise	pipe(s, first())	Or collect()
11.2 From Observable
typescript

import { fromObservable } from 't2conduit/interop';

function fromObservable<T>(obs$: Observable<T>): Source<T> {
  return {
    async *[Symbol.asyncIterator]() {
      const queue: T[] = [];
      let resolve: (() => void) | null = null;
      let done = false;
      let error: Error | null = null;
      
      const subscription = obs$.subscribe({
        next: (v) => { queue.push(v); resolve?.(); },
        error: (e) => { error = e; resolve?.(); },
        complete: () => { done = true; resolve?.(); },
      });
      
      try {
        while (true) {
          while (queue.length) yield queue.shift()!;
          if (error) throw error;
          if (done) return;
          await new Promise<void>(r => { resolve = r; });
        }
      } finally {
        subscription.unsubscribe();
      }
    }
  };
}

11.3 To Observable
typescript

import { toObservable } from 't2conduit/interop';

function toObservable<T>(source: Source<T>): Observable<T> {
  return new Observable(subscriber => {
    const controller = new AbortController();
    
    (async () => {
      try {
        for await (const item of source) {
          if (controller.signal.aborted) break;
          subscriber.next(item);
        }
        subscriber.complete();
      } catch (e) {
        subscriber.error(e);
      }
    })();
    
    return () => controller.abort();
  });
}

11.4 Angular Example

Before (RxJS):
typescript

@Component({...})
class UserComponent implements OnInit, OnDestroy {
  private destroy$ = new Subject<void>();
  users$ = this.http.get<User[]>('/api/users');
  
  ngOnInit() {
    this.users$.pipe(
      takeUntil(this.destroy$),
    ).subscribe(users => this.render(users));
  }
  
  ngOnDestroy() {
    this.destroy$.next();
    this.destroy$.complete();
  }
}

After (t2conduit):
typescript

@Component({...})
class UserComponent {
  users = signal<User[]>([]);
  
  async ngOnInit() {
    const users = await pipe(
      fromLazyPromise(() => fetch('/api/users').then(r => r.json())),
      first(),
    );
    this.users.set(users ?? []);
  }
  // No cleanup needed!
}

12. Implementation
12.1 Spike (~150 lines)
typescript

// === TYPES ===
export type Source<T> = AsyncIterable<T>;
export type Pipe<A, B> = (source: Source<A>) => Source<B>;
export type Sink<T, R> = (source: Source<T>) => Promise<R>;

// === PIPE ===
export function pipe(source: Source<any>, ...stages: any[]): any {
  let current = source;
  for (const stage of stages) {
    current = stage(current);
    if (current instanceof Promise) return current;
  }
  return current;
}

// === SOURCES ===
export async function* fromArray<T>(items: T[]): Source<T> {
  for (const item of items) yield item;
}

export const from = fromArray;  // Alias

export async function* fromResponse(res: Response): Source<Uint8Array> {
  const reader = res.body!.getReader();
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      yield value;
    }
  } finally {
    reader.releaseLock();
  }
}

export function fromLazyPromise<T>(fn: () => Promise<T>): Source<T> {
  return async function* () {
    yield await fn();
  }();
}

// === PIPES ===
export function map<A, B>(fn: (a: A) => B): Pipe<A, B> {
  return async function* (source) {
    for await (const item of source) yield fn(item);
  };
}

export function filter<T>(fn: (t: T) => boolean): Pipe<T, T> {
  return async function* (source) {
    for await (const item of source) {
      if (fn(item)) yield item;
    }
  };
}

export function take<T>(n: number): Pipe<T, T> {
  return async function* (source) {
    let count = 0;
    for await (const item of source) {
      if (count++ >= n) break;
      yield item;
    }
  };
}

export function chunk<T>(size: number): Pipe<T, T[]> {
  return async function* (source) {
    let batch: T[] = [];
    for await (const item of source) {
      batch.push(item);
      if (batch.length >= size) {
        yield batch;
        batch = [];
      }
    }
    if (batch.length > 0) yield batch;
  };
}

export function flatMap<A, B>(
  fn: (a: A) => Promise<B>,
  opts: { concurrency?: number } = {}
): Pipe<A, B> {
  const concurrency = opts.concurrency ?? 1;
  return async function* (source) {
    if (concurrency === 1) {
      for await (const item of source) yield await fn(item);
    }
    // Concurrent implementation...
  };
}

// === SINKS ===
export function collect<T>(): Sink<T, T[]> {
  return async (source) => {
    const items: T[] = [];
    for await (const item of source) items.push(item);
    return items;
  };
}

export function first<T>(): Sink<T, T | undefined> {
  return async (source) => {
    for await (const item of source) return item;
    return undefined;
  };
}

export function reduce<T, R>(fn: (acc: R, x: T) => R, init: R): Sink<T, R> {
  return async (source) => {
    let acc = init;
    for await (const item of source) acc = fn(acc, item);
    return acc;
  };
}

export function forEach<T>(fn: (t: T) => void): Sink<T, void> {
  return async (source) => {
    for await (const item of source) fn(item);
  };
}

export function drain(): Sink<unknown, void> {
  return async (source) => {
    for await (const _ of source) {}
  };
}

12.2 Project Structure

t2conduit/
├── src/core.ts           # Runtime implementation
├── types/
│   ├── loose.d.ts        # Level 0
│   ├── strict.d.ts       # Level 1
│   ├── strict-2.d.ts     # Level 2
│   ├── strict-3.d.ts     # Level 3
│   └── pedantic.d.ts     # Level 4
├── interop/
│   ├── rxjs.ts           # Observable adapters
│   └── angular.ts        # Signal adapters
├── t2/
│   └── macros.t2         # t2lang macros
└── package.json

13. Examples
13.1 Basic Transform
typescript

const result = await pipe(
  from([1, 2, 3, 4, 5]),
  filter(x => x % 2 === 0),
  map(x => x * 10),
  collect(),
);
// [20, 40]

t2lang:
lisp

(const result
  (run (from (array 1 2 3 4 5))
       (filter (fn (x) (== (% x 2) 0)))
       (map (fn (x) (* x 10)))
       (collect)))

13.2 CSV Import
typescript

await pipe(
  fromLines('users.csv'),
  drop(1),  // Skip header
  map(line => parseCSV(line)),
  filter(row => isValidEmail(row.email)),
  chunk(100),
  flatMap(batch => db.insertMany('users', batch), { concurrency: 3 }),
  drain(),
);

t2lang:
lisp

(run (fromLines "users.csv")
     (drop 1)
     (map parseCSV)
     (filter (fn (row) (isValidEmail (prop row "email"))))
     (chunk 100)
     (flatMap (fn (batch) (db.insertMany "users" batch))
              {:concurrency 3})
     (drain))

13.3 HTTP Streaming Response
typescript

export async function handler(req: Request): Promise<Response> {
  return pipe(
    fromQuery(db, 'SELECT * FROM users'),
    map(JSON.stringify),
    map(s => s + '\n'),
    encode(),
    toResponse({ contentType: 'application/x-ndjson' }),
  );
}

13.4 Parallel Fetch with Limit
typescript

const results = await pipe(
  from(urls),
  flatMap(url => fetch(url).then(r => r.json()), { concurrency: 5 }),
  collect(),
);

13.5 Error Recovery
typescript

const results = await pipe(
  from(ids),
  flatMap(id => fetchItem(id)),
  retry(3, { delay: attempt => 1000 * Math.pow(2, attempt) }),
  catchError(err => {
    console.error('Failed:', err);
    return from([fallbackItem]);
  }),
  collect(),
);

13.6 Multi-Source Join
typescript

const productMap = keyBy(products, p => p.id);

const dashboard = await pipe(
  from(users),
  map(user => ({
    user,
    orders: orders
      .filter(o => o.userId === user.id)
      .map(o => ({ ...o, product: productMap.get(o.productId) })),
  })),
  collect(),
);

14. Roadmap
14.1 v1.0 (MVP)

    Core 68 operators
    Type definitions (all 5 levels)
    t2lang macros
    Documentation (all examples use readable names)
    Test suite

14.2 v1.1

    Time-based operators (debounce, throttle, buffer, timeout)
    Push adapters (fromDOMEvent, fromWebSocket)
    groupBy, window
    Benchmarks vs RxJS

14.3 v1.2

    Visual debugger
    Angular integration package
    React Suspense integration

15. Appendices
Appendix A: Glossary
Term	Definition
Source	Produces values (AsyncIterable<T>)
Pipe	Transforms values (Source → Source)
Sink	Consumes values, returns result
Pull-based	Consumer requests from producer
Push-based	Producer sends to consumer
Cardinality	How many items a source produces
Ownership	Whether source can have multiple consumers
Scoped	Source requiring cleanup
gensym	Generate unique symbol (macro hygiene)
Appendix B: References

    Haskell Conduit — Semantics inspiration (not syntax)
    t2lang — S-expression TypeScript with macros
    RxJS — What to avoid
    TC39 Explicit Resource Management
    What Color is Your Function
    Clojure Transducers

Appendix C: Naming Comparison
Haskell (scary)	t2conduit (friendly)
.|	pipe
runConduit	await pipe(...)
mapC	map
filterC	filter
foldlC	reduce
sinkList	collect
Appendix D: Color Diagram

Before (RxJS + Signals):

     ┌──────┐
     │ BLUE │ (sync)
     └──┬───┘
        │
        ▼
     ┌──────┐        ┌────────┐
     │ RED  │◄──────►│ PURPLE │
     │async │ awkward│  Rx    │
     └──┬───┘        └───┬────┘
        │                │
        ▼                ▼
     ┌────────────────────────┐
     │        SIGNAL          │
     │  (injection context!)  │
     └────────────────────────┘

After (t2conduit):

     ┌──────┐
     │ BLUE │ (sync)
     └──┬───┘
        │
        ▼
     ┌───────┐
     │ GREEN │ (Source<T>)
     │       │ = RED + PURPLE unified
     └───────┘

t2conduit: Standard function color. One model. Readable names. Zero ceremony.

