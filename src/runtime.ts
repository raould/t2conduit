export type Stage<Ctx, I, O> = (ctx: Ctx) => (upstream: AsyncGenerator<I>) => AsyncGenerator<O>;

export type Transformer<I, O> =
  | O
  | AsyncGenerator<O>
  | Promise<O>
  | Promise<AsyncGenerator<O>>;

export interface AsyncDisposable {
  [Symbol.asyncDispose](): Promise<void>;
}

export interface StageLog {
  slot: string;
  count: number;
  errors: unknown[];
}

export interface InstrumentationHooks {
  onStart?(log: StageLog): void;
  onItem?(log: StageLog, item: unknown): void;
  onError?(log: StageLog, err: unknown): void;
  onComplete?(log: StageLog): void;
}

export interface RunOptions {
  errorMode?: 'fail-fast' | 'drop' | 'propagate';
  hooks?: InstrumentationHooks;
}

export interface RunResult<O> {
  results: O[];
  logs: StageLog[];
}

export interface PipelineSlot<Name extends string, Ctx, I, O> {
  name: Name;
  stage: Stage<Ctx, I, O>;
}

export interface ComposedPipeline<Slots extends readonly PipelineSlot<string, any, any, any>[]> {
  slots: Slots;
}

export interface StageErrorRecord<E = unknown> {
  slot: string;
  context: unknown;
  log: StageLog;
  error: E;
}

export interface Err<E = unknown> {
  readonly type: 'Err';
  readonly payload: StageErrorRecord<E>;
}

/**
 * Builds an Err payload so propagate mode can stream errors as values.
 *
 * @param slot the slot name where the error occurred
 * @param context the capability context bound to that slot
 * @param log the mutable log record for that slot (included for tracing)
 * @param error the original error value thrown by the stage
 */
export const makeErr = <E = unknown>(
  slot: string,
  context: unknown,
  log: StageLog,
  error: E
): Err<E> => ({
  type: 'Err',
  payload: { slot, context, log, error },
});

/**
 * Guards whether a value is an Err produced by propagate mode.
 *
 * @param value arbitrary value or pipeline payload
 * @returns true when the value matches the Err shape
 */
export const isErr = (value: unknown): value is Err =>
  typeof value === 'object' && value !== null && (value as Err).type === 'Err';

const createStageLog = (slot: string): StageLog => ({ slot, count: 0, errors: [] });

/**
 * Wraps a stage generator with instrumentation hooks and the active error policy.
 *
 * @param gen the underlying generator produced by a stage
 * @param log mutable log record for the slot
 * @param hooks optional instrumentation hooks invoked at lifecycle events
 * @param slotName name of the slot owning this generator
 * @param context capability container bound to the stage
 * @param errorMode configured error policy for the run
 * @returns a generator that mirrors the stage output while emitting hooks and error values
 */
const instrument = async function* (
  gen: AsyncGenerator<any>,
  log: StageLog,
  hooks: InstrumentationHooks | undefined,
  slotName: string,
  context: unknown,
  errorMode: RunOptions['errorMode']
): AsyncGenerator<any> {
  hooks?.onStart?.(log);
  const mode = errorMode ?? 'fail-fast';
  try {
    for await (const item of gen) {
      log.count += 1;
      hooks?.onItem?.(log, item);
      yield item;
    }
    hooks?.onComplete?.(log);
  } catch (err) {
    log.errors.push(err);
    hooks?.onError?.(log, err);
    if (mode === 'drop') {
      return;
    }
    if (mode === 'propagate') {
      yield makeErr(slotName, context, log, err) as any;
      return;
    }
    throw err;
  }
};

const composeGenerators = <I, O>(
  slots: PipelineSlot<string, any, any, any>[],
  context: Record<string, any>,
  source: AsyncGenerator<I>,
  logs: StageLog[],
  options: RunOptions
): AsyncGenerator<O> => {
  const { hooks, errorMode } = options;
  return slots.reduce<AsyncGenerator<any>>((gen, slot, index) => {
    const stageCtx = context[slot.name];
    const stageGen = slot.stage(stageCtx)(gen);
    return instrument(stageGen, logs[index], hooks, slot.name, stageCtx, errorMode);
  }, source) as AsyncGenerator<O>;
};

/**
 * Validates the pipeline configuration and freezes the ordered slot list.
 *
 * @param slots ordered slot entries that describe every stage in the pipeline
 * @returns a pipeline token that can be executed later
 * @throws when two slots expose the same name so instrumentation remains deterministic
 */
export const createPipeline = <
  Slots extends readonly PipelineSlot<string, any, any, any>[]
>(slots: Slots): ComposedPipeline<Slots> => {
  const seen = new Set<string>();
  for (const slot of slots) {
    if (seen.has(slot.name)) {
      throw new Error(`duplicate pipeline slot name: ${slot.name}`);
    }
    seen.add(slot.name);
  }
  return { slots };
};

/**
 * Elevates a simple transformer into a composable stage that streams every result.
 *
 * @param transformer synchronous or asynchronous fn that maps an input value to another value or async generator
 * @returns a stage that applies the transformer to every upstream item
 */
export const lift = <I, O>(transformer: (input: I) => Transformer<I, O>): Stage<{}, I, O> => {
  return () => async function* (upstream) {
    for await (const item of upstream) {
      const result = await transformer(item);
      if (typeof result === 'object' && result !== null && Symbol.asyncIterator in result) {
        yield* (result as AsyncGenerator<O>);
      } else {
        yield result as O;
      }
    }
  };
};

export const PURE = Symbol('t2-conduit/pure');

/**
 * Tags a stage as pure so it can be reused without leaking state across pipelines.
 *
 * @param stage stage implementation that does not close over mutable values
 * @returns the same stage with a PURE symbol flag used by helpers later
 */
export const pure = <Ctx, I, O>(stage: Stage<Ctx, I, O>): Stage<Ctx, I, O> => {
  return Object.assign(stage, { [PURE]: true });
};

/**
 * Executes the pipeline using the context provided by the caller and collects every output.
 *
 * @param ctxFactory produces an async-disposable execution context for the run
 * @param pipeline ordered pipeline configuration
 * @param source source async generator that feeds the first slot
 * @param options runtime knobs such as instrumentation hooks and error handling
 * @returns collected results and instrumentation logs for the entire run
 */
export const run = async <
  Ctx extends AsyncDisposable,
  Slots extends readonly PipelineSlot<string, any, any, any>[],
  I,
  O
>(
  ctxFactory: () => Promise<Ctx> | Ctx,
  pipeline: ComposedPipeline<Slots>,
  source: AsyncGenerator<I>,
  options: RunOptions = {}
): Promise<RunResult<O>> => {
  const ctx = await ctxFactory();
  const logContexts = pipeline.slots.map((slot) => createStageLog(slot.name));
  const composed = composeGenerators<I, O>(pipeline.slots, ctx as Record<string, any>, source, logContexts, options);
  const results: O[] = [];
  try {
    for await (const item of composed) {
      results.push(item);
    }
  } finally {
    await ctx[Symbol.asyncDispose]();
  }
  return { results, logs: logContexts };
};

/**
 * Executes a pipeline but leaves final consumption logic to the caller.
 *
 * @param ctxFactory async disposable context producer
 * @param pipeline prepared pipeline with instrumentation metadata
 * @param source upstream generator feeding the first slot
 * @param consumer consumer that drains the final slot generator
 * @param options runtime knobs such as error handling and instrumentation
 * @returns the instrumentation logs recorded during the run
 */
export const runWith = async <
  Ctx extends AsyncDisposable,
  Slots extends readonly PipelineSlot<string, any, any, any>[],
  I,
  O
>(
  ctxFactory: () => Promise<Ctx> | Ctx,
  pipeline: ComposedPipeline<Slots>,
  source: AsyncGenerator<I>,
  consumer: (gen: AsyncGenerator<O>) => Promise<void>,
  options: RunOptions = {}
): Promise<{ logs: StageLog[] }> => {
  const ctx = await ctxFactory();
  const logContexts = pipeline.slots.map((slot) => createStageLog(slot.name));
  const composed = composeGenerators<I, O>(pipeline.slots, ctx as Record<string, any>, source, logContexts, options);
  try {
    await consumer(composed);
  } finally {
    await ctx[Symbol.asyncDispose]();
  }
  return { logs: logContexts };
};
