import { AsyncDisposable } from './runtime';

export type ContextFactory<Ctx extends AsyncDisposable> = () => Promise<Ctx> | Ctx;

export type DisposableContext<Ctx extends object> = Ctx & AsyncDisposable;

/**
 * Creates a context factory that instantiates a disposable class via constructor.
 *
 * @param ctor async-disposable class constructor
 * @returns factory that produces a fresh instance each call
 */
export const contextFactoryFromConstructor = <Ctx extends AsyncDisposable>(
  ctor: new () => Ctx
): ContextFactory<Ctx> => {
  return () => new ctor();
};

/**
 * Wraps an existing factory that already produces an async-disposable context.
 *
 * @param factory user-supplied builder that allocates the context
 * @returns nothing more than the same factory but correctly typed
 */
export const contextFactoryFromFactory = <Ctx extends AsyncDisposable>(
  factory: ContextFactory<Ctx>
): ContextFactory<Ctx> => {
  return factory;
};

/**
 * Extends a plain object with a no-op async disposal hook to satisfy the runner.
 *
 * @param context capability map that will back the pipeline slots
 * @param dispose optional cleanup callback invoked once per run
 * @returns object that implements AsyncDisposable via [Symbol.asyncDispose]
 */
export const createDisposableContext = <Ctx extends object>(
  context: Ctx,
  dispose: () => Promise<void> | void = () => undefined
): DisposableContext<Ctx> => {
  return Object.assign({}, context, {
    async [Symbol.asyncDispose]() {
      await dispose();
    },
  });
};

/**
 * Vends an async generator that enumerates the supplied iterable values.
 *
 * @param items sync iterable (typically an array) to materialize
 * @returns async generator that yields every item in order
 */
export async function* fromArray<T>(items: Iterable<T>): AsyncGenerator<T> {
  for (const item of items) {
    yield item;
  }
}

/**
 * Adapts any sync or async iterable into a reusable async generator source.
 *
 * @param iterable an Iterable or AsyncIterable that can be pulled repeatedly
 * @returns async generator that proxies the iterable via `for await`
 */
export async function* fromIterable<T>(
  iterable: Iterable<T> | AsyncIterable<T>
): AsyncGenerator<T> {
  for await (const item of iterable) {
    yield item;
  }
}

/**
 * Collects every emitted value from a generator into an array.
 *
 * @param source generator to drain
 * @returns resolved array containing every yielded element
 */
export const collectToArray = async <T>(source: AsyncGenerator<T>): Promise<T[]> => {
  const results: T[] = [];
  for await (const item of source) {
    results.push(item);
  }
  return results;
};

/**
 * Drains a generator without retaining the produced values.
 *
 * @param source generator to exhaust
 * @returns promise that resolves once the generator completes
 */
export const drain = async <T>(source: AsyncGenerator<T>): Promise<void> => {
  for await (const _ of source) {
    // intentionally ignore value
  }
};

/**
 * Provides a sink that runs a callback for every emitted value.
 *
 * @param source generator to consume
 * @param onItem callback invoked sequentially for every value
 * @returns promise resolved after the generator finishes
 */
export const forEach = async <T>(
  source: AsyncGenerator<T>,
  onItem: (value: T) => Promise<void> | void
): Promise<void> => {
  for await (const value of source) {
    await onItem(value);
  }
};
