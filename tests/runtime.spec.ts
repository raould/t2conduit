import { describe, expect, test } from 'vitest';
import { createPipeline, isErr, run, Stage } from '../src/runtime';
import { createDisposableContext, fromArray } from '../src/helpers';

const multiplyStage: Stage<{}, number, number> = () => async function* (upstream) {
  for await (const value of upstream) {
    if (value === 2) {
      throw new Error('boom');
    }
    yield value * 2;
  }
};

const pipeline = createPipeline([{ name: 'mul', stage: multiplyStage }]);

const makeContext = () => createDisposableContext({ mul: {} });


describe('runtime integration', () => {
  test('fail-fast rejects when a stage throws', async () => {
    await expect(run(makeContext, pipeline, fromArray([1, 2, 3]))).rejects.toThrow('boom');
  });

  test('drop mode resolves with partial results when a stage fails', async () => {
    const { results } = await run(makeContext, pipeline, fromArray([1, 2, 3]), {
      errorMode: 'drop',
    });

    expect(results).toEqual([2]);
  });

  test('propagate mode emits Err payloads instead of throwing', async () => {
    const { results } = await run(makeContext, pipeline, fromArray([1, 2, 3]), {
      errorMode: 'propagate',
    });

    const [first, second] = results;
    expect(first).toBe(2);
    expect(isErr(second)).toBe(true);
    if (isErr(second)) {
      expect(second.payload.slot).toBe('mul');
      expect(second.payload.error).toBeInstanceOf(Error);
      expect(second.payload.error.message).toBe('boom');
    }
  });
});
