import { AsyncDisposable, Stage, createPipeline, run } from '../src/runtime';
import { fromArray } from '../src/helpers';

type DoublerCtx = { doubler: {} } & AsyncDisposable;

class MacrosStyleContext implements AsyncDisposable {
  readonly doubler = {};

  async [Symbol.asyncDispose](): Promise<void> {
    // intentionally empty to emulate the macro-generated stub when no :dispose clause is present
  }
}

const doublerStage: Stage<DoublerCtx, number, number> = () => async function* (upstream) {
  for await (const value of upstream) {
    yield value * 2;
  }
};

const pipeline = createPipeline([{ name: 'doubler', stage: doublerStage }]);

test('pipelines run when the context has a blank async dispose stub', async () => {
  const ctxFactory = () => new MacrosStyleContext();
  const { results } = await run(ctxFactory, pipeline, fromArray([1, 2, 3]));
  expect(results).toEqual([2, 4, 6]);
});
