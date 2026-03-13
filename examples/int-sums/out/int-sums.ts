import { createPipeline, run, pure, lift } from "t2conduit/dist/runtime.js";
import { fromArray } from "t2conduit/dist/helpers.js";
const range_source  = async function*(start: number, end: number) {
  for (let i = start; (i <= end); i = (i + 1)) {
    (yield i);
  }
};
const double_evens  = pure(lift(async (value: number): Promise<number> => {
  return (((value % 2) == 0) ? (value * 2) : value);
}));
const sum_integers  = (ctx: unknown) => {
  return async function*(upstream: AsyncGenerator<number>): AsyncGenerator<number> {
    {
      let total  = 0;
      for await (const value of upstream) {
        total = (total + value);
      }
      (yield total);
    }
  };
};
const sum_pipeline  = createPipeline([({
  name: "sum",
  stage: sum_integers
})]);
const doubled_sum_pipeline  = createPipeline([({
  name: "double",
  stage: double_evens
}), ({
  name: "sum",
  stage: sum_integers
})]);
class example_context implements AsyncDisposable {
  double = ({
    
  });
  sum = ({
    
  });
  [Symbol.asyncDispose](): Promise<void> {
    return Promise.resolve(undefined);
  }
}
const base_results  = (await run(() => {
  return new example_context();
}, sum_pipeline, range_source(1, 10), ({
  errorMode: "fail-fast"
})));
const doubled_results  = (await run(() => {
  return new example_context();
}, doubled_sum_pipeline, range_source(1, 10), ({
  errorMode: "fail-fast"
})));
console.log(`Base sum (1..10): ${base_results.results}`);
console.log(`Doubled even sum (1..10): ${doubled_results.results}`);
