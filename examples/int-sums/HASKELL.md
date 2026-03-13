# Int Summation Examples

Goal: Convert Haskell examples to t2conduit.

## 1. First version

### Generate int sequence.

```t2
(program
  (let (int_gen) (async-generator-fn ((start) (end))
    (for (let (i) start) (<= i end) (set! i (+ i 1))
        (yield i))))
  (export-named (int_gen)))
```

### Sink iterative development.

Now we’ll write a sink that will take these values and add them together. This will take the form of a recursive function with an accumulator argument. Sinks often function recursively to receive their next input.

#### v1

So we’ll start off by awaiting some value from the upstream conduit.

```haskell
myIntegerSink :: Integer -> Sink Integer Identity Integer
myIntegerSink accum = do
  maybeFirstVal <- await
  -- to be continued
```

#### v2

If that value is Nothing, we’ll know there are no more values coming. We can then proceed to return whatever accumulated value we have.

```haskell
myIntegerSink :: Integer -> Sink Integer Identity Integer
myIntegerSink accum = do
  maybeFirstVal <- await
  case maybeFirstVal of
    Nothing -> return accum
  -- to be continued
```

#### v3

If that value contains another Int, we’ll first calculate the new sum by adding them together. Then we’ll make a recursive call to the sink function with the new accumulator argument:

```haskell
myIntegerSink :: Integer -> Sink Integer Identity Integer
myIntegerSink accum = do
  maybeFirstVal <- await
  case maybeFirstVal of
    Nothing -> return accum
    Just val -> do
      let newSum = val + accum
      myIntegerSink newSum
```

# 2. Combined sinks

Combining Our Conduits

Now we’ll want to combine our conduits. We do this with the “fuse” operator, written out as =$=. Haskell libraries can be notorious for having strange operators. This library isn’t necessarily an exception. But think of conduits as a tunnel, and this operator looks like it's connecting two parts of a tunnel.

With this operator, the output type of the first conduit needs to match the input type of the second conduit. With how we’ve set up our conduits, this is the case. So now to polish things off, we use runConduitPure with our combined conduit:

```haskell
fullConduit :: Integer
fullConduit = runConduitPure $ 
  myIntegerSource =$= myIntegerSink 0
```

It’s generally quite easy using fuse to add more conduits. For instance, suppose we wanted even numbers to count double for our purposes. We could accomplish this in our source or sink, but we could also add another conduit. It will take an Int as input and yield an Int as output. It will want for its input integer, check its value, and then double the value if it is even. Then we will yield the resulting value. Once again, if we haven’t seen the end of the conduit, we need to recursively jump back into the conduit.

```haskell
myDoublingConduit :: Conduit Integer Identity Integer
myDoublingConduit = do
  maybeVal <- await
  case maybeVal of
    Nothing -> return () 
    Just val -> do
      let newVal = if val `mod` 2 == 0
            then val * 2
            else val
      yield newVal
      myDoublingConduit
```

Then we can stick this conduit between our other conduits with the fuse operator!

```haskell
fullConduit :: Integer
fullConduit = runConduitPure $ 
  myIntegerSource =$= myDoublingConduit =$= myIntegerSink 0
```
