# Warning

Don't import dspy into production ETL code. Run utilities via `zavod.tune`.

Something in DSPy interacts with leveldb in a way that crashes when the process excits unless you load leveldb before importing dspy.

It looks like this:

```
src/tcmalloc.cc:309] Attempt to free invalid pointer 0x600002f2ede0
```

It appears to be caused by https://github.com/google/leveldb/issues/634
