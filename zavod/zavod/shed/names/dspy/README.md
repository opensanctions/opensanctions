Load leveldb before importing dspy to prevent

```
src/tcmalloc.cc:309] Attempt to free invalid pointer 0x600002f2ede0
```

on exit. See: https://github.com/google/leveldb/issues/634
