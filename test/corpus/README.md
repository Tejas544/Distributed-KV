# Regression seed corpus

One file per fixed bug. Each holds the seed and configuration that reproduced it.

```
# ANV-0001
seed        = 0x8f3a91c40d2e77b1
config      = workloads/tpcc_5node.toml
invariant   = INV-RAFT-04
found_at    = <commit sha where it was found>
fixed_at    = <commit sha of the fix>
```

CI runs every seed here on every commit, and asserts both directions: the seed
must **fail** on the parent of its fix commit and **pass** on the fix commit
itself. A regression test that has never been observed to fail is not a
regression test.

These are the smallest and most valuable files in the repository. A bug ledger
row without one of these is an anecdote — see the three rules at the top of
[BUGS.md](../../BUGS.md).

Empty until P1, when the simulator can produce a failure worth pinning.
