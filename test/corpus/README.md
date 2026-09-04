# Regression seed corpus

One file per fixed bug. Each is free-form prose rather than strict key=value,
because what a reproduction actually needs varies by suite — but every file
states, at minimum, the seed, the suite binary and seed *count* to run it with
(a suite's own seed-to-scenario mapping means the seed alone is not enough;
`anvil_shard_faults 20` and `anvil_shard_faults 12` do not reach the same
scenario at seed 19), and the invariant that fires. For example:

```
# ANV-0051 -- the read-freshness check compared concurrent reads as sequential
seed        = 19
workload    = shard_kv
nodes       = 5
faults      = drawn from seed; requires reply reordering between two clients
invariant   = INV-SHARD-CLIENT
reproduce   = anvil_shard_faults 20
note        = ...
```

`tools/verify_ledger_seeds.py` (P8 exit criterion 4) is what actually checks
this, for the rows where it's checkable: it reads each row's `reproduce` line
from here and its `fix_commit` from `BUGS.md`, and for every row whose
`fix_commit` is a real, resolvable git commit (not a phase label), builds the
named suite at that commit's parent and at the commit itself via `git
worktree`, and asserts the seed fails on the parent and passes on the commit.
There is no `--commit` flag on any binary — reproducing at a specific commit
means checking that commit out and building it, which is exactly what the tool
automates. Most ledger rows predate individual per-bug commits (their fixes
were squashed into bulk phase commits) and are not checkable this way; that is
reported honestly rather than skipped silently.

These are the smallest and most valuable files in the repository. A bug ledger
row without one of these is an anecdote — see the three rules at the top of
[BUGS.md](../../BUGS.md).
