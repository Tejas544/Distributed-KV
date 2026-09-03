# TLA+ specifications

P7 exit criterion 3. Two specifications, nine TLC configurations, and a runner
that grades each one against what it is supposed to do.

```bash
tools/fetch_tla.sh          # once: tla2tools.jar
tools/tlc.sh                # all nine, graded
tools/tlc.sh SsiSnapshot    # one of them
```

## What is here

| File | What it specifies |
|---|---|
| `Raft.tla` | Raft's safety argument, including joint consensus and learners: who may vote, who may become leader, what a leader may declare committed. |
| `RaftFigure8.tla` | The same spec started from a constructed five-voter configuration, one election short of the commit the Figure-8 restriction forbids. |
| `SsiCommit.tla` | The distributed transaction commit protocol: the record status lattice, prewrites, lazy resolution, the push protocol, the read refresh, and parallel commit's implicit-commit predicate. |

## The configurations, and which are meant to fail

**Four of the nine are required to FAIL, on a *named* property.** That is not
decoration. A specification whose properties have only ever been observed to hold
is indistinguishable from one whose properties are vacuous, and this repository
has a ledger row about exactly that mistake ([ANV-0005](../BUGS.md)): an armed
invariant that could never fire, because a cheaper one shadowed it. A control
that fails on the *wrong* property is reported by `tools/tlc.sh` as a failure,
not as a pass.

| Configuration | Expected | Why it exists |
|---|---|---|
| `Raft` | clean | Elections, replication and the commit rule over three voters, with symmetry reduction. |
| `RaftJoint` | clean | Joint consensus. Separate because a configuration change names particular servers, which destroys the symmetry `Raft.cfg` depends on. |
| `RaftFigure8Ok` | clean | The constructed five-voter region with the Figure-8 restriction in place. |
| `RaftFigure8` | **violates `Figure8Rule`** | The same region with `commit_only_current_term` off. |
| `RaftNoJointCommit` | **violates `LeaderCompleteness`** | `joint_requires_commit` off, with a single-node `C_new` so the two configurations have majorities that do not overlap. |
| `SsiSerializable` | clean | The serializable engine: the read refresh is on and write skew must be impossible. |
| `SsiParallelSnapshot` | clean | Parallel commit at snapshot isolation — the control that isolates ANV-0065. |
| `SsiSnapshot` | **violates `NoWriteSkew`** | Snapshot isolation permits write skew. This is what makes the serializable run mean something: a property that forbids something unreachable forbids nothing. |
| `SsiParallel` | **violates `NoWriteSkew`** | [ANV-0065](../BUGS.md), and a finding rather than a control. |

## Three things worth knowing before editing any of this

**A wrong specification produces a counterexample that looks exactly like a
protocol bug.** Three of the first four violations TLC reported here were bugs in
the spec: a configuration modelled as a global variable any leader could change
atomically; Leader Completeness guarded on the entry's term rather than on the
term the commit *decision* was made in; and a follower clamping its commit index
to its whole log rather than to the prefix the message verified. Each read as a
serious defect. This is the harness-versus-engine trap the bug ledger keeps
recording, and specifications are not exempt from it.

**`=` binds tighter than `\/`.** `x' = a \/ b` parses as `(x' = a) \/ b`, which
is satisfied by `b` alone and leaves `x'` unassigned. TLC catches it as
"successor state is not completely specified".

**When a run cannot find something, suspect the branching factor, not the
depth.** The five-voter Figure-8 counterexample is about fifteen steps deep.
Breadth-first search was still at depth 14 after ten minutes and 137 million
states; random simulation checked 112 million states without reaching it.
Switching heartbeats off and restricting who may campaign — both named constants,
both documented in the `.cfg` — took that run to 11,746 states.

## What is not here

**Trace validation.** Exporting simulator traces in these specifications'
variable vocabulary and replaying them against `Next` is P7's one unmet
deliverable. Without it, the specifications are checked and the implementation is
checked, but *separately* — which is worth considerably less than the two
together, and the difference should not be blurred. See CONTEXT.md §15.
