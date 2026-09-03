----------------------------- MODULE SsiCommit -----------------------------
(***************************************************************************)
(* The distributed transaction commit protocol, as implemented in           *)
(* anvil/core/txn/coordinator.cc.                                           *)
(*                                                                         *)
(* One coordinator per transaction. A transaction takes a start timestamp   *)
(* from the oracle, reads at that timestamp, buffers its writes, and at     *)
(* commit writes a *record* on the primary key's range and an *intent* on   *)
(* every key it wrote. The record's status is the commit point: the         *)
(* transaction happened if and only if its record says kCommitted, and      *)
(* every intent is cleanup that any later reader may perform on the         *)
(* coordinator's behalf. That single sentence is the whole atomicity        *)
(* argument, and it is INV-TXN-02.                                          *)
(*                                                                         *)
(* Three engines share the mechanism and differ by two booleans, exactly as *)
(* CoordinatorOptions does:                                                 *)
(*                                                                         *)
(*   RefreshReadsOnPush   a transaction pushed to a later commit timestamp  *)
(*      must re-verify that nothing it read changed in between, or restart. *)
(*      This is what separates serializable from snapshot isolation, and    *)
(*      write skew is *legal* without it -- so the spec is checked at both  *)
(*      settings and expects a different answer at each.                    *)
(*   ParallelCommit       the record goes to kStaging with the full key     *)
(*      list and the transaction is implicitly committed once every listed  *)
(*      key carries its intent. INV-TXN-11.                                 *)
(*                                                                         *)
(* What is modelled: the status lattice and its terminality, prewrites,     *)
(* lazy resolution by a reader that meets an intent, the push protocol, the *)
(* read refresh, and the parallel-commit predicate. What is not: clock      *)
(* uncertainty and commit-wait (they buy real-time ordering, which is a     *)
(* property about an external observer this spec does not have), sharding   *)
(* and ranges (a record's location does not change what it means), and      *)
(* crash recovery (the record is durable by construction here).             *)
(*                                                                         *)
(* The two findings this spec exists to have caught, both S0 and both found *)
(* the hard way in P6:                                                      *)
(*                                                                         *)
(*   ANV-0060  a heartbeat narrowed a staging record's key list, so the     *)
(*      parallel-commit predicate answered "committed" over fewer keys than *)
(*      the transaction actually wrote. Modelled by KeyListIsComplete.      *)
(*   INV-TXN-02  a coordinator that died before writing its record must     *)
(*      never come back and commit over a verdict a reader already          *)
(*      published. Modelled by TerminalIsFinal.                             *)
(***************************************************************************)

EXTENDS Naturals, FiniteSets, Sequences, TLC

CONSTANTS Key,                  \* the keys transactions read and write
          Txn,                  \* the transactions
          MaxTs,                \* bound: highest timestamp TLC will explore
          RefreshReadsOnPush,   \* TRUE = serializable, FALSE = snapshot isolation
          ParallelCommit        \* TRUE = staging records

\* Record statuses. kPending and kStaging are non-terminal; kCommitted and
\* kAborted are terminal and nothing may leave them.
CONSTANTS NoRecord, Pending, Staging, Committed, Aborted

----------------------------------------------------------------------------

VARIABLES
    nextTs,        \* the oracle: a single monotonic counter
    startTs,       \* [Txn -> Nat]; 0 means "not begun"
    commitTs,      \* [Txn -> Nat]; 0 means "no commit timestamp yet"
    status,        \* [Txn -> status]
    keyList,       \* [Txn -> SUBSET Key]; what the record says the txn wrote
    writeSet,      \* [Txn -> SUBSET Key]; what the txn actually intends to write
    readSet,       \* [Txn -> SUBSET Key]
    intents,       \* [Key -> SUBSET Txn]; live intents, by owner
    versions,      \* [Key -> SUBSET [ts: Nat, txn: Txn]]; committed versions
    observed       \* the history: [txn, key, ts] triples a reader actually saw

vars == <<nextTs, startTs, commitTs, status, keyList, writeSet, readSet,
          intents, versions, observed>>

Terminal(st) == st \in {Committed, Aborted}

Max2(a, b) == IF a > b THEN a ELSE b

\* The version a reader at timestamp `ts` should see for key `k`: the newest
\* committed version at or below ts, or 0 for none.
VisibleTs(k, ts) ==
    LET below == {v.ts : v \in {vv \in versions[k] : vv.ts <= ts}}
    IN IF below = {} THEN 0 ELSE CHOOSE m \in below : \A o \in below : o <= m

----------------------------------------------------------------------------

Init ==
    /\ nextTs = 1
    /\ startTs = [t \in Txn |-> 0]
    /\ commitTs = [t \in Txn |-> 0]
    /\ status = [t \in Txn |-> NoRecord]
    /\ keyList = [t \in Txn |-> {}]
    /\ writeSet = [t \in Txn |-> {}]
    /\ readSet = [t \in Txn |-> {}]
    /\ intents = [k \in Key |-> {}]
    /\ versions = [k \in Key |-> {}]
    /\ observed = {}

----------------------------------------------------------------------------
(***************************************************************************)
(* Actions                                                                 *)
(***************************************************************************)

\* Take a start timestamp. The oracle is one monotonic counter, which is what
\* the replicated oracle provides and what INV-TXN-09 protects.
Begin(t) ==
    /\ startTs[t] = 0
    /\ nextTs < MaxTs
    /\ startTs' = [startTs EXCEPT ![t] = nextTs]
    /\ nextTs' = nextTs + 1
    /\ UNCHANGED <<commitTs, status, keyList, writeSet, readSet, intents,
                   versions, observed>>

\* Declare intent to write a key. Buffered locally; nothing is visible yet.
Write(t, k) ==
    /\ startTs[t] > 0
    /\ status[t] = NoRecord
    /\ k \notin writeSet[t]
    /\ writeSet' = [writeSet EXCEPT ![t] = writeSet[t] \cup {k}]
    /\ UNCHANGED <<nextTs, startTs, commitTs, status, keyList, readSet,
                   intents, versions, observed>>

\* Read at the start timestamp. A reader that meets a live intent belonging to
\* another transaction cannot proceed until that transaction's record is
\* resolved -- which is Resolve below, and is the only place a transaction's
\* fate is ever decided by somebody other than its own coordinator.
Read(t, k) ==
    /\ startTs[t] > 0
    /\ status[t] = NoRecord
    /\ \A other \in intents[k] : other = t \/ Terminal(status[other])
    /\ readSet' = [readSet EXCEPT ![t] = readSet[t] \cup {k}]
    /\ observed' = observed \cup {[rtxn |-> t, rkey |-> k,
                                   rts |-> VisibleTs(k, startTs[t])]}
    /\ UNCHANGED <<nextTs, startTs, commitTs, status, keyList, writeSet,
                   intents, versions>>

\* Write the record. Under parallel commit it goes straight to kStaging with the
\* complete key list; otherwise kPending.
\*
\* The record is written *before* any prewrite, in either order of the
\* secondaries -- which is why `secondaries before primary` is a control in the
\* drill rather than a must-detect: the window Percolator has, where the primary
\* lock *is* the commit record, never opens here.
WriteRecord(t) ==
    /\ startTs[t] > 0
    /\ status[t] = NoRecord
    /\ writeSet[t] # {}
    /\ status' = [status EXCEPT ![t] = IF ParallelCommit THEN Staging ELSE Pending]
    /\ keyList' = [keyList EXCEPT ![t] = writeSet[t]]
    /\ UNCHANGED <<nextTs, startTs, commitTs, writeSet, readSet, intents,
                   versions, observed>>

\* Lay down an intent on a key this transaction intends to write.
Prewrite(t, k) ==
    /\ status[t] \in {Pending, Staging}
    /\ k \in writeSet[t]
    /\ t \notin intents[k]
    /\ intents[k] \cap {o \in Txn : ~Terminal(status[o])} \subseteq {t}
    /\ intents' = [intents EXCEPT ![k] = intents[k] \cup {t}]
    /\ UNCHANGED <<nextTs, startTs, commitTs, status, keyList, writeSet,
                   readSet, versions, observed>>

\* Has anything this transaction read been overwritten since it started? This is
\* the refresh, and switching it off is what turns serializable back into
\* snapshot isolation -- the deliberate-bug knob refresh_reads_on_push.
ReadsAreStillValid(t, ts) ==
    \A k \in readSet[t] : VisibleTs(k, ts) = VisibleTs(k, startTs[t])

\* Commit. Takes a commit timestamp, refreshes the reads if the engine says so,
\* and moves the record to its terminal state.
Commit(t) ==
    /\ status[t] \in {Pending, Staging}
    /\ writeSet[t] \subseteq {k \in Key : t \in intents[k]}   \* every intent present
    /\ nextTs < MaxTs
    /\ LET ts == nextTs IN
         /\ nextTs' = nextTs + 1
         /\ commitTs' = [commitTs EXCEPT ![t] = ts]
         /\ IF RefreshReadsOnPush => ReadsAreStillValid(t, ts)
            THEN /\ status' = [status EXCEPT ![t] = Committed]
                 /\ versions' = [k \in Key |->
                        IF k \in writeSet[t]
                        THEN versions[k] \cup {[ts |-> ts, txn |-> t]}
                        ELSE versions[k]]
            ELSE /\ status' = [status EXCEPT ![t] = Aborted]
                 /\ versions' = versions
    /\ UNCHANGED <<startTs, keyList, writeSet, readSet, intents, observed>>

\* A coordinator may give up at any point before its record is terminal.
Abort(t) ==
    /\ status[t] \in {Pending, Staging}
    /\ status' = [status EXCEPT ![t] = Aborted]
    /\ UNCHANGED <<nextTs, startTs, commitTs, keyList, writeSet, readSet,
                   intents, versions, observed>>

\* Recovery, and the subtle one.
\*
\* A reader that meets an intent whose owner is still kStaging evaluates the
\* parallel-commit predicate on the owner's behalf: a staging record is
\* committed exactly when every key it *lists* carries its intent. ANV-0060 was
\* this predicate answering yes over a narrowed key list, so the list is what
\* KeyListIsComplete watches.
StagingIsImplicitlyCommitted(t) ==
    /\ status[t] = Staging
    /\ \A k \in keyList[t] : t \in intents[k]

\* Somebody other than the owner resolves a staging record. Note it cannot
\* resolve a kPending one: kPending carries no verdict, so the only sound move
\* against a pending record is to abort it, which is what push_record does.
ResolveStaging(t) ==
    /\ StagingIsImplicitlyCommitted(t)
    /\ commitTs[t] = 0
    /\ nextTs < MaxTs
    /\ LET ts == nextTs IN
         /\ nextTs' = nextTs + 1
         /\ commitTs' = [commitTs EXCEPT ![t] = ts]
         /\ status' = [status EXCEPT ![t] = Committed]
         /\ versions' = [k \in Key |->
                IF k \in keyList[t]
                THEN versions[k] \cup {[ts |-> ts, txn |-> t]}
                ELSE versions[k]]
    /\ UNCHANGED <<startTs, keyList, writeSet, readSet, intents, observed>>

\* A blocked reader pushes a transaction that holds an intent in its way. A push
\* against a transaction with no record writes an *aborted* record, so a
\* coordinator that died before writing its own can never come back and commit
\* over the verdict. That is the whole of INV-TXN-02.
Push(t, blocker) ==
    /\ t # blocker
    /\ status[blocker] = NoRecord
    /\ startTs[blocker] > 0
    /\ \E k \in Key : blocker \in intents[k]
    /\ status' = [status EXCEPT ![blocker] = Aborted]
    /\ UNCHANGED <<nextTs, startTs, commitTs, keyList, writeSet, readSet,
                   intents, versions, observed>>

Next ==
    \/ \E t \in Txn : Begin(t)
    \/ \E t \in Txn, k \in Key : Write(t, k)
    \/ \E t \in Txn, k \in Key : Read(t, k)
    \/ \E t \in Txn : WriteRecord(t)
    \/ \E t \in Txn, k \in Key : Prewrite(t, k)
    \/ \E t \in Txn : Commit(t)
    \/ \E t \in Txn : Abort(t)
    \/ \E t \in Txn : ResolveStaging(t)
    \/ \E t, b \in Txn : Push(t, b)

Spec == Init /\ [][Next]_vars

----------------------------------------------------------------------------
(***************************************************************************)
(* Properties                                                              *)
(***************************************************************************)

\* INV-TXN-02, the atomicity argument: a record that has reached a terminal
\* status never leaves it. Stated as an action property, because "never
\* changes its mind" is about a step and not about a state.
TerminalIsFinal ==
    [][\A t \in Txn : Terminal(status[t]) => status'[t] = status[t]]_vars

\* INV-TXN-11: a staging record's key list covers everything the transaction
\* actually wrote. A *narrowed* list is the quiet failure -- the predicate still
\* evaluates, over fewer keys than the transaction wrote, and comes out
\* committed. ANV-0060.
KeyListIsComplete ==
    \A t \in Txn :
        (status[t] \in {Staging, Committed}) => writeSet[t] \subseteq keyList[t]

\* Atomicity as a client sees it: either every key a committed transaction
\* wrote carries its version, or none does. There is no half.
CommitIsAllOrNothing ==
    \A t \in Txn :
        (status[t] = Committed) =>
            \A k \in writeSet[t] : \E v \in versions[k] : v.txn = t

\* Nothing an aborted transaction wrote is ever visible.
AbortLeavesNothing ==
    \A t \in Txn :
        (status[t] = Aborted) => \A k \in Key : ~\E v \in versions[k] : v.txn = t

\* Every committed version carries its own transaction's commit timestamp, so a
\* reader's snapshot means the same thing on every key.
VersionsCarryCommitTs ==
    \A k \in Key : \A v \in versions[k] : v.ts = commitTs[v.txn]

\* Snapshot isolation: no reader ever observes a version above its own start
\* timestamp.
SnapshotIsolation ==
    \A o \in observed :
        \A v \in versions[o.rkey] :
            (v.ts = o.rts) => v.ts <= startTs[o.rtxn]

\* Write skew, which is the anomaly that separates the two engines. Two
\* transactions overlap in real time, each reads what the other writes, and both
\* commit. That is legal at snapshot isolation and forbidden at serializable, so
\* it is checked as an invariant *only* in the serializable configuration -- and
\* the snapshot-isolation configuration exists to show it actually occurs there,
\* because a property that forbids something unreachable forbids nothing.
NoWriteSkew ==
    \A t1, t2 \in Txn :
        (/\ t1 # t2
         /\ status[t1] = Committed
         /\ status[t2] = Committed
         /\ readSet[t1] \cap writeSet[t2] # {}
         /\ readSet[t2] \cap writeSet[t1] # {}
         /\ startTs[t1] < commitTs[t2]
         /\ startTs[t2] < commitTs[t1])
        => FALSE

\* The witness for the paragraph above: TRUE once a write skew exists. Checked
\* as an invariant in the snapshot-isolation configuration, where it is
\* *expected to fail* -- the failure is the evidence that NoWriteSkew is not
\* vacuous when it holds at serializable.
NoWriteSkewWitness == NoWriteSkew

Safety ==
    /\ KeyListIsComplete
    /\ CommitIsAllOrNothing
    /\ AbortLeavesNothing
    /\ VersionsCarryCommitTs
    /\ SnapshotIsolation

----------------------------------------------------------------------------

StateConstraint == nextTs <= MaxTs

TypeOk ==
    /\ nextTs \in Nat
    /\ status \in [Txn -> {NoRecord, Pending, Staging, Committed, Aborted}]
    /\ startTs \in [Txn -> Nat]
    /\ commitTs \in [Txn -> Nat]

=============================================================================
