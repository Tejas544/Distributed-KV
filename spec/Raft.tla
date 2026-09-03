-------------------------------- MODULE Raft --------------------------------
(***************************************************************************)
(* Raft, as implemented in anvil/core/raft/raft.cc, including joint         *)
(* consensus.                                                              *)
(*                                                                         *)
(* This is a specification of the *safety* argument, not a transcription of *)
(* the code. The distinction matters and is worth stating before the first  *)
(* operator: a spec that mirrors an implementation line by line proves      *)
(* nothing, because it inherits the implementation's mistakes. What is      *)
(* modelled here is what the protocol is *allowed* to do -- who may vote,   *)
(* who may become leader, what a leader may declare committed -- and the    *)
(* implementation is compared against it separately, by trace validation    *)
(* (TraceRaft.tla).                                                        *)
(*                                                                         *)
(* Deliberately included, because these are where the bugs were:           *)
(*                                                                         *)
(*   The Figure-8 restriction.  A leader may advance commitIndex only onto  *)
(*      an entry of its own term. In the state-space search over the        *)
(*      implementation this is INV-RAFT-10, and switching it off there is   *)
(*      caught in 4,131 states.                                            *)
(*   Joint consensus.  A quorum during a configuration change is a majority *)
(*      of *both* the outgoing and the incoming voter sets. ANV-0013 was    *)
(*      exactly this arithmetic done as (n-1)/2 rather than n/2, which      *)
(*      agrees for odd n -- every hand-written test -- and differs for even *)
(*      n, which is every joint transition.                                *)
(*   Learners.  Non-voting members that count toward no quorum at all.      *)
(*                                                                         *)
(* A configuration is a *log entry*, and each server's configuration is     *)
(* derived from its own log. That is not a detail. The first version of     *)
(* this spec made the configuration a global variable that any leader could *)
(* change atomically, and TLC immediately produced two leaders in one term  *)
(* -- one leader had shrunk the cluster out from under the other. The       *)
(* violation was in the specification rather than in Raft, and it is        *)
(* recorded here because it is the same class of mistake the ledger keeps   *)
(* finding in checkers: the model was wrong in a way that looked exactly    *)
(* like the system being wrong.                                            *)
(*                                                                         *)
(* Deliberately excluded, each a scope statement rather than an oversight:  *)
(* log compaction and snapshots (a performance mechanism; they do not       *)
(* change what may be committed), pre-vote and CheckQuorum (both only       *)
(* restrict when an election may *start*, so omitting them gives this spec  *)
(* strictly more behaviours than the implementation has -- the safe         *)
(* direction), leases and ReadIndex (about reads, and this spec has none),  *)
(* and the boundary between "written" and "fsynced" (the implementation's   *)
(* Ready loop makes the two atomic from the protocol's point of view --     *)
(* CONTEXT.md gotcha 10.8).                                                *)
(***************************************************************************)

EXTENDS Naturals, FiniteSets, Sequences, TLC

CONSTANTS Server,          \* the set of all servers
          Nil,             \* "no vote"; a model value, distinct from every server
          Value,           \* the values a client may propose
          InitialVoters,   \* the configuration the cluster is bootstrapped with
          Learners,        \* non-voting members; counted in no quorum
          ConfigChoices,   \* the voter sets a configuration change may target
          MaxTerm,         \* bound: highest term TLC will explore
          MaxLogLen,       \* bound: longest log TLC will explore
          MaxMessages,     \* bound: messages in flight
          \* The two deliberate-bug knobs, mirroring RaftOptions in
          \* anvil/core/raft/types.h. Both default to TRUE in Raft.cfg; the
          \* .cfg files that set one of them FALSE are negative controls, and
          \* they exist because a specification only ever observed to pass is
          \* indistinguishable from one whose properties are vacuous. Each is
          \* required to produce a *named* violation, not merely some
          \* violation.
          CommitOnlyCurrentTerm,
          JointRequiresCommit,
          \* Scope knobs, for the constructed regions in RaftFigure8.tla. Both
          \* are permissive in Raft.cfg (heartbeats on, everybody may
          \* campaign) and restricted only where the point of the run is a
          \* discrimination inside a region rather than coverage of a whole
          \* state space. Every behaviour they remove is a behaviour, so a run
          \* with them restricted proves less -- which is why they are named
          \* here rather than achieved by commenting an action out.
          EnableHeartbeat,
          Campaigners

\* Server states.
CONSTANTS Follower, Candidate, Leader

\* Message types.
CONSTANTS RequestVoteRequest, RequestVoteResponse,
          AppendEntriesRequest, AppendEntriesResponse

----------------------------------------------------------------------------
(***************************************************************************)
(* Variables                                                               *)
(***************************************************************************)

VARIABLES
    messages,      \* set of messages in flight. A *set*, so duplication and
                   \* reordering are free and loss is modelled by simply never
                   \* taking the receive step -- the three network faults the
                   \* simulator injects, expressed by leaving something out
                   \* rather than by adding machinery.
    currentTerm,
    state,
    votedFor,
    log,           \* Seq of entries; see ValueEntry / ConfigEntry
    commitIndex,
    votesGranted,  \* [Server -> SUBSET Server]: who has granted this term
    \* History variables. Not part of the implementation; they exist so the
    \* properties below can be stated at all. TLC treats them as state, which
    \* is why they are bounded along with everything else.
    elections,     \* every election that has ever succeeded
    committedLog,  \* every (index, entry, term-it-was-committed-in)
    commitRuleBroken  \* has any leader ever advanced onto a previous term?

serverVars  == <<currentTerm, state, votedFor, votesGranted>>
logVars     == <<log, commitIndex>>
historyVars == <<elections, committedLog, commitRuleBroken>>
vars        == <<messages, serverVars, logVars, historyVars>>

----------------------------------------------------------------------------
(***************************************************************************)
(* Entries, and the configuration derived from them                        *)
(***************************************************************************)

ValueEntry(t, v) == [term |-> t, kind |-> "val", value |-> v]

ConfigEntry(t, cin, cout) ==
    [term |-> t, kind |-> "cfg", cin |-> cin, cout |-> cout]

SimpleConfig(voters) == [cin |-> voters, cout |-> {}]

\* A server's configuration is the newest configuration entry in its own log,
\* in force as soon as it is *appended* -- not when it is committed. That is
\* Raft as specified and it is what makes joint consensus necessary: a server
\* can be operating under a configuration that is later truncated away, and
\* deriving the configuration from the log rather than storing it separately
\* means the truncation takes the configuration with it, for free.
ConfigOf(xlog) ==
    LET idxs == {i \in 1..Len(xlog) : xlog[i].kind = "cfg"}
    IN IF idxs = {}
       THEN SimpleConfig(InitialVoters)
       ELSE LET i == CHOOSE i \in idxs : \A j \in idxs : j <= i
            IN [cin |-> xlog[i].cin, cout |-> xlog[i].cout]

\* The index of the newest configuration entry, or 0. Used by JointFinish to
\* ask whether the joint configuration itself has committed yet.
ConfigIndexOf(xlog) ==
    LET idxs == {i \in 1..Len(xlog) : xlog[i].kind = "cfg"}
    IN IF idxs = {} THEN 0 ELSE CHOOSE i \in idxs : \A j \in idxs : j <= i

----------------------------------------------------------------------------
(***************************************************************************)
(* Quorums                                                                 *)
(*                                                                         *)
(* The whole of ANV-0013 lives in these definitions. A quorum of a set is a *)
(* strict majority of it; a quorum of a *joint* configuration is a set that *)
(* is simultaneously a majority of the outgoing voters and a majority of    *)
(* the incoming ones. Learners appear in neither, which is INV-RAFT-15.     *)
(***************************************************************************)

IsMajority(sub, whole) ==
    /\ Cardinality(sub \cap whole) * 2 > Cardinality(whole)

IsJoint(cfg) == cfg.cout # {}

VotersOf(cfg) == cfg.cin \cup cfg.cout

IsQuorumOf(cfg, sub) ==
    IF IsJoint(cfg)
    THEN /\ IsMajority(sub, cfg.cin)
         /\ IsMajority(sub, cfg.cout)
    ELSE IsMajority(sub, cfg.cin)

----------------------------------------------------------------------------
(***************************************************************************)
(* Log helpers                                                             *)
(***************************************************************************)

LastTerm(xlog) == IF Len(xlog) = 0 THEN 0 ELSE xlog[Len(xlog)].term

\* The up-to-date test a voter applies. The deliberate-bug knob
\* restrict_vote_by_log switches this off in the implementation, and the
\* state-space search catches it as a LeaderCompleteness violation.
LogIsUpToDate(candLastTerm, candLastIndex, voter) ==
    \/ candLastTerm > LastTerm(log[voter])
    \/ /\ candLastTerm = LastTerm(log[voter])
       /\ candLastIndex >= Len(log[voter])

\* A handled message is *removed*, and its reply takes its place.
\*
\* The first version of this spec left handled messages in the set, on the
\* reasoning that a set already models duplication and reordering for free.
\* That is true and it made MaxMessages meaningless: with nothing ever
\* removed, "at most N messages" bounds the total a behaviour may ever send
\* rather than the number in flight, so the constraint cut every behaviour off
\* after a handful of steps. The Figure-8 control found it, by being unable to
\* reach a commit that was plainly reachable -- both settings of the knob
\* returned byte-identical state counts, which is the signature of a
\* constraint biting before the interesting action ever becomes enabled.
\*
\* Duplication is still free: a message is removed only by the step that
\* handles it, and nothing forces that step to happen at all, which is loss.
Send(m)       == messages' = messages \cup {m}
Reply(rsp, m) == messages' = (messages \ {m}) \cup {rsp}
Discard(m)    == messages' = messages \ {m}

----------------------------------------------------------------------------
(***************************************************************************)
(* Initial state                                                           *)
(***************************************************************************)

Init ==
    /\ messages = {}
    /\ currentTerm = [s \in Server |-> 1]
    /\ state = [s \in Server |-> Follower]
    /\ votedFor = [s \in Server |-> Nil]
    /\ log = [s \in Server |-> <<>>]
    /\ commitIndex = [s \in Server |-> 0]
    /\ votesGranted = [s \in Server |-> {}]
    /\ elections = {}
    /\ committedLog = {}
    /\ commitRuleBroken = FALSE

----------------------------------------------------------------------------
(***************************************************************************)
(* Actions                                                                 *)
(***************************************************************************)

\* A server times out and starts an election. Pre-vote and CheckQuorum are not
\* modelled: both only restrict when this may happen, so leaving them out
\* gives the spec strictly more behaviours than the implementation has, which
\* is the safe direction for a safety argument.
Timeout(s) ==
    /\ s \in Campaigners
    /\ s \in VotersOf(ConfigOf(log[s]))
    /\ state[s] \in {Follower, Candidate}
    /\ currentTerm[s] < MaxTerm
    /\ state' = [state EXCEPT ![s] = Candidate]
    /\ currentTerm' = [currentTerm EXCEPT ![s] = currentTerm[s] + 1]
    /\ votedFor' = [votedFor EXCEPT ![s] = s]
    /\ votesGranted' = [votesGranted EXCEPT ![s] = {s}]
    /\ UNCHANGED <<messages, logVars, historyVars>>

RequestVote(s, t) ==
    /\ state[s] = Candidate
    /\ s # t
    /\ t \in VotersOf(ConfigOf(log[s]))
    /\ Send([mtype      |-> RequestVoteRequest,
             mterm      |-> currentTerm[s],
             mlastTerm  |-> LastTerm(log[s]),
             mlastIndex |-> Len(log[s]),
             msource    |-> s,
             mdest      |-> t])
    /\ UNCHANGED <<serverVars, logVars, historyVars>>

\* A voter grants at most one vote per term, and only to a candidate whose log
\* is at least as up to date as its own.
HandleRequestVote(m) ==
    LET s == m.mdest
        t == m.msource
        grant == /\ m.mterm = currentTerm[s]
                 /\ votedFor[s] \in {Nil, t}
                 /\ LogIsUpToDate(m.mlastTerm, m.mlastIndex, s)
    IN /\ m.mtype = RequestVoteRequest
       /\ votedFor' = IF grant THEN [votedFor EXCEPT ![s] = t] ELSE votedFor
       /\ Reply([mtype    |-> RequestVoteResponse,
                 mterm    |-> currentTerm[s],
                 mgranted |-> grant,
                 msource  |-> s,
                 mdest    |-> t], m)
       /\ UNCHANGED <<currentTerm, state, votesGranted, logVars, historyVars>>

\* A candidate that has collected a quorum of grants becomes leader. The grants
\* are read out of the message set rather than accumulated in a variable: it
\* says the same thing with one less variable, and the state space is the
\* reason to care.
HandleRequestVoteResponse(m) ==
    LET s == m.mdest IN
      /\ m.mtype = RequestVoteResponse
      /\ votesGranted' =
             IF m.mgranted /\ state[s] = Candidate
             THEN [votesGranted EXCEPT ![s] = votesGranted[s] \cup {m.msource}]
             ELSE votesGranted
      /\ Discard(m)
      /\ UNCHANGED <<currentTerm, state, votedFor, logVars, historyVars>>

BecomeLeader(s) ==
    /\ state[s] = Candidate
    /\ IsQuorumOf(ConfigOf(log[s]), votesGranted[s])
    /\ state' = [state EXCEPT ![s] = Leader]
    /\ elections' = elections \cup
           {[eterm |-> currentTerm[s], eleader |-> s, elog |-> log[s],
             evoters |-> votesGranted[s]]}
    /\ UNCHANGED <<messages, currentTerm, votedFor, votesGranted, logVars,
                   committedLog, commitRuleBroken>>

\* A candidate abandons its election.
\*
\* The implementation does this when a quorum has *rejected* it -- see
\* step_vote_reply, where the comment notes that waiting for the election
\* timeout would also work but stepping down immediately shortens every
\* split-vote round by a full timeout. Modelling the reason would need a
\* votesRejected variable that nothing else in this specification uses, so the
\* action is allowed unconditionally instead.
\*
\* Unconditionally is the safe direction, and the direction matters. A
\* specification used as a refinement target has to permit everything the
\* implementation does; permitting *more* costs only that the specification
\* proves a slightly weaker theorem, while permitting less makes a correct
\* implementation look non-conforming. And a candidate that gives up does
\* strictly less than one that carries on, so this cannot introduce a safety
\* violation. It was added because trace validation found a real run stalling
\* here, which is the point of trace validation.
StepDown(s) ==
    /\ state[s] = Candidate
    /\ state' = [state EXCEPT ![s] = Follower]
    /\ UNCHANGED <<messages, currentTerm, votedFor, votesGranted, logVars, historyVars>>

ClientRequest(s, v) ==
    /\ state[s] = Leader
    /\ Len(log[s]) < MaxLogLen
    /\ log' = [log EXCEPT ![s] = Append(log[s], ValueEntry(currentTerm[s], v))]
    /\ UNCHANGED <<messages, serverVars, commitIndex, historyVars>>

\* One entry per message, which is the implementation's max_entries_per_append
\* set to 1 -- and that is not a simplification for the spec's convenience. It
\* is the configuration under which the Figure-8 window is reachable at all
\* (test/dpor_test.cc explains why at more length), so a spec that shipped
\* whole tails would be unable to express the very behaviour the restriction
\* exists to forbid.
AppendEntries(s, t) ==
    /\ state[s] = Leader
    /\ s # t
    /\ t \in (VotersOf(ConfigOf(log[s])) \cup Learners)
    /\ \E i \in 1..Len(log[s]) :
          Send([mtype      |-> AppendEntriesRequest,
                mterm      |-> currentTerm[s],
                mprevIndex |-> i - 1,
                mprevTerm  |-> IF i = 1 THEN 0 ELSE log[s][i - 1].term,
                mentry     |-> log[s][i],
                mindex     |-> i,
                mcommit    |-> commitIndex[s],
                msource    |-> s,
                mdest      |-> t])
    /\ UNCHANGED <<serverVars, logVars, historyVars>>

\* Heartbeat: carries the commit index and no entry. Needed so a follower can
\* learn of a commit without a new entry arriving, which is what makes
\* StateMachineSafety a property about more than one server.
Heartbeat(s, t) ==
    /\ EnableHeartbeat
    /\ state[s] = Leader
    /\ s # t
    /\ t \in (VotersOf(ConfigOf(log[s])) \cup Learners)
    /\ Send([mtype      |-> AppendEntriesRequest,
             mterm      |-> currentTerm[s],
             mprevIndex |-> Len(log[s]),
             mprevTerm  |-> LastTerm(log[s]),
             mentry     |-> ValueEntry(0, CHOOSE v \in Value : TRUE),
             mindex     |-> 0,
             mcommit    |-> commitIndex[s],
             msource    |-> s,
             mdest      |-> t])
    /\ UNCHANGED <<serverVars, logVars, historyVars>>

\* The consistency check. The knob check_prev_term_on_append switches it off in
\* the implementation, and the state-space search catches that as a LogMatching
\* violation.
LogOk(s, m) ==
    \/ m.mprevIndex = 0
    \/ /\ m.mprevIndex > 0
       /\ m.mprevIndex <= Len(log[s])
       /\ m.mprevTerm = log[s][m.mprevIndex].term

Min2(a, b) == IF a < b THEN a ELSE b
Max2(a, b) == IF a > b THEN a ELSE b

HandleAppendEntries(m) ==
    LET s  == m.mdest
        ok == LogOk(s, m)
        \* Truncate anything from the new entry's index onward that disagrees,
        \* then append. Writing it as "truncate then append" rather than
        \* "overwrite in place" is deliberate: the two differ exactly when the
        \* follower holds entries beyond the one being written, which is the
        \* case that matters.
        newLog ==
            IF ~ok \/ m.mindex = 0
            THEN log[s]
            ELSE IF /\ m.mindex <= Len(log[s])
                    /\ log[s][m.mindex].term = m.mentry.term
                 THEN log[s]
                 ELSE Append(SubSeq(log[s], 1, m.mprevIndex), m.mentry)
        \* A follower may only advance its commit index over the prefix *this
        \* message verified* -- min(leaderCommit, index of the last new entry) --
        \* and not over its whole log.
        \*
        \* The difference is not cosmetic and TLC found it. Clamping to
        \* Len(newLog) lets a follower that still holds a divergent tail from an
        \* earlier leader record those entries as committed: the message checked
        \* the log only up to mprevIndex, so everything above that is
        \* unverified, and the follower ends up claiming it committed an entry
        \* the leader has never had. StateMachineSafety fails, and it looks
        \* exactly like a protocol bug rather than a specification bug.
        verified == IF m.mindex = 0 THEN m.mprevIndex ELSE m.mindex
        newCommit ==
            IF ok THEN Max2(commitIndex[s], Min2(m.mcommit, Min2(verified, Len(newLog))))
                  ELSE commitIndex[s]
    IN /\ m.mtype = AppendEntriesRequest
       /\ state' = [state EXCEPT ![s] = IF state[s] = Candidate THEN Follower ELSE state[s]]
       /\ log' = [log EXCEPT ![s] = newLog]
       /\ commitIndex' = [commitIndex EXCEPT ![s] = newCommit]
       \* Only what this step newly commits. committedLog records commit
       \* *decisions*, and re-adding an index that was already committed --
       \* tagged with whatever term the message carried -- invents a decision
       \* nobody made. It also makes LeaderCompleteness weaker than it should
       \* be, because the entry then also appears under a later term.
       /\ committedLog' = committedLog \cup
              {[cindex |-> i, centry |-> newLog[i], cterm |-> m.mterm] :
                  i \in (commitIndex[s] + 1)..newCommit}
       /\ UNCHANGED commitRuleBroken
       /\ Reply([mtype    |-> AppendEntriesResponse,
                 mterm    |-> currentTerm[s],
                 msuccess |-> ok,
                 \* Len(newLog) either way, which for a rejection is the
                 \* follower's own log length. The implementation answers a
                 \* rejection with exactly that (step_append sets
                 \* reply.match = log_.last_index()), and nothing in this
                 \* specification reads mmatch -- AdvanceCommitIndex reads the
                 \* logs directly -- so this is wire-field alignment and not a
                 \* change of meaning. Trace validation is what noticed.
                 mmatch   |-> Len(newLog),
                 msource  |-> s,
                 mdest    |-> m.msource], m)
       /\ UNCHANGED <<currentTerm, votedFor, votesGranted, elections>>

\* The commit rule, and the single most important conjunct in the file.
\*
\* `log[s][idx].term = currentTerm[s]` is the Figure-8 restriction. Removing it
\* leaves a spec that still satisfies ElectionSafety and LogMatching and
\* violates LeaderCompleteness -- which is exactly the shape of the finding the
\* state-space search reports when commit_only_current_term is switched off in
\* the implementation.
AdvanceCommitIndex(s) ==
    /\ state[s] = Leader
    /\ \E idx \in (commitIndex[s] + 1)..Len(log[s]) :
          LET agreed == {t \in Server :
                            /\ Len(log[t]) >= idx
                            /\ log[t][idx].term = log[s][idx].term}
          IN /\ IsQuorumOf(ConfigOf(log[s]), agreed \cup {s})
             /\ (CommitOnlyCurrentTerm => log[s][idx].term = currentTerm[s])
             /\ commitIndex' = [commitIndex EXCEPT ![s] = idx]
             \* Same as in HandleAppendEntries: only the newly committed range.
             /\ committedLog' = committedLog \cup
                    {[cindex |-> i, centry |-> log[s][i], cterm |-> currentTerm[s]] :
                        i \in (commitIndex[s] + 1)..idx}
             \* INV-RAFT-10, recorded rather than assumed. With
             \* CommitOnlyCurrentTerm this can never be set; the negative
             \* control switches the guard off and this is what fires.
             \* Parenthesised, and not for taste: `=` binds tighter than `\/`
             \* in TLA+, so `x' = a \/ b` parses as `(x' = a) \/ b` -- which is
             \* satisfied by b alone, leaving x' unassigned. TLC catches it as
             \* "successor state is not completely specified", which is a much
             \* better error than the silent one it would be in most languages.
             /\ commitRuleBroken' =
                    (commitRuleBroken \/ (log[s][idx].term # currentTerm[s]))
    /\ UNCHANGED <<messages, serverVars, log, elections>>

\* Anything carrying a higher term makes the receiver a follower at that term.
UpdateTerm(m) ==
    /\ m.mterm > currentTerm[m.mdest]
    /\ currentTerm' = [currentTerm EXCEPT ![m.mdest] = m.mterm]
    /\ state' = [state EXCEPT ![m.mdest] = Follower]
    /\ votedFor' = [votedFor EXCEPT ![m.mdest] = Nil]
    /\ votesGranted' = [votesGranted EXCEPT ![m.mdest] = {}]
    \* The message is deliberately *not* discarded: the receiver steps down to
    \* the new term and then handles it there, which is what the
    \* implementation does and is the only way a vote request that carries a
    \* higher term can ever be granted.
    /\ UNCHANGED <<messages, logVars, historyVars>>

\* A message from a stale term is ignored. Modelled as a drop, because the only
\* effect of the reply is an UpdateTerm the sender can reach by other means.
DropStale(m) ==
    /\ m.mterm < currentTerm[m.mdest]
    /\ messages' = messages \ {m}
    /\ UNCHANGED <<serverVars, logVars, historyVars>>

Receive(m) ==
    \/ UpdateTerm(m)
    \/ DropStale(m)
    \/ /\ m.mterm = currentTerm[m.mdest]
       /\ \/ HandleRequestVote(m)
          \/ HandleAppendEntries(m)
          \/ HandleRequestVoteResponse(m)
          \* An append response needs no state: AdvanceCommitIndex reads the
          \* logs directly rather than a match index, which is the same
          \* statement with one less variable to keep consistent.
          \/ /\ m.mtype = AppendEntriesResponse
             /\ Discard(m)
             /\ UNCHANGED <<serverVars, logVars, historyVars>>

----------------------------------------------------------------------------
(***************************************************************************)
(* Joint consensus                                                         *)
(*                                                                         *)
(* Both halves are ordinary log entries proposed by the leader, which is    *)
(* what makes them safe: a configuration a server is operating under can be *)
(* truncated away by a later leader, and because ConfigOf derives the       *)
(* configuration from the log, the truncation reverts it automatically.     *)
(*                                                                         *)
(* JointFinish requires the joint entry to be *committed*, which is the     *)
(* deliberate-bug knob joint_requires_commit. Without it, C_new alone       *)
(* becomes sufficient while a majority of C_old still believes it is in     *)
(* charge, and two leaders in one term follow.                             *)
(***************************************************************************)

JointBegin(s, newVoters) ==
    /\ state[s] = Leader
    /\ Len(log[s]) < MaxLogLen
    /\ LET cfg == ConfigOf(log[s]) IN
         /\ ~IsJoint(cfg)
         /\ newVoters # cfg.cin
         /\ log' = [log EXCEPT ![s] =
                Append(log[s], ConfigEntry(currentTerm[s], newVoters, cfg.cin))]
    /\ UNCHANGED <<messages, serverVars, commitIndex, historyVars>>

JointFinish(s) ==
    /\ state[s] = Leader
    /\ Len(log[s]) < MaxLogLen
    /\ LET cfg == ConfigOf(log[s]) IN
         /\ IsJoint(cfg)
         /\ (JointRequiresCommit => ConfigIndexOf(log[s]) <= commitIndex[s])
         /\ log' = [log EXCEPT ![s] =
                Append(log[s], ConfigEntry(currentTerm[s], cfg.cin, {}))]
    /\ UNCHANGED <<messages, serverVars, commitIndex, historyVars>>

----------------------------------------------------------------------------

Next ==
    \/ \E s \in Server : Timeout(s)
    \/ \E s, t \in Server : RequestVote(s, t)
    \/ \E s \in Server : BecomeLeader(s)
    \/ \E s \in Server : StepDown(s)
    \/ \E s \in Server, v \in Value : ClientRequest(s, v)
    \/ \E s, t \in Server : AppendEntries(s, t)
    \/ \E s, t \in Server : Heartbeat(s, t)
    \/ \E s \in Server : AdvanceCommitIndex(s)
    \/ \E m \in messages : Receive(m)
    \/ \E s \in Server, c \in ConfigChoices : JointBegin(s, c)
    \/ \E s \in Server : JointFinish(s)

Spec == Init /\ [][Next]_vars

----------------------------------------------------------------------------
(***************************************************************************)
(* Properties                                                              *)
(*                                                                         *)
(* The same properties the state-space search over the implementation       *)
(* evaluates, under the same names, so that "TLC found no violation" and    *)
(* "the search found no violation" are statements about the same things     *)
(* rather than two different sets of words.                                *)
(***************************************************************************)

\* INV-RAFT-01: at most one leader per term.
ElectionSafety ==
    \A e1, e2 \in elections :
        (e1.eterm = e2.eterm) => (e1.eleader = e2.eleader)

\* INV-RAFT-03: two logs agreeing at an index and term agree on the whole
\* prefix.
LogMatching ==
    \A s, t \in Server :
        \A i \in 1..Len(log[s]) :
            (/\ i <= Len(log[t])
             /\ log[s][i].term = log[t][i].term)
            => \A j \in 1..i : log[s][j] = log[t][j]

\* INV-RAFT-09: a leader holds every entry committed in an earlier term.
\* The guard is `c.cterm`, the term the commit *decision* was made in, and not
\* `c.centry.term`, the term the entry was created in. The two differ whenever
\* an old entry becomes committed under a later leader, which is exactly the
\* Figure-8 situation, and using the entry's term states a property Raft does
\* not have: it would require a leader of term 3 to hold an entry that nobody
\* committed until term 4. TLC found that within five minutes of the property
\* being written down, which is the argument for running the specification
\* rather than admiring it.
LeaderCompleteness ==
    \A e \in elections :
        \A c \in committedLog :
            (c.cterm < e.eterm)
            => /\ c.cindex <= Len(e.elog)
               /\ e.elog[c.cindex] = c.centry

\* INV-RAFT-02: no index is ever committed carrying two different entries.
StateMachineSafety ==
    \A c1, c2 \in committedLog :
        (c1.cindex = c2.cindex) => (c1.centry = c2.centry)

\* INV-RAFT-06: a server never commits past the end of its own log.
CommitIsGrounded ==
    \A s \in Server : commitIndex[s] <= Len(log[s])

\* INV-RAFT-15: learners are counted in no quorum, in any configuration any
\* server currently holds. Stated over the configurations rather than over any
\* particular election, because the property is about the arithmetic.
LearnersAreNotVoters ==
    \A s \in Server : Learners \cap VotersOf(ConfigOf(log[s])) = {}

\* INV-RAFT-04: a server casts at most one vote per term. Expressed as "the
\* recorded vote never changes while the term does not", which is what the
\* variable can carry.
VoteIsRecorded ==
    \A s \in Server : votedFor[s] \in (Server \cup {Nil})

\* INV-RAFT-10: a leader counts replicas only for entries of its own term. The
\* rule rather than its consequence, and deliberately so -- the consequence
\* (a later leader missing a committed entry) needs five voters and two more
\* terms to be reachable, while the rule is violated immediately. Same choice,
\* and the same reasoning, as the state-space search over the implementation.
Figure8Rule == ~commitRuleBroken

Safety ==
    /\ Figure8Rule
    /\ ElectionSafety
    /\ LogMatching
    /\ LeaderCompleteness
    /\ StateMachineSafety
    /\ CommitIsGrounded
    /\ LearnersAreNotVoters

----------------------------------------------------------------------------
(***************************************************************************)
(* Bounds                                                                  *)
(*                                                                         *)
(* The state space is infinite without these -- terms and logs both grow    *)
(* without limit -- so TLC is given a finite slice, and the slice is stated *)
(* in the .cfg rather than buried here. A model-checking result is a        *)
(* statement about the slice and nothing more, and pretending otherwise is  *)
(* the most common way a TLA+ claim turns out to be worth less than it      *)
(* sounded.                                                                *)
(***************************************************************************)

\* Symmetry for TLC. Permuting the servers maps a behaviour to an equivalent
\* one, so only one representative of each orbit needs exploring -- worth about
\* a factor of six at three servers, which is the difference between this slice
\* finishing and not. Sound only where no constant names a particular server,
\* which is why Raft.cfg declares it and RaftJoint.cfg does not.
ServerSymmetry == Permutations(Server)

StateConstraint ==
    /\ \A s \in Server : currentTerm[s] <= MaxTerm
    /\ \A s \in Server : Len(log[s]) <= MaxLogLen
    /\ Cardinality(messages) <= MaxMessages

TypeOk ==
    /\ currentTerm \in [Server -> Nat]
    /\ state \in [Server -> {Follower, Candidate, Leader}]
    /\ commitIndex \in [Server -> Nat]
    /\ votedFor \in [Server -> Server \cup {Nil}]
    /\ votesGranted \in [Server -> SUBSET Server]

=============================================================================
