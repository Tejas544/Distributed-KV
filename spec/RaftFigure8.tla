----------------------------- MODULE RaftFigure8 ----------------------------
(***************************************************************************)
(* The Figure-8 negative control, started from a constructed configuration. *)
(*                                                                         *)
(* Why this module exists rather than another .cfg for Raft.tla:            *)
(*                                                                         *)
(* The Figure-8 scenario needs an entry replicated to a *minority* that a   *)
(* later leader does not hold. With three voters a minority is one node --  *)
(* the leader itself -- so any node that subsequently wins an election has  *)
(* a log at least as long and the entry is never absent from a later        *)
(* leader. The scenario therefore needs five voters, and five voters is     *)
(* past what a search from the initial state finishes: breadth-first was    *)
(* still at depth 14 after ten minutes and 137 million states, and random   *)
(* simulation checked 65 million states without reaching a sequence about   *)
(* twenty steps long in a space with dozens of enabled actions per state.   *)
(*                                                                         *)
(* So the search starts where the interesting part begins. This is the same *)
(* technique test/dpor_test.cc uses for the same bug and for the same       *)
(* reason, and it is the technique P3 used for the tenth of its ten planted *)
(* bugs. The claim it supports is a *discrimination* -- with the            *)
(* restriction removed the region contains a violation, and with it in      *)
(* place the same region does not -- and not a claim of exhaustiveness from *)
(* an initial state.                                                       *)
(*                                                                         *)
(* The constructed state is one a real cluster can be in, and every part of *)
(* it is reachable:                                                        *)
(*                                                                         *)
(*   LeaderOld  holds one entry from term 2, replicated nowhere, uncommitted*)
(*   everyone   is at term 3, having learned it from LeaderNew's election   *)
(*   LeaderNew  won term 3 with an empty log -- legal, because every other  *)
(*              voter's log is empty too, and its own vote plus three       *)
(*              others is a quorum of five                                  *)
(*                                                                         *)
(* From there the protocol does the rest by itself: LeaderOld campaigns for *)
(* term 4, wins on the votes of the empty-log servers, replicates its       *)
(* term-2 entry to a majority, and -- with the restriction removed --       *)
(* declares it committed. LeaderCompleteness then fails against the term-3  *)
(* election already in the history, which is the data loss the restriction  *)
(* exists to prevent.                                                      *)
(***************************************************************************)

EXTENDS Raft

CONSTANTS LeaderOld,   \* the server holding the uncommitted term-2 entry
          LeaderNew,   \* the server that won term 3 without it
          StaleValue   \* the value of that entry

F8Init ==
    /\ messages = {}
    /\ currentTerm = [s \in Server |-> 3]
    /\ state = [s \in Server |-> Follower]
    \* LeaderOld learned term 3 from a message, which clears its vote; everybody
    \* else voted for LeaderNew in that term and still remembers it.
    /\ votedFor = [s \in Server |-> IF s = LeaderOld THEN Nil ELSE LeaderNew]
    /\ log = [s \in Server |->
                 IF s = LeaderOld THEN <<ValueEntry(2, StaleValue)>> ELSE <<>>]
    /\ commitIndex = [s \in Server |-> 0]
    /\ votesGranted = [s \in Server |-> {}]
    /\ elections = {[eterm |-> 3,
                     eleader |-> LeaderNew,
                     elog |-> <<>>,
                     evoters |-> Server \ {LeaderOld}]}
    /\ committedLog = {}
    /\ commitRuleBroken = FALSE

F8Spec == F8Init /\ [][Next]_vars

=============================================================================
