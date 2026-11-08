---------------------------- MODULE MCRaft ----------------------------
(* Raft, bounded for TLC: the servers interchangeable where they are, and at most MaxMsgs *)
(* messages in flight at once.                                                            *)
EXTENDS Raft, TLC

CONSTANT MaxMsgs

Symmetry == Permutations(Server)

\* The members a cluster starts with are interchangeable, not one that joins.
InitialSymmetry == Permutations(Initial)

Bounded == Cardinality(msgs) <= MaxMsgs
=============================================================================
