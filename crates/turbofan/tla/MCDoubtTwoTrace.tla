-------------------------- MODULE MCDoubtTwoTrace --------------------------
(* One run of MCDoubtTwo, step by step, where its search can't reach (26 steps): each step   *)
(* one of Raft's own actions, taken only in its turn. s1 leads term 1 and commits an entry *)
(* on s1, s2 and s3; s2 and s3 tear it off and doubt; s4, which never had it, stands in    *)
(* term 2 and gets s2, s3 and s5. Four votes of five, two in doubt: the rule needs all     *)
(* five, so s4 loses and the run ends. With one vote more for any doubt, not one each, s4  *)
(* wins without the entry, and LeaderCompleteness fails. A step that can no longer be       *)
(* taken stops the run short: TLC reports a deadlock, with the step in `pc`.               *)
EXTENDS MCRaft

CONSTANTS s1, s2, s3, s4, s5

VARIABLE pc

\* The message of type `t` from `f` to `to`, delivered once.
Deliver(t, f, to) ==
    \E m \in msgs :
        /\ m.type = t /\ m.from = f /\ m.to = to
        /\ LET rest == msgs \ {m}
           IN \/ HandleRequestVote(to, m, rest) \/ HandleVote(to, m, rest)
              \/ HandleAppendEntries(to, m, rest) \/ HandleAppendAck(to, m, rest)

Last == 26

Step ==
    CASE pc = 1  -> StartElection(s1)                    \* term 1
      [] pc = 2  -> Deliver("RequestVote", s1, s2)
      [] pc = 3  -> Deliver("RequestVote", s1, s3)
      [] pc = 4  -> Deliver("RequestVote", s1, s4)        \* s4 into term 1
      [] pc = 5  -> Deliver("Vote", s2, s1)
      [] pc = 6  -> Deliver("Vote", s3, s1)
      [] pc = 7  -> Deliver("Vote", s4, s1)
      [] pc = 8  -> BecomeLeader(s1)
      [] pc = 9  -> ClientRequest(s1)
      [] pc = 10 -> AppendEntries(s1, s2)
      [] pc = 11 -> AppendEntries(s1, s3)
      [] pc = 12 -> Deliver("AppendEntries", s1, s2)
      [] pc = 13 -> Deliver("AppendEntries", s1, s3)
      [] pc = 14 -> Deliver("AppendAck", s2, s1)
      [] pc = 15 -> Deliver("AppendAck", s3, s1)
      [] pc = 16 -> AdvanceCommit(s1)                     \* on s1, s2 and s3
      [] pc = 17 -> TornCrash(s2)                         \* s1 still holds it
      [] pc = 18 -> TornCrash(s3)
      [] pc = 19 -> StartElection(s4)                    \* term 2
      [] pc = 20 -> Deliver("RequestVote", s4, s2)
      [] pc = 21 -> Deliver("RequestVote", s4, s3)
      [] pc = 22 -> Deliver("RequestVote", s4, s5)
      [] pc = 23 -> Deliver("Vote", s2, s4)               \* in doubt
      [] pc = 24 -> Deliver("Vote", s3, s4)               \* in doubt
      [] pc = 25 -> Deliver("Vote", s5, s4)
      [] pc = 26 -> IF Won(s4) THEN BecomeLeader(s4) ELSE UNCHANGED vars

\* Each step in turn, then the end, which isn't a deadlock.
TraceNext ==
    IF pc > Last
    THEN UNCHANGED <<vars, pc>>
    ELSE Step /\ pc' = pc + 1

TraceSpec == Init /\ pc = 1 /\ [][TraceNext]_<<vars, pc>>
=============================================================================
