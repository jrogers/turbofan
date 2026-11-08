-------------------------------- MODULE Raft --------------------------------
(***************************************************************************)
(* Turbofan's Raft (`crates/turbofan/src/raft/`), for TLC to check every    *)
(* interleaving of a small cluster: the rules textbook Raft lacks, which    *)
(* the simulation only samples.                                             *)
(*                                                                          *)
(* Doubt (`election.rs`): a node whose log lost a torn tail on restart      *)
(* doubts until it appends an entry of a term after the one it restarted    *)
(* in; its votes say so, and an election needs a vote more than a majority  *)
(* for each voter in doubt, the candidate's own included, but never more    *)
(* than every voter.                                                        *)
(*                                                                          *)
(* Members (`membership.rs`): single-server changes through the log, each   *)
(* taking effect as it's appended and undone if it's truncated; learners,   *)
(* whose votes and acks don't count; a change only once the leader has      *)
(* committed an entry of its term and the last change; a node standing      *)
(* even when it isn't among the members it knows, but not when it knows     *)
(* none, nor as a learner, unless a change it hasn't seen committed made it *)
(* one; a leader that removed itself leading until that's committed.        *)
(*                                                                          *)
(* Pre-vote, check-quorum and leadership transfer only hold elections back: *)
(* a node may start one here whenever it may stand at all, which allows     *)
(* every election they do and more, so what's safe here is safe with them.  *)
(* Likewise a leader may step down at any time. Messages are a set: one     *)
(* delivered is taken out, or left for another delivery, and any may be     *)
(* dropped; so each may be delivered any number of times, in any order, or  *)
(* never.                                                                   *)
(*                                                                          *)
(* A node's log is durable when it acks, as with `--fsync`. A crash keeps   *)
(* it, or tears a suffix off it and leaves the node in doubt: only while    *)
(* every committed entry is still held by some node (else no rule could     *)
(* bring it back, and the doubt rule promises nothing).                     *)
(*                                                                          *)
(* Left out: sessions, services, snapshots, the log window, versions,       *)
(* reads, stopping and promotion.                                           *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS
    Server,      \* every node that may ever be a member
    Initial,     \* the members a new cluster starts with, all voters
    MaxTerm,     \* bounds, for TLC
    MaxLog,
    MaxCrashes,
    MaxTorn,
    MaxChanges,
    Nil

ASSUME Initial \subseteq Server /\ Initial # {}

VARIABLES
    currentTerm, votedFor, role, log, commitIndex,
    votes,       \* a candidate's votes, from voters
    doubted,     \* those of them in doubt
    matchIndex,  \* a leader's: how much of its log each node is known to have
    nextIndex,   \* a leader's: where it sends each node its log from
    doubt,       \* durable: the log lost a torn tail and isn't trusted since
    doubtSince,  \* the term the node restarted in, in doubt
    msgs,
    \* History, for the invariants.
    elected,     \* <<term, leader>>
    committed,   \* [index, term, at]: an entry a leader committed in term `at`
    truncatedCommitted,
    crashes, torn, changes

vars == <<currentTerm, votedFor, role, log, commitIndex, votes, doubted, matchIndex, nextIndex,
          doubt, doubtSince, msgs, elected, committed, truncatedCommitted,
          crashes, torn, changes>>

-----------------------------------------------------------------------------
Max(S) == CHOOSE x \in S : \A y \in S : y <= x
Min(S) == CHOOSE x \in S : \A y \in S : x <= y

NoMembers == [voters |-> {}, learners |-> {}]
Members(c) == c.voters \cup c.learners
Quorum(c) == Cardinality(c.voters) \div 2 + 1

\* A node's members before any `Members` entry: a new cluster's, or none for one joining.
Base(i) == IF i \in Initial THEN [voters |-> Initial, learners |-> {}] ELSE NoMembers

TermAt(l, n) == IF n = 0 THEN 0 ELSE l[n].term
LastTerm(l) == TermAt(l, Len(l))

ConfigIndices(l) == {k \in 1..Len(l) : l[k].isConfig}
ConfigIndex(l) == IF ConfigIndices(l) = {} THEN 0 ELSE Max(ConfigIndices(l))
ConfigAt(i, l, n) ==
    LET at == {k \in ConfigIndices(l) : k <= n}
    IN IF at = {} THEN Base(i) ELSE l[Max(at)].config
\* `Raft::members`: the latest `Members` entry's, in effect as it's appended.
Config(i) == ConfigAt(i, log[i], Len(log[i]))

\* `may_stand`: it knows members, and isn't a learner, unless a change it hasn't seen
\* committed made it one.
MayStand(i) ==
    LET c == Config(i)
        at == ConfigIndex(log[i])
        demoted == at > commitIndex[i] /\ i \in ConfigAt(i, log[i], at - 1).voters
    IN Members(c) # {} /\ (i \notin c.learners \/ demoted)

\* `won`: a majority, and one more for each voter in doubt, but at most every voter.
Won(i) ==
    LET c == Config(i)
        needed == Min({Quorum(c) + Cardinality(doubted[i]), Cardinality(c.voters)})
    IN Cardinality(votes[i]) >= needed

\* `can_change`: one change at a time, each committed, and an entry of this term committed.
CanChange(i) ==
    /\ ConfigIndex(log[i]) <= commitIndex[i]
    /\ commitIndex[i] > 0
    /\ log[i][commitIndex[i]].term = currentTerm[i]

\* One member added, as a voter or a learner; removed, leaving a voter; or its role changed.
NextConfigs(c) ==
    LET others == Server \ Members(c)
    IN {[voters |-> c.voters \cup {s}, learners |-> c.learners] : s \in others}
       \cup {[voters |-> c.voters, learners |-> c.learners \cup {s}] : s \in others}
       \cup {[voters |-> c.voters \ {s}, learners |-> c.learners \ {s}] :
                s \in {x \in Members(c) : c.voters \ {x} # {}}}
       \cup {[voters |-> c.voters \ {s}, learners |-> c.learners \cup {s}] :
                s \in IF Cardinality(c.voters) > 1 THEN c.voters ELSE {}}
       \cup {[voters |-> c.voters \cup {s}, learners |-> c.learners \ {s}] : s \in c.learners}

\* The first index of the run of entries of `n`'s term.
FirstOfTerm(l, n) == Min({k \in 1..n : \A x \in k..n : l[x].term = l[n].term})

Send(m) == msgs' = msgs \cup {m}

\* A handler's reply, into `rest`: the messages but the one it handled, or all of them if that
\* one's left to be delivered again.
Reply(rest, m) == msgs' = rest \cup {m}

\* `become_follower`, in `term`.
StepDownTo(i, term) ==
    /\ currentTerm' = [currentTerm EXCEPT ![i] = term]
    /\ votedFor' = [votedFor EXCEPT ![i] = IF term > currentTerm[i] THEN Nil ELSE @]
    /\ role' = [role EXCEPT ![i] = "Follower"]
    /\ votes' = [votes EXCEPT ![i] = {}]
    /\ doubted' = [doubted EXCEPT ![i] = {}]

-----------------------------------------------------------------------------
Init ==
    /\ currentTerm = [i \in Server |-> 0]
    /\ votedFor = [i \in Server |-> Nil]
    /\ role = [i \in Server |-> "Follower"]
    /\ log = [i \in Server |-> << >>]
    /\ commitIndex = [i \in Server |-> 0]
    /\ votes = [i \in Server |-> {}]
    /\ doubted = [i \in Server |-> {}]
    /\ matchIndex = [i \in Server |-> [j \in Server |-> 0]]
    /\ nextIndex = [i \in Server |-> [j \in Server |-> 1]]
    /\ doubt = [i \in Server |-> FALSE]
    /\ doubtSince = [i \in Server |-> 0]
    /\ msgs = {}
    /\ elected = {}
    /\ committed = {}
    /\ truncatedCommitted = FALSE
    /\ crashes = 0
    /\ torn = 0
    /\ changes = 0

\* `become_candidate`: its own vote counts if it's a voter, in doubt if it doubts.
StartElection(i) ==
    /\ role[i] \in {"Follower", "Candidate"}
    /\ MayStand(i)
    /\ currentTerm[i] < MaxTerm
    /\ LET t == currentTerm[i] + 1
           own == IF i \in Config(i).voters THEN {i} ELSE {}
       IN /\ currentTerm' = [currentTerm EXCEPT ![i] = t]
          /\ votedFor' = [votedFor EXCEPT ![i] = i]
          /\ role' = [role EXCEPT ![i] = "Candidate"]
          /\ votes' = [votes EXCEPT ![i] = own]
          /\ doubted' = [doubted EXCEPT ![i] = IF doubt[i] THEN own ELSE {}]
          /\ msgs' = msgs \cup
                {[type |-> "RequestVote", term |-> t, from |-> i, to |-> j,
                  lastTerm |-> LastTerm(log[i]), lastIndex |-> Len(log[i])] : j \in Server \ {i}}
    /\ UNCHANGED <<log, commitIndex, matchIndex, nextIndex, doubt, doubtSince, elected, committed,
                   truncatedCommitted, crashes, torn, changes>>

\* `on_request_vote`: one that knows nothing doesn't vote; the vote says whether it doubts.
HandleRequestVote(i, m, rest) ==
    /\ m.type = "RequestVote" /\ m.to = i
    /\ LET newer == m.term > currentTerm[i]
           term == IF newer THEN m.term ELSE currentTerm[i]
           vote == IF newer THEN Nil ELSE votedFor[i]
           knows == Members(Config(i)) # {} \/ Len(log[i]) > 0
           upToDate == \/ m.lastTerm > LastTerm(log[i])
                       \/ m.lastTerm = LastTerm(log[i]) /\ m.lastIndex >= Len(log[i])
           grant == m.term = term /\ vote \in {Nil, m.from} /\ upToDate /\ knows
       IN /\ currentTerm' = [currentTerm EXCEPT ![i] = term]
          /\ votedFor' = [votedFor EXCEPT ![i] = IF grant THEN m.from ELSE vote]
          /\ IF newer
             THEN /\ role' = [role EXCEPT ![i] = "Follower"]
                  /\ votes' = [votes EXCEPT ![i] = {}]
                  /\ doubted' = [doubted EXCEPT ![i] = {}]
             ELSE UNCHANGED <<role, votes, doubted>>
          /\ Reply(rest, [type |-> "Vote", term |-> term, granted |-> grant, doubt |-> doubt[i],
                          from |-> i, to |-> m.from])
    /\ UNCHANGED <<log, commitIndex, matchIndex, nextIndex, doubt, doubtSince, elected, committed,
                   truncatedCommitted, crashes, torn, changes>>

\* `on_vote`: a voter's vote, counted once, and as in doubt if it said so.
HandleVote(i, m, rest) ==
    /\ m.type = "Vote" /\ m.to = i
    /\ IF m.term > currentTerm[i] /\ ~m.granted
       THEN StepDownTo(i, m.term)
       ELSE /\ role[i] = "Candidate" /\ m.term = currentTerm[i] /\ m.granted
            /\ m.from \in Config(i).voters /\ m.from \notin votes[i]
            /\ votes' = [votes EXCEPT ![i] = @ \cup {m.from}]
            /\ doubted' = [doubted EXCEPT ![i] = IF m.doubt THEN @ \cup {m.from} ELSE @]
            /\ UNCHANGED <<currentTerm, votedFor, role>>
    /\ msgs' = rest
    /\ UNCHANGED <<log, commitIndex, matchIndex, nextIndex, doubt, doubtSince, elected, committed,
                   truncatedCommitted, crashes, torn, changes>>

BecomeLeader(i) ==
    /\ role[i] = "Candidate"
    /\ Won(i)
    /\ role' = [role EXCEPT ![i] = "Leader"]
    /\ matchIndex' = [matchIndex EXCEPT ![i] = [j \in Server |-> 0]]
    /\ nextIndex' = [nextIndex EXCEPT ![i] = [j \in Server |-> Len(log[i]) + 1]]
    /\ elected' = elected \cup {<<currentTerm[i], i>>}
    /\ UNCHANGED <<currentTerm, votedFor, log, commitIndex, votes, doubted, doubt, doubtSince,
                   msgs, committed, truncatedCommitted, crashes, torn, changes>>

\* Check-quorum, a leader that removed itself, or any other reason.
StepDown(i) ==
    /\ role[i] = "Leader"
    /\ StepDownTo(i, currentTerm[i])
    /\ UNCHANGED <<log, commitIndex, matchIndex, nextIndex, doubt, doubtSince, msgs, elected, committed,
                   truncatedCommitted, crashes, torn, changes>>

\* The leader appends `entry`, which ends its doubt if its term is later (`trust`).
AppendOwn(i, entry) ==
    /\ log' = [log EXCEPT ![i] = Append(@, entry)]
    /\ doubt' = [doubt EXCEPT ![i] = @ /\ ~(entry.term > doubtSince[i])]

ClientRequest(i) ==
    /\ role[i] = "Leader"
    /\ Len(log[i]) < MaxLog
    /\ AppendOwn(i, [term |-> currentTerm[i], isConfig |-> FALSE, config |-> NoMembers])
    /\ UNCHANGED <<currentTerm, votedFor, role, commitIndex, votes, doubted, matchIndex, nextIndex,
                   doubtSince, msgs, elected, committed, truncatedCommitted, crashes, torn, changes>>

ChangeMembers(i) ==
    /\ role[i] = "Leader"
    /\ CanChange(i)
    /\ changes < MaxChanges
    /\ Len(log[i]) < MaxLog
    /\ \E c \in NextConfigs(Config(i)) :
          AppendOwn(i, [term |-> currentTerm[i], isConfig |-> TRUE, config |-> c])
    /\ changes' = changes + 1
    /\ UNCHANGED <<currentTerm, votedFor, role, commitIndex, votes, doubted, matchIndex, nextIndex,
                   doubtSince, msgs, elected, committed, truncatedCommitted, crashes, torn>>

\* `send_append`: to a member, its log from where it sends it that member, to its end; none, a
\* heartbeat, once the member has it all.
AppendEntries(i, j) ==
    /\ role[i] = "Leader"
    /\ j \in Members(Config(i)) \ {i}
    /\ LET next == Min({nextIndex[i][j], Len(log[i]) + 1})
       IN Send([type |-> "AppendEntries", term |-> currentTerm[i], from |-> i, to |-> j,
                prevIndex |-> next - 1, prevTerm |-> TermAt(log[i], next - 1),
                entries |-> SubSeq(log[i], next, Len(log[i])), commit |-> commitIndex[i]])
    /\ UNCHANGED <<currentTerm, votedFor, role, log, commitIndex, votes, doubted, matchIndex,
                   nextIndex, doubt, doubtSince, elected, committed, truncatedCommitted, crashes,
                   torn, changes>>

Ack(i, m, rest, term, success, index) ==
    Reply(rest, [type |-> "AppendAck", term |-> term, success |-> success, index |-> index,
                 from |-> i, to |-> m.from])

\* `on_append_entries` and `accept_records`: where the records are new, a conflicting suffix
\* truncated first; an appended record of a later term ends a doubt; and not the commit, nor
\* reaching the leader's last index.
Accept(i, m, rest) ==
    LET l == log[i]
        ent == m.entries
        prev == m.prevIndex
        conflicts == {k \in 1..Len(ent) : prev + k > Len(l) \/ l[prev + k].term # ent[k].term}
        c == IF conflicts = {} THEN 0 ELSE Min(conflicts)
        newLog == IF c = 0 THEN l ELSE SubSeq(l, 1, prev + c - 1) \o SubSeq(ent, c, Len(ent))
        cut == c # 0 /\ prev + c <= Len(l)
        trusts == c # 0 /\ \E k \in c..Len(ent) : ent[k].term > doubtSince[i]
        matched == prev + Len(ent)
    IN /\ log' = [log EXCEPT ![i] = newLog]
       /\ truncatedCommitted' = (truncatedCommitted \/ (cut /\ prev + c <= commitIndex[i]))
       /\ doubt' = [doubt EXCEPT ![i] = @ /\ ~trusts]
       /\ commitIndex' = [commitIndex EXCEPT ![i] = Max({@, Min({m.commit, matched})})]
       /\ Ack(i, m, rest, m.term, TRUE, matched)

HandleAppendEntries(i, m, rest) ==
    /\ m.type = "AppendEntries" /\ m.to = i
    /\ IF m.term < currentTerm[i]
       THEN /\ Ack(i, m, rest, currentTerm[i], FALSE, Len(log[i]) + 1)
            /\ UNCHANGED <<currentTerm, votedFor, role, votes, doubted, log, commitIndex,
                           doubt, truncatedCommitted>>
       ELSE /\ IF m.term > currentTerm[i] \/ role[i] # "Follower"
               THEN StepDownTo(i, m.term)
               ELSE UNCHANGED <<currentTerm, votedFor, role, votes, doubted>>
            /\ IF m.prevIndex > Len(log[i])
               THEN /\ Ack(i, m, rest, m.term, FALSE, Len(log[i]) + 1)
                    /\ UNCHANGED <<log, commitIndex, doubt, truncatedCommitted>>
               ELSE IF m.prevIndex > 0 /\ log[i][m.prevIndex].term # m.prevTerm
               THEN /\ Ack(i, m, rest, m.term, FALSE,
                           Max({FirstOfTerm(log[i], m.prevIndex), commitIndex[i] + 1}))
                    /\ UNCHANGED <<log, commitIndex, doubt, truncatedCommitted>>
               ELSE Accept(i, m, rest)
    /\ UNCHANGED <<matchIndex, nextIndex, doubtSince, elected, committed, crashes, torn, changes>>

\* `on_append_ack`: a refusal from no further than it had matched lowers it (the follower lost
\* entries it acked), and sends from where the follower said. One left unreceived stands for one
\* the code ignores.
HandleAppendAck(i, m, rest) ==
    /\ m.type = "AppendAck" /\ m.to = i
    /\ IF m.term > currentTerm[i]
       THEN /\ StepDownTo(i, m.term)
            /\ UNCHANGED <<matchIndex, nextIndex>>
       ELSE /\ m.term = currentTerm[i] /\ role[i] = "Leader"
            /\ LET was == matchIndex[i][m.from]
                   now == IF m.success THEN Max({was, Min({m.index, Len(log[i])})})
                          ELSE IF m.index <= was THEN m.index - 1 ELSE was
                   next == IF m.success THEN Max({nextIndex[i][m.from], now + 1})
                           ELSE Max({now + 1, Min({m.index, Len(log[i]) + 1})})
               IN /\ matchIndex' = [matchIndex EXCEPT ![i][m.from] = now]
                  /\ nextIndex' = [nextIndex EXCEPT ![i][m.from] = next]
            /\ UNCHANGED <<currentTerm, votedFor, role, votes, doubted>>
    /\ msgs' = rest
    /\ UNCHANGED <<log, commitIndex, doubt, doubtSince, elected, committed,
                   truncatedCommitted, crashes, torn, changes>>

\* `advance_commit`: the voters' matched, its own log if it's one; only an entry of this term.
AdvanceCommit(i) ==
    /\ role[i] = "Leader"
    /\ LET c == Config(i)
           matched(v) == IF v = i THEN Len(log[i]) ELSE matchIndex[i][v]
           held(n) == Cardinality({v \in c.voters : matched(v) >= n})
           n == Max({k \in 0..Len(log[i]) : held(k) >= Quorum(c)})
       IN /\ n > commitIndex[i]
          /\ log[i][n].term = currentTerm[i]
          /\ commitIndex' = [commitIndex EXCEPT ![i] = n]
          /\ committed' = committed \cup
                {[index |-> k, term |-> log[i][k].term, at |-> currentTerm[i]] : k \in 1..n}
    /\ UNCHANGED <<currentTerm, votedFor, role, log, votes, doubted, matchIndex, nextIndex, doubt,
                   doubtSince, msgs, elected, truncatedCommitted, crashes, torn, changes>>

\* Restarted: the term, vote and log kept, the rest lost; still in doubt if it was, since now.
Restart(i) ==
    /\ role' = [role EXCEPT ![i] = "Follower"]
    /\ votes' = [votes EXCEPT ![i] = {}]
    /\ doubted' = [doubted EXCEPT ![i] = {}]
    /\ commitIndex' = [commitIndex EXCEPT ![i] = 0]
    /\ matchIndex' = [matchIndex EXCEPT ![i] = [j \in Server |-> 0]]
    /\ nextIndex' = [nextIndex EXCEPT ![i] = [j \in Server |-> 1]]

Crash(i) ==
    /\ crashes < MaxCrashes
    /\ crashes' = crashes + 1
    /\ Restart(i)
    /\ doubtSince' = [doubtSince EXCEPT ![i] = IF doubt[i] THEN currentTerm[i] ELSE @]
    /\ UNCHANGED <<currentTerm, votedFor, log, doubt, msgs, elected, committed,
                   truncatedCommitted, torn, changes>>

\* Some node still holds every committed entry, with `i`'s log as `l`.
Held(i, l) ==
    \A e \in committed : \E s \in Server :
        LET h == IF s = i THEN l ELSE log[s]
        IN e.index <= Len(h) /\ h[e.index].term = e.term

\* A crash that tears a suffix off the log: the node restarts in doubt.
TornCrash(i) ==
    /\ torn < MaxTorn
    /\ \E k \in 0..(Len(log[i]) - 1) :
          /\ Held(i, SubSeq(log[i], 1, k))
          /\ log' = [log EXCEPT ![i] = SubSeq(@, 1, k)]
    /\ torn' = torn + 1
    /\ Restart(i)
    /\ doubt' = [doubt EXCEPT ![i] = TRUE]
    /\ doubtSince' = [doubtSince EXCEPT ![i] = currentTerm[i]]
    /\ UNCHANGED <<currentTerm, votedFor, msgs, elected, committed, truncatedCommitted,
                   crashes, changes>>

\* A message lost.
Drop(m) ==
    /\ msgs' = msgs \ {m}
    /\ UNCHANGED <<currentTerm, votedFor, role, log, commitIndex, votes, doubted, matchIndex, nextIndex,
                   doubt, doubtSince, elected, committed, truncatedCommitted, crashes, torn, changes>>

Next ==
    \/ \E i \in Server :
          \/ StartElection(i) \/ BecomeLeader(i) \/ StepDown(i)
          \/ ClientRequest(i) \/ ChangeMembers(i) \/ AdvanceCommit(i)
          \/ Crash(i) \/ TornCrash(i)
          \/ \E j \in Server : AppendEntries(i, j)
    \/ \E m \in msgs :
          \/ Drop(m)
          \/ \E again \in BOOLEAN :
                LET rest == IF again THEN msgs ELSE msgs \ {m}
                IN \/ HandleRequestVote(m.to, m, rest) \/ HandleVote(m.to, m, rest)
                   \/ HandleAppendEntries(m.to, m, rest) \/ HandleAppendAck(m.to, m, rest)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
\* At most one leader a term.
ElectionSafety == \A a, b \in elected : a[1] = b[1] => a[2] = b[2]

\* Two logs with an entry of the same index and term are the same up to it.
LogMatching ==
    \A i, j \in Server : \A n \in 1..Len(log[i]) :
        (n <= Len(log[j]) /\ log[i][n].term = log[j][n].term)
            => SubSeq(log[i], 1, n) = SubSeq(log[j], 1, n)

\* A leader has every entry committed before its term.
LeaderCompleteness ==
    \A i \in Server : role[i] = "Leader" =>
        \A e \in committed : e.at < currentTerm[i] =>
            e.index <= Len(log[i]) /\ log[i][e.index].term = e.term

\* No index is committed with two different entries.
StateMachineSafety == \A a, b \in committed : a.index = b.index => a.term = b.term

\* No node truncates what it knows committed (`truncate_from`'s assertion).
NoCommittedTruncated == ~truncatedCommitted

\* What a node takes as committed was: what it applies is the cluster's.
CommitsAreCommitted ==
    \A i \in Server : \A n \in 1..commitIndex[i] :
        n <= Len(log[i]) /\ \E e \in committed : e.index = n /\ e.term = log[i][n].term
=============================================================================
