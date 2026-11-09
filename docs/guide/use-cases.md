# What it's for

Turbofan runs a service as a replicated state machine. The leader orders every client's messages
into one log, a quorum of nodes makes each entry durable, and every node applies the log, in that
order, to the same deterministic code. Three things come of that, together: one order for
everything that happens, agreed by every node; the state on several machines, current on each;
and client sessions that outlast a failover, every message applied exactly once and every
response delivered exactly once, in order.

That's a particular shape of system, and where it fits, it replaces a lot of machinery.

## The shape it fits

- **The state fits in memory.** Each node holds the whole of it, and snapshots it by taking a
  copy: gigabytes are fine, with data structures that copy cheaply (see
  [Performance][performance]); terabytes aren't.
- **Every change goes through the service.** Clients send messages, and the service changes its
  state in answer, one message at a time, on one thread. Nothing else writes to the state.
- **Order matters.** Who came first, whether a balance covers an order, which of two requests
  wins: the log answers every such question the same way on every node, for good.
- **Latency matters.** On Linux servers with local NVMe, every batch synced before it's
  acknowledged, a message is committed and answered in a few hundred microseconds or less, and a
  cluster takes over a million small messages a second (see
  [Performance][performance]). On a network, a commit costs a round trip to the nearest
  follower.
- **Losing or repeating a message is a bug.** Not a retry policy and a deduplication table in
  each client: the session guarantees it, through failovers and reconnects.

## Replacing a consensus stack

A service made highly available the usual way is several systems, each replicated on its own
terms: application servers, with a coordination service (ZooKeeper, etcd, Consul) for leader
election, locks and leases; a replicated log (Kafka, say) to order the inputs; a database for the
state; and idempotency keys, deduplication tables and fencing tokens to stitch them together. Each
is sound on its own. The failures live between them: a leader that lost its lease and doesn't know
it yet, still writing; a request retried after a timeout and applied twice; a consumer that
committed its offset but not its write, or the other way round; two replicas that applied the same
inputs in a different order.

Turbofan makes the application the replicated part, so those seams aren't there:

- **Leadership isn't something the service checks.** Only the leader takes new messages, and
  a leader cut off from a quorum stands down; a deposed leader's proposals never commit, so
  nothing it did after losing the leadership counts. There's no lease to check and no fencing
  token to pass along.
- **Ordering, replication and the state are one thing.** The log is the order, and the state is
  what applying it gives. There's no offset to commit beside a write, and no second copy of the
  truth to reconcile.
- **Exactly once is the session's job.** A client numbers its messages; the cluster applies each
  once, and a client that reconnects to the new leader resends only what wasn't applied, and
  receives only the responses it missed (see [Clients][clients]).
- **Time is in the log.** Timers fire through the log, at the leader's clock as it logged it, so a
  timeout, an expiry or a scheduled job happens once, on every node, at the same point in the
  state, even across a failover (see [Services in depth][services]).

What it doesn't remove: work with the outside world. A service can't call another system, since
every node would make the call and none could rely on its answer. That work goes in a gateway, a
client of the cluster that does the IO and sends the results back in as messages, which the
service treats as inputs like any other.

## Trading systems

This is what Turbofan was built for. Trading systems want everything the shape above describes,
and want it fast: one order of events that everyone agrees on, no message lost or repeated, state
that survives a machine, and microseconds, not milliseconds.

- **A sequencer.** The log is a sequencer: every input, from every gateway, in one total order,
  stamped with the leader's time. Downstream systems take its output in that order, and every
  replica has the same state at the same sequence number. This is the central-sequencer
  architecture of many exchanges, with the sequencer's failover built in.
- **A matching engine.** Price-time priority depends only on the order of inputs, which is what
  the log fixes, and a matching engine is single-threaded and deterministic anyway. Every node
  holds the book, so a failover loses no resting order and no fill.
- **An order management system.** Orders from clients' gateways, routed to venues through venue
  gateways, with their acks, fills and cancels coming back as messages: the OMS's state is each
  order's, and every execution report goes out once, in order, across a failover. Pre-trade risk
  (limits, positions, credit) is checked in the same state machine as the order it guards, so
  checking a limit and taking up part of it is one step, with no race between the two.
- **Positions, limits and P&L.** Fills come in through the log, so every node agrees on every
  position, and a limit crossed is crossed once.

Gateways sit at the edges, speaking FIX or a venue's own protocol, translating to and from the
service's messages. A venue that resends a fill after reconnecting sends it to a gateway, which
passes it on; the service, which knows what it has counted, drops it.

Operating one asks for things Turbofan has:

- **Replay.** Every node applies the same entries, at the times logged in them, so `capture` and
  `replay` reproduce a day's trading offline, exactly: to debug what happened in production, to
  audit it, or to check a new version of the service against real traffic before it runs (see
  [Operations][operations]).
- **Disaster recovery.** A learner in another region takes the log without slowing commits, and
  `promote` makes it a cluster of its own if the main one is lost. A client whose session
  the standby doesn't wholly have is told so, rather than carrying on as if nothing were lost
  (see [Members][membership]).
- **Upgrades while trading.** A new version of the service is rolled out node by node, then
  activated at one point in the log, so every node switches at the same message (see
  [Upgrades][upgrades]).

## Other systems of the same shape

- **Ledgers, balances and payments**: a debit is checked against the balance and taken in one
  step, in one order, so nothing is spent twice.
- **Reservations and inventory**: seats, tickets, stock, capacity; two requests for the last one
  are ordered, and one wins, the same on every node.
- **Quotas and rate limits** that hold across a fleet, counted in one place rather than
  approximately in each.
- **Schedulers and job queues**, where a job dispatched must be dispatched once, and a timer
  fire once, though the node that set it fails.
- **Game and session servers**, matchmaking and lobbies: shared state, many clients, order
  matters.
- **Coordination**: locks, leader election for other systems, configuration. If that's all you
  need, etcd or ZooKeeper does it already; Turbofan is for when the logic around them is yours.

## When it isn't the right tool

- **State bigger than memory**, or queried in ways a service's own structures can't answer:
  analytics, search, reports. Keep the state machine to what its decisions need, and project its
  output into a database for the rest, from a client that follows it.
- **Reads at scale.** A read goes through the log like any message, so a cluster reads as fast as
  it writes. Serve heavy reads from a projection, or from a gateway that keeps its own copy.
- **Writes from several regions.** A commit waits for a quorum, so the voters belong close
  together. A standby in another region is a learner, which commits don't wait for.
- **More than one core's worth of work.** One cluster applies one message at a time. Past that,
  partition: several clusters, each with its share of the keys (accounts, instruments), and one
  service each.
- **Work that can't be deterministic**: calls to other systems, randomness, the machine's clock.
  Those go in gateways, outside the state machine.

And Turbofan is pre-1.0, and not used in production yet. It's simulated, fuzzed and tested over a
real network, and its API will change before 1.0.

[clients]: clients.md
[membership]: membership.md
[operations]: operations.md
[performance]: performance.md
[services]: services.md
[upgrades]: upgrades.md
