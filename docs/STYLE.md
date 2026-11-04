# Turbofan style

How Turbofan is written, and why. It borrows from TigerBeetle's
[Tiger Style](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/TIGER_STYLE.md),
which applies NASA's [Power of Ten](https://spinroot.com/gerard/pdf/P10.pdf) rules to a
database. The last section says where Turbofan departs from Tiger Style, and why.

Turbofan replicates a service's state across a cluster. A bug in it can apply a message twice,
lose one that was acknowledged, or let two leaders take messages at once. So the
priorities, in order, are **safety, performance and developer experience**. Where they conflict,
safety wins. Mostly they don't: simple code tends to be easier to make correct, to make fast and
to read.

## Simplicity and debt

- The first design that works is rarely the simple one. Spend the thinking up front, and revise
  until the design is simple. It costs less than fixing it once it's in use.
- Don't ship a known showstopper: work that grows without limit, a latency spike on a common
  path, or an error that's silently ignored. If a problem is accepted for now, write it down: in
  [ROADMAP.md](../crates/turbofan-cluster/docs/ROADMAP.md), or as a `ponytail:` comment at the
  code that has the ceiling, saying what the ceiling is and what would lift it.

## The core is sans-IO

`raft/` and `node/` take inputs and return outputs. They do no IO and never read a clock: time
is an input. The driver (`driver.rs`) does the IO on tokio, and the simulator (`tests/sim.rs`)
drives the same `Node` over a model network, a model clock and real files. So every decision is
tested without sockets, and a simulated failure replays from its seed.

- Keep it that way. A change that wants IO or the time inside the core wants a new input or
  output instead.
- `turbofan-cluster` never depends on a service, or mentions what one does. A service's messages
  are opaque bytes to it.
- **Run at your own pace.** The driver takes what has arrived, hands it to the node as a batch
  (at most `MAX_BATCH_INPUTS`), and carries out the outputs together. Outside events never drive
  the work directly.

## Safety

### Put a limit on everything

Every queue, buffer, retry and loop over outside input has a fixed upper bound. The bound is a
named constant or a configuration field, with a comment saying why it has that value. A limit
turns an unbounded failure (memory exhaustion, a stalled task, a latency tail) into one that is
bounded and visible. Examples: `MAX_FRAME_BYTES` and `MAX_RECORD_BYTES` in `codec.rs`,
`MAX_SESSIONS` and `MAX_EGRESS_RING` in `node/mod.rs`, and `MAX_QUEUED_INPUTS` in `driver.rs`.

Recursion must have a depth that Turbofan's own types fix, never one that input controls.

### Assert invariants

Assertions turn a correctness bug into a crash, which is noisy and easy to find, rather than
corrupt state, and on a replicated log a corrupted state spreads. They also multiply what the
simulator and the fuzzer find, because a broken invariant shows the moment it happens, not when
it eventually causes a visible failure.

- Assert preconditions, postconditions and invariants: what a function requires, what it
  guarantees, and what must always be true of a type.
- **Pair assertions.** Check a property on two different code paths, for example as a record is
  written and again as it is read back. A bug has to get past both.
- Assert the negative space as well as the positive: what must not happen, not just what should.
- Split compound assertions. `assert!(a); assert!(b);` says which one failed; `assert!(a && b)`
  doesn't.
- Check relationships between constants at compile time, as `codec.rs` does:
  `const _: () = assert!(MAX_PAYLOAD_BYTES <= MAX_FRAME_BYTES / 2);`.
- **Control plane and data plane.** Use `assert!` on the control plane (elections, membership,
  snapshots, recovery from disk), where one check costs nothing measurable. Use `debug_assert!` on
  the per-entry path. Debug assertions run in every test and in the simulator (the test profile
  is optimised but keeps them), and in the fuzz targets.

Assertions don't replace understanding the code. They check that the reader's model of it
matches what actually happens.

### Handle every error

Most catastrophic failures in distributed systems come from errors that were handled wrongly or
not at all ([Yuan et al., OSDI 2014](https://www.usenix.org/conference/osdi14/technical-sessions/presentation/yuan)).

- Return errors, or handle them where they arise. Don't discard them.
- Use `unwrap` only in tests, benchmarks and examples. Where an invariant rules out failure, use
  `expect` and name the invariant: `.expect("client messages are within their limits")`.
- If a panic is the right response, make it a deliberate panic with a message, not an overflow.
- What the cluster reads from the network or the disk is untrusted: it is refused, never
  panicked on. `scripts/fuzz.sh` checks this.

### Be explicit

- No `unsafe` code. The workspace denies it; an exception needs an `#[allow]` with a `reason`,
  and review.
- Convert between integer types with `From` and `TryFrom`, not `as`. Where truncation is the
  point, allow the lint on that statement with a reason. The cast lints are on.
- Build with no warnings. `scripts/check.sh` runs clippy with `-D warnings`, with and without
  every feature.

### Keep functions short

A function should fit on a screen: about 70 lines, which clippy checks (`clippy.toml`).
Functions still over the limit are marked `#[expect(clippy::too_many_lines)]` until they are
split. Shape functions so that:

- the parent keeps the control flow (the `match` and the `if`s) and the state;
- helpers compute what the parent then acts on;
- leaf functions are pure where possible.

Declare variables in the smallest scope they need, and close to where they are used.

### Change the wire only as WIRE.md allows

Old and new nodes run side by side during an upgrade, and a log written today is read by the
next version. Change `schema/cluster.xml`, never the generated code, and only in the ways
[WIRE.md](../crates/turbofan-cluster/docs/WIRE.md) allows.

## Performance

- **Sketch first.** The largest gains come at design time, before there is anything to measure.
  Sketch the four resources (network, disk, memory and CPU), for both bandwidth and latency, and
  aim to be roughly right. Optimise the slowest resource first, weighted by how often it is used:
  for a cluster, that is usually the fsync and the round trip to a quorum.
- **Separate the control plane from the data plane.** Elections, membership and recovery can
  afford checks and allocation. The per-entry path can't.
- **Batch.** Amortise system calls, disk writes and wake-ups over many entries: one fsync and one
  append to each follower for everything that arrived together.
- **Keep hot loops simple.** Put a hot loop in a function that takes plain values rather than
  `&self`, so both the compiler and the reader can see what it touches.
- **Measure what you change.** `cargo bench -p turbofan-cluster --bench cluster` measures
  throughput and latency of three nodes over real TCP and disks. Record before and after numbers
  in the commit message.

## Developer experience

### Names

- Say what a thing is or does, without abbreviations, in the terms Raft itself uses (`term`,
  `commit_index`, `AppendEntries`).
- Name a limit `max_` then what it limits, and put units last: `MAX_FRAME_BYTES`,
  `MAX_SESSIONS`; a timeout by what it times. One pattern everywhere, public or not, so a name
  can be guessed. Use `Duration` rather than a unit suffix where possible, except in the sans-IO
  core, where time is a `u64` of nanoseconds (`now`).
- An index, a count and a size are different things. Name them differently, and convert between
  them explicitly.
- Don't give one name two meanings in different contexts.

### Comments and commits

- Comments explain why. The code already says what it does.
- Write comments as sentences, with capitals and full stops.
- A commit message says why the change was made, and gives measurements when it's about
  performance. Pull request descriptions aren't in the history and `git blame` doesn't show them,
  so the reasoning belongs in the commits.
- A test says what it checks and how, in its name or a comment.

### Order

Put the important things first. A file is read top to bottom: the public type and its main entry
points come before the helpers.

## Where Turbofan differs from Tiger Style, and why

- **120 columns, not 100.** rustfmt enforces 120. Prose and comments wrap at
  about 100.
- **`usize` for indexing.** Tiger Style avoids architecture-sized integers. Rust indexes with
  `usize`, so Turbofan uses it for indices and lengths in memory, and fixed-size types for
  everything on the wire and on disk.
- **Few dependencies, not zero.** Turbofan uses tokio for IO, crc32c for checksums, metrics for
  counters, rustls for TLS, and little else. A new dependency must justify itself, with a comment
  in the workspace `Cargo.toml` saying what it's for, and `cargo-deny` checks every dependency's
  licence and known advisories.
- **Heap allocation.** Tiger Style allocates everything statically at startup. Turbofan bounds
  what it holds instead, by the limits above, and allocates within them.
- **Scripts in shell.** Tiger Style writes scripts in the project's own language. Turbofan's few
  scripts (`scripts/`) are short enough to stay in bash.
