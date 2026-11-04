# Upgrades

A cluster upgrades while it runs, a node at a time (see [Operations][operations]). Old and new
code then run side by side, on the same log, which raises one question: what if they'd apply an
entry differently? Every node must reach the same state, so a change in what the service does
can't simply ship.

## Changes that are safe as they are

A release that doesn't change what any entry does to the state needs nothing more than a rolling
restart: faster code, more logging, a fix to `authenticate` (only the leader asks it, and its
answer is what's logged), or a new kind of message that only new clients send once every node
has the code.

Careful with the last: a message only new code understands, sent while an old node is still in
the cluster, is applied by both, and the old one does something else with it. Wait for the
rollout to finish before clients use it, or gate it as below.

## Changing what the service does

For anything else (a new rule, a fixed calculation, a field read differently), ship both
behaviours, then switch every node at the same point in the log:

1. The new code says which version of the behaviour it has, with `Service::version`, and does the
   old thing until the cluster runs the new version, as `Ctx::service_version` says:

   ```rust
   # use std::io::{self, Read};
   # use turbofan_cluster::{Ctx, Service, SessionId, SnapshotWriter};
   #[derive(Debug, Default)]
   struct Fees {
       charged: u64,
   }

   impl Service for Fees {
       fn version(&self) -> u32 {
           1
       }

       fn on_message(&mut self, ctx: &mut Ctx, _session: SessionId, payload: &[u8]) {
           let size = u64::try_from(payload.len()).unwrap_or(u64::MAX);
           // Version 1 charges by size; until it's active, a flat fee.
           self.charged += if ctx.service_version() >= 1 { size } else { 10 };
       }
   #     fn snapshot(&self) -> SnapshotWriter { Box::new(|_| Ok(())) }
   #     fn restore(&mut self, _from: &mut dyn Read) -> io::Result<()> { Ok(()) }
   }

   let mut fees = Fees::default();
   fees.on_message(&mut Ctx::new(0), SessionId(1), b"four");
   fees.on_message(&mut Ctx::new(0).with_service_version(1), SessionId(1), b"four");
   assert_eq!(fees.charged, 10 + 4);
   ```

2. Roll it out, node by node. Each node tells the leader the highest version its code has.
3. Switch:

   ```sh
   my-service activate --admin 10.0.0.1:7201 --service 1
   ```

   The leader refuses while any member's code doesn't have version 1. Otherwise it logs the
   switch, and every node applies it at the same index: `on_activate(1)` is called there, for
   anything that must change with it, and from the next entry `service_version()` is 1. A member
   whose code is older is no longer added.

Versions only go up. Once the switch is made and nothing will roll back, a later release can drop
the old behaviour. The version is part of the replicated state, so a node replaying the log, or
loading a snapshot, runs each entry under the version that was active when it was first applied.

## What a snapshot holds

A new node, or one far behind, is sent another's snapshot, so every version of the code that
might run side by side must read every other's. Change the snapshot's format as you change a
message's: add fields an older reader skips, or give it a version of its own at the start and
read every version you've ever written. Write the new format only once the new behaviour is
active, so an old node can still read the snapshots it's sent until then.

## The cluster's own protocol

Turbofan's own messages (between nodes, to clients, in the log and in its part of a snapshot)
change by the same rules, in `crates/turbofan-cluster/docs/WIRE.md`. Most changes need nothing:
a reader skips fields and messages newer than it. A change every node must act on alike is
switched on the same way, with `activate --protocol <n>`. A release's notes say when that's
needed.

`activate --protocol <n> --service <m>` does both at once. Once a protocol is active, name it in
every `activate`: the default, 0, would be a step back, which is refused.

Protocol 1 makes the responses each session keeps (`egress_ring_entries`) the cluster's: every
`activate` logs the leader's, for every node. To change it, restart the nodes with the new value,
then `activate` again.
