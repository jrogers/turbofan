# Changing the wire protocol

`crates/turbofan/schema/cluster.xml` (paths here are in that crate) defines everything a node writes for another to read: the peer, client and
admin messages, the log entries, and what a snapshot holds of the node's state and the members,
as messages framed as on the wire. During a rolling upgrade old and new nodes, and old and new
clients, run side by side, so every change must leave them able to work together. This is how.

The schema as it stands at version 0 is the baseline. Changes before this file were made in place,
which was fine only because nothing ran yet.

## What every reader does

A reader tolerates what's newer than it:

- **More than it knows of a message.** A message whose header says it's from a newer version may
  carry fields added at the end of its block, and groups or data added after the ones it knows.
  The reader reads what it knows and skips the rest (`codec::decode_frame`).
- **A message type it doesn't know.** It skips the frame, by its length, and reads the next
  (`CodecError::is_unknown_message`); it doesn't drop the connection.
- **An enum value it doesn't know.** It decodes as `Unknown(n)`, which code must handle as "not
  one of mine" (`CloseReason::Unknown`, say).

And refuses what's malformed: a message from another schema, a block shorter than its version
needs, or bytes left over in a frame that claims a version the reader knows.

## Changes that need no coordination

Any mix of old and new nodes and clients keeps working through these:

- **Adding a field** at the end of a message's block, with `sinceVersion` set to the new schema
  version, and bumping the schema's `version`. Older readers skip it; newer readers see it absent
  (its null value, or `None`) in older messages, and must treat that as the old behaviour.
- **Adding a group or a data field** after the existing ones, with `sinceVersion`.
- **Adding an enum value**, where older readers handle `Unknown` sensibly.
- **Adding a message type that a receiver may ignore**: a sender to an older node gets no answer,
  and must cope (a request that times out, a hint that's never taken).

## Changes that are never made

- Changing a field's type, size, offset or meaning; removing, renaming on the wire, or reordering a
  field, group or data field.
- Reusing a template ID or an enum value, even for one that was removed.
- Changing what an existing message means under the same template ID: add a new one instead.

## Changes every member must be able to read first

Some things every node must act on identically, or not at all:

- **A new log entry type, or a new field that changes how an entry applies.** Every node applies
  every entry, so one that some node can't read stops it
  (`a log entry of a version this node knows`), and one that nodes read differently breaks
  determinism.
- **A change to what a snapshot holds** (`NodeState` and the messages after it: sessions, timers,
  the outbox) that changes how the state behaves. A field added with `sinceVersion` decodes on an
  older node, which ignores it: harmless only if ignoring it is, since a leader sends any
  follower its snapshot. A new message in the snapshot's sequence an older node can't read at
  all.
- **A peer message Raft depends on for safety or progress**, as opposed to one it may ignore.
- **Raising a data field's `maxValue`** (`MAX_PAYLOAD_BYTES`, say). A reader refuses data over
  the `maxValue` it knows, so an older node refuses a longer one: a malformed frame, which drops
  the connection, or a log entry it can't read, which stops it.

These may only be used once every member runs a version that reads them, and switch on at the
same point in the log on every node. So a change of this kind:

1. Raises `node::PROTOCOL`, and does the new thing only once the cluster's protocol (the
   replicated `protocol` in `NodeState`, set by an `Activated` entry) is at least the new one.
2. Ships, node by node: every node advertises the highest protocol its code supports on its
   acks, and the leader knows the lowest among the members.
3. Is switched on by an operator's `activate --protocol <n>`, which the leader refuses while a
   member doesn't support it, logs as an `Activated` entry otherwise, and every node applies at
   the same index. A member older than that is no longer added.

A service changes its behaviour the same way, by `Service::version` and `--service <n>`.

## The snapshot file

`snapshot.rs`'s `MAGIC` (`CLSNAP01`) names the file's fixed layout: the header, the bytes, the
members. It doesn't change again: that would make every existing snapshot unreadable, so a node
with one wouldn't start. What's in the bytes changes by this file's rules instead.

## Checking a change

- Mark new fields, groups and data with `sinceVersion`, and bump `version` in `cluster.xml`.
- `tests/golden.rs` reads what each released version wrote (`tests/golden/<version>/`): a
  snapshot, and the client protocol's frames. They must still read, as they say.
- A test that an older version's message still decodes, and a newer one's is read past
  (`codec::tests::a_newer_versions_frames_are_tolerated` is the pattern).
- For a change of the third kind, the simulator's rolling upgrade: the nodes start on the old
  version and are upgraded one by one, and the operator's activation must be refused until
  every member is new.
