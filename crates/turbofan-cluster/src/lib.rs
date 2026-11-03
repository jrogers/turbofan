//! Turbofan: a Raft-replicated state machine framework. A leader orders client input into a log,
//! a quorum makes it durable, and every node applies it to the same deterministic [`Service`].
//! Nothing here knows what the service does; its messages are opaque bytes.
//!
//! A service implements [`Service`] and runs with [`run`], or with [`driver::serve`] for control
//! over its [`driver::Config`]. Gateways connect with [`ClusterClient`], or [`client::Client`]
//! to do their own IO.
#![warn(clippy::unwrap_used)]
// Conversions go through `From`, `TryFrom` or `widen`, never `as`: the cast lints miss
// `usize as u64`, which is lossless only on targets where usize fits.
#![warn(clippy::as_conversions)]

mod cli;
pub mod client;
pub mod codec;
pub mod driver;
pub mod metrics;
mod remote;
pub mod tls;

// The core and its storage: public for the simulation, the fuzz targets and the benchmark, but not
// part of the API. What a service needs from them is re-exported below.
#[doc(hidden)]
pub mod log;
#[doc(hidden)]
pub mod meta;
#[doc(hidden)]
pub mod node;
#[doc(hidden)]
pub mod pipeline;
#[doc(hidden)]
pub mod raft;
#[doc(hidden)]
pub mod snapshot;

pub use cli::run;
pub use node::{
    Config as NodeConfig, Ctx, Effect, MAX_EGRESS_RING, MAX_PRINCIPAL_BYTES, MAX_SESSIONS, PROTOCOL, PayloadTooLong,
    Service, SessionId, SnapshotWriter,
};
pub use raft::{Config as RaftConfig, MAX_MEMBERS, Member, NodeId};
pub use remote::{Admin, AdminError, ClusterClient, ClusterStatus, MemberStatus};
pub use wire::CloseReason;

/// The codecs generated from `schema/cluster.xml`.
#[allow(
    dead_code,
    missing_docs,
    missing_debug_implementations,
    clippy::all,
    clippy::pedantic,
    clippy::as_conversions,
    reason = "generated"
)]
pub mod wire {
    include!(concat!(env!("OUT_DIR"), "/wire.rs"));
}

const _: () = assert!(usize::BITS <= u64::BITS, "a length fits a u64");

/// A length or count as a `u64`: lossless, as the assertion above guarantees.
#[allow(clippy::as_conversions, reason = "lossless: usize is at most 64 bits")]
pub(crate) const fn widen(n: usize) -> u64 {
    n as u64
}
