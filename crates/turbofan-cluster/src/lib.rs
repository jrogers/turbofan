//! Turbofan: a Raft-replicated state machine framework. A leader orders client input into a log,
//! a quorum makes it durable, and every node applies it to the same deterministic [`Service`].
//! Nothing here knows what the service does; its messages are opaque bytes.
//!
//! A service implements [`Service`] and runs with [`run`], or with [`driver::serve`] for control
//! over its [`driver::Config`]. Gateways connect with [`ClusterClient`], or [`client::Client`]
//! to do their own IO.
//!
//! The [`guide`] goes from a first service to running a cluster in production:
//! clients, members, operations, upgrades and performance.
#![warn(clippy::unwrap_used)]
// Conversions go through `From`, `TryFrom` or `widen`, never `as`: the cast lints miss
// `usize as u64`, which is lossless only on targets where usize fits.
#![warn(clippy::as_conversions)]

/// The guide's chapter links, as rustdoc resolves them: see `guide.rs`.
macro_rules! guide_links {
    () => {
        "[guide]: crate::guide\n\
         [first-service]: crate::guide::first_service\n\
         [services]: crate::guide::services\n\
         [clients]: crate::guide::clients\n\
         [running]: crate::guide::running\n\
         [membership]: crate::guide::membership\n\
         [operations]: crate::guide::operations\n\
         [upgrades]: crate::guide::upgrades\n\
         [performance]: crate::guide::performance\n"
    };
}

// The guide: from a first service to running a cluster in production (docs/guide).
pub mod guide;

pub mod capture;
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

pub use cli::{run, run_with};
pub use node::{
    Config as NodeConfig, Ctx, Effect, MAX_EGRESS_RING, MAX_PRINCIPAL_BYTES, MAX_SESSIONS, PROTOCOL, PayloadTooLong,
    Service, SessionId, SnapshotWriter,
};
pub use raft::{Config as RaftConfig, MAX_MEMBERS, Member, NodeId};
pub use remote::{Admin, AdminError, Captured, ClusterClient, ClusterStatus, MemberStatus};
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
