//! A Raft-replicated state machine framework, after Aeron Cluster: a leader orders client input
//! into a log, a quorum makes it durable, and every node applies it to the same deterministic
//! [`Service`]. Nothing here knows what the service does; its messages are opaque bytes.
#![warn(clippy::unwrap_used)]

mod cli;
pub mod client;
pub mod codec;
pub mod driver;
pub mod log;
pub mod meta;
pub mod node;
pub mod raft;
mod remote;
pub mod snapshot;

pub use cli::run;
pub use node::{Ctx, Service, SessionId};
pub use remote::{ClusterClient, request_snapshot};
pub use wire::CloseReason;

/// The codecs generated from `schema/cluster.xml`.
#[allow(dead_code, missing_docs, missing_debug_implementations, clippy::all, clippy::pedantic, reason = "generated")]
pub mod wire {
    include!(concat!(env!("OUT_DIR"), "/wire.rs"));
}
