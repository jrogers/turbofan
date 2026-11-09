//! A node of the benchmark's service, as a binary of its own: each message and query answered with
//! its first 8 bytes, nothing kept. So a cluster of processes, on machines of their own, runs what
//! `cargo bench -p turbofan --bench cluster` runs in one, and `BENCH_CLUSTER` drives it from
//! another machine:
//!
//! ```text
//! cargo build --release -p turbofan --example echo
//! target/release/examples/echo serve --id 1 --data ./n1 \
//!     --peers   1=10.0.0.1:7001,2=10.0.0.2:7001,3=10.0.0.3:7001 \
//!     --clients 1=10.0.0.1:7101,2=10.0.0.2:7101,3=10.0.0.3:7101 \
//!     --admin   1=10.0.0.1:7201,2=10.0.0.2:7201,3=10.0.0.3:7201
//! ```
//!
//! Every other command of [`turbofan::run`] too: `--no-fsync`, TLS, `add-member` and the rest.

use std::io::{self, Read};
use std::process::ExitCode;

use turbofan::client::MAX_UNACKED;
use turbofan::{Ctx, Service, SessionId, SnapshotWriter};

/// The benchmark's: its clients connect to this cluster.
const SERVICE_ID: u32 = 9;

struct Echo;

impl Service for Echo {
    fn on_message(&mut self, ctx: &mut Ctx, session: SessionId, payload: &[u8]) {
        ctx.send(session, payload.get(..8).unwrap_or(payload)).expect("at most 8 bytes");
    }

    fn on_query(&self, _session: SessionId, query: &[u8]) -> Option<Vec<u8>> {
        Some(query.get(..8).unwrap_or(query).to_vec())
    }

    fn snapshot(&self) -> SnapshotWriter {
        Box::new(|_| Ok(()))
    }

    fn restore(&mut self, _from: &mut dyn Read) -> io::Result<()> {
        Ok(())
    }
}

fn main() -> ExitCode {
    // Every response a client can have in flight, kept for it: as the benchmark's nodes.
    turbofan::run_with(SERVICE_ID, Echo, |config| config.node.egress_ring_entries = MAX_UNACKED)
}
