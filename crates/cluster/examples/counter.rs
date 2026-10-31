//! The smallest service: counts each session's messages and answers each with the count so far.
//!
//! ```sh
//! cargo run --example counter -- serve --id 1 --data /tmp/counter/1 \
//!     --peers 1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003 \
//!     --clients 1=127.0.0.1:7101,2=127.0.0.1:7102,3=127.0.0.1:7103 \
//!     --admin 1=127.0.0.1:7201,2=127.0.0.1:7202,3=127.0.0.1:7203
//! ```
//! and the same for nodes 2 and 3.

use std::collections::BTreeMap;
use std::process::ExitCode;

use cluster::{CloseReason, Ctx, Service, SessionId};

/// This cluster's `Hello.service_id`.
const SERVICE_ID: u32 = 1;

#[derive(Debug, Default)]
struct Counter {
    counts: BTreeMap<SessionId, u64>,
}

impl Service for Counter {
    fn on_message(&mut self, ctx: &mut Ctx, session: SessionId, _payload: &[u8]) {
        let count = self.counts.entry(session).or_default();
        *count += 1;
        ctx.send(session, count.to_string().as_bytes());
    }

    fn on_session_close(&mut self, _ctx: &mut Ctx, session: SessionId, _reason: CloseReason) {
        self.counts.remove(&session);
    }

    fn snapshot(&self, out: &mut Vec<u8>) {
        for (session, count) in &self.counts {
            out.extend_from_slice(&session.to_le_bytes());
            out.extend_from_slice(&count.to_le_bytes());
        }
    }

    fn restore(&mut self, bytes: &[u8]) {
        let words = bytes.as_chunks::<8>().0.iter().map(|w| u64::from_le_bytes(*w));
        let words: Vec<u64> = words.collect();
        self.counts = words.as_chunks::<2>().0.iter().map(|&[session, count]| (session, count)).collect();
    }
}

fn main() -> ExitCode {
    cluster::run(SERVICE_ID, Counter::default())
}
