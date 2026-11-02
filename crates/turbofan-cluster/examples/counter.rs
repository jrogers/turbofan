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
use std::io::{self, Read};
use std::process::ExitCode;

use turbofan_cluster::{CloseReason, Ctx, Service, SessionId, SnapshotWriter};

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
        ctx.send(session, count.to_string().as_bytes()).expect("a count is a few bytes");
    }

    fn on_session_close(&mut self, _ctx: &mut Ctx, session: SessionId, _reason: CloseReason) {
        self.counts.remove(&session);
    }

    /// A copy of the counts, written out while the node goes on.
    fn snapshot(&self) -> SnapshotWriter {
        let counts = self.counts.clone();
        Box::new(move |out| {
            for (session, count) in &counts {
                out.write_all(&session.0.to_le_bytes())?;
                out.write_all(&count.to_le_bytes())?;
            }
            Ok(())
        })
    }

    fn restore(&mut self, from: &mut dyn Read) -> io::Result<()> {
        let mut bytes = Vec::new();
        from.read_to_end(&mut bytes)?;
        let words: Vec<u64> = bytes.as_chunks::<8>().0.iter().map(|w| u64::from_le_bytes(*w)).collect();
        self.counts = words.as_chunks::<2>().0.iter().map(|&[session, count]| (SessionId(session), count)).collect();
        Ok(())
    }
}

fn main() -> ExitCode {
    turbofan_cluster::run(SERVICE_ID, Counter::default())
}
