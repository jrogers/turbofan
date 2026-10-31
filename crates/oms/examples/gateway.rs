//! A gateway and a venue against a running OMS cluster: sends orders, has the venue ack each
//! and fill it in two parts, and checks every order's reports came once each, in order:
//! PendingNew, New, Trade, Trade (Filled).
//!
//! ```sh
//! cargo run -p oms --example gateway -- 127.0.0.1:7101,127.0.0.1:7102,127.0.0.1:7103 10000
//! ```
//!
//! Kill and restart nodes while it runs: it should still finish, with nothing lost or doubled.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use oms::wire::{self, ExecType, OrdStatus, Side};
use turbofan_cluster::ClusterClient;
use turbofan_cluster::client::{Event, SendError};
use turbojet::sbe::Encode;

/// Orders sent but not yet filled: the gateway stops sending at this many.
const MAX_OPEN_ORDERS: u64 = 1000;
const KEEPALIVE: Duration = Duration::from_millis(500);
/// How long the whole run may take, failovers and all.
const DEADLINE: Duration = Duration::from_secs(120);

/// How a run went.
#[derive(Debug)]
pub struct Summary {
    /// Orders filled.
    pub orders: u64,
    /// Execution reports received.
    pub reports: u64,
    /// How long it took.
    pub elapsed: Duration,
}

fn encode(message: &impl Encode) -> Vec<u8> {
    let mut out = Vec::new();
    message.encode_into(&mut out).expect("small");
    out
}

/// Sends `orders` orders through the cluster at `addrs`, plays the venue, and checks the reports.
///
/// # Errors
///
/// If a report is out of sequence, doubled or missing, or the run takes too long.
pub async fn trade(addrs: Vec<String>, orders: u64) -> Result<Summary, String> {
    let start = Instant::now();
    let run = async {
        let mut venue = ClusterClient::connect(addrs.clone(), oms::SERVICE_ID, KEEPALIVE);
        venue.send(encode(&wire::RegisterVenue {})).await.map_err(|e| e.to_string())?;
        loop {
            if let Event::Message(payload) = venue.recv().await.ok_or("venue session ended")?
                && matches!(wire::decode(&payload), Ok((wire::Decoded::VenueRegistered(_), _)))
            {
                break;
            }
        }
        let mut gateway = ClusterClient::connect(addrs, oms::SERVICE_ID, KEEPALIVE);
        let mut seen: BTreeMap<u64, Vec<ExecType>> = BTreeMap::new();
        let (mut sent, mut filled, mut reports) = (0, 0, 0);
        while filled < orders {
            while sent < orders && sent - filled < MAX_OPEN_ORDERS {
                let cl_ord_id = (sent + 1).to_string();
                let order = wire::NewOrder {
                    side: Side::Buy,
                    qty: 100,
                    price: 1000 + i64::try_from(sent % 100).expect("small"),
                    cl_ord_id: cl_ord_id.as_bytes(),
                    symbol: b"ACME",
                };
                match gateway.send(encode(&order)).await {
                    Ok(_) => sent += 1,
                    Err(SendError::Full) => break,
                    Err(e) => return Err(format!("gateway: {e}")),
                }
            }
            tokio::select! {
                event = venue.recv() => play_venue(&venue, event.ok_or("venue session ended")?).await?,
                event = gateway.recv() => {
                    let Some(report) = check_report(&mut seen, event.ok_or("gateway session ended")?)? else { continue };
                    reports += 1;
                    filled += u64::from(report == OrdStatus::Filled);
                }
            }
        }
        Ok::<_, String>((filled, reports))
    };
    let (orders, reports) = tokio::time::timeout(DEADLINE, run).await.map_err(|_| "timed out")??;
    Ok(Summary { orders, reports, elapsed: start.elapsed() })
}

/// The venue: acks each new order and fills it, 60 then 40.
async fn play_venue(venue: &ClusterClient, event: Event) -> Result<(), String> {
    let payload = match event {
        Event::Message(payload) => payload,
        Event::Opened(_) => return Ok(()),
        event => return Err(format!("venue: {event:?}")),
    };
    let Ok((wire::Decoded::VenueNewOrder(o), _)) = wire::decode(&payload) else { return Ok(()) };
    let (order_id, price) = (o.order_id(), o.price());
    for message in [
        encode(&wire::VenueAck { order_id }),
        encode(&wire::Fill { order_id, qty: o.qty() * 6 / 10, price }),
        encode(&wire::Fill { order_id, qty: o.qty() - o.qty() * 6 / 10, price }),
    ] {
        venue.send(message).await.map_err(|e| format!("venue: {e}"))?;
    }
    Ok(())
}

/// Checks a report is the next its order should get, returning the order's status.
fn check_report(seen: &mut BTreeMap<u64, Vec<ExecType>>, event: Event) -> Result<Option<OrdStatus>, String> {
    const SEQUENCE: [ExecType; 4] = [ExecType::PendingNew, ExecType::New, ExecType::Trade, ExecType::Trade];
    let payload = match event {
        Event::Message(payload) => payload,
        Event::Opened(_) => return Ok(None),
        event => return Err(format!("gateway: {event:?}")),
    };
    let Ok((wire::Decoded::ExecReport(r), _)) = wire::decode(&payload) else { return Err("not a report".into()) };
    let cl_ord_id: u64 = std::str::from_utf8(r.cl_ord_id()).ok().and_then(|s| s.parse().ok()).ok_or("bad clOrdId")?;
    let reports = seen.entry(cl_ord_id).or_default();
    if SEQUENCE.get(reports.len()) != Some(&r.exec_type()) {
        return Err(format!("order {cl_ord_id}: {:?} after {reports:?}", r.exec_type()));
    }
    reports.push(r.exec_type());
    Ok(Some(r.ord_status()))
}

#[allow(dead_code, reason = "a test includes this file for `trade`")]
#[tokio::main]
async fn main() -> std::process::ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (Some(addrs), orders) = (args.first(), args.get(1).and_then(|n| n.parse().ok()).unwrap_or(10_000)) else {
        eprintln!("usage: gateway <client addr,...> [orders]");
        return std::process::ExitCode::FAILURE;
    };
    match trade(addrs.split(',').map(String::from).collect(), orders).await {
        Ok(s) => {
            let rate = s.orders as f64 / s.elapsed.as_secs_f64();
            println!("{} orders filled, {} reports, in {:.2?}: {rate:.0} orders/s", s.orders, s.reports, s.elapsed);
            std::process::ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("failed: {error}");
            std::process::ExitCode::FAILURE
        }
    }
}
