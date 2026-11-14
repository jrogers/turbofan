//! A matching engine (#32): limit and market orders on any number of symbols, matched by price
//! and then time, cancelled and modified, each one's events sent to the session that placed it.
//!
//! Its nodes run as any service's ([`turbofan::run`]). `client` sends the lines typed into it and
//! prints the events that come back:
//!
//! ```sh
//! cargo run --example matching -- serve --id 1 --data /tmp/matching/1 \
//!     --peers 1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003 \
//!     --clients 1=127.0.0.1:7101,2=127.0.0.1:7102,3=127.0.0.1:7103 \
//!     --admin 1=127.0.0.1:7201,2=127.0.0.1:7202,3=127.0.0.1:7203
//! cargo run --example matching -- client --clients 127.0.0.1:7101,127.0.0.1:7102,127.0.0.1:7103
//! ```
//!
//! A request is a line, quantities and prices whole numbers above zero (prices in ticks):
//!
//! - `buy <symbol> <quantity> [<price>]`, `sell ...`: a limit order, or a market order without a
//!   price. A market order takes what the book has, and what it can't fill is cancelled.
//! - `cancel <id>`
//! - `modify <id> <quantity> <price>`: a resting order's new size and price. Only a smaller one at
//!   the same price keeps its place in the queue.
//! - `book <symbol>`: the best price levels on each side, as a query, never logged.
//!
//! The events, each a line to the session whose order it concerns:
//!
//! - `accepted <id> <side> <symbol> <quantity> <price|market>`
//! - `fill <id> <quantity> <price> <left>`: to both sides, at the resting order's price.
//! - `modified <id> <quantity> <price>`
//! - `cancelled <id> <left>`
//! - `rejected <why>`
//!
//! A session's resting orders are cancelled when it closes: there's no one left to tell of fills.

use std::collections::BTreeMap;
use std::io::{self, Read};
use std::process::ExitCode;

use turbofan::{CloseReason, Ctx, Service, SessionId, SnapshotWriter};

/// This cluster's `Hello.service_id`.
const SERVICE_ID: u32 = 11;
/// The most orders resting at once, across every symbol: a market order sweeps at most this many,
/// so it sends at most twice this many events, well within a session's egress ring.
const MAX_ORDERS: usize = 10_000;
/// The longest symbol: a ticker.
const MAX_SYMBOL_BYTES: usize = 8;
/// The most price levels a side of `book` shows: its answer stays well within a payload.
const MAX_BOOK_LEVELS: usize = 10;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Side {
    Buy,
    Sell,
}

impl Side {
    fn from_name(name: &str) -> Option<Self> {
        match name {
            "buy" => Some(Self::Buy),
            "sell" => Some(Self::Sell),
            _ => None,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Buy => "buy",
            Self::Sell => "sell",
        }
    }

    /// Whether a limit of `limit` on this side trades with an order resting at `resting`.
    fn crosses(self, limit: u64, resting: u64) -> bool {
        match self {
            Self::Buy => limit >= resting,
            Self::Sell => limit <= resting,
        }
    }

    /// Where an order sits in its side of the book: best price first, then oldest first.
    fn key(self, price: u64, seq: u64) -> (u64, u64) {
        match self {
            Self::Buy => (u64::MAX - price, seq),
            Self::Sell => (price, seq),
        }
    }
}

/// An order resting in a book.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Order {
    session: SessionId,
    side: Side,
    symbol: String,
    price: u64,
    quantity: u64,
    /// Its place in the queue at its price: later orders, and orders that lost their place, have
    /// higher ones.
    seq: u64,
}

impl Order {
    fn key(&self) -> (u64, u64) {
        self.side.key(self.price, self.seq)
    }
}

/// A symbol's resting orders' ids, by [`Side::key`].
#[derive(Debug, Default, PartialEq, Eq)]
struct Book {
    bids: BTreeMap<(u64, u64), u64>,
    asks: BTreeMap<(u64, u64), u64>,
}

impl Book {
    fn side(&mut self, side: Side) -> &mut BTreeMap<(u64, u64), u64> {
        match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        }
    }
}

#[derive(Debug, Default)]
struct Matching {
    /// The last order id given.
    last_id: u64,
    /// The last place in a queue given.
    last_seq: u64,
    /// Every resting order, by id.
    orders: BTreeMap<u64, Order>,
    /// Every symbol with an order resting: an empty book is removed.
    books: BTreeMap<String, Book>,
}

impl Service for Matching {
    fn on_message(&mut self, ctx: &mut Ctx, session: SessionId, payload: &[u8]) {
        match parse(payload) {
            Err(why) => send(ctx, session, format!("rejected {why}")),
            Ok(Request::Place { side, symbol, quantity, limit }) => {
                if limit.is_some() && self.orders.len() == MAX_ORDERS {
                    return send(ctx, session, "rejected book-full".to_owned());
                }
                self.last_id += 1;
                let id = self.last_id;
                let price = limit.map_or_else(|| "market".to_owned(), |p| p.to_string());
                send(ctx, session, format!("accepted {id} {} {symbol} {quantity} {price}", side.name()));
                self.execute(ctx, id, session, side, symbol, quantity, limit);
            }
            Ok(Request::Cancel { id }) => match self.take(session, id) {
                Some(order) => send(ctx, session, format!("cancelled {id} {}", order.quantity)),
                None => send(ctx, session, "rejected unknown-order".to_owned()),
            },
            Ok(Request::Modify { id, quantity, price }) => {
                let Some(mut order) = self.take(session, id) else {
                    return send(ctx, session, "rejected unknown-order".to_owned());
                };
                send(ctx, session, format!("modified {id} {quantity} {price}"));
                if price == order.price && quantity <= order.quantity {
                    order.quantity = quantity;
                    self.rest(id, order);
                } else {
                    self.execute(ctx, id, session, order.side, order.symbol, quantity, Some(price));
                }
            }
        }
    }

    fn on_query(&self, _session: SessionId, query: &[u8]) -> Option<Vec<u8>> {
        let symbol = std::str::from_utf8(query).ok()?.strip_prefix("book ").filter(|s| valid_symbol(s))?;
        let Some(book) = self.books.get(symbol) else { return Some(b"bids\nasks".to_vec()) };
        let levels = |side: &BTreeMap<(u64, u64), u64>| {
            let mut levels: Vec<(u64, u64)> = Vec::new();
            for id in side.values() {
                let order = &self.orders[id];
                if let Some((price, quantity)) = levels.last_mut()
                    && *price == order.price
                {
                    *quantity += order.quantity;
                } else if levels.len() < MAX_BOOK_LEVELS {
                    levels.push((order.price, order.quantity));
                } else {
                    break;
                }
            }
            levels.iter().map(|(price, quantity)| format!(" {quantity}@{price}")).collect::<String>()
        };
        Some(format!("bids{}\nasks{}", levels(&book.bids), levels(&book.asks)).into_bytes())
    }

    fn on_session_close(&mut self, _ctx: &mut Ctx, session: SessionId, _reason: CloseReason) {
        // ponytail: a scan of every resting order, at most MAX_ORDERS; an index by session if
        // sessions come and go often.
        let ids: Vec<u64> = self.orders.iter().filter(|(_, o)| o.session == session).map(|(id, _)| *id).collect();
        for id in ids {
            self.take(session, id).expect("the order rests, and is the session's");
        }
    }

    /// The resting orders as text, a line each, after the last id and seq given.
    fn snapshot(&self) -> SnapshotWriter {
        let mut text = format!("{} {}\n", self.last_id, self.last_seq);
        for (id, o) in &self.orders {
            let (session, side, symbol) = (o.session.0, o.side.name(), &o.symbol);
            text += &format!("{id} {session} {side} {symbol} {} {} {}\n", o.price, o.quantity, o.seq);
        }
        Box::new(move |out| out.write_all(text.as_bytes()))
    }

    fn restore(&mut self, from: &mut dyn Read) -> io::Result<()> {
        let mut text = String::new();
        from.read_to_string(&mut text)?;
        let damaged = || io::Error::new(io::ErrorKind::InvalidData, "a damaged matching snapshot");
        let mut lines = text.lines();
        let header = lines.next().and_then(|l| l.split_once(' '));
        let (last_id, last_seq) =
            header.and_then(|(i, s)| Some((i.parse().ok()?, s.parse().ok()?))).ok_or_else(damaged)?;
        let mut restored = Self { last_id, last_seq, ..Self::default() };
        let mut seqs = std::collections::BTreeSet::new();
        for line in lines {
            let (id, order) = parse_order(line).ok_or_else(damaged)?;
            let fresh = !restored.orders.contains_key(&id) && seqs.insert(order.seq);
            if !fresh || id > last_id || order.seq > last_seq {
                return Err(damaged());
            }
            restored.rest(id, order);
        }
        if !text.ends_with('\n') {
            return Err(damaged());
        }
        *self = restored;
        Ok(())
    }
}

impl Matching {
    /// Matches an order against the other side of its book, best price then oldest first, and
    /// rests what's left of it at its limit, or cancels what's left of a market order.
    #[expect(clippy::too_many_arguments, reason = "an order's fields, before it is one")]
    fn execute(
        &mut self,
        ctx: &mut Ctx,
        id: u64,
        session: SessionId,
        side: Side,
        symbol: String,
        mut quantity: u64,
        limit: Option<u64>,
    ) {
        if let Some(book) = self.books.get_mut(&symbol) {
            let other = book.side(match side {
                Side::Buy => Side::Sell,
                Side::Sell => Side::Buy,
            });
            while quantity > 0 {
                let Some(best) = other.first_entry() else { break };
                let resting = self.orders.get_mut(best.get()).expect("a book holds only resting orders");
                if limit.is_some_and(|limit| !side.crosses(limit, resting.price)) {
                    break;
                }
                let filled = quantity.min(resting.quantity);
                quantity -= filled;
                resting.quantity -= filled;
                let (resting_id, price) = (*best.get(), resting.price);
                send(ctx, resting.session, format!("fill {resting_id} {filled} {price} {}", resting.quantity));
                send(ctx, session, format!("fill {id} {filled} {price} {quantity}"));
                if resting.quantity == 0 {
                    best.remove();
                    self.orders.remove(&resting_id);
                }
            }
            if book.bids.is_empty() && book.asks.is_empty() {
                self.books.remove(&symbol);
            }
        }
        match limit {
            _ if quantity == 0 => {}
            Some(price) => {
                self.last_seq += 1;
                let seq = self.last_seq;
                self.rest(id, Order { session, side, symbol, price, quantity, seq });
            }
            None => send(ctx, session, format!("cancelled {id} {quantity}")),
        }
    }

    fn rest(&mut self, id: u64, order: Order) {
        debug_assert!(order.quantity > 0);
        let book = self.books.entry(order.symbol.clone()).or_default();
        let replaced = book.side(order.side).insert(order.key(), id);
        debug_assert!(replaced.is_none(), "each order has its own seq");
        let replaced = self.orders.insert(id, order);
        debug_assert!(replaced.is_none(), "each order has its own id");
    }

    /// Takes a session's resting order out of its book, if it's there and the session's.
    fn take(&mut self, session: SessionId, id: u64) -> Option<Order> {
        if self.orders.get(&id)?.session != session {
            return None;
        }
        let order = self.orders.remove(&id).expect("just found");
        let book = self.books.get_mut(&order.symbol).expect("a resting order has a book");
        book.side(order.side).remove(&order.key()).expect("a resting order is in its book");
        if book.bids.is_empty() && book.asks.is_empty() {
            self.books.remove(&order.symbol);
        }
        Some(order)
    }
}

fn send(ctx: &mut Ctx, session: SessionId, event: String) {
    ctx.send(session, event).expect("an event is a few bounded fields");
}

#[derive(Debug, PartialEq, Eq)]
enum Request {
    Place { side: Side, symbol: String, quantity: u64, limit: Option<u64> },
    Cancel { id: u64 },
    Modify { id: u64, quantity: u64, price: u64 },
}

/// A request, from its text. Untrusted, from any client: refused, never panicked on.
fn parse(text: &[u8]) -> Result<Request, &'static str> {
    let text = std::str::from_utf8(text).map_err(|_| "malformed")?;
    let number = |word: &str| word.parse::<u64>().ok().filter(|n| *n > 0).ok_or("malformed");
    let place = |side: &str, symbol: &str, quantity, limit| match (Side::from_name(side), valid_symbol(symbol)) {
        (Some(side), true) => Ok(Request::Place { side, symbol: symbol.to_owned(), quantity, limit }),
        _ => Err("malformed"),
    };
    // The words of a payload, at most MAX_PAYLOAD_BYTES of them.
    match text.split_ascii_whitespace().collect::<Vec<_>>()[..] {
        [side @ ("buy" | "sell"), symbol, quantity] => place(side, symbol, number(quantity)?, None),
        [side @ ("buy" | "sell"), symbol, quantity, price] => {
            place(side, symbol, number(quantity)?, Some(number(price)?))
        }
        ["cancel", id] => Ok(Request::Cancel { id: number(id)? }),
        ["modify", id, quantity, price] => {
            Ok(Request::Modify { id: number(id)?, quantity: number(quantity)?, price: number(price)? })
        }
        _ => Err("malformed"),
    }
}

fn valid_symbol(symbol: &str) -> bool {
    (1..=MAX_SYMBOL_BYTES).contains(&symbol.len()) && symbol.bytes().all(|b| b.is_ascii_alphanumeric())
}

/// A snapshot's line: `<id> <session> <side> <symbol> <price> <quantity> <seq>`.
fn parse_order(line: &str) -> Option<(u64, Order)> {
    let words: Vec<&str> = line.split(' ').collect();
    let [id, session, side, symbol, price, quantity, seq] = words[..] else { return None };
    let number = |w: &str| w.parse::<u64>().ok();
    let side = Side::from_name(side)?;
    let (price, quantity) = (number(price).filter(|p| *p > 0)?, number(quantity).filter(|q| *q > 0)?);
    let order = Order {
        session: SessionId(number(session)?),
        side,
        symbol: symbol.to_owned(),
        price,
        quantity,
        seq: number(seq)?,
    };
    valid_symbol(symbol).then_some((number(id)?, order))
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.first().map(String::as_str) == Some("client") {
        return client::main(&args[1..]);
    }
    turbofan::run(SERVICE_ID, Matching::default())
}

/// A session with the cluster: each line typed is a request (`book` a query), and each event is
/// printed as it comes.
mod client {
    use std::io::BufRead;
    use std::process::ExitCode;
    use std::time::Duration;

    use tokio::sync::mpsc;
    use turbofan::ClusterClient;
    use turbofan::client::Event;

    use super::SERVICE_ID;

    /// As the client guide's examples.
    const KEEPALIVE: Duration = Duration::from_millis(500);
    /// Lines typed ahead of the cluster: a person types far fewer.
    const MAX_QUEUED_LINES: usize = 64;

    pub fn main(args: &[String]) -> ExitCode {
        let Some(["--clients", clients]) = args.get(..2).map(|a| [a[0].as_str(), a[1].as_str()]) else {
            eprintln!("usage: matching client --clients <addr>,<addr>,...");
            return ExitCode::from(2);
        };
        let (lines, mut typed) = mpsc::channel(MAX_QUEUED_LINES);
        std::thread::spawn(move || {
            for line in std::io::stdin().lock().lines().map_while(Result::ok) {
                if lines.blocking_send(line).is_err() {
                    break;
                }
            }
        });
        let runtime = tokio::runtime::Runtime::new().expect("a runtime starts");
        runtime.block_on(async {
            let mut session = match ClusterClient::connect(clients.split(','), SERVICE_ID, Vec::new(), KEEPALIVE) {
                Ok(session) => session,
                Err(e) => {
                    eprintln!("matching client: {e}");
                    return ExitCode::FAILURE;
                }
            };
            let (mut applied, mut last_sent) = (session.applied(), 0);
            loop {
                tokio::select! {
                    line = typed.recv() => match line {
                        None => break,
                        Some(line) if line.starts_with("book ") => {
                            // A query isn't ordered with the messages: it sees what was sent
                            // before it once that's applied.
                            if applied.wait_for(|applied| *applied >= last_sent).await.is_err() {
                                break;
                            }
                            // Their events first: they've arrived, and wait only to be printed.
                            while let Ok(event) = tokio::time::timeout(Duration::ZERO, session.recv()).await {
                                if !print(event) {
                                    return ExitCode::FAILURE;
                                }
                            }
                            match session.query(line).await {
                                Ok(answer) => println!("{}", String::from_utf8_lossy(&answer)),
                                Err(e) => eprintln!("query: {e}"),
                            }
                        }
                        Some(line) => match session.send(line).await {
                            Ok(seq) => last_sent = seq,
                            Err(e) => eprintln!("send: {e}"),
                        },
                    },
                    event = session.recv() => if !print(event) {
                        return ExitCode::FAILURE;
                    },
                }
            }
            session.close().await;
            ExitCode::SUCCESS
        })
    }

    /// Prints an event: false once the session is over.
    fn print(event: Option<Event>) -> bool {
        match event {
            Some(Event::Message(event)) => println!("{}", String::from_utf8_lossy(&event)),
            Some(Event::Closed { .. }) | None => {
                eprintln!("the session closed");
                return false;
            }
            Some(other) => eprintln!("{other:?}"),
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use turbofan::Effect;

    use super::*;

    const ALICE: SessionId = SessionId(1);
    const BOB: SessionId = SessionId(2);

    /// The events a request sends, each with the session it goes to.
    fn request(service: &mut Matching, session: SessionId, text: &str) -> Vec<(u64, String)> {
        let mut ctx = Ctx::new(0);
        service.on_message(&mut ctx, session, text.as_bytes());
        ctx.take_effects()
            .into_iter()
            .map(|effect| match effect {
                Effect::Send(to, event) => (to.0, String::from_utf8(event).unwrap()),
                other => panic!("{other:?}"),
            })
            .collect()
    }

    fn book(service: &Matching, symbol: &str) -> String {
        String::from_utf8(service.on_query(ALICE, format!("book {symbol}").as_bytes()).unwrap()).unwrap()
    }

    #[test]
    fn orders_match_by_price_then_time_at_the_resting_price() {
        let mut service = Matching::default();
        request(&mut service, ALICE, "sell ACME 10 101");
        request(&mut service, ALICE, "sell ACME 10 100");
        request(&mut service, BOB, "sell ACME 5 100");
        request(&mut service, BOB, "sell OTHER 5 50");
        assert_eq!(book(&service, "ACME"), "bids\nasks 15@100 10@101");
        let events = request(&mut service, BOB, "buy ACME 30 101");
        assert_eq!(
            events,
            [
                (2, "accepted 5 buy ACME 30 101".to_owned()),
                (1, "fill 2 10 100 0".to_owned()),
                (2, "fill 5 10 100 20".to_owned()),
                (2, "fill 3 5 100 0".to_owned()),
                (2, "fill 5 5 100 15".to_owned()),
                (1, "fill 1 10 101 0".to_owned()),
                (2, "fill 5 10 101 5".to_owned()),
            ]
        );
        assert_eq!(book(&service, "ACME"), "bids 5@101\nasks");
        assert_eq!(book(&service, "OTHER"), "bids\nasks 5@50", "another symbol's book is its own");
        assert_eq!(book(&service, "NONE"), "bids\nasks");
    }

    #[test]
    fn a_market_order_takes_what_there_is_and_cancels_the_rest() {
        let mut service = Matching::default();
        request(&mut service, ALICE, "buy ACME 3 99");
        request(&mut service, ALICE, "buy ACME 3 98");
        let events = request(&mut service, BOB, "sell ACME 10");
        assert_eq!(events[0], (2, "accepted 3 sell ACME 10 market".to_owned()));
        assert_eq!(events[1..].iter().filter(|(to, _)| *to == 1).count(), 2);
        assert_eq!(events.last().unwrap(), &(2, "cancelled 3 4".to_owned()));
        assert!(service.books.is_empty(), "an empty book is removed");
        assert!(service.orders.is_empty(), "a market order never rests");
    }

    #[test]
    fn a_smaller_modify_keeps_its_place_and_any_other_loses_it() {
        let mut service = Matching::default();
        request(&mut service, ALICE, "buy ACME 10 100");
        request(&mut service, BOB, "buy ACME 10 100");
        assert_eq!(request(&mut service, ALICE, "modify 1 5 100"), [(1, "modified 1 5 100".to_owned())]);
        let fills = request(&mut service, BOB, "sell ACME 1 100");
        assert_eq!(fills[1], (1, "fill 1 1 100 4".to_owned()), "alice is still first");
        request(&mut service, ALICE, "modify 1 6 100");
        let fills = request(&mut service, BOB, "sell ACME 1 100");
        assert_eq!(fills[1], (2, "fill 2 1 100 9".to_owned()), "a bigger order goes to the back");
        // A new price that crosses trades at once.
        request(&mut service, BOB, "sell ACME 2 101");
        let events = request(&mut service, ALICE, "modify 1 6 101");
        assert_eq!(events[1], (2, "fill 5 2 101 0".to_owned()));
        assert_eq!(book(&service, "ACME"), "bids 4@101 9@100\nasks");
    }

    #[test]
    fn only_its_session_cancels_an_order_and_closing_cancels_them_all() {
        let mut service = Matching::default();
        request(&mut service, ALICE, "buy ACME 10 100");
        request(&mut service, ALICE, "sell ACME 10 105");
        request(&mut service, BOB, "buy ACME 1 99");
        assert_eq!(request(&mut service, BOB, "cancel 1"), [(2, "rejected unknown-order".to_owned())]);
        assert_eq!(request(&mut service, BOB, "modify 1 1 1"), [(2, "rejected unknown-order".to_owned())]);
        assert_eq!(request(&mut service, ALICE, "cancel 1"), [(1, "cancelled 1 10".to_owned())]);
        assert_eq!(request(&mut service, ALICE, "cancel 1"), [(1, "rejected unknown-order".to_owned())]);
        service.on_session_close(&mut Ctx::new(0), ALICE, CloseReason::Client);
        assert_eq!(book(&service, "ACME"), "bids 1@99\nasks");
    }

    #[test]
    fn a_malformed_request_is_rejected_and_changes_nothing() {
        let mut service = Matching::default();
        for bad in [
            "",
            "buy",
            "buy ACME",
            "buy ACME 0",
            "buy ACME 1 0",
            "buy ACME -1",
            "buy ACME 1 2 3",
            "buy TOOLONGSYM 1",
            "buy AC.ME 1",
            "sell ACME x",
            "cancel",
            "cancel 1 2",
            "modify 1 2",
            "modify 1 2 3 4",
            "hold ACME 1",
            "buy ACME 99999999999999999999",
        ] {
            assert_eq!(request(&mut service, ALICE, bad), [(1, "rejected malformed".to_owned())], "{bad:?}");
        }
        assert_eq!(request(&mut service, ALICE, "buy \u{ff} 1"), [(1, "rejected malformed".to_owned())]);
        assert_eq!(service.last_id, 0);
        assert_eq!(service.on_query(ALICE, b"book TOOLONGSYM"), None);
    }

    #[test]
    fn a_full_book_refuses_a_limit_order_but_not_a_market_one() {
        let mut service = Matching::default();
        for _ in 0..MAX_ORDERS {
            request(&mut service, ALICE, "buy ACME 1 100");
        }
        assert_eq!(request(&mut service, BOB, "buy ACME 1 100"), [(2, "rejected book-full".to_owned())]);
        assert_eq!(request(&mut service, BOB, "sell ACME 1").len(), 3);
    }

    #[test]
    fn a_snapshot_restores_the_same_books_and_a_damaged_one_is_refused() {
        let mut service = Matching::default();
        request(&mut service, ALICE, "buy ACME 10 100");
        request(&mut service, BOB, "buy ACME 10 100");
        request(&mut service, BOB, "sell OTHER 3 7");
        request(&mut service, ALICE, "modify 1 11 100");
        let mut bytes = Vec::new();
        service.snapshot()(&mut bytes).unwrap();
        let mut restored = Matching::default();
        restored.restore(&mut bytes.as_slice()).unwrap();
        assert_eq!((restored.last_id, restored.last_seq), (service.last_id, service.last_seq));
        assert_eq!(restored.orders, service.orders);
        assert_eq!(restored.books, service.books);
        for cut in [0, 1, 2, bytes.len() - 1] {
            assert!(Matching::default().restore(&mut &bytes[..cut]).is_err(), "cut at {cut}");
        }
        assert!(Matching::default().restore(&mut &b"1 1\n5 1 buy ACME 1 1 1\n"[..]).is_err(), "an id from the future");
    }
}
