//! A matching engine (#32): limit and market orders on any number of symbols, matched by price
//! and then time, cancelled and modified, each one's events sent to the session that placed it.
//!
//! Its messages are SBE, from `schema.xml`: requests, the events sent back, the `book` query and
//! its answer, and its snapshot. `codec.rs` is generated from it, and checked in (an example has no `build.rs`
//! of its own); a test says when it's stale. Its nodes run as any service's ([`turbofan::run`]).
//! `client` turns each line typed into it into a request, and prints the events that come back as
//! lines:
//!
//! ```sh
//! cargo run --example matching -- serve --id 1 --data /tmp/matching/1 \
//!     --peers 1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003 \
//!     --clients 1=127.0.0.1:7101,2=127.0.0.1:7102,3=127.0.0.1:7103 \
//!     --admin 1=127.0.0.1:7201,2=127.0.0.1:7202,3=127.0.0.1:7203
//! cargo run --example matching -- client --clients 127.0.0.1:7101,127.0.0.1:7102,127.0.0.1:7103
//! ```
//!
//! A request, as a line typed into `client`, quantities and prices whole numbers above zero (prices
//! in ticks):
//!
//! - `buy <symbol> <quantity> [<price>] [<time in force>]`, `sell ...`: a limit order, or a market
//!   order without a price. The time in force says what becomes of what doesn't trade at once:
//!   - none: a limit order rests until it's filled or cancelled; a market order is cancelled.
//!   - `ioc`, immediate or cancel: it's cancelled.
//!   - `fok`, fill or kill: the order is cancelled whole unless it can be filled whole at once.
//!   - `post`, post only (a limit order): it's cancelled whole if it would trade at all, so it only
//!     ever rests.
//!   - `gtd <seconds>`, good till date (a limit order): it rests, and expires after that long.
//!
//!   An order that rests can end in `show <quantity>`, an iceberg: the book shows only that much of
//!   it at a time, and when that's filled, shows that much more of what's left, at the back of the
//!   queue at its price.
//!
//!   `stop <trigger>` after the quantity makes it a stop order: it waits, off the book, until its
//!   symbol trades at the trigger or beyond (at or above it for a buy, at or below for a sell),
//!   then comes in as the market or limit order it is. One that's already been reached comes in at
//!   once; one whose trades set off more stops sets them off in turn, oldest first.
//! - `cancel <id>`: a resting order, or a stop still waiting.
//! - `modify <id> <quantity> <price>`: a resting order's new size (all that's left of it, shown
//!   or not) and price. Only a smaller one at the same price keeps its place in the queue; any other
//!   trades as a new limit order would, keeping its expiry and what it shows. A waiting stop is
//!   cancelled and placed again instead.
//! - `book <symbol>`: the best price levels on each side, as a query, never logged.
//!
//! The events, each to the session whose order it concerns, as `client` prints them:
//!
//! - `accepted <id> <side> <symbol> <quantity> <price|market> [stop <trigger>]`
//! - `triggered <id>`: a stop came in.
//! - `fill <id> <quantity> <price> <left>`: to both sides, at the resting order's price.
//! - `modified <id> <quantity> <price>`
//! - `cancelled <id> <left>`
//! - `expired <id> <left>`
//! - `rejected <why>`
//!
//! An order never trades with one of its own session's: the resting one is cancelled instead
//! (self-trade prevention). A session's resting orders are cancelled when it closes: there's no
//! one left to tell of fills.

use std::collections::BTreeMap;
use std::io::{self, Read};
use std::process::ExitCode;

use turbofan::{CloseReason, Ctx, Service, SessionId, SnapshotWriter};
use turbofan_sbe::Encode;

use codec::{Decoded, RejectReason};

#[allow(dead_code, clippy::all, clippy::pedantic, reason = "generated, by turbofan_sbe::codegen")]
mod codec;

/// This cluster's `Hello.service_id`.
const SERVICE_ID: u32 = 11;
/// The most orders resting or waiting at once, across every symbol, an iceberg counting once for
/// each time it will show: a market order sweeps at most this many, so it sends at most twice this
/// many events. That's more than a session's egress ring keeps (8192 by default): a client that
/// falls that far behind misses some, and is told so.
const MAX_ORDERS: u64 = 10_000;
/// The longest symbol: a ticker.
const MAX_SYMBOL_BYTES: usize = 8;
/// The most price levels a side of `book` shows: its answer stays well within a payload.
const MAX_BOOK_LEVELS: usize = 10;
/// Cluster time's unit, a nanosecond, in a second.
const SECOND: u64 = 1_000_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Side {
    Buy,
    Sell,
}

impl Side {
    fn other(self) -> Self {
        match self {
            Self::Buy => Self::Sell,
            Self::Sell => Self::Buy,
        }
    }

    fn from_wire(side: codec::Side) -> Option<Self> {
        match side {
            codec::Side::Buy => Some(Self::Buy),
            codec::Side::Sell => Some(Self::Sell),
            codec::Side::Unknown(_) => None,
        }
    }

    fn wire(self) -> codec::Side {
        match self {
            Self::Buy => codec::Side::Buy,
            Self::Sell => codec::Side::Sell,
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

/// What becomes of the part of an order that doesn't trade at once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TimeInForce {
    /// Good till cancelled: it rests.
    Gtc,
    /// Immediate or cancel.
    Ioc,
    /// Fill or kill: all at once, or none.
    Fok,
    /// Post only: never trades on arrival, only rests.
    Post,
    /// Good till date: in a request, it rests for this many seconds; once placed, until this
    /// cluster time.
    Gtd(u64),
}

impl TimeInForce {
    fn wire(self) -> codec::TimeInForce {
        match self {
            Self::Gtc => codec::TimeInForce::Gtc,
            Self::Ioc => codec::TimeInForce::Ioc,
            Self::Fok => codec::TimeInForce::Fok,
            Self::Post => codec::TimeInForce::Post,
            Self::Gtd(_) => codec::TimeInForce::Gtd,
        }
    }

    /// Whether what doesn't trade at once rests, if it has a limit.
    fn rests(self) -> bool {
        matches!(self, Self::Gtc | Self::Post | Self::Gtd(_))
    }
}

/// How many times an order of `quantity` showing `show` at a time (0: all of it) shows: how many
/// of [`MAX_ORDERS`] it takes.
fn tranches(quantity: u64, show: u64) -> u64 {
    if show == 0 { 1 } else { quantity.div_ceil(show) }
}

/// An order as it comes in, or back from a modify or a stop, before it trades.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Incoming {
    id: u64,
    session: SessionId,
    side: Side,
    symbol: String,
    quantity: u64,
    limit: Option<u64>,
    /// Placed: a `gtd`'s is its expiry.
    tif: TimeInForce,
    /// As [`Order::show`].
    show: u64,
}

impl Incoming {
    /// As [`Order::expires`].
    fn expires(&self) -> Option<u64> {
        match self.tif {
            TimeInForce::Gtd(at) => Some(at),
            _ => None,
        }
    }
}

/// A stop order, waiting for its symbol to trade at `trigger` or beyond.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Stop {
    trigger: u64,
    order: Incoming,
}

impl Stop {
    /// Where it waits in [`Matching::triggers`]: the next to go first, then the oldest.
    fn key(&self) -> (String, Side, (u64, u64)) {
        let side = self.order.side;
        (self.order.symbol.clone(), side, side.other().key(self.trigger, self.order.id))
    }
}

/// An order resting in a book.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Order {
    session: SessionId,
    side: Side,
    symbol: String,
    price: u64,
    /// What the book shows of it.
    quantity: u64,
    /// What it doesn't: an iceberg's.
    hidden: u64,
    /// How much an iceberg shows at a time, or 0.
    show: u64,
    /// Its place in the queue at its price: later orders, and orders that lost their place, have
    /// higher ones.
    seq: u64,
    /// The cluster time it expires at, a timer with its id: a `gtd` order's.
    expires: Option<u64>,
}

impl Order {
    fn key(&self) -> (u64, u64) {
        self.side.key(self.price, self.seq)
    }

    fn left(&self) -> u64 {
        self.quantity + self.hidden
    }

    /// How many more times it shows, this one included: its share of [`MAX_ORDERS`].
    fn tranches(&self) -> u64 {
        1 + if self.show == 0 { 0 } else { self.hidden.div_ceil(self.show) }
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
    /// Every stop waiting, by id.
    stops: BTreeMap<u64, Stop>,
    /// Every stop waiting, by [`Stop::key`].
    triggers: std::collections::BTreeSet<(String, Side, (u64, u64))>,
    /// Each symbol's last trade's price.
    last: BTreeMap<String, u64>,
    /// The resting orders' [`Order::tranches`], and the waiting stops' [`tranches`], summed.
    tranches: u64,
}

impl Service for Matching {
    fn on_message(&mut self, ctx: &mut Ctx, session: SessionId, payload: &[u8]) {
        match parse(payload) {
            Err(reason) => send(ctx, session, &codec::Rejected { reason }),
            Ok(Request::Place { side, symbol, quantity, limit, tif, show, stop }) => {
                let waits = tif.rests() || stop.is_some();
                if waits && self.tranches + tranches(quantity, show) > MAX_ORDERS {
                    return send(ctx, session, &codec::Rejected { reason: RejectReason::BookFull });
                }
                self.last_id += 1;
                let id = self.last_id;
                let wire_symbol = turbofan_sbe::pad(symbol.as_bytes());
                let side_ = side.wire();
                send(
                    ctx,
                    session,
                    &codec::Accepted { order_id: id, side: side_, symbol: wire_symbol, quantity, price: limit, stop },
                );
                let tif = match tif {
                    TimeInForce::Gtd(seconds) => {
                        TimeInForce::Gtd(ctx.now().saturating_add(seconds.saturating_mul(SECOND)))
                    }
                    tif => tif,
                };
                let order = Incoming { id, session, side, symbol: symbol.clone(), quantity, limit, tif, show };
                match stop {
                    // It waits, if only until `fire` finds it already reached.
                    Some(trigger) => self.wait(ctx, Stop { trigger, order }),
                    None => self.execute(ctx, order),
                }
                self.fire(ctx, &symbol);
            }
            Ok(Request::Cancel { id }) => {
                let left = match self.stops.get(&id) {
                    Some(stop) if stop.order.session == session => {
                        let stop = self.unwait(id);
                        if stop.order.expires().is_some() {
                            ctx.cancel_timer(id);
                        }
                        Some(stop.order.quantity)
                    }
                    _ => self.take(ctx, session, id).map(|order| order.left()),
                };
                match left {
                    Some(left) => send(ctx, session, &codec::Cancelled { order_id: id, left }),
                    None => send(ctx, session, &codec::Rejected { reason: RejectReason::UnknownOrder }),
                }
            }
            Ok(Request::Modify { id, quantity, price }) => {
                let Some(order) = self.orders.get_mut(&id).filter(|o| o.session == session) else {
                    return send(ctx, session, &codec::Rejected { reason: RejectReason::UnknownOrder });
                };
                if self.tranches - order.tranches() + tranches(quantity, order.show) > MAX_ORDERS {
                    return send(ctx, session, &codec::Rejected { reason: RejectReason::BookFull });
                }
                send(ctx, session, &codec::Modified { order_id: id, quantity, price });
                if price == order.price && quantity <= order.left() {
                    let before = order.tranches();
                    order.quantity = order.quantity.min(quantity);
                    order.hidden = quantity - order.quantity;
                    self.tranches -= before - order.tranches();
                } else {
                    let Order { side, symbol, expires, show, .. } = self.take(ctx, session, id).expect("just found");
                    let (limit, tif) = (Some(price), expires.map_or(TimeInForce::Gtc, TimeInForce::Gtd));
                    self.execute(
                        ctx,
                        Incoming { id, session, side, symbol: symbol.clone(), quantity, limit, tif, show },
                    );
                    self.fire(ctx, &symbol);
                }
            }
        }
    }

    fn on_timer(&mut self, ctx: &mut Ctx, id: u64) {
        // A timer is cancelled with its order, so its order still rests, or its stop waits.
        let (session, left) = if self.stops.contains_key(&id) {
            let stop = self.unwait(id);
            (stop.order.session, stop.order.quantity)
        } else {
            let order = self.remove(id);
            (order.session, order.left())
        };
        send(ctx, session, &codec::Expired { order_id: id, left });
    }

    fn on_query(&self, _session: SessionId, query: &[u8]) -> Option<Vec<u8>> {
        let Ok((Decoded::BookRequest(request), len)) = codec::decode(query) else { return None };
        let symbol = std::str::from_utf8(request.symbol()).ok().filter(|s| valid_symbol(s) && len == query.len())?;
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
            levels
        };
        let (mut bids, mut asks) = (Vec::new(), Vec::new());
        if let Some(book) = self.books.get(symbol) {
            bids =
                levels(&book.bids).into_iter().map(|(price, quantity)| codec::BookBids { price, quantity }).collect();
            asks =
                levels(&book.asks).into_iter().map(|(price, quantity)| codec::BookAsks { price, quantity }).collect();
        }
        let mut answer = Vec::new();
        codec::Book { bids: &bids, asks: &asks }.encode_into(&mut answer).expect("at most MAX_BOOK_LEVELS a side");
        Some(answer)
    }

    fn on_session_close(&mut self, ctx: &mut Ctx, session: SessionId, _reason: CloseReason) {
        // ponytail: a scan of every resting order and waiting stop, at most MAX_ORDERS; an index
        // by session if sessions come and go often.
        let ids: Vec<u64> = self.orders.iter().filter(|(_, o)| o.session == session).map(|(id, _)| *id).collect();
        for id in ids {
            self.take(ctx, session, id).expect("the order rests, and is the session's");
        }
        let stops: Vec<u64> =
            self.stops.iter().filter(|(_, s)| s.order.session == session).map(|(id, _)| *id).collect();
        for id in stops {
            if self.unwait(id).order.expires().is_some() {
                ctx.cancel_timer(id);
            }
        }
    }

    /// The resting orders, waiting stops and last prices, as an SBE `Snapshot`. Their expiry
    /// timers are the cluster's, snapshotted with it.
    fn snapshot(&self) -> SnapshotWriter {
        let orders: Vec<_> = (self.orders.iter())
            .map(|(&order_id, o)| codec::SnapshotOrders {
                order_id,
                session: o.session.0,
                side: o.side.wire(),
                symbol: turbofan_sbe::pad(o.symbol.as_bytes()),
                price: o.price,
                quantity: o.quantity,
                hidden: o.hidden,
                show: o.show,
                seq: o.seq,
                expires: o.expires,
            })
            .collect();
        let stops: Vec<_> = (self.stops.iter())
            .map(|(&order_id, Stop { trigger, order: o })| codec::SnapshotStops {
                order_id,
                session: o.session.0,
                side: o.side.wire(),
                symbol: turbofan_sbe::pad(o.symbol.as_bytes()),
                trigger: *trigger,
                quantity: o.quantity,
                price: o.limit,
                time_in_force: o.tif.wire(),
                expires: o.expires(),
                show: o.show,
            })
            .collect();
        let last_prices: Vec<_> = (self.last.iter())
            .map(|(symbol, &price)| codec::SnapshotLastPrices { symbol: turbofan_sbe::pad(symbol.as_bytes()), price })
            .collect();
        let snapshot = codec::Snapshot {
            last_id: self.last_id,
            last_seq: self.last_seq,
            orders: &orders,
            stops: &stops,
            last_prices: &last_prices,
        };
        let mut bytes = Vec::new();
        snapshot.encode_into(&mut bytes).expect("at most MAX_ORDERS orders and stops, and a u32 of symbols");
        Box::new(move |out| out.write_all(&bytes))
    }

    fn restore(&mut self, from: &mut dyn Read) -> io::Result<()> {
        let mut bytes = Vec::new();
        from.read_to_end(&mut bytes)?;
        let damaged = || io::Error::new(io::ErrorKind::InvalidData, "a damaged matching snapshot");
        let Ok((Decoded::Snapshot(snapshot), len)) = codec::decode(&bytes) else { return Err(damaged()) };
        let (last_id, last_seq) = (snapshot.last_id(), snapshot.last_seq());
        if len != bytes.len() {
            return Err(damaged());
        }
        let mut restored = Self { last_id, last_seq, ..Self::default() };
        let mut seqs = std::collections::BTreeSet::new();
        let fresh = |restored: &Self, id| {
            id <= last_id && !restored.orders.contains_key(&id) && !restored.stops.contains_key(&id)
        };
        for entry in snapshot.orders() {
            let (id, order) = restore_order(entry).ok_or_else(damaged)?;
            if !fresh(&restored, id) || !seqs.insert(order.seq) || order.seq > last_seq {
                return Err(damaged());
            }
            restored.rest(id, order);
        }
        for entry in snapshot.stops() {
            let stop = restore_stop(entry).ok_or_else(damaged)?;
            if !fresh(&restored, stop.order.id) {
                return Err(damaged());
            }
            restored.tranches += tranches(stop.order.quantity, stop.order.show);
            restored.triggers.insert(stop.key());
            restored.stops.insert(stop.order.id, stop);
        }
        for entry in snapshot.last_prices() {
            let symbol = symbol(entry.symbol()).ok_or_else(damaged)?;
            let price = Some(entry.price()).filter(|p| *p > 0).ok_or_else(damaged)?;
            if restored.last.insert(symbol, price).is_some() {
                return Err(damaged());
            }
        }
        *self = restored;
        Ok(())
    }
}

impl Matching {
    /// Matches an order against the other side of its book, best price then oldest first, and
    /// rests what's left of it at its limit, or cancels it, as its time in force says.
    fn execute(&mut self, ctx: &mut Ctx, incoming: Incoming) {
        let expires = incoming.expires();
        let Incoming { id, session, side, symbol, mut quantity, limit, tif, show } = incoming;
        let crosses = |resting: &Order| limit.is_none_or(|limit| side.crosses(limit, resting.price));
        let killed = self.killed(session, side, &symbol, quantity, limit, tif);
        if killed {
            return send(ctx, session, &codec::Cancelled { order_id: id, left: quantity });
        }
        let mut traded = None;
        if let Some(book) = self.books.get_mut(&symbol) {
            let other = book.side(side.other());
            while quantity > 0 {
                let Some(best) = other.first_entry() else { break };
                let resting_id = *best.get();
                let resting = self.orders.get_mut(&resting_id).expect("a book holds only resting orders");
                if !crosses(resting) {
                    break;
                }
                let before = resting.tranches();
                if resting.session == session {
                    // Self-trade prevention: the resting order goes, and the new one goes on.
                    send(ctx, session, &codec::Cancelled { order_id: resting_id, left: resting.left() });
                    (resting.quantity, resting.hidden) = (0, 0);
                } else {
                    let filled = quantity.min(resting.quantity);
                    quantity -= filled;
                    resting.quantity -= filled;
                    let (price, left) = (resting.price, resting.left());
                    traded = Some(price);
                    let fill = codec::Fill { order_id: resting_id, quantity: filled, price, left };
                    send(ctx, resting.session, &fill);
                    send(ctx, session, &codec::Fill { order_id: id, quantity: filled, price, left: quantity });
                }
                if resting.left() == 0 {
                    if resting.expires.is_some() {
                        ctx.cancel_timer(resting_id);
                    }
                    best.remove();
                    self.orders.remove(&resting_id);
                    self.tranches -= before;
                } else if resting.quantity == 0 {
                    // An iceberg shows more, at the back of the queue.
                    resting.quantity = resting.show.min(resting.hidden);
                    resting.hidden -= resting.quantity;
                    self.last_seq += 1;
                    resting.seq = self.last_seq;
                    self.tranches -= before - resting.tranches();
                    let key = resting.key();
                    best.remove();
                    other.insert(key, resting_id);
                }
            }
            if book.bids.is_empty() && book.asks.is_empty() {
                self.books.remove(&symbol);
            }
        }
        if let Some(price) = traded {
            self.last.insert(symbol.clone(), price);
        }
        match limit {
            _ if quantity == 0 => {}
            Some(price) if tif.rests() => {
                self.last_seq += 1;
                let seq = self.last_seq;
                if let Some(at) = expires {
                    ctx.schedule_timer(id, at);
                }
                let shown = if show == 0 { quantity } else { quantity.min(show) };
                let hidden = quantity - shown;
                self.rest(id, Order { session, side, symbol, price, quantity: shown, hidden, show, seq, expires });
            }
            _ => send(ctx, session, &codec::Cancelled { order_id: id, left: quantity }),
        }
    }

    /// Whether a post only order would trade, or a fill or kill one can't be filled whole.
    fn killed(
        &self,
        session: SessionId,
        side: Side,
        symbol: &str,
        quantity: u64,
        limit: Option<u64>,
        tif: TimeInForce,
    ) -> bool {
        let crosses = |resting: &Order| limit.is_none_or(|limit| side.crosses(limit, resting.price));
        // What it would trade with: the other side's orders that cross, but for its own.
        let against = self.books.get(symbol).into_iter().flat_map(|book| match side {
            Side::Buy => book.asks.values(),
            Side::Sell => book.bids.values(),
        });
        let mut against =
            against.map(|id| &self.orders[id]).take_while(|o| crosses(o)).filter(|o| o.session != session);
        match tif {
            TimeInForce::Post => against.next().is_some(),
            // ponytail: a walk of every order it would trade with, at most MAX_ORDERS; a
            // quantity per price level if fill or kill orders are big and common.
            TimeInForce::Fok => against.fold(0, |n: u64, o| n.saturating_add(o.left())) < quantity,
            _ => false,
        }
    }

    /// Brings in each stop on `symbol` its last trade has reached, and each that those trades
    /// reach in turn, oldest first.
    fn fire(&mut self, ctx: &mut Ctx, symbol: &str) {
        while let Some(&last) = self.last.get(symbol) {
            let reached = [Side::Buy, Side::Sell].into_iter().filter_map(|side| {
                let next = self.triggers.range((symbol.to_owned(), side, (0, 0))..).next();
                let (_, _, (_, id)) = next.filter(|(s, d, _)| s == symbol && *d == side)?;
                side.crosses(last, self.stops[id].trigger).then_some(*id)
            });
            let Some(id) = reached.min() else { return };
            let Stop { order, .. } = self.unwait(id);
            if order.expires().is_some() {
                // Back again if it rests.
                ctx.cancel_timer(id);
            }
            send(ctx, order.session, &codec::Triggered { order_id: id });
            self.execute(ctx, order);
        }
    }

    fn wait(&mut self, ctx: &mut Ctx, stop: Stop) {
        if let Some(at) = stop.order.expires() {
            ctx.schedule_timer(stop.order.id, at);
        }
        self.tranches += tranches(stop.order.quantity, stop.order.show);
        self.triggers.insert(stop.key());
        let replaced = self.stops.insert(stop.order.id, stop);
        debug_assert!(replaced.is_none(), "each order has its own id");
    }

    /// Takes a waiting stop out.
    fn unwait(&mut self, id: u64) -> Stop {
        let stop = self.stops.remove(&id).expect("the stop waits");
        self.triggers.remove(&stop.key());
        self.tranches -= tranches(stop.order.quantity, stop.order.show);
        stop
    }

    fn rest(&mut self, id: u64, order: Order) {
        debug_assert!(order.quantity > 0);
        debug_assert!(order.hidden == 0 || order.show > 0, "an iceberg shows some of itself");
        self.tranches += order.tranches();
        let book = self.books.entry(order.symbol.clone()).or_default();
        let replaced = book.side(order.side).insert(order.key(), id);
        debug_assert!(replaced.is_none(), "each order has its own seq");
        let replaced = self.orders.insert(id, order);
        debug_assert!(replaced.is_none(), "each order has its own id");
    }

    /// Takes a session's resting order out of its book, and cancels its expiry, if it's there
    /// and the session's.
    fn take(&mut self, ctx: &mut Ctx, session: SessionId, id: u64) -> Option<Order> {
        if self.orders.get(&id)?.session != session {
            return None;
        }
        let order = self.remove(id);
        if order.expires.is_some() {
            ctx.cancel_timer(id);
        }
        Some(order)
    }

    /// Takes a resting order out of its book.
    fn remove(&mut self, id: u64) -> Order {
        let order = self.orders.remove(&id).expect("the order rests");
        self.tranches -= order.tranches();
        let book = self.books.get_mut(&order.symbol).expect("a resting order has a book");
        book.side(order.side).remove(&order.key()).expect("a resting order is in its book");
        if book.bids.is_empty() && book.asks.is_empty() {
            self.books.remove(&order.symbol);
        }
        order
    }
}

fn send(ctx: &mut Ctx, session: SessionId, event: &impl Encode) {
    let mut payload = Vec::new();
    event.encode_into(&mut payload).expect("an event has no groups or data");
    ctx.send(session, payload).expect("an event is a few fixed fields");
}

#[derive(Debug, PartialEq, Eq)]
enum Request {
    /// `show` is 0 but for an iceberg.
    Place {
        side: Side,
        symbol: String,
        quantity: u64,
        limit: Option<u64>,
        tif: TimeInForce,
        show: u64,
        stop: Option<u64>,
    },
    Cancel {
        id: u64,
    },
    Modify {
        id: u64,
        quantity: u64,
        price: u64,
    },
}

/// A request, from its SBE message. Untrusted, from any client: refused, never panicked on.
fn parse(payload: &[u8]) -> Result<Request, RejectReason> {
    let malformed = RejectReason::Malformed;
    let positive = |n: u64| if n > 0 { Ok(n) } else { Err(malformed) };
    let (message, len) = codec::decode(payload).map_err(|_| malformed)?;
    if len != payload.len() {
        return Err(malformed);
    }
    match message {
        Decoded::NewOrder(order) => {
            let side = Side::from_wire(order.side()).ok_or(malformed)?;
            let symbol = symbol(order.symbol()).ok_or(malformed)?;
            let limit = order.price().map(positive).transpose()?;
            let tif = match (order.time_in_force(), order.seconds()) {
                (codec::TimeInForce::Gtc, None) => TimeInForce::Gtc,
                (codec::TimeInForce::Ioc, None) => TimeInForce::Ioc,
                (codec::TimeInForce::Fok, None) => TimeInForce::Fok,
                (codec::TimeInForce::Post, None) => TimeInForce::Post,
                (codec::TimeInForce::Gtd, Some(seconds)) => TimeInForce::Gtd(positive(seconds)?),
                _ => return Err(malformed),
            };
            let show = order.show().map_or(Ok(0), positive)?;
            // A market order never rests; an iceberg only rests.
            let market_rests = limit.is_none() && tif.rests();
            if market_rests || (show > 0 && !(limit.is_some() && tif.rests())) {
                return Err(malformed);
            }
            let (quantity, stop) = (positive(order.quantity())?, order.stop().map(positive).transpose()?);
            Ok(Request::Place { side, symbol, quantity, limit, tif, show, stop })
        }
        Decoded::CancelOrder(cancel) => Ok(Request::Cancel { id: cancel.order_id() }),
        Decoded::ModifyOrder(modify) => Ok(Request::Modify {
            id: modify.order_id(),
            quantity: positive(modify.quantity())?,
            price: positive(modify.price())?,
        }),
        // An event, or a query sent as a message.
        _ => Err(malformed),
    }
}

fn valid_symbol(symbol: &str) -> bool {
    (1..=MAX_SYMBOL_BYTES).contains(&symbol.len()) && symbol.bytes().all(|b| b.is_ascii_alphanumeric())
}

/// A symbol, from its char array, if it's a valid one.
fn symbol(chars: &[u8]) -> Option<String> {
    std::str::from_utf8(chars).ok().filter(|s| valid_symbol(s)).map(str::to_owned)
}

/// A snapshot's resting order, and its id.
fn restore_order(entry: codec::SnapshotOrdersRef<'_>) -> Option<(u64, Order)> {
    let order = Order {
        session: SessionId(entry.session()),
        side: Side::from_wire(entry.side())?,
        symbol: symbol(entry.symbol())?,
        price: Some(entry.price()).filter(|p| *p > 0)?,
        quantity: Some(entry.quantity()).filter(|q| *q > 0)?,
        hidden: entry.hidden(),
        show: entry.show(),
        seq: entry.seq(),
        expires: entry.expires(),
    };
    let iceberg_shows = order.hidden == 0 || order.show > 0;
    iceberg_shows.then_some((entry.order_id(), order))
}

/// A snapshot's waiting stop.
fn restore_stop(entry: codec::SnapshotStopsRef<'_>) -> Option<Stop> {
    let tif = match (entry.time_in_force(), entry.expires()) {
        (codec::TimeInForce::Gtc, None) => TimeInForce::Gtc,
        (codec::TimeInForce::Ioc, None) => TimeInForce::Ioc,
        (codec::TimeInForce::Fok, None) => TimeInForce::Fok,
        (codec::TimeInForce::Post, None) => TimeInForce::Post,
        (codec::TimeInForce::Gtd, Some(at)) => TimeInForce::Gtd(at),
        _ => return None,
    };
    let order = Incoming {
        id: entry.order_id(),
        session: SessionId(entry.session()),
        side: Side::from_wire(entry.side())?,
        symbol: symbol(entry.symbol())?,
        quantity: Some(entry.quantity()).filter(|q| *q > 0)?,
        limit: entry.price().filter(|p| *p > 0),
        tif,
        show: entry.show(),
    };
    Some(Stop { trigger: Some(entry.trigger()).filter(|t| *t > 0)?, order })
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.first().map(String::as_str) == Some("client") {
        return client::main(&args[1..]);
    }
    turbofan::run(SERVICE_ID, Matching::default())
}

/// The requests and events as lines of text, for people: what `client` reads and prints.
mod text {
    use super::codec::{self, Decoded, RejectReason};

    /// A line typed, as what's sent.
    #[derive(Debug, PartialEq, Eq)]
    pub enum Line {
        /// A request, to send as a message.
        Message(Vec<u8>),
        /// A `book`, to ask as a query.
        Query(Vec<u8>),
    }

    /// The request a line asks for, or `None` if it's not one. It reads the words, no more: the
    /// service judges what they say (a quantity of 0, say).
    pub fn request(line: &str) -> Option<Line> {
        // The null of an optional field, never a price.
        let number = |word: &str| word.parse::<u64>().ok().filter(|n| *n < u64::MAX);
        let symbol = |word: &str| (word.len() <= 8).then(|| turbofan_sbe::pad(word.as_bytes()));
        let mut out = Vec::new();
        let encoded = match line.split_ascii_whitespace().collect::<Vec<_>>()[..] {
            [side @ ("buy" | "sell"), word, quantity, ref rest @ ..] => {
                let (rest, stop) = match *rest {
                    ["stop", trigger, ref rest @ ..] => (rest, Some(number(trigger)?)),
                    _ => (rest, None),
                };
                let (rest, show) = match *rest {
                    [ref rest @ .., "show", show] => (rest, Some(number(show)?)),
                    _ => (rest, None),
                };
                use codec::TimeInForce::{Fok, Gtc, Gtd, Ioc, Post};
                let (price, time_in_force, seconds) = match *rest {
                    [] | ["ioc"] => (None, Ioc, None),
                    ["fok"] => (None, Fok, None),
                    [price] => (Some(number(price)?), Gtc, None),
                    [price, "ioc"] => (Some(number(price)?), Ioc, None),
                    [price, "fok"] => (Some(number(price)?), Fok, None),
                    [price, "post"] => (Some(number(price)?), Post, None),
                    [price, "gtd", seconds] => (Some(number(price)?), Gtd, Some(number(seconds)?)),
                    _ => return None,
                };
                let side = if side == "buy" { codec::Side::Buy } else { codec::Side::Sell };
                let (symbol, quantity) = (symbol(word)?, number(quantity)?);
                codec::NewOrder { side, symbol, quantity, price, time_in_force, seconds, show, stop }
                    .encode_into(&mut out)
            }
            ["cancel", id] => codec::CancelOrder { order_id: number(id)? }.encode_into(&mut out),
            ["modify", id, quantity, price] => {
                let (order_id, quantity, price) = (number(id)?, number(quantity)?, number(price)?);
                codec::ModifyOrder { order_id, quantity, price }.encode_into(&mut out)
            }
            ["book", word] => {
                codec::BookRequest { symbol: symbol(word)? }.encode_into(&mut out).ok()?;
                return Some(Line::Query(out));
            }
            _ => return None,
        };
        encoded.ok()?;
        Some(Line::Message(out))
    }

    /// An event, or `book`'s answer, as a line (two, for a book).
    pub fn describe(payload: &[u8]) -> String {
        let side = |side| match side {
            codec::Side::Buy => "buy",
            codec::Side::Sell => "sell",
            codec::Side::Unknown(_) => "?",
        };
        let Ok((message, _)) = codec::decode(payload) else { return format!("undecodable: {payload:?}") };
        match message {
            Decoded::Accepted(e) => {
                let symbol = String::from_utf8_lossy(e.symbol());
                let price = e.price().map_or_else(|| "market".to_owned(), |p| p.to_string());
                let stop = e.stop().map_or_else(String::new, |t| format!(" stop {t}"));
                format!("accepted {} {} {symbol} {} {price}{stop}", e.order_id(), side(e.side()), e.quantity())
            }
            Decoded::Fill(e) => format!("fill {} {} {} {}", e.order_id(), e.quantity(), e.price(), e.left()),
            Decoded::Modified(e) => format!("modified {} {} {}", e.order_id(), e.quantity(), e.price()),
            Decoded::Cancelled(e) => format!("cancelled {} {}", e.order_id(), e.left()),
            Decoded::Expired(e) => format!("expired {} {}", e.order_id(), e.left()),
            Decoded::Triggered(e) => format!("triggered {}", e.order_id()),
            Decoded::Rejected(e) => match e.reason() {
                RejectReason::Malformed => "rejected malformed".to_owned(),
                RejectReason::UnknownOrder => "rejected unknown-order".to_owned(),
                RejectReason::BookFull => "rejected book-full".to_owned(),
                RejectReason::Unknown(raw) => format!("rejected {raw}"),
            },
            Decoded::Book(book) => {
                let bids: String = book.bids().map(|l| format!(" {}@{}", l.quantity(), l.price())).collect();
                let asks: String = book.asks().map(|l| format!(" {}@{}", l.quantity(), l.price())).collect();
                format!("bids{bids}\nasks{asks}")
            }
            request => format!("{request:?}"),
        }
    }
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
    use super::text::{self, Line};

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
                    line = typed.recv() => match line.as_deref().map(|line| (line, text::request(line))) {
                        None => break,
                        Some((line, None)) => eprintln!("not a request: {line}"),
                        Some((_, Some(Line::Query(query)))) => {
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
                            match session.query(query).await {
                                Ok(answer) => println!("{}", text::describe(&answer)),
                                Err(e) => eprintln!("query: {e}"),
                            }
                        }
                        Some((_, Some(Line::Message(request)))) => match session.send(request).await {
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
            Some(Event::Message(event)) => println!("{}", text::describe(&event)),
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
    const CAROL: SessionId = SessionId(3);

    /// The events a request, typed as a line, sends, each with the session it goes to.
    fn request(service: &mut Matching, session: SessionId, line: &str) -> Vec<(u64, String)> {
        let mut ctx = Ctx::new(0);
        service.on_message(&mut ctx, session, &message(line));
        events(ctx.take_effects())
    }

    /// The message a line is.
    fn message(line: &str) -> Vec<u8> {
        match text::request(line) {
            Some(text::Line::Message(message)) => message,
            other => panic!("{line:?}: {other:?}"),
        }
    }

    /// The events among effects, each with the session it goes to: timers are left out.
    fn events(effects: Vec<Effect>) -> Vec<(u64, String)> {
        let sends = effects.into_iter().filter_map(|effect| match effect {
            Effect::Send(to, event) => Some((to.0, text::describe(&event))),
            _ => None,
        });
        sends.collect()
    }

    fn book(service: &Matching, symbol: &str) -> String {
        let Some(text::Line::Query(query)) = text::request(&format!("book {symbol}")) else { panic!("{symbol:?}") };
        text::describe(&service.on_query(ALICE, &query).unwrap())
    }

    #[test]
    fn orders_match_by_price_then_time_at_the_resting_price() {
        let mut service = Matching::default();
        request(&mut service, ALICE, "sell ACME 10 101");
        request(&mut service, ALICE, "sell ACME 10 100");
        request(&mut service, CAROL, "sell ACME 5 100");
        request(&mut service, BOB, "sell OTHER 5 50");
        assert_eq!(book(&service, "ACME"), "bids\nasks 15@100 10@101");
        let events = request(&mut service, BOB, "buy ACME 30 101");
        assert_eq!(
            events,
            [
                (2, "accepted 5 buy ACME 30 101".to_owned()),
                (1, "fill 2 10 100 0".to_owned()),
                (2, "fill 5 10 100 20".to_owned()),
                (3, "fill 3 5 100 0".to_owned()),
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
        let fills = request(&mut service, CAROL, "sell ACME 1 100");
        assert_eq!(fills[1], (1, "fill 1 1 100 4".to_owned()), "alice is still first");
        request(&mut service, ALICE, "modify 1 6 100");
        let fills = request(&mut service, CAROL, "sell ACME 1 100");
        assert_eq!(fills[1], (2, "fill 2 1 100 9".to_owned()), "a bigger order goes to the back");
        // A new price that crosses trades at once.
        request(&mut service, CAROL, "sell ACME 2 101");
        let events = request(&mut service, ALICE, "modify 1 6 101");
        assert_eq!(events[1], (3, "fill 5 2 101 0".to_owned()));
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
            "buy ACME 1 post",
            "buy ACME 1 gtd 5",
            "buy ACME 1 2 gtd",
            "buy ACME 1 2 gtd 0",
            "buy ACME 1 2 ioc fok",
            "buy ACME 1 2 day",
            "buy ACME 5 show 1",
            "buy ACME 5 2 ioc show 1",
            "buy ACME 5 2 show 0",
            "buy ACME 5 2 show",
            "buy ACME 5 stop",
            "buy ACME 5 stop 0",
            "buy ACME 5 stop x 100",
            "buy ACME 5 100 stop 90",
            "buy ACME 5 stop 90 show 1",
            "buy \u{ff} 1",
        ] {
            // The client refuses what isn't a request at all, and the service what says nonsense.
            if let Some(line) = text::request(bad) {
                let text::Line::Message(message) = line else { panic!("{bad:?}") };
                let mut ctx = Ctx::new(0);
                service.on_message(&mut ctx, ALICE, &message);
                assert_eq!(events(ctx.take_effects()), [(1, "rejected malformed".to_owned())], "{bad:?}");
            }
        }
        assert_eq!(service.last_id, 0);
        let Some(text::Line::Query(query)) = text::request("book AC.ME") else { panic!() };
        assert_eq!(service.on_query(ALICE, &query), None);
    }

    /// What the client never sends, but another client could.
    #[test]
    fn a_malformed_message_is_rejected_and_changes_nothing() {
        let mut service = Matching::default();
        let order = codec::NewOrder {
            side: codec::Side::Buy,
            symbol: *b"ACME\0\0\0\0",
            quantity: 1,
            price: Some(100),
            time_in_force: codec::TimeInForce::Gtc,
            seconds: None,
            show: None,
            stop: None,
        };
        let encode = |message: &dyn Fn(&mut Vec<u8>)| {
            let mut out = Vec::new();
            message(&mut out);
            out
        };
        let new_order = |order: codec::NewOrder| encode(&|out| order.encode_into(out).unwrap());
        let mut trailing = new_order(order.clone());
        trailing.push(0);
        for bad in [
            Vec::new(),
            b"buy ACME 1 100".to_vec(),
            trailing,
            new_order(codec::NewOrder { side: codec::Side::Unknown(9), ..order.clone() }),
            new_order(codec::NewOrder { time_in_force: codec::TimeInForce::Unknown(9), ..order.clone() }),
            new_order(codec::NewOrder { symbol: *b"\0\0\0\0\0\0\0\0", ..order.clone() }),
            new_order(codec::NewOrder { price: None, ..order.clone() }),
            new_order(codec::NewOrder { seconds: Some(5), ..order.clone() }),
            new_order(codec::NewOrder { time_in_force: codec::TimeInForce::Gtd, ..order.clone() }),
            new_order(codec::NewOrder {
                price: None,
                show: Some(1),
                time_in_force: codec::TimeInForce::Ioc,
                ..order.clone()
            }),
            new_order(codec::NewOrder { stop: Some(0), ..order.clone() }),
            encode(&|out| codec::Triggered { order_id: 1 }.encode_into(out).unwrap()),
            encode(&|out| codec::BookRequest { symbol: order.symbol }.encode_into(out).unwrap()),
        ] {
            let mut ctx = Ctx::new(0);
            service.on_message(&mut ctx, ALICE, &bad);
            assert_eq!(events(ctx.take_effects()), [(1, "rejected malformed".to_owned())], "{bad:?}");
        }
        assert_eq!(service.last_id, 0);
        assert_eq!(request(&mut service, ALICE, "buy ACME 1 100").len(), 1, "the order it was made from is fine");
    }

    /// `codec.rs` is what the generator writes from `schema.xml` now, formatted as `turbofan-sbe`'s
    /// checked-in codecs are. `SBE_REGENERATE=1` rewrites it instead.
    #[test]
    fn the_codec_is_current() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/matching");
        let code = turbofan_sbe::codegen::SbeGenerator::load(dir.join("schema.xml")).unwrap().render().unwrap();
        let mut rustfmt = std::process::Command::new("rustfmt")
            .args(["--edition", "2024", "--config", "max_width=120,use_small_heuristics=Max"])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn()
            .expect("rustfmt runs");
        std::io::Write::write_all(&mut rustfmt.stdin.take().unwrap(), code.as_bytes()).unwrap();
        let formatted = String::from_utf8(rustfmt.wait_with_output().unwrap().stdout).unwrap();
        let path = dir.join("codec.rs");
        if std::env::var_os("SBE_REGENERATE").is_some() {
            std::fs::write(&path, formatted).unwrap();
        } else {
            let stale = "codec.rs is stale: SBE_REGENERATE=1 cargo test -p turbofan --example matching";
            assert!(std::fs::read_to_string(&path).unwrap() == formatted, "{stale}");
        }
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
    fn immediate_or_cancel_and_fill_or_kill_never_rest() {
        let mut service = Matching::default();
        request(&mut service, ALICE, "sell ACME 5 100");
        request(&mut service, ALICE, "sell ACME 5 101");
        let events = request(&mut service, BOB, "buy ACME 8 100 ioc");
        assert_eq!(
            events[1..],
            [(1, "fill 1 5 100 0".to_owned()), (2, "fill 3 5 100 3".to_owned()), (2, "cancelled 3 3".to_owned()),]
        );
        // 5 at 101 is too little: nothing trades.
        assert_eq!(request(&mut service, BOB, "buy ACME 6 101 fok")[1..], [(2, "cancelled 4 6".to_owned())]);
        assert_eq!(book(&service, "ACME"), "bids\nasks 5@101");
        assert_eq!(request(&mut service, BOB, "buy ACME 5 fok").len(), 3, "a market order, filled whole");
        assert!(service.orders.is_empty());
    }

    #[test]
    fn a_post_only_order_that_would_trade_is_cancelled_whole() {
        let mut service = Matching::default();
        request(&mut service, ALICE, "sell ACME 5 100");
        assert_eq!(request(&mut service, BOB, "buy ACME 5 100 post")[1..], [(2, "cancelled 2 5".to_owned())]);
        request(&mut service, BOB, "buy ACME 5 99 post");
        assert_eq!(book(&service, "ACME"), "bids 5@99\nasks 5@100");
    }

    #[test]
    fn an_order_cancels_its_own_sessions_resting_orders_instead_of_trading_with_them() {
        let mut service = Matching::default();
        request(&mut service, ALICE, "sell ACME 5 100");
        request(&mut service, BOB, "sell ACME 5 100");
        let events = request(&mut service, ALICE, "buy ACME 10 100");
        assert_eq!(
            events[1..],
            [(1, "cancelled 1 5".to_owned()), (2, "fill 2 5 100 0".to_owned()), (1, "fill 3 5 100 5".to_owned()),]
        );
        assert_eq!(book(&service, "ACME"), "bids 5@100\nasks");
        // Its own orders don't count towards filling a fill or kill order.
        request(&mut service, BOB, "sell ACME 5 100");
        assert_eq!(request(&mut service, BOB, "buy ACME 5 100 fok")[1..], [(2, "cancelled 5 5".to_owned())]);
    }

    #[test]
    fn a_good_till_date_order_expires_unless_it_goes_first() {
        let mut service = Matching::default();
        let mut ctx = Ctx::new(SECOND);
        service.on_message(&mut ctx, ALICE, &message("buy ACME 5 100 gtd 30"));
        service.on_message(&mut ctx, ALICE, &message("buy ACME 5 99 gtd 60"));
        let effects = ctx.take_effects();
        assert!(effects.contains(&Effect::Schedule { id: 1, at: 31 * SECOND }));
        assert!(effects.contains(&Effect::Schedule { id: 2, at: 61 * SECOND }));
        // A fill, a cancel and a modify take the timer with them; a modify that moves it puts
        // it back.
        service.on_message(&mut ctx, BOB, &message("sell ACME 5 100"));
        service.on_message(&mut ctx, ALICE, &message("modify 2 5 98"));
        let effects = ctx.take_effects();
        assert!(effects.contains(&Effect::Cancel(1)));
        assert!(effects.ends_with(&[Effect::Cancel(2), Effect::Schedule { id: 2, at: 61 * SECOND }]));
        service.on_timer(&mut ctx, 2);
        assert_eq!(events(ctx.take_effects()), [(1, "expired 2 5".to_owned())]);
        assert!(service.books.is_empty());
    }

    #[test]
    fn an_iceberg_shows_a_part_at_a_time_each_at_the_back_of_the_queue() {
        let mut service = Matching::default();
        request(&mut service, ALICE, "sell ACME 25 100 show 10");
        request(&mut service, BOB, "sell ACME 5 100");
        assert_eq!(book(&service, "ACME"), "bids\nasks 15@100");
        let events = request(&mut service, CAROL, "buy ACME 12 100");
        assert_eq!(
            events[1..],
            [
                (1, "fill 1 10 100 15".to_owned()),
                (3, "fill 3 10 100 2".to_owned()),
                // Alice showed 10 more, behind Bob.
                (2, "fill 2 2 100 3".to_owned()),
                (3, "fill 3 2 100 0".to_owned()),
            ]
        );
        assert_eq!(book(&service, "ACME"), "bids\nasks 13@100");
        // A smaller modify keeps its place, and what it shows.
        request(&mut service, ALICE, "modify 1 12 100");
        assert_eq!(book(&service, "ACME"), "bids\nasks 13@100");
        assert_eq!((service.orders[&1].quantity, service.orders[&1].hidden), (10, 2));
        assert_eq!(service.tranches, 3, "bob's, and alice's twice more");
        let events = request(&mut service, CAROL, "buy ACME 20");
        assert_eq!(events.last().unwrap(), &(3, "cancelled 4 5".to_owned()), "the hidden part trades too");
        assert!(service.orders.is_empty());
        assert_eq!(service.tranches, 0);
    }

    #[test]
    fn an_iceberg_takes_a_place_in_the_book_for_each_time_it_shows() {
        let mut service = Matching::default();
        let all = format!("buy ACME {} 100 show 1", MAX_ORDERS + 1);
        assert_eq!(request(&mut service, ALICE, &all), [(1, "rejected book-full".to_owned())]);
        request(&mut service, ALICE, &format!("buy ACME {MAX_ORDERS} 100 show 1"));
        assert_eq!(request(&mut service, BOB, "buy ACME 1 100"), [(2, "rejected book-full".to_owned())]);
        assert_eq!(request(&mut service, ALICE, "modify 1 1 101"), [(1, "modified 1 1 101".to_owned())]);
        request(&mut service, ALICE, &format!("modify 1 {MAX_ORDERS} 100"));
        let big = format!("modify 1 {} 100", MAX_ORDERS + 1);
        assert_eq!(request(&mut service, ALICE, &big), [(1, "rejected book-full".to_owned())]);
        // Each time it shows trades on its own: a market order's events stay within the bound.
        let events = request(&mut service, BOB, "sell ACME 20000");
        assert_eq!(events.len(), 2 + 2 * usize::try_from(MAX_ORDERS).unwrap());
    }

    #[test]
    fn a_stop_comes_in_when_a_trade_reaches_its_trigger_and_can_set_off_more() {
        let mut service = Matching::default();
        request(&mut service, ALICE, "sell ACME 5 100");
        request(&mut service, ALICE, "sell ACME 5 101");
        request(&mut service, ALICE, "sell ACME 5 102");
        // Nothing has traded yet: they wait.
        assert_eq!(
            request(&mut service, BOB, "buy ACME 5 stop 100"),
            [(2, "accepted 4 buy ACME 5 market stop 100".to_owned())]
        );
        request(&mut service, BOB, "buy ACME 5 stop 101 101");
        request(&mut service, BOB, "sell ACME 1 stop 90");
        assert_eq!(book(&service, "ACME"), "bids\nasks 5@100 5@101 5@102", "a stop isn't in the book");
        // Carol's trade at 100 brings in Bob's stop at 100, whose trade at 101 brings in his next.
        let events = request(&mut service, CAROL, "buy ACME 1 100");
        assert_eq!(
            events[3..],
            [
                (2, "triggered 4".to_owned()),
                (1, "fill 1 4 100 0".to_owned()),
                (2, "fill 4 4 100 1".to_owned()),
                (1, "fill 2 1 101 4".to_owned()),
                (2, "fill 4 1 101 0".to_owned()),
                (2, "triggered 5".to_owned()),
                (1, "fill 2 4 101 0".to_owned()),
                (2, "fill 5 4 101 1".to_owned()),
            ]
        );
        assert_eq!(book(&service, "ACME"), "bids 1@101\nasks 5@102", "a stop limit rests what's left");
        assert_eq!(service.stops.len(), 1, "the sell stop at 90 still waits");
        // One already reached comes in at once.
        let events = request(&mut service, CAROL, "sell ACME 1 stop 105 101");
        assert_eq!(events[1], (3, "triggered 8".to_owned()));
        assert_eq!(book(&service, "ACME"), "bids\nasks 5@102");
    }

    #[test]
    fn a_waiting_stop_is_cancelled_expires_and_goes_with_its_session() {
        let mut service = Matching::default();
        request(&mut service, ALICE, "buy ACME 5 stop 100");
        request(&mut service, ALICE, "sell ACME 5 stop 90");
        assert_eq!(request(&mut service, BOB, "cancel 1"), [(2, "rejected unknown-order".to_owned())]);
        assert_eq!(request(&mut service, ALICE, "modify 1 5 100"), [(1, "rejected unknown-order".to_owned())]);
        assert_eq!(request(&mut service, ALICE, "cancel 1"), [(1, "cancelled 1 5".to_owned())]);
        let mut ctx = Ctx::new(0);
        service.on_message(&mut ctx, BOB, &message("buy ACME 3 stop 100 100 gtd 10"));
        assert!(ctx.take_effects().contains(&Effect::Schedule { id: 3, at: 10 * SECOND }));
        service.on_timer(&mut ctx, 3);
        assert_eq!(events(ctx.take_effects()), [(2, "expired 3 3".to_owned())]);
        service.on_session_close(&mut ctx, ALICE, CloseReason::Client);
        assert!(service.stops.is_empty() && service.triggers.is_empty());
        assert_eq!(service.tranches, 0);
    }

    #[test]
    fn a_snapshot_restores_the_same_books_and_a_damaged_one_is_refused() {
        let mut service = Matching::default();
        request(&mut service, ALICE, "buy ACME 10 100");
        request(&mut service, BOB, "buy ACME 10 100");
        request(&mut service, BOB, "sell OTHER 3 7");
        request(&mut service, ALICE, "modify 1 11 100");
        request(&mut service, ALICE, "sell OTHER 3 9 gtd 5");
        request(&mut service, CAROL, "sell OTHER 30 10 show 4");
        request(&mut service, BOB, "buy OTHER 1 10");
        request(&mut service, BOB, "sell OTHER 2 stop 8 7 gtd 9");
        request(&mut service, ALICE, "buy OTHER 2 stop 11");
        let mut bytes = Vec::new();
        service.snapshot()(&mut bytes).unwrap();
        let mut restored = Matching::default();
        restored.restore(&mut bytes.as_slice()).unwrap();
        assert_eq!((restored.last_id, restored.last_seq), (service.last_id, service.last_seq));
        assert_eq!(restored.orders, service.orders);
        assert_eq!(restored.books, service.books);
        assert_eq!(restored.tranches, service.tranches);
        assert_eq!((&restored.stops, &restored.triggers), (&service.stops, &service.triggers));
        assert_eq!(restored.last, service.last);
        for cut in [0, 1, 2, bytes.len() - 1] {
            assert!(Matching::default().restore(&mut &bytes[..cut]).is_err(), "cut at {cut}");
        }
        let mut trailing = bytes.clone();
        trailing.push(0);
        assert!(Matching::default().restore(&mut trailing.as_slice()).is_err(), "a byte after it");
        let mut request = Vec::new();
        codec::CancelOrder { order_id: 1 }.encode_into(&mut request).unwrap();
        assert!(Matching::default().restore(&mut request.as_slice()).is_err(), "another message");
    }

    /// Snapshots that decode, but whose state can't be.
    #[test]
    fn a_snapshot_that_says_nonsense_is_refused() {
        let order = codec::SnapshotOrders {
            order_id: 1,
            session: 1,
            side: codec::Side::Buy,
            symbol: *b"ACME\0\0\0\0",
            price: 100,
            quantity: 5,
            hidden: 0,
            show: 0,
            seq: 1,
            expires: None,
        };
        let stop = codec::SnapshotStops {
            order_id: 2,
            session: 1,
            side: codec::Side::Sell,
            symbol: *b"ACME\0\0\0\0",
            trigger: 90,
            quantity: 5,
            price: None,
            time_in_force: codec::TimeInForce::Ioc,
            expires: None,
            show: 0,
        };
        let last = codec::SnapshotLastPrices { symbol: *b"ACME\0\0\0\0", price: 100 };
        let restore = |orders: &[codec::SnapshotOrders],
                       stops: &[codec::SnapshotStops],
                       last_prices: &[codec::SnapshotLastPrices]| {
            let snapshot = codec::Snapshot { last_id: 2, last_seq: 1, orders, stops, last_prices };
            let mut bytes = Vec::new();
            snapshot.encode_into(&mut bytes).unwrap();
            Matching::default().restore(&mut bytes.as_slice())
        };
        restore(std::slice::from_ref(&order), std::slice::from_ref(&stop), std::slice::from_ref(&last))
            .expect("the one these are made from is fine");
        for (orders, why) in [
            (vec![codec::SnapshotOrders { order_id: 3, ..order.clone() }], "an id from the future"),
            (vec![codec::SnapshotOrders { seq: 2, ..order.clone() }], "a seq from the future"),
            (vec![order.clone(), codec::SnapshotOrders { order_id: 2, ..order.clone() }], "a seq twice"),
            (vec![codec::SnapshotOrders { hidden: 5, ..order.clone() }], "an iceberg that shows none of itself"),
            (vec![codec::SnapshotOrders { quantity: 0, ..order.clone() }], "nothing left"),
            (vec![codec::SnapshotOrders { symbol: *b"AC.ME\0\0\0", ..order.clone() }], "a bad symbol"),
            (vec![codec::SnapshotOrders { side: codec::Side::Unknown(9), ..order.clone() }], "an unknown side"),
        ] {
            assert!(restore(&orders, &[], &[]).is_err(), "{why}");
        }
        for (stops, why) in [
            (vec![codec::SnapshotStops { order_id: 1, ..stop.clone() }], "an order's id"),
            (vec![codec::SnapshotStops { time_in_force: codec::TimeInForce::Gtd, ..stop.clone() }], "gtd, no expiry"),
            (vec![codec::SnapshotStops { expires: Some(5), ..stop.clone() }], "an expiry, not gtd"),
            (vec![codec::SnapshotStops { trigger: 0, ..stop.clone() }], "no trigger"),
        ] {
            assert!(restore(std::slice::from_ref(&order), &stops, &[]).is_err(), "{why}");
        }
        assert!(restore(&[], &[], &[last.clone(), last.clone()]).is_err(), "a symbol's last price twice");
        assert!(restore(&[], &[], &[codec::SnapshotLastPrices { price: 0, ..last }]).is_err(), "a price of 0");
    }
}
