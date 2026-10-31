//! An order management system, as a [`turbofan_cluster::Service`]: the cluster replicates it, and every
//! node runs the same orders through the same states.
//!
//! Gateways send [`wire::NewOrder`], [`wire::CancelOrder`] and [`wire::ReplaceOrder`], and get a
//! [`wire::ExecReport`] for each change to an order, or a [`wire::CancelReject`]. One session
//! registers as the venue's ([`wire::RegisterVenue`]): the OMS sends it the orders to work, and
//! it reports acks, rejects, fills and cancels back. An order's statuses, as FIX's:
//!
//! ```text
//! PendingNew ─ack→ New ─fill→ PartiallyFilled ─fill→ Filled
//!     └─reject→ Rejected     New, PartiallyFilled ─cancel→ PendingCancel ─venue→ Canceled
//!                            New, PartiallyFilled ─replace→ PendingReplace ─venue→ New, PartiallyFilled
//! ```
//!
//! Prices are integer ticks, never floats. Limit orders only.

use std::collections::BTreeMap;

use turbofan_cluster::{CloseReason, Ctx, Service, SessionId};
use turbofan_sbe::Encode;
use wire::{ExecType, OrdStatus, Reason, Side};

/// This cluster's `Hello.service_id`.
pub const SERVICE_ID: u32 = 200;
/// The largest order quantity.
pub const MAX_QTY: u64 = 1_000_000_000_000;
/// The highest price, in ticks.
pub const MAX_PRICE: i64 = 1_000_000_000_000;
/// The most orders held, terminal ones included.
// ponytail: terminal orders are kept forever; purge them at end of day once there is a day.
pub const MAX_ORDERS: usize = 1_000_000;

const _: () = assert!((MAX_QTY as u128) * (MAX_PRICE as u128) * (MAX_QTY as u128) < u128::MAX, "notional fits");

/// The codecs generated from `schema/oms.xml`.
#[allow(dead_code, missing_docs, missing_debug_implementations, clippy::all, clippy::pedantic, reason = "generated")]
pub mod wire {
    include!(concat!(env!("OUT_DIR"), "/oms.rs"));
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Order {
    session: SessionId,
    cl_ord_id: Vec<u8>,
    symbol: Vec<u8>,
    side: Side,
    qty: u64,
    price: i64,
    cum_qty: u64,
    /// The sum of each fill's quantity times its price.
    notional: u128,
    status: OrdStatus,
    /// The quantity and price a pending replace asks for.
    pending: Option<(u64, i64)>,
}

impl Order {
    fn is_terminal(&self) -> bool {
        matches!(self.status, OrdStatus::Filled | OrdStatus::Canceled | OrdStatus::Rejected)
    }

    fn is_working(&self) -> bool {
        matches!(self.status, OrdStatus::New | OrdStatus::PartiallyFilled)
    }

    fn leaves(&self) -> u64 {
        if self.is_terminal() { 0 } else { self.qty - self.cum_qty }
    }

    fn avg_px(&self) -> i64 {
        if self.cum_qty == 0 {
            0
        } else {
            i64::try_from(self.notional / u128::from(self.cum_qty)).expect("an average of prices")
        }
    }

    /// Its status once nothing is pending.
    fn settled(&self) -> OrdStatus {
        match self.cum_qty {
            0 => OrdStatus::New,
            cum if cum >= self.qty => OrdStatus::Filled,
            _ => OrdStatus::PartiallyFilled,
        }
    }
}

/// The order book: every order and the venue's session.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Oms {
    orders: BTreeMap<u64, Order>,
    /// Each session's client order IDs, which must be unique.
    by_client: BTreeMap<(SessionId, Vec<u8>), u64>,
    /// The last order ID given out.
    last_order_id: u64,
    venue: Option<SessionId>,
}

impl Service for Oms {
    fn on_message(&mut self, ctx: &mut Ctx, session: SessionId, payload: &[u8]) {
        // A message that doesn't decode, or has bytes after it, is dropped: on every node alike.
        let Ok((message, len)) = wire::decode(payload) else { return };
        if len != payload.len() {
            return;
        }
        match &message {
            wire::Decoded::NewOrder(m) => self.new_order(ctx, session, m),
            wire::Decoded::CancelOrder(m) => self.cancel(ctx, session, m.order_id()),
            wire::Decoded::ReplaceOrder(m) => self.replace(ctx, session, m.order_id(), m.qty(), m.price()),
            wire::Decoded::RegisterVenue(_) => {
                self.venue = Some(session);
                send(ctx, session, &wire::VenueRegistered {});
            }
            _ if self.venue == Some(session) => self.on_venue(ctx, &message),
            _ => {}
        }
    }

    fn on_session_close(&mut self, _ctx: &mut Ctx, session: SessionId, _reason: CloseReason) {
        if self.venue == Some(session) {
            self.venue = None;
        }
    }

    fn snapshot(&self, out: &mut Vec<u8>) {
        let state = wire::OmsState {
            next_order_id: self.last_order_id,
            venue: self.venue.unwrap_or(0),
            orders: self.orders.len() as u64,
        };
        state.encode_into(out).expect("fixed size");
        for (&order_id, o) in &self.orders {
            let notional_high = u64::try_from(o.notional >> 64).expect("high half");
            let notional_low = u64::try_from(o.notional & u128::from(u64::MAX)).expect("low half");
            let (pending_qty, pending_price) = o.pending.unwrap_or((0, 0));
            let order = wire::OrderState {
                order_id,
                session: o.session,
                side: o.side,
                qty: o.qty,
                price: o.price,
                cum_qty: o.cum_qty,
                notional_high,
                notional_low,
                status: o.status,
                pending_qty,
                pending_price,
                cl_ord_id: &o.cl_ord_id,
                symbol: &o.symbol,
            };
            order.encode_into(out).expect("its fields were checked when it was accepted");
        }
    }

    /// # Panics
    ///
    /// If `bytes` aren't what [`Oms::snapshot`] wrote.
    fn restore(&mut self, bytes: &[u8]) {
        let (wire::Decoded::OmsState(state), mut at) = wire::decode(bytes).expect("a snapshot starts with OmsState")
        else {
            panic!("a snapshot starts with OmsState");
        };
        *self = Self {
            last_order_id: state.next_order_id(),
            venue: Some(state.venue()).filter(|&v| v > 0),
            ..Self::default()
        };
        for _ in 0..state.orders() {
            let (wire::Decoded::OrderState(o), len) = wire::decode(&bytes[at..]).expect("then its orders") else {
                panic!("a snapshot's orders are OrderStates");
            };
            at += len;
            let notional = (u128::from(o.notional_high()) << 64) | u128::from(o.notional_low());
            let pending = (o.status() == OrdStatus::PendingReplace).then_some((o.pending_qty(), o.pending_price()));
            let order = Order {
                session: o.session(),
                cl_ord_id: o.cl_ord_id().to_vec(),
                symbol: o.symbol().to_vec(),
                side: o.side(),
                qty: o.qty(),
                price: o.price(),
                cum_qty: o.cum_qty(),
                notional,
                status: o.status(),
                pending,
            };
            self.by_client.insert((order.session, order.cl_ord_id.clone()), o.order_id());
            self.orders.insert(o.order_id(), order);
        }
        assert_eq!(at, bytes.len(), "nothing after the orders");
    }
}

impl Oms {
    fn new_order(&mut self, ctx: &mut Ctx, session: SessionId, m: &wire::NewOrderRef<'_>) {
        let (cl_ord_id, symbol) = (m.cl_ord_id(), m.symbol());
        let refused = if cl_ord_id.is_empty() {
            Some(Reason::InvalidClOrdId)
        } else if self.by_client.contains_key(&(session, cl_ord_id.to_vec())) {
            Some(Reason::DuplicateClOrdId)
        } else if !matches!(m.side(), Side::Buy | Side::Sell) {
            Some(Reason::InvalidSide)
        } else if !(1..=MAX_QTY).contains(&m.qty()) {
            Some(Reason::InvalidQty)
        } else if !(1..=MAX_PRICE).contains(&m.price()) {
            Some(Reason::InvalidPrice)
        } else if symbol.is_empty() {
            Some(Reason::InvalidSymbol)
        } else if self.orders.len() >= MAX_ORDERS {
            Some(Reason::Full)
        } else if self.venue.is_none() {
            Some(Reason::NoVenue)
        } else {
            None
        };
        let order = Order {
            session,
            cl_ord_id: cl_ord_id.to_vec(),
            symbol: symbol.to_vec(),
            side: m.side(),
            qty: m.qty(),
            price: m.price(),
            cum_qty: 0,
            notional: 0,
            status: OrdStatus::PendingNew,
            pending: None,
        };
        if let Some(reason) = refused {
            let order = Order { status: OrdStatus::Rejected, ..order };
            report(ctx, 0, &order, ExecType::Rejected, (0, 0), reason);
            return;
        }
        self.last_order_id += 1;
        let order_id = self.last_order_id;
        report(ctx, order_id, &order, ExecType::PendingNew, (0, 0), Reason::None);
        let venue = self.venue.expect("checked");
        let to_venue = wire::VenueNewOrder { order_id, side: order.side, qty: order.qty, price: order.price, symbol };
        send(ctx, venue, &to_venue);
        self.by_client.insert((session, order.cl_ord_id.clone()), order_id);
        self.orders.insert(order_id, order);
    }

    /// The order `session` may change, or why it can't.
    fn changeable(&mut self, session: SessionId, order_id: u64) -> Result<&mut Order, (Reason, Vec<u8>)> {
        let venue = self.venue;
        let Some(order) = self.orders.get_mut(&order_id) else { return Err((Reason::UnknownOrder, Vec::new())) };
        let refused = if order.session != session {
            return Err((Reason::NotYourOrder, Vec::new()));
        } else if order.is_terminal() {
            Some(Reason::TooLate)
        } else if !order.is_working() {
            Some(Reason::Pending)
        } else if venue.is_none() {
            Some(Reason::NoVenue)
        } else {
            None
        };
        match refused {
            Some(reason) => Err((reason, order.cl_ord_id.clone())),
            None => Ok(order),
        }
    }

    fn cancel(&mut self, ctx: &mut Ctx, session: SessionId, order_id: u64) {
        let venue = self.venue;
        match self.changeable(session, order_id) {
            Err((reason, cl_ord_id)) => cancel_reject(ctx, session, order_id, &cl_ord_id, reason),
            Ok(order) => {
                order.status = OrdStatus::PendingCancel;
                report(ctx, order_id, order, ExecType::PendingCancel, (0, 0), Reason::None);
                send(ctx, venue.expect("checked"), &wire::VenueCancelOrder { order_id });
            }
        }
    }

    fn replace(&mut self, ctx: &mut Ctx, session: SessionId, order_id: u64, qty: u64, price: i64) {
        let venue = self.venue;
        match self.changeable(session, order_id) {
            Err((reason, cl_ord_id)) => cancel_reject(ctx, session, order_id, &cl_ord_id, reason),
            Ok(order) if qty <= order.cum_qty || qty > MAX_QTY => {
                let cl_ord_id = order.cl_ord_id.clone();
                cancel_reject(ctx, session, order_id, &cl_ord_id, Reason::InvalidQty);
            }
            Ok(order) if !(1..=MAX_PRICE).contains(&price) => {
                let cl_ord_id = order.cl_ord_id.clone();
                cancel_reject(ctx, session, order_id, &cl_ord_id, Reason::InvalidPrice);
            }
            Ok(order) => {
                order.status = OrdStatus::PendingReplace;
                order.pending = Some((qty, price));
                report(ctx, order_id, order, ExecType::PendingReplace, (0, 0), Reason::None);
                send(ctx, venue.expect("checked"), &wire::VenueReplaceOrder { order_id, qty, price });
            }
        }
    }

    /// What the venue did. One that doesn't fit the order's state is ignored.
    fn on_venue(&mut self, ctx: &mut Ctx, message: &wire::Decoded<'_>) {
        let order_id = match message {
            wire::Decoded::VenueAck(m) => m.order_id(),
            wire::Decoded::VenueReject(m) => m.order_id(),
            wire::Decoded::Fill(m) => m.order_id(),
            wire::Decoded::VenueCanceled(m) => m.order_id(),
            wire::Decoded::VenueReplaced(m) => m.order_id(),
            wire::Decoded::VenueRequestReject(m) => m.order_id(),
            _ => return,
        };
        let Some(order) = self.orders.get_mut(&order_id) else { return };
        let pending_new = order.status == OrdStatus::PendingNew;
        let live = !order.is_terminal() && !pending_new;
        match message {
            wire::Decoded::VenueAck(_) if pending_new => {
                order.status = OrdStatus::New;
                report(ctx, order_id, order, ExecType::New, (0, 0), Reason::None);
            }
            wire::Decoded::VenueReject(_) if pending_new => {
                order.status = OrdStatus::Rejected;
                report(ctx, order_id, order, ExecType::Rejected, (0, 0), Reason::VenueRejected);
            }
            wire::Decoded::Fill(m)
                if live && (1..=order.leaves()).contains(&m.qty()) && (1..=MAX_PRICE).contains(&m.price()) =>
            {
                order.cum_qty += m.qty();
                order.notional += u128::from(m.qty()) * u128::from(m.price().unsigned_abs());
                if order.leaves() == 0 {
                    order.status = OrdStatus::Filled;
                    order.pending = None;
                } else if order.is_working() {
                    order.status = OrdStatus::PartiallyFilled;
                }
                report(ctx, order_id, order, ExecType::Trade, (m.qty(), m.price()), Reason::None);
            }
            wire::Decoded::VenueCanceled(_) if live => {
                order.status = OrdStatus::Canceled;
                order.pending = None;
                report(ctx, order_id, order, ExecType::Canceled, (0, 0), Reason::None);
            }
            wire::Decoded::VenueReplaced(_) if order.status == OrdStatus::PendingReplace => {
                let (qty, price) = order.pending.take().expect("a pending replace has its terms");
                // Fills since the request may have reached the new quantity.
                (order.qty, order.price) = (qty.max(order.cum_qty), price);
                order.status = order.settled();
                report(ctx, order_id, order, ExecType::Replaced, (0, 0), Reason::None);
            }
            wire::Decoded::VenueRequestReject(_)
                if matches!(order.status, OrdStatus::PendingCancel | OrdStatus::PendingReplace) =>
            {
                order.status = order.settled();
                order.pending = None;
                let (session, cl_ord_id) = (order.session, order.cl_ord_id.clone());
                cancel_reject(ctx, session, order_id, &cl_ord_id, Reason::VenueRejected);
            }
            _ => {}
        }
    }
}

fn report(ctx: &mut Ctx, order_id: u64, order: &Order, exec_type: ExecType, last: (u64, i64), reason: Reason) {
    let report = wire::ExecReport {
        order_id,
        exec_type,
        ord_status: order.status,
        side: order.side,
        qty: order.qty,
        price: order.price,
        cum_qty: order.cum_qty,
        leaves_qty: order.leaves(),
        avg_px: order.avg_px(),
        last_qty: last.0,
        last_px: last.1,
        reason,
        cl_ord_id: &order.cl_ord_id,
    };
    send(ctx, order.session, &report);
}

fn cancel_reject(ctx: &mut Ctx, session: SessionId, order_id: u64, cl_ord_id: &[u8], reason: Reason) {
    send(ctx, session, &wire::CancelReject { order_id, reason, cl_ord_id });
}

fn send(ctx: &mut Ctx, session: SessionId, message: &impl Encode) {
    let mut out = Vec::new();
    message.encode_into(&mut out).expect("the OMS's messages are within their limits");
    ctx.send(session, &out);
}
