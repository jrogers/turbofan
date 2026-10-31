//! The OMS as a service, without a cluster: messages in, decoded reports out.

use oms::Oms;
use oms::wire::{self, ExecType, OrdStatus, Reason, Side};
use turbofan_cluster::node::Effect;
use turbofan_cluster::{Ctx, Service, SessionId};
use turbojet::sbe::Encode;

const VENUE: SessionId = 1;
const ALICE: SessionId = 2;
const BOB: SessionId = 3;

/// What the OMS sent, decoded to what the tests look at.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Out {
    Report {
        to: SessionId,
        order_id: u64,
        exec: ExecType,
        status: OrdStatus,
        qty: u64,
        cum: u64,
        leaves: u64,
        avg: i64,
        reason: Reason,
    },
    CancelReject {
        to: SessionId,
        order_id: u64,
        reason: Reason,
    },
    VenueNew {
        order_id: u64,
        qty: u64,
        price: i64,
    },
    VenueCancel {
        order_id: u64,
    },
    VenueReplace {
        order_id: u64,
        qty: u64,
        price: i64,
    },
    Registered,
}

#[derive(Default)]
struct Book {
    oms: Oms,
}

impl Book {
    fn send(&mut self, session: SessionId, message: &impl Encode) -> Vec<Out> {
        let mut payload = Vec::new();
        message.encode_into(&mut payload).unwrap();
        self.raw(session, &payload)
    }

    fn raw(&mut self, session: SessionId, payload: &[u8]) -> Vec<Out> {
        let mut ctx = Ctx::new(0);
        self.oms.on_message(&mut ctx, session, payload);
        ctx.take_effects().into_iter().map(decode).collect()
    }

    /// A venue, and Alice's order for 100 at 10, acked.
    fn with_order() -> (Self, u64) {
        let mut book = Self::default();
        book.send(VENUE, &wire::RegisterVenue {});
        let out = book.send(ALICE, &new_order(b"a1", 100, 10));
        let Out::Report { order_id, .. } = out[0] else { panic!("{out:?}") };
        book.send(VENUE, &wire::VenueAck { order_id });
        (book, order_id)
    }
}

fn decode(effect: Effect) -> Out {
    let Effect::Send(to, payload) = effect else { panic!("the OMS only sends: {effect:?}") };
    match wire::decode(&payload).unwrap().0 {
        wire::Decoded::ExecReport(r) => Out::Report {
            to,
            order_id: r.order_id(),
            exec: r.exec_type(),
            status: r.ord_status(),
            qty: r.qty(),
            cum: r.cum_qty(),
            leaves: r.leaves_qty(),
            avg: r.avg_px(),
            reason: r.reason(),
        },
        wire::Decoded::CancelReject(r) => Out::CancelReject { to, order_id: r.order_id(), reason: r.reason() },
        wire::Decoded::VenueNewOrder(o) => Out::VenueNew { order_id: o.order_id(), qty: o.qty(), price: o.price() },
        wire::Decoded::VenueCancelOrder(o) => Out::VenueCancel { order_id: o.order_id() },
        wire::Decoded::VenueReplaceOrder(o) => {
            Out::VenueReplace { order_id: o.order_id(), qty: o.qty(), price: o.price() }
        }
        wire::Decoded::VenueRegistered(_) => Out::Registered,
        other => panic!("unexpected {other:?}"),
    }
}

fn new_order<'a>(cl_ord_id: &'a [u8], qty: u64, price: i64) -> wire::NewOrder<'a> {
    wire::NewOrder { side: Side::Buy, qty, price, cl_ord_id, symbol: b"ACME" }
}

#[allow(clippy::too_many_arguments, reason = "a report's fields, for the assertions")]
fn report(
    order_id: u64,
    exec: ExecType,
    status: OrdStatus,
    qty: u64,
    cum: u64,
    leaves: u64,
    avg: i64,
    reason: Reason,
) -> Out {
    Out::Report { to: ALICE, order_id, exec, status, qty, cum, leaves, avg, reason }
}

#[test]
fn an_order_goes_to_the_venue_and_fills() {
    let mut book = Book::default();
    assert_eq!(book.send(VENUE, &wire::RegisterVenue {}), [Out::Registered]);
    let out = book.send(ALICE, &new_order(b"a1", 100, 10));
    assert_eq!(
        out,
        [
            report(1, ExecType::PendingNew, OrdStatus::PendingNew, 100, 0, 100, 0, Reason::None),
            Out::VenueNew { order_id: 1, qty: 100, price: 10 },
        ]
    );
    let out = book.send(VENUE, &wire::VenueAck { order_id: 1 });
    assert_eq!(out, [report(1, ExecType::New, OrdStatus::New, 100, 0, 100, 0, Reason::None)]);
    let out = book.send(VENUE, &wire::Fill { order_id: 1, qty: 60, price: 10 });
    assert_eq!(out, [report(1, ExecType::Trade, OrdStatus::PartiallyFilled, 100, 60, 40, 10, Reason::None)]);
    // (60 × 10 + 40 × 11) / 100 = 10.4, in whole ticks.
    let out = book.send(VENUE, &wire::Fill { order_id: 1, qty: 40, price: 11 });
    assert_eq!(out, [report(1, ExecType::Trade, OrdStatus::Filled, 100, 100, 0, 10, Reason::None)]);
    assert!(book.send(VENUE, &wire::Fill { order_id: 1, qty: 1, price: 11 }).is_empty(), "nothing left to fill");
}

#[test]
fn bad_orders_are_rejected_before_they_reach_the_venue() {
    let mut book = Book::default();
    let rejected = |reason| report(0, ExecType::Rejected, OrdStatus::Rejected, 100, 0, 0, 0, reason);
    assert_eq!(book.send(ALICE, &new_order(b"a1", 100, 10)), [rejected(Reason::NoVenue)]);
    book.send(VENUE, &wire::RegisterVenue {});
    book.send(ALICE, &new_order(b"a1", 100, 10));
    assert_eq!(book.send(ALICE, &new_order(b"a1", 100, 10)), [rejected(Reason::DuplicateClOrdId)]);
    assert_eq!(book.send(ALICE, &new_order(b"", 100, 10)), [rejected(Reason::InvalidClOrdId)]);
    assert_eq!(book.send(ALICE, &new_order(b"a2", 100, 0)), [rejected(Reason::InvalidPrice)]);
    let out = book.send(ALICE, &wire::NewOrder { symbol: b"", ..new_order(b"a3", 100, 10) });
    assert_eq!(out, [rejected(Reason::InvalidSymbol)]);
    let zero = report(0, ExecType::Rejected, OrdStatus::Rejected, 0, 0, 0, 0, Reason::InvalidQty);
    assert_eq!(book.send(ALICE, &new_order(b"a4", 0, 10)), [zero]);
    // Another session may use the same client order ID.
    assert!(matches!(book.send(BOB, &new_order(b"a1", 100, 10))[0], Out::Report { exec: ExecType::PendingNew, .. }));
}

#[test]
fn a_cancel_goes_through_the_venue_and_only_the_owner_may_ask() {
    let (mut book, id) = Book::with_order();
    assert_eq!(
        book.send(BOB, &wire::CancelOrder { order_id: id }),
        [Out::CancelReject { to: BOB, order_id: id, reason: Reason::NotYourOrder }]
    );
    assert_eq!(
        book.send(ALICE, &wire::CancelOrder { order_id: 99 }),
        [Out::CancelReject { to: ALICE, order_id: 99, reason: Reason::UnknownOrder }]
    );
    assert_eq!(
        book.send(ALICE, &wire::CancelOrder { order_id: id }),
        [
            report(id, ExecType::PendingCancel, OrdStatus::PendingCancel, 100, 0, 100, 0, Reason::None),
            Out::VenueCancel { order_id: id }
        ]
    );
    let pending = Out::CancelReject { to: ALICE, order_id: id, reason: Reason::Pending };
    assert_eq!(book.send(ALICE, &wire::CancelOrder { order_id: id }), [pending]);
    // A fill can still land while the cancel is pending.
    let out = book.send(VENUE, &wire::Fill { order_id: id, qty: 30, price: 10 });
    assert_eq!(out, [report(id, ExecType::Trade, OrdStatus::PendingCancel, 100, 30, 70, 10, Reason::None)]);
    let out = book.send(VENUE, &wire::VenueCanceled { order_id: id });
    assert_eq!(out, [report(id, ExecType::Canceled, OrdStatus::Canceled, 100, 30, 0, 10, Reason::None)]);
    let too_late = Out::CancelReject { to: ALICE, order_id: id, reason: Reason::TooLate };
    assert_eq!(book.send(ALICE, &wire::CancelOrder { order_id: id }), [too_late]);
}

#[test]
fn a_replace_changes_the_order_once_the_venue_confirms_it() {
    let (mut book, id) = Book::with_order();
    book.send(VENUE, &wire::Fill { order_id: id, qty: 60, price: 10 });
    let invalid = Out::CancelReject { to: ALICE, order_id: id, reason: Reason::InvalidQty };
    assert_eq!(
        book.send(ALICE, &wire::ReplaceOrder { order_id: id, qty: 60, price: 10 }),
        [invalid],
        "not above what's filled"
    );
    assert_eq!(
        book.send(ALICE, &wire::ReplaceOrder { order_id: id, qty: 150, price: 12 }),
        [
            report(id, ExecType::PendingReplace, OrdStatus::PendingReplace, 100, 60, 40, 10, Reason::None),
            Out::VenueReplace { order_id: id, qty: 150, price: 12 }
        ]
    );
    let out = book.send(VENUE, &wire::VenueReplaced { order_id: id });
    assert_eq!(out, [report(id, ExecType::Replaced, OrdStatus::PartiallyFilled, 150, 60, 90, 10, Reason::None)]);

    // A replace the venue refuses leaves the order as it was.
    book.send(ALICE, &wire::ReplaceOrder { order_id: id, qty: 200, price: 12 });
    let out = book.send(VENUE, &wire::VenueRequestReject { order_id: id });
    assert_eq!(out, [Out::CancelReject { to: ALICE, order_id: id, reason: Reason::VenueRejected }]);
    let out = book.send(VENUE, &wire::Fill { order_id: id, qty: 90, price: 10 });
    assert_eq!(out, [report(id, ExecType::Trade, OrdStatus::Filled, 150, 150, 0, 10, Reason::None)]);
}

#[test]
fn only_the_venue_reports_fills_and_nonsense_is_dropped() {
    let (mut book, id) = Book::with_order();
    assert!(book.send(ALICE, &wire::Fill { order_id: id, qty: 10, price: 10 }).is_empty(), "Alice isn't the venue");
    assert!(book.send(VENUE, &wire::Fill { order_id: id, qty: 101, price: 10 }).is_empty(), "more than is left");
    assert!(book.send(VENUE, &wire::Fill { order_id: id, qty: 10, price: 0 }).is_empty(), "no price");
    let mut payload = Vec::new();
    new_order(b"a2", 100, 10).encode_into(&mut payload).unwrap();
    for cut in 0..payload.len() {
        assert!(book.raw(ALICE, &payload[..cut]).is_empty(), "cut at {cut}");
    }
}

/// A restored book is the same book: same orders, same answers to what comes next.
#[test]
fn a_snapshot_restores_the_book() {
    let (mut book, id) = Book::with_order();
    book.send(VENUE, &wire::Fill { order_id: id, qty: 60, price: 10 });
    book.send(ALICE, &wire::ReplaceOrder { order_id: id, qty: 150, price: 12 });
    book.send(BOB, &new_order(b"b1", 5, 7));
    let mut bytes = Vec::new();
    book.oms.snapshot(&mut bytes);
    let mut restored = Book::default();
    restored.oms.restore(&bytes);
    assert_eq!(restored.oms, book.oms);
    for b in [&mut book, &mut restored] {
        let out = b.send(VENUE, &wire::VenueReplaced { order_id: id });
        assert_eq!(out, [report(id, ExecType::Replaced, OrdStatus::PartiallyFilled, 150, 60, 90, 10, Reason::None)]);
        let out = b.send(BOB, &new_order(b"b2", 5, 7));
        assert!(matches!(out[0], Out::Report { order_id: 3, .. }), "{out:?}");
    }
}
