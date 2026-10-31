//! Codecs generated from SBE's example schema (`sbe/example-schema.xml`) and `sbe/versioned.xml`,
//! checked in, and checked here to be what the generator writes now. The golden car
//! (`sbe/golden/car.bin`) was encoded by real-logic's own SBE codecs: the generated codec must read
//! it and write the same bytes.

#[allow(dead_code)]
#[path = "sbe/car.rs"]
mod car;

#[allow(dead_code)]
#[path = "sbe/versioned.rs"]
mod versioned;

use turbofan_sbe::{Block, SbeError};

const CAR: &[u8] = include_bytes!("sbe/golden/car.bin");

/// The car Golden.java encodes.
fn car_message() -> car::Car<'static> {
    use car::*;
    const ACCELERATION_95: &[CarPerformanceFiguresAcceleration] = &[
        CarPerformanceFiguresAcceleration { mph: 30, seconds: 4.0 },
        CarPerformanceFiguresAcceleration { mph: 60, seconds: 7.5 },
        CarPerformanceFiguresAcceleration { mph: 100, seconds: 12.2 },
    ];
    const ACCELERATION_99: &[CarPerformanceFiguresAcceleration] =
        &[CarPerformanceFiguresAcceleration { mph: 30, seconds: 3.8 }];
    Car {
        serial_number: 1234,
        model_year: 2013,
        available: BooleanType::T,
        code: Model::A,
        some_numbers: [0, 10, 20, 30],
        vehicle_code: *b"abcdef",
        extras: OptionalExtras::default().with_cruise_control(true).with_sports_pack(true),
        engine: Engine {
            capacity: 2000,
            num_cylinders: 4,
            manufacturer_code: *b"123",
            efficiency: 35,
            booster_enabled: BooleanType::T,
            booster: Booster { boost_type: BoostType::Nitrous, horse_power: 200 },
        },
        fuel_figures: &[
            CarFuelFigures { speed: 30, mpg: 35.9, usage_description: b"Urban Cycle" },
            CarFuelFigures { speed: 55, mpg: 49.0, usage_description: b"Combined Cycle" },
            CarFuelFigures { speed: 75, mpg: 40.0, usage_description: b"Highway Cycle" },
        ],
        performance_figures: &[
            CarPerformanceFigures { octane_rating: 95, acceleration: ACCELERATION_95 },
            CarPerformanceFigures { octane_rating: 99, acceleration: ACCELERATION_99 },
        ],
        manufacturer: b"Honda",
        model: b"Civic VTi",
        activation_code: b"abcdef",
    }
}

/// The order Golden.java encodes.
fn encode(encode_into: impl Fn(&mut Vec<u8>) -> Result<(), SbeError>) -> Vec<u8> {
    let mut out = Vec::new();
    encode_into(&mut out).unwrap();
    out
}

#[test]
fn encoding_matches_real_logics_codecs() {
    assert_eq!(encode(|out| car_message().encode_into(out)), CAR);
}

#[test]
fn a_car_from_real_logics_codecs_decodes() {
    let (car::Decoded::Car(car), len) = car::decode(CAR).unwrap();
    assert_eq!(len, CAR.len());
    assert_eq!(
        (car.serial_number(), car.model_year(), car.available(), car.code()),
        (1234, 2013, car::BooleanType::T, car::Model::A)
    );
    assert_eq!(car.some_numbers(), [0, 10, 20, 30]);
    assert_eq!(car.vehicle_code(), b"abcdef");
    let extras = car.extras();
    assert!(extras.cruise_control() && extras.sports_pack() && !extras.sun_roof());
    assert_eq!(car.discounted_model(), car::Model::C);
    let engine = car.engine();
    assert_eq!((engine.capacity, engine.num_cylinders, engine.booster.boost_type), (2000, 4, car::BoostType::Nitrous));
    assert_eq!((car::Engine::MAX_RPM, car::Engine::FUEL), (9000, &b"Petrol"[..]));

    let fuel: Vec<_> = car.fuel_figures().map(|f| (f.speed(), f.mpg(), f.usage_description())).collect();
    assert_eq!(fuel, [(30, 35.9, &b"Urban Cycle"[..]), (55, 49.0, b"Combined Cycle"), (75, 40.0, b"Highway Cycle")]);
    let performance: Vec<_> = car
        .performance_figures()
        .map(|p| (p.octane_rating(), p.acceleration().map(|a| a.mph()).collect::<Vec<_>>()))
        .collect();
    assert_eq!(performance, [(95, vec![30, 60, 100]), (99, vec![30])]);
    assert_eq!(
        (car.manufacturer(), car.model(), car.activation_code()),
        (&b"Honda"[..], &b"Civic VTi"[..], &b"abcdef"[..])
    );
}

#[test]
fn every_truncation_is_an_error_not_a_panic() {
    for len in 0..CAR.len() {
        assert_eq!(car::decode(&CAR[..len]).unwrap_err(), SbeError::Truncated, "{len} bytes");
    }
}

#[test]
fn the_header_must_name_the_schema_and_a_template() {
    let mut other_schema = CAR.to_vec();
    other_schema[4] = 9;
    assert_eq!(car::decode(&other_schema).unwrap_err(), SbeError::SchemaId(9));
    let mut unknown = CAR.to_vec();
    unknown[2] = 250;
    assert_eq!(car::decode(&unknown).unwrap_err(), SbeError::UnknownTemplate(250));
}

#[test]
fn a_longer_block_from_a_newer_version_is_read_as_far_as_the_schema_knows() {
    // Four more bytes at the end of the root block, before the groups.
    let block_length = car::Car::BLOCK_LENGTH;
    let mut newer = CAR.to_vec();
    newer[0..2].copy_from_slice(&u16::try_from(block_length + 4).unwrap().to_le_bytes());
    newer[6..8].copy_from_slice(&9_u16.to_le_bytes());
    newer.splice(8 + block_length..8 + block_length, [0xAA; 4]);
    let (car::Decoded::Car(car), len) = car::decode(&newer).unwrap();
    assert_eq!((len, car.sbe_version(), car.serial_number()), (newer.len(), 9, 1234));
    assert_eq!(car.fuel_figures().len(), 3);
    assert_eq!(car.activation_code(), b"abcdef");
}

#[test]
fn a_block_shorter_than_its_version_needs_is_an_error() {
    let mut short = CAR.to_vec();
    short[0] -= 1;
    assert!(matches!(car::decode(&short).unwrap_err(), SbeError::BlockLength { .. }));
}

#[test]
fn fields_newer_than_the_message_read_as_absent() {
    use versioned::*;
    let order = Order { id: 7, qty: 50, price: Some(1.5), legs: &[OrderLegs { ratio: -2 }], note: b"hi" };
    let current = encode(|out| order.encode_into(out));
    let (Decoded::Order(read), len) = decode(&current).unwrap();
    assert_eq!(
        (len, read.id(), read.qty(), read.price(), read.note()),
        (current.len(), 7, Some(50), Some(1.5), &b"hi"[..])
    );
    assert_eq!(read.legs().map(|l| l.ratio()).collect::<Vec<_>>(), [-2]);
    // Big-endian on the wire.
    assert_eq!(current[8..12], 7_u32.to_be_bytes());

    // Version 0: a block of just the id, and no legs group or note after it.
    let version_0 = [&[0, 4, 0, 1, 0, 7, 0, 0][..], &7_u32.to_be_bytes()].concat();
    let (Decoded::Order(read), len) = decode(&version_0).unwrap();
    assert_eq!((len, read.id(), read.qty(), read.price(), read.note()), (12, 7, None, None, &b""[..]));
    assert!(read.legs().is_empty());

    // Version 1: the qty and legs, but no price or note.
    let version_1 =
        [&[0, 8, 0, 1, 0, 7, 0, 1][..], &7_u32.to_be_bytes(), &50_u32.to_be_bytes(), &[0, 2, 0, 1, 0xFF, 0xFE]]
            .concat();
    let (Decoded::Order(read), len) = decode(&version_1).unwrap();
    assert_eq!(
        (len, read.qty(), read.price(), read.legs().map(|l| l.ratio()).collect::<Vec<_>>()),
        (version_1.len(), Some(50), None, vec![-2])
    );
}

#[test]
fn too_much_data_is_an_error_and_leaves_the_buffer_as_it_was() {
    let order = versioned::Order { id: 1, qty: 1, price: None, legs: &[], note: b"more than eight" };
    let mut out = b"before".to_vec();
    let error = order.encode_into(&mut out).unwrap_err();
    assert_eq!(error, SbeError::TooLong { name: "note", len: 15, max: 8 });
    assert_eq!(out, b"before");
}

#[test]
fn group_entries_are_checked_when_the_message_is_wrapped() {
    // A fuel figure whose description runs past the end of the buffer fails the wrap, so the
    // group iterator never meets it.
    let body = &CAR[8..];
    let cut = body.len() - 30;
    assert_eq!(<car::CarRef as Block>::wrap(&body[..cut], car::Car::BLOCK_LENGTH, 0).unwrap_err(), SbeError::Truncated);
}

#[test]
fn unknown_enum_values_are_kept() {
    let car = car::Car { code: car::Model::Unknown(b'Z'), ..car_message() };
    let bytes = encode(|out| car.encode_into(out));
    let (car::Decoded::Car(read), _) = car::decode(&bytes).unwrap();
    assert_eq!(read.code(), car::Model::Unknown(b'Z'));
    assert_eq!(car::Model::Unknown(b'Z').raw(), b'Z');
}

/// Generated enums compare by their value on the wire, so `Unknown` holding a listed value is
/// that value's variant.
#[test]
fn enums_compare_by_their_value_on_the_wire() {
    assert_eq!(car::BoostType::Unknown(b'T'), car::BoostType::Turbo);
    assert_ne!(car::BoostType::Unknown(b'X'), car::BoostType::Turbo);
    let set = std::collections::HashSet::from([car::BoostType::Turbo]);
    assert!(set.contains(&car::BoostType::from_raw(b'T')) && set.contains(&car::BoostType::Unknown(b'T')));
}

/// The checked-in codecs are what the generator writes now, formatted with rustfmt.
/// `SBE_REGENERATE=1` rewrites them instead.
#[test]
fn checked_in_codecs_are_current() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/sbe");
    for (schema, codec) in [("example-schema.xml", "car.rs"), ("versioned.xml", "versioned.rs")] {
        let code = turbofan_sbe::codegen::SbeGenerator::load(dir.join(schema)).unwrap().render().unwrap();
        let mut rustfmt = std::process::Command::new("rustfmt")
            .args(["--edition", "2024", "--config", "max_width=120,use_small_heuristics=Max"])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn()
            .expect("rustfmt runs");
        std::io::Write::write_all(&mut rustfmt.stdin.take().unwrap(), code.as_bytes()).unwrap();
        let formatted = String::from_utf8(rustfmt.wait_with_output().unwrap().stdout).unwrap();
        let path = dir.join(codec);
        if std::env::var_os("SBE_REGENERATE").is_some() {
            std::fs::write(&path, formatted).unwrap();
        } else {
            assert!(
                std::fs::read_to_string(&path).unwrap() == formatted,
                "{codec} is stale: SBE_REGENERATE=1 cargo test"
            );
        }
    }
}
