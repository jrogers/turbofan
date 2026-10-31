//! A node of the OMS cluster: `oms-node serve ...` or `oms-node snapshot ...` (see
//! [`turbofan::run`]).

use std::process::ExitCode;

fn main() -> ExitCode {
    turbofan::run(oms::SERVICE_ID, oms::Oms::default())
}
