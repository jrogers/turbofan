//! A node of the OMS cluster: `oms-node serve ...` or `oms-node snapshot ...` (see
//! [`cluster::run`]).

use std::process::ExitCode;

fn main() -> ExitCode {
    cluster::run(oms::SERVICE_ID, oms::Oms::default())
}
