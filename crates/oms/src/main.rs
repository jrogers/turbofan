//! A node of the OMS cluster: `oms-node serve ...` or `oms-node snapshot ...` (see
//! [`turbofan_cluster::run`]).

use std::process::ExitCode;

fn main() -> ExitCode {
    turbofan_cluster::run(oms::SERVICE_ID, oms::Oms::default())
}
