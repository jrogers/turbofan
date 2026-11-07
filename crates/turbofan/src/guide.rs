#![doc = guide_links!()]
#![doc = include_str!("../docs/guide/README.md")]

// Each chapter is a file in the repository's docs/guide/, which crate's docs/guide links to, as
// cargo publish packages only the crate's own files: read on GitHub as well as here, with its
// examples run as doctests. The chapters link to each other with reference-style links whose definitions name the
// files; `guide_links!` defines the same names first, as module paths, and in Markdown the first
// definition of a name is the one used.

#[doc = guide_links!()]
#[doc = include_str!("../docs/guide/use-cases.md")]
pub mod use_cases {}

#[doc = guide_links!()]
#[doc = include_str!("../docs/guide/first-service.md")]
pub mod first_service {}

#[doc = guide_links!()]
#[doc = include_str!("../docs/guide/services.md")]
pub mod services {}

#[doc = guide_links!()]
#[doc = include_str!("../docs/guide/clients.md")]
pub mod clients {}

#[doc = guide_links!()]
#[doc = include_str!("../docs/guide/running.md")]
pub mod running {}

#[doc = guide_links!()]
#[doc = include_str!("../docs/guide/membership.md")]
pub mod membership {}

#[doc = guide_links!()]
#[doc = include_str!("../docs/guide/operations.md")]
pub mod operations {}

#[doc = guide_links!()]
#[doc = include_str!("../docs/guide/upgrades.md")]
pub mod upgrades {}

#[doc = guide_links!()]
#[doc = include_str!("../docs/guide/performance.md")]
pub mod performance {}
