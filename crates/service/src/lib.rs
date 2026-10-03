//! Shared CompactTxStreamer behavior with gRPC and Zakura p2p transport adapters.

pub mod grpc;
pub mod p2p;
mod serve;
mod service;

pub use service::{
    CompactService, HeadFollowerConfig, HeadFollowerError, Readiness, ServingSnapshot,
    SnapshotError,
};

mod telemetry;
