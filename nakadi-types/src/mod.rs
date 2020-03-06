//! Types that are part of the Nakadi API
//!
//! In cases where Nakadi returns a collections of items there is usually a wrapper
//! struct present which provides additional methods for introspecting the
//! contents of the original response.
pub mod event;
pub mod event_type;
pub mod misc;
pub mod partition;
pub mod publishing;
pub mod subscription;
