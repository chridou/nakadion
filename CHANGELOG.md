//! ## Changelog
//!
//! * 0.13.1
//!     * TypeBatchHandler also gets cursors on deserialization errors
//! * 0.13.0
//!     * Small breaking change: Handlers are given the current cursor.
//!     Simply adjust the signature of your handlers to take the parameter.
//!     * Pushed Url crate to 2.0
//! * 0.12.0
//!     * Breaking change in NakadiPubisher: Flag on retry startegy required on creation
//! * 0.10.2
//!     * update crate uuid to 0.7
//! * 0.10.1
//!     * Event types must be an optional vec in the incoming metadata
//! * 0.10.0
//!     * Improved typed `TypedHandler` to handle deserialization failures on
//! individual events   * Updated metrix to 0.8
//! * 0.9.0
//!    * Updated metrix to 0.7
//!
