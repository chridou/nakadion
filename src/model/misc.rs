//! Types that appear at multiple locations

/// Indicator of the Application owning this EventType.
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#definition_EventType*owning_application)
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct OwningApplication(String);

impl OwningApplication {
    pub fn new(v: impl Into<String>) -> Self {
        OwningApplication(v.into())
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct AuthorizationAttribute {
    data_type: String,
    value: String,
}
