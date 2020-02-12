//! Types that are shared throughout the model
use std::fmt;

use serde::{Deserialize, Serialize};

new_type! {
/// Indicator of the application owning this EventType.
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#definition_EventType*owning_application)
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
    pub struct OwningApplication(String, env="OWNING_APPLICATION");
}

/// An attribute for authorization.
///
/// This object includes a data type, which represents the type of the
/// attribute attribute (which data types are allowed depends on which authorization
/// plugin is deployed, and how it is configured), and a value.
/// A wildcard can be represented with data type and value. It means that
/// all authenticated users are allowed to perform an operation.
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#definition_AuthorizationAttribute)
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct AuthorizationAttribute {
    /// The type of attribute (e.g., ‘team’, or ‘permission’, depending on the Nakadi configuration)
    pub data_type: AuthAttDataType,
    /// The value of the attribute
    pub value: AuthAttValue,
}

impl AuthorizationAttribute {
    pub fn new<D: Into<AuthAttDataType>, V: Into<AuthAttValue>>(data_type: D, value: V) -> Self {
        Self {
            data_type: data_type.into(),
            value: value.into(),
        }
    }
}

impl<U, V> From<(U, V)> for AuthorizationAttribute
where
    U: Into<AuthAttDataType>,
    V: Into<AuthAttValue>,
{
    fn from(tuple: (U, V)) -> Self {
        AuthorizationAttribute::new(tuple.0, tuple.1)
    }
}

impl fmt::Display for AuthorizationAttribute {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}->{}", self.data_type, self.value)?;
        Ok(())
    }
}

new_type! {
/// Data type of `AuthorizationAttribute`
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#definition_AuthorizationAttribute)
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
    pub struct AuthAttDataType(String);
}

impl AuthAttDataType {
    /// Creates an `AuthorizationAttribute` with the this data type and the given value
    pub fn with_value<V: Into<AuthAttValue>>(self, value: V) -> AuthorizationAttribute {
        AuthorizationAttribute::new(self.0, value)
    }
}

new_type! {
/// Value of `AuthorizationAttribute`
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#definition_AuthorizationAttribute)
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
    pub struct AuthAttValue(String);
}
