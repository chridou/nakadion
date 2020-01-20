use std::fmt;

use serde::{Deserialize, Serialize};

/// Indicator of the application owning this EventType.
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#definition_EventType*owning_application)
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct OwningApplication(String);

impl OwningApplication {
    pub fn new<T: Into<String>>(v: T) -> Self {
        OwningApplication(v.into())
    }

    pub fn into_inner(self) -> String {
        self.0
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for OwningApplication {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
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

impl fmt::Display for AuthorizationAttribute {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}->{}", self.data_type, self.value)?;
        Ok(())
    }
}

/// Data type of `AuthorizationAttribute`
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#definition_AuthorizationAttribute)
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct AuthAttDataType(String);

impl AuthAttDataType {
    pub fn new<T: Into<String>>(v: T) -> Self {
        AuthAttDataType(v.into())
    }

    pub fn into_inner(self) -> String {
        self.0
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for AuthAttDataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)?;
        Ok(())
    }
}

/// Value of `AuthorizationAttribute`
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#definition_AuthorizationAttribute)
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct AuthAttValue(String);

impl AuthAttValue {
    pub fn new<T: Into<String>>(v: T) -> Self {
        AuthAttValue(v.into())
    }

    pub fn into_inner(self) -> String {
        self.0
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for AuthAttValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)?;
        Ok(())
    }
}
