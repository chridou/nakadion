//! Types that are shared throughout the model
use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::Error;

new_type! {
/// Indicator of the application owning this EventType.
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#definition_EventType*owning_application)
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
    pub struct OwningApplication(String, env="OWNING_APPLICATION");
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthorizationAttributes(Vec<AuthorizationAttribute>);

impl AuthorizationAttributes {
    pub fn new<T: Into<Vec<AuthorizationAttribute>>>(v: T) -> Self {
        Self(v.into())
    }

    pub fn att<T: Into<AuthorizationAttribute>>(mut self, v: T) -> Self {
        self.0.push(v.into());
        self
    }

    pub fn push<T: Into<AuthorizationAttribute>>(&mut self, v: T) {
        self.0.push(v.into());
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = &AuthorizationAttribute> {
        self.0.iter()
    }
}

impl IntoIterator for AuthorizationAttributes {
    type Item = AuthorizationAttribute;
    type IntoIter = std::vec::IntoIter<AuthorizationAttribute>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<A> From<A> for AuthorizationAttributes
where
    A: Into<AuthorizationAttribute>,
{
    fn from(k: A) -> Self {
        Self::new(vec![k.into()])
    }
}

impl<A, B, C> From<(A, B, C)> for AuthorizationAttributes
where
    A: Into<AuthorizationAttribute>,
    B: Into<AuthorizationAttribute>,
    C: Into<AuthorizationAttribute>,
{
    fn from((a, b, c): (A, B, C)) -> Self {
        Self::new(vec![a.into(), b.into(), c.into()])
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

impl FromStr for AuthorizationAttribute {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split(':').map(|s| s.trim()).collect();

        if parts.len() != 2 {
            return Err(Error::new(format!(
                "{} is not a valid AuthorizationAttribute",
                s
            )));
        }

        Ok(Self::new(parts[0], parts[1]))
    }
}

impl<U, V> From<(U, V)> for AuthorizationAttribute
where
    U: Into<AuthAttDataType>,
    V: Into<AuthAttValue>,
{
    fn from((u, v): (U, V)) -> Self {
        AuthorizationAttribute::new(u, v)
    }
}

impl fmt::Display for AuthorizationAttribute {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.data_type, self.value)?;
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
