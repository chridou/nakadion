/// A token used for authentication against `Nakadi`.
#[derive(Clone, Debug)]
pub struct AccessToken(pub String);

impl AccessToken {
    /// Creates a new token.
    pub fn new<T: Into<String>>(token: T) -> AccessToken {
        AccessToken(token.into())
    }
}

/// Provides an `AccessToken`.
///
/// Authentication can be disabled by returning `None` on `get_token`.
pub trait ProvidesAccessToken {
    /// Get a new `Token`. Return `None` to disable authentication.
    fn get_token(&self) -> error::Result<Option<AccessToken>>;
}

pub mod error {
    error_chain! {
        types {
            Error, ErrorKind, ResultExt, Result;
        }
    }
}
