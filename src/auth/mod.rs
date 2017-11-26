use std::fmt;

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
    fn get_token(&self) -> Result<Option<AccessToken>, TokenError>;
}

#[derive(Debug, Clone)]
pub enum TokenError {
    Client { message: String },
    Server { message: String },
    Other { message: String },
}

impl fmt::Display for TokenError {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match *self {
            TokenError::Client { ref message } => write!(f, "TokenError::Client({})", message),
            TokenError::Server { ref message } => write!(f, "TokenError::Server({})", message),
            TokenError::Other { ref message } => write!(f, "TokenError::Other({})", message),
        }
    }
}
