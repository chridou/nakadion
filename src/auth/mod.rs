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

impl fmt::Display for AccessToken {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "<secret-access-token>")
    }
}

/// Provides an `AccessToken`.
///
/// Authentication can be disabled by returning `None` on `get_token`.
pub trait ProvidesAccessToken {
    /// Get a new `Token`. Return `None` to disable authentication.
    fn get_token(&self) -> Result<Option<AccessToken>, TokenError>;
}

/// Using this access token provider disables OAUTH.
pub struct NoAuthAccessTokenProvider;

impl ProvidesAccessToken for NoAuthAccessTokenProvider {
    fn get_token(&self) -> Result<Option<AccessToken>, TokenError> {
        None
    }
}

#[derive(Fail, Debug, Clone)]
pub enum TokenError {
    #[fail(display = "Client Error: {}", message)]
    Client { message: String },
    #[fail(display = "Server Error: {}", message)]
    Server { message: String },
    #[fail(display = "Other Error: {}", message)]
    Other { message: String },
}
