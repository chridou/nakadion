//! Optional OAUTH authorization for connecting to Nakadi
use std::fmt;

/// A token used for authentication against `Nakadi`.
#[derive(Clone, Debug)]
pub struct AccessToken(String);

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
        Ok(None)
    }
}

/// An error returned when no access token was available when there should be one.
///
/// This struct does not contain any details or reasons for logging.
///
/// It is the duty of the implementor of `ProvidesAccessToken` to log errors.
#[derive(Fail, Debug, Clone)]
#[fail(display = "Failed to get access token")]
pub struct TokenError;
