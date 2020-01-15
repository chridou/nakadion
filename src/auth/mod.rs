//! Optional OAUTH authorization for connecting to Nakadi
//!
use std::convert::AsRef;
use std::error::Error;
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;

use futures::future::{self, BoxFuture, FutureExt, TryFutureExt};
use tokio::fs;

use must_env_parsed;

use crate::env_vars::*;
use crate::helpers::MessageError;

pub type TokenFuture<'a> = BoxFuture<'a, Result<Option<AccessToken>, TokenError>>;

/// A token used for authentication against `Nakadi`.
#[derive(Clone)]
pub struct AccessToken(String);

impl AccessToken {
    /// Creates a new token.
    pub fn new<T: Into<String>>(token: T) -> AccessToken {
        AccessToken(token.into())
    }

    pub fn into_inner(self) -> String {
        self.0
    }
}

impl fmt::Display for AccessToken {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "<secret-access-token>")
    }
}

impl fmt::Debug for AccessToken {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "AccessToken(<secret>)")
    }
}

impl AsRef<str> for AccessToken {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

/// Provides an `AccessToken`.
///
/// Authentication can be disabled by returning `None` on `get_token`.
pub trait ProvidesAccessToken {
    /// Get a new `Token`. Return `None` to disable authentication.
    fn get_token(&self) -> TokenFuture;
}

pub struct AccessTokenProvider {
    inner: Arc<dyn ProvidesAccessToken + Send + Sync + 'static>,
}

impl AccessTokenProvider {
    pub fn new<P>(provider: P) -> Self
    where
        P: ProvidesAccessToken + Send + Sync + 'static,
    {
        Self {
            inner: Arc::new(provider),
        }
    }

    /// Creates a new `AccessTokenProvider` from the environment
    ///
    /// This will attempt to create the following providers in the given
    /// order with all their restrictions as if configured from the
    /// environment individually
    ///
    /// 1. `FileAccessTokenProvider`
    /// 2. `FixedAccessTokenProvider`
    /// 3. `NoAuthAccessTokenProvider`
    /// 4. Fail
    pub fn from_env() -> Result<Self, Box<dyn std::error::Error>> {
        if let Ok(provider) = FileAccessTokenProvider::from_env() {
            return Ok(Self::new(provider));
        }

        if let Ok(provider) = FixedAccessTokenProvider::from_env() {
            return Ok(Self::new(provider));
        }

        if let Ok(provider) = NoAuthAccessTokenProvider::from_env() {
            return Ok(Self::new(provider));
        }

        Err(MessageError::new("no access token provider could be initialized").boxed())
    }
}

impl ProvidesAccessToken for AccessTokenProvider {
    fn get_token(&self) -> TokenFuture {
        self.inner.get_token()
    }
}

/// Using this access token provider disables OAUTH.
pub struct NoAuthAccessTokenProvider;

impl NoAuthAccessTokenProvider {
    /// Initializes from the env var `NAKADION_ACCESS_TOKEN_ALLOW_NONE`.
    ///
    /// The env var must exist and must be set to `true` to not make this
    /// function fail.
    pub fn from_env() -> Result<Self, Box<dyn std::error::Error>> {
        let allowed: bool = must_env_parsed!(ALLOW_NO_TOKEN_ENV_VAR)?;

        if allowed {
            Ok(Self)
        } else {
            Err(MessageError::new("'NAKADION_ACCESS_TOKEN_ALLOW_NONE' was set to 'false'.").boxed())
        }
    }
}

impl ProvidesAccessToken for NoAuthAccessTokenProvider {
    fn get_token(&self) -> TokenFuture {
        future::ok(None).boxed()
    }
}

/// Reads an `AccessToken` from a file.
///
/// Simply clone it around.
#[derive(Clone)]
pub struct FileAccessTokenProvider {
    path: PathBuf,
}

impl FileAccessTokenProvider {
    pub fn new<P: Into<PathBuf>>(path: P) -> FileAccessTokenProvider {
        FileAccessTokenProvider { path: path.into() }
    }

    /// Create a new `FileAccessTokenProvider` which reads the token from a file
    /// at the given path.
    ///
    /// The path must be a fully qualified file path contained
    /// in the env var `NAKADION_ACCESS_TOKEN_PATH`.
    ///
    /// The existence of the file at the given path is not checked.
    pub fn from_env() -> Result<FileAccessTokenProvider, Box<dyn Error>> {
        let path: PathBuf = must_env_parsed!(TOKEN_PATH_ENV_VAR)?;
        Ok(FileAccessTokenProvider::new(path))
    }
}

impl ProvidesAccessToken for FileAccessTokenProvider {
    fn get_token(&self) -> TokenFuture {
        async move {
            let bytes = fs::read(self.path.clone())
                .map_err(|err| TokenError(err.to_string()))
                .await?;

            let token = String::from_utf8(bytes)
                .map(AccessToken)
                .map_err(|err| TokenError(err.to_string()))?;

            Ok(Some(token))
        }
        .boxed()
    }
}

/// Always returns the same token.
///
/// Simply clone it around.
#[derive(Clone)]
pub struct FixedAccessTokenProvider {
    token: AccessToken,
}

impl FixedAccessTokenProvider {
    pub fn new<P: Into<String>>(token: P) -> Self {
        FixedAccessTokenProvider {
            token: AccessToken(token.into()),
        }
    }

    /// Create a new `FixedAccessTokenProvider` initializes the token with the
    /// the value of the given env var `NAKADION_ACCESS_TOKEN_FIXED`.
    pub fn from_env() -> Result<Self, Box<dyn Error>> {
        let token = must_env!(TOKEN_FIXED_ENV_VAR)?;
        Ok(Self::new(token))
    }
}

impl ProvidesAccessToken for FixedAccessTokenProvider {
    fn get_token(&self) -> TokenFuture {
        future::ok(Some(self.token.clone())).boxed()
    }
}

/// An error returned when no access token was available when there should be one.
///
/// This struct does not contain any details or reasons for logging.
///
/// It is the duty of the implementor of `ProvidesAccessToken` to log errors.
#[derive(Debug)]
pub struct TokenError(String);

impl fmt::Display for TokenError {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "Failed to get access token: {}", self.0)
    }
}

impl Error for TokenError {
    fn cause(&self) -> Option<&dyn Error> {
        None
    }
}
