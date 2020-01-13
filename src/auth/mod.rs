//! Optional OAUTH authorization for connecting to Nakadi
use std::convert::AsRef;
use std::env;
use std::error::Error;
use std::fmt;
use std::path::PathBuf;

use futures::future::{self, BoxFuture, FutureExt, TryFutureExt};
use tokio::fs;

pub type TokenFuture<'a> = BoxFuture<'a, Result<Option<AccessToken>, TokenError>>;

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

/// Using this access token provider disables OAUTH.
pub struct NoAuthAccessTokenProvider;

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

    /// Create a new `FileAccessTokenProvider` which reads the token from a
    /// fully qualified file path contained in the env var `file_path_env_var`.
    pub fn from_env<V: AsRef<str>>(
        file_path_env_var: V,
    ) -> Result<FileAccessTokenProvider, Box<dyn Error>> {
        let path = env::var(file_path_env_var.as_ref()).map_err(Box::new)?;
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
