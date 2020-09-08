use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::nakadi_types::{subscription::StreamParameters, Error};

new_type! {
    #[doc="If `true` abort the consumer when an auth error occurs while connecting to a stream.\n\n\
        In some environments it can be good retry on auth errors since it \
        it might take some time for auth to be set up correctly\n\n\
        The default is `false` which means no abort occurs on auth errors.\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub copy struct ConnectAbortOnAuthError(bool, env="CONNECT_ABORT_ON_AUTH_ERROR");
}
impl Default for ConnectAbortOnAuthError {
    fn default() -> Self {
        false.into()
    }
}

new_type! {
    #[doc="If `true` abort the consumer when a conflict (409) occurs while connecting to a stream.\n\n\
        This can happen when there are no free slots or cursors are being reset.\n\n
        The default is `false` which means no abort occurs on conflicts.\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub copy struct ConnectAbortOnConflict(bool, env="CONNECT_ABORT_ON_CONFLICT");
}
impl Default for ConnectAbortOnConflict {
    fn default() -> Self {
        false.into()
    }
}

new_type! {
    #[doc="If `true` abort the consumer when a subscription does not exist when connection to a stream.\n\n\
    The default is `true`\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub copy struct ConnectAbortOnSubscriptionNotFound(bool, env="CONNECT_ABORT_ON_SUBSCRIPTION_NOT_FOUND");
}
impl Default for ConnectAbortOnSubscriptionNotFound {
    fn default() -> Self {
        true.into()
    }
}

new_type! {
    #[doc="The maximum time for connecting to a stream.\n\n\
    Default is infinite."]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub secs struct ConnectTimeoutSecs(u64, env="CONNECT_TIMEOUT_SECS");
}

new_type! {
    #[doc="The maximum retry delay between failed attempts to connect to a stream.\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub secs struct ConnectMaxRetryDelaySecs(u64, env="CONNECT_MAX_RETRY_DELAY_SECS");
}
impl Default for ConnectMaxRetryDelaySecs {
    fn default() -> Self {
        300.into()
    }
}
new_type! {
    #[doc="The timeout for a single request made to Nakadi to connect to a stream.\n\n\
    The default is 10 seconds\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub secs struct ConnectAttemptTimeoutSecs(u64, env="CONNECT_ATTEMPT_TIMEOUT_SECS");
}
impl Default for ConnectAttemptTimeoutSecs {
    fn default() -> Self {
        10.into()
    }
}

/// The timeout for a request made to Nakadi to connect to a stream including retries
///
/// ## FromStr
///
/// `ConnectTimeout` can be parsed from a `str` slice.
///
/// ```rust
/// use nakadion::components::connector::ConnectTimeout;
///
/// let ct: ConnectTimeout = "infinite".parse().unwrap();
/// assert_eq!(ct, ConnectTimeout::Infinite);
///
/// let ct: ConnectTimeout = "60".parse().unwrap();
/// assert_eq!(ct, ConnectTimeout::Seconds(60));
///
/// // JSON also works
///
/// let ct: ConnectTimeout = "\"infinite\"".parse().unwrap();
/// assert_eq!(ct, ConnectTimeout::Infinite);
///
/// let ct: ConnectTimeout = r#"{"seconds":60}"#.parse().unwrap();
/// assert_eq!(ct, ConnectTimeout::Seconds(60));
///
/// let ct: ConnectTimeout = "59".parse().unwrap();
/// assert_eq!(ct, ConnectTimeout::Seconds(59));
/// ```
///
/// ## Serialize/Deserialize
///
/// ```rust
/// use nakadion::components::connector::ConnectTimeout;
/// use serde_json::{self, json};
///
/// let infinite_json = json!("infinite");
/// let ct: ConnectTimeout = serde_json::from_value(infinite_json).unwrap();
/// assert_eq!(ct, ConnectTimeout::Infinite);
///
/// let seconds_json = json!({"seconds": 25});
/// let ct: ConnectTimeout = serde_json::from_value(seconds_json).unwrap();
/// assert_eq!(ct, ConnectTimeout::Seconds(25));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectTimeout {
    /// There is no limit on the number of connect attempts being done
    Infinite,
    /// Retry only for the given time.
    ///
    /// This not an exact value and the effective timeout
    /// might be longer than the value given here.
    Seconds(u64),
}

impl ConnectTimeout {
    env_funs!("CONNECT_TIMEOUT");

    pub fn into_duration_opt(self) -> Option<Duration> {
        match self {
            ConnectTimeout::Infinite => None,
            ConnectTimeout::Seconds(secs) => Some(Duration::from_secs(secs)),
        }
    }
}

impl Default for ConnectTimeout {
    fn default() -> Self {
        Self::Infinite
    }
}

impl<T> From<T> for ConnectTimeout
where
    T: Into<u64>,
{
    fn from(v: T) -> Self {
        Self::Seconds(v.into())
    }
}

impl fmt::Display for ConnectTimeout {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConnectTimeout::Infinite => write!(f, "infinite")?,
            ConnectTimeout::Seconds(secs) => write!(f, "{} s", secs)?,
        }

        Ok(())
    }
}

impl FromStr for ConnectTimeout {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim();

        if s.starts_with('{') || s.starts_with('\"') {
            return Ok(serde_json::from_str(s)?);
        }

        match s {
            "infinite" => Ok(ConnectTimeout::Infinite),
            x => {
                let seconds: u64 = x.parse().map_err(|err| {
                    Error::new(format!("{} is not a connector timeout: {}", s, err))
                })?;
                Ok(ConnectTimeout::Seconds(seconds))
            }
        }
    }
}

/// Parameters to configure the `Connector`
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#/subscriptions/subscription_id/events_get)
///
/// # Example
///
/// ```rust
/// use nakadion::components::connector::{ConnectConfig, ConnectTimeout};
///
/// let cfg = ConnectConfig::default()
///     .abort_on_auth_error(true)
///     .timeout_secs(25u64);
///
/// assert_eq!(cfg.abort_on_auth_error, Some(true.into()));
/// assert_eq!(cfg.timeout_secs, Some(ConnectTimeout::Seconds(25)));
///```
#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct ConnectConfig {
    /// `StreamParameters` used to configure the stream consumed from Nakadi
    pub stream_parameters: StreamParameters,
    /// If set to `true` auth error will not cause a retry on connect attempts
    pub abort_on_auth_error: Option<ConnectAbortOnAuthError>,
    /// If set to `true` conflicts (Status 409) will not cause a retry on connect attempts
    pub abort_on_conflict: Option<ConnectAbortOnConflict>,
    /// If `true` abort the consumer when a subscription does not exist when connection to a stream.
    ///
    /// It can make sense if the subscription is expected to not already be there
    /// on startup and waiting for it to became available is intended.
    ///
    /// The default is `true`
    pub abort_connect_on_subscription_not_found: Option<ConnectAbortOnSubscriptionNotFound>,
    /// The maximum time for until a connection to a stream has to be established.
    ///
    /// When elapsed and no stream could be connected to, the `Connector` fails
    ///
    /// Default is to retry indefinitely
    pub timeout_secs: Option<ConnectTimeout>,
    /// The maximum retry delay between failed attempts to connect to a stream.
    pub max_retry_delay_secs: Option<ConnectMaxRetryDelaySecs>,
    /// The timeout for a request made to Nakadi to connect to a stream.
    pub attempt_timeout_secs: Option<ConnectAttemptTimeoutSecs>,
}

impl ConnectConfig {
    env_ctors!();
    fn fill_from_env_prefixed_internal<T: AsRef<str>>(&mut self, prefix: T) -> Result<(), Error> {
        self.stream_parameters
            .fill_from_env_prefixed(prefix.as_ref())?;

        if self.abort_on_auth_error.is_none() {
            self.abort_on_auth_error =
                ConnectAbortOnAuthError::try_from_env_prefixed(prefix.as_ref())?;
        }
        if self.abort_on_conflict.is_none() {
            self.abort_on_conflict =
                ConnectAbortOnConflict::try_from_env_prefixed(prefix.as_ref())?;
        }
        if self.abort_connect_on_subscription_not_found.is_none() {
            self.abort_connect_on_subscription_not_found =
                ConnectAbortOnSubscriptionNotFound::try_from_env_prefixed(prefix.as_ref())?;
        }
        if self.timeout_secs.is_none() {
            self.timeout_secs = ConnectTimeout::try_from_env_prefixed(prefix.as_ref())?;
        }
        if self.max_retry_delay_secs.is_none() {
            self.max_retry_delay_secs =
                ConnectMaxRetryDelaySecs::try_from_env_prefixed(prefix.as_ref())?;
        }
        if self.attempt_timeout_secs.is_none() {
            self.attempt_timeout_secs =
                ConnectAttemptTimeoutSecs::try_from_env_prefixed(prefix.as_ref())?;
        }

        Ok(())
    }

    /// Fills not set values with default values.
    ///
    /// The filled in values are those which would be used if a value was not set.
    pub fn apply_defaults(&mut self) {
        self.stream_parameters.apply_defaults();
        if self.abort_on_auth_error.is_none() {
            self.abort_on_auth_error = Some(ConnectAbortOnAuthError::default());
        }
        if self.abort_on_conflict.is_none() {
            self.abort_on_conflict = Some(ConnectAbortOnConflict::default());
        }
        if self.abort_connect_on_subscription_not_found.is_none() {
            self.abort_connect_on_subscription_not_found =
                Some(ConnectAbortOnSubscriptionNotFound::default());
        }
        if self.timeout_secs.is_none() {
            self.timeout_secs = Some(ConnectTimeout::default());
        }
        if self.max_retry_delay_secs.is_none() {
            self.max_retry_delay_secs = Some(ConnectMaxRetryDelaySecs::default());
        }
        if self.attempt_timeout_secs.is_none() {
            self.attempt_timeout_secs = Some(ConnectAttemptTimeoutSecs::default());
        }
    }

    /// `StreamParameters` used to configure the stream consumed from Nakadi
    pub fn stream_parameters<T: Into<StreamParameters>>(mut self, v: T) -> Self {
        self.stream_parameters = v.into();
        self
    }
    /// If set to `true` auth error will not cause a retry on connect attempts
    pub fn abort_on_auth_error<T: Into<ConnectAbortOnAuthError>>(mut self, v: T) -> Self {
        self.abort_on_auth_error = Some(v.into());
        self
    }
    /// If set to `true` conflicts (Status 409) will not cause a retry on connect attempts
    pub fn abort_on_conflict<T: Into<ConnectAbortOnConflict>>(mut self, v: T) -> Self {
        self.abort_on_conflict = Some(v.into());
        self
    }
    /// If `true` abort the consumer when a subscription does not exist when connection to a stream.
    pub fn abort_connect_on_subscription_not_found<T: Into<ConnectAbortOnSubscriptionNotFound>>(
        mut self,
        v: T,
    ) -> Self {
        self.abort_connect_on_subscription_not_found = Some(v.into());
        self
    }
    /// The maximum time for until a connection to a stream has to be established.
    ///
    /// Default is to retry indefinitely
    pub fn timeout_secs<T: Into<ConnectTimeout>>(mut self, v: T) -> Self {
        self.timeout_secs = Some(v.into());
        self
    }
    /// The maximum retry delay between failed attempts to connect to a stream.
    pub fn max_retry_delay_secs<T: Into<ConnectMaxRetryDelaySecs>>(mut self, v: T) -> Self {
        self.max_retry_delay_secs = Some(v.into());
        self
    }
    /// The timeout for a request made to Nakadi to connect to a stream.
    pub fn attempt_timeout_secs<T: Into<ConnectAttemptTimeoutSecs>>(mut self, v: T) -> Self {
        self.attempt_timeout_secs = Some(v.into());
        self
    }

    /// Modify the current `StreamParameters` with a closure.
    pub fn configure_stream_parameters<F>(self, mut f: F) -> Self
    where
        F: FnMut(StreamParameters) -> StreamParameters,
    {
        self.try_configure_stream_parameters(|params| Ok(f(params)))
            .unwrap()
    }

    /// Modify the current `StreamParameters` with a closure.
    pub fn try_configure_stream_parameters<F>(mut self, mut f: F) -> Result<Self, Error>
    where
        F: FnMut(StreamParameters) -> Result<StreamParameters, Error>,
    {
        self.stream_parameters = f(self.stream_parameters)?;
        Ok(self)
    }
}
