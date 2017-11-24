pub mod stream_connector {
    use SubscriptionId;
    use auth::{AccessToken, ProvidesAccessToken};
    use nakadi::model::{BatchCommitData, StreamId};
    use reqwest::{Client, Response};
    use reqwest::header::{Authorization, Bearer, Headers};
    use std::env;
    use std::time::Duration;
    use std::io::{BufRead, BufReader, Error as IoError, Split};
    use std::borrow::Cow;

    const LINE_SPLIT_BYTE: u8 = b'\n';

    pub type LineResult = ::std::result::Result<Vec<u8>, IoError>;

    /// Settings for establishing a connection to `Nakadi`.
    #[derive(Builder, Debug)]
    #[builder(pattern = "owned")]
    pub struct NakadiConnectorSettings {
        /// Maximum number of empty keep alive batches to get in a row before closing the
        /// connection. If 0 or undefined will send keep alive messages indefinitely.
        #[builder(default = "0")]
        pub stream_keep_alive_limit: usize,
        /// Maximum number of `Event`s in this stream (over all partitions being streamed
        /// in this
        /// connection).
        ///
        /// * If 0 or undefined, will stream batches indefinitely.
        /// * Stream initialization will fail if `stream_limit` is lower than `batch_limit`.
        #[builder(default = "0")]
        pub stream_limit: usize,
        /// Maximum time in seconds a stream will live before connection is closed by the
        /// server.
        ///
        /// If 0 or unspecified will stream indefinitely.
        /// If this timeout is reached, any pending messages (in the sense of
        /// `stream_limit`)
        /// will be flushed to the client.
        /// Stream initialization will fail if `stream_timeout` is lower than
        /// `batch_flush_timeout`.
        #[builder(default = "Duration::from_secs(0)")]
        pub stream_timeout: Duration,
        /// Maximum time in seconds to wait for the flushing of each chunk (per partition).
        ///
        ///  * If the amount of buffered Events reaches `batch_limit`
        /// before this `batch_flush_timeout` is reached, the messages are immediately
        /// flushed to the client and batch flush timer is reset.
        ///  * If 0 or undefined, will assume 30 seconds.
        #[builder(default = "Duration::from_secs(0)")]
        pub batch_flush_timeout: Duration,
        /// Maximum number of `Event`s in each chunk (and therefore per partition) of the
        /// stream.
        ///
        ///  * If 0 or unspecified will buffer Events indefinitely and flush on reaching of
        ///  `batch_flush_timeout`.
        #[builder(default = "0")]
        pub batch_limit: usize,
        /// The amount of uncommitted events Nakadi will stream before pausing the stream.
        /// When in paused state and commit comes - the stream will resume. Minimal value
        /// is 1.
        ///
        /// When using the concurrent worker you should adjust this value to safe your
        /// workers from running dry.
        #[builder(default = "0")]
        pub max_uncommitted_events: usize,
        /// The URI prefix for the Nakadi Host, e.g. "https://my.nakadi.com"
        pub nakadi_host: String,
        /// The subscription id to use, e.g. "https://my.nakadi.com"
        pub subscription_id: SubscriptionId,
    }

    impl NakadiConnectorSettingsBuilder {
        /// Create a builder from environment variables.
        ///
        /// For variables not found except 'NAKADION_NAKADI_HOST' a default will be set.
        ///
        /// Variables:
        ///
        /// * NAKADION_NAKADI_HOST: See `ConnectorSettings::nakadi_host`
        /// * NAKADION_SUBSCRIPTION_ID: See `ConnectorSettings::subscription_id`
        /// * NAKADION_MAX_UNCOMMITED_EVENTS: See
        /// `ConnectorSettings::max_uncommitted_events`
        /// * NAKADION_BATCH_LIMIT: See `ConnectorSettings::batch_limit`
        /// * NAKADION_BATCH_FLUSH_TIMEOUT_SECS: See
        /// `ConnectorSettings::batch_flush_timeout`
        /// * NAKADION_STREAM_TIMEOUT_SECS: See `ConnectorSettings::stream_timeout`
        /// * NAKADION_STREAM_LIMIT: See `ConnectorSettings::stream_limit`
        /// * NAKADION_STREAM_KEEP_ALIVE_LIMIT: See
        /// `ConnectorSettings::stream_keep_alive_limit`
        pub fn from_env() -> Result<NakadiConnectorSettingsBuilder, String> {
            let builder = NakadiConnectorSettingsBuilder::default();
            let builder = if let Some(env_val) = env::var("NAKADION_STREAM_KEEP_ALIVE_LIMIT").ok() {
                builder.stream_keep_alive_limit(env_val.parse().map_err(|err| {
                    format!(
                        "Could not parse 'NAKADION_STREAM_KEEP_ALIVE_LIMIT': {}",
                        err
                    )
                })?)
            } else {
                warn!(
                    "Environment variable 'NAKADION_STREAM_KEEP_ALIVE_LIMIT' not found. Using \
                     default."
                );
                builder
            };
            let builder = if let Some(env_val) = env::var("NAKADION_STREAM_LIMIT").ok() {
                builder.stream_limit(env_val.parse().map_err(|err| {
                    format!("Could not parse 'NAKADION_STREAM_LIMIT': {}", err)
                })?)
            } else {
                warn!("Environment variable 'NAKADION_STREAM_LIMIT' not found. Using default.");
                builder
            };
            let builder = if let Some(env_val) = env::var("NAKADION_STREAM_TIMEOUT_SECS").ok() {
                builder.stream_timeout(Duration::from_secs(env_val.parse().map_err(|err| {
                    format!("Could not parse 'NAKADION_STREAM_TIMEOUT_SECS': {}", err)
                })?))
            } else {
                warn!(
                    "Environment variable 'NAKADION_STREAM_TIMEOUT_SECS' not found. Using default."
                );
                builder
            };
            let builder = if let Some(env_val) = env::var("NAKADION_BATCH_FLUSH_TIMEOUT_SECS").ok()
            {
                builder.batch_flush_timeout(Duration::from_secs(env_val.parse().map_err(|err| {
                    format!(
                        "Could not parse 'NAKADION_BATCH_FLUSH_TIMEOUT_SECS': {}",
                        err
                    )
                })?))
            } else {
                warn!(
                    "Environment variable 'NAKADION_BATCH_FLUSH_TIMEOUT_SECS' not found. Using \
                     default."
                );
                builder
            };
            let builder = if let Some(env_val) = env::var("NAKADION_BATCH_LIMIT").ok() {
                builder.batch_limit(env_val.parse().map_err(|err| {
                    format!("Could not parse 'NAKADION_BATCH_LIMIT': {}", err)
                })?)
            } else {
                warn!("Environment variable 'NAKADION_BATCH_LIMIT' not found. Using default.");
                builder
            };
            let builder = if let Some(env_val) = env::var("NAKADION_MAX_UNCOMMITED_EVENTS").ok() {
                builder.max_uncommitted_events(env_val.parse().map_err(|err| {
                    format!("Could not parse 'NAKADION_MAX_UNCOMMITED_EVENTS': {}", err)
                })?)
            } else {
                warn!(
                    "Environment variable 'NAKADION_MAX_UNCOMMITED_EVENTS' not found. Using \
                     default."
                );
                builder
            };
            let builder = if let Some(env_val) = env::var("NAKADION_NAKADI_HOST").ok() {
                builder.nakadi_host(env_val)
            } else {
                warn!(
                    "Environment variable 'NAKADION_NAKADI_HOST' not found. It will have to be set \
                     manually."
                );
                builder
            };
            let builder = if let Some(env_val) = env::var("NAKADION_SUBSCRIPTION_ID").ok() {
                builder.subscription_id(SubscriptionId(env_val))
            } else {
                warn!(
                    "Environment variable 'NAKADION_SUBSCRIPTION_ID' not found. It will have \
                     to be set manually."
                );
                builder
            };
            Ok(builder)
        }
    }

    pub trait StreamConnector<T: Iterator<Item = LineResult>> {
        fn connect(&self) -> ::std::result::Result<(StreamId, T), ConnectError>;
        fn commit(&self, batch: &BatchCommitData) -> ::std::result::Result<(), CommitError>;
    }

    pub struct NakadiLineIterator {
        lines: Split<BufReader<Response>>,
    }

    impl NakadiLineIterator {
        pub fn new(response: Response) -> Self {
            let reader = BufReader::new(response);
            NakadiLineIterator {
                lines: reader.split(LINE_SPLIT_BYTE),
            }
        }
    }

    impl Iterator for NakadiLineIterator {
        type Item = LineResult;

        fn next(&mut self) -> Option<LineResult> {
            self.lines.next()
        }
    }

    pub struct NakadiStreamConnector {
        connect_url: String,
        commit_url: String,
        subscription_id: SubscriptionId,
        http_client: Client,
        token_provider: Box<ProvidesAccessToken>,
    }

    impl NakadiStreamConnector {
        pub fn new(
            settings: NakadiConnectorSettings,
            token_provider: Box<ProvidesAccessToken>,
        ) -> NakadiStreamConnector {
            let mut connect_url = String::new();
            connect_url.push_str(&settings.nakadi_host);
            if !connect_url.ends_with("/") {
                connect_url.push('/');
            }
            connect_url.push_str("subscriptions/");
            connect_url.push_str(&settings.subscription_id.0);
            connect_url.push_str("/events");

            let mut connect_params = Vec::new();
            if settings.stream_keep_alive_limit != 0 {
                connect_params.push(format!(
                    "stream_keep_alive_limit={}",
                    settings.stream_keep_alive_limit
                ));
            }
            if settings.stream_limit != 0 {
                connect_params.push(format!("stream_limit={}", settings.stream_limit));
            }
            if settings.stream_timeout != Duration::from_secs(0) {
                connect_params.push(format!(
                    "stream_timeout={}",
                    settings.stream_timeout.as_secs()
                ));
            }
            if settings.batch_flush_timeout != Duration::from_secs(0) {
                connect_params.push(format!(
                    "batch_flush_timeout={}",
                    settings.batch_flush_timeout.as_secs()
                ));
            }
            if settings.batch_limit != 0 {
                connect_params.push(format!("batch_limit={}", settings.batch_limit));
            }
            if settings.max_uncommitted_events != 0 {
                connect_params.push(format!(
                    "max_uncommitted_events={}",
                    settings.max_uncommitted_events
                ));
            }

            if !connect_params.is_empty() {
                connect_url.push('?');
                connect_url.push_str(&connect_params.join("&"));
            };

            let mut commit_url = String::new();
            commit_url.push_str(&settings.nakadi_host);
            if !commit_url.ends_with("/") {
                commit_url.push('/');
            }
            commit_url.push_str("subscriptions/");
            commit_url.push_str(&settings.subscription_id.0);
            commit_url.push_str("/cursors");

            let http_client = Client::new();

            NakadiStreamConnector {
                http_client,
                connect_url,
                commit_url,
                subscription_id: settings.subscription_id,
                token_provider,
            }
        }
    }

    impl StreamConnector<NakadiLineIterator> for NakadiStreamConnector {
        fn connect(&self) -> ::std::result::Result<(StreamId, NakadiLineIterator), ConnectError> {
            let mut headers = Headers::new();
            if let Some(AccessToken(token)) = self.token_provider.get_token()? {
                headers.set(Authorization(Bearer { token }));
            }

            let mut response = self.http_client
                .get(&self.connect_url)
                .headers(headers)
                .send()?;

            Ok((StreamId("".into()), NakadiLineIterator::new(response)))
        }

        fn commit(&self, batch: &BatchCommitData) -> ::std::result::Result<(), CommitError> {
            unimplemented!()
        }
    }

    pub enum ConnectError {
        TokenError { message: String },
        Connection { message: String },
        Server { message: String },
        Client { message: String },
    }

    pub enum CommitError {
        x,
    }


    impl From<::auth::error::Error> for ConnectError {
        fn from(e: ::auth::error::Error) -> ConnectError {
            unimplemented!()
        }
    }

    impl From<::reqwest::Error> for ConnectError {
        fn from(e: ::reqwest::Error) -> ConnectError {
            unimplemented!()
        }
    }

}
