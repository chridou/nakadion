#![macro_use]

pub const NAKADION_PREFIX: &str = "NAKADION";
pub const NAKADI_BASE_URL_ENV_VAR: &str = "NAKADI_BASE_URL";

pub const SUBSCRIPTION_ID_ENV_VAR: &str = "SUBSCRIPTION_ID";
pub const EVENT_TYPE_ENV_VAR: &str = "EVENT_TYPE";

pub const MAX_UNCOMMITTED_EVENTS_ENV_VAR: &str = "MAX_UNCOMMITTED_EVENTS";
pub const BATCH_LIMIT_ENV_VAR: &str = "BATCH_LIMIT";
pub const BATCH_FLUSH_TIMEOUT_SECS_ENV_VAR: &str = "BATCH_FLUSH_TIMEOUT_SECS";
pub const BATCH_TIMESPAN_SECS_ENV_VAR: &str = "BATCH_TIMESPAN_SECS";
pub const STREAM_TIMEOUT_SECS_ENV_VAR: &str = "STREAM_TIMEOUT_SECS";
pub const COMMIT_TIMEOUT_SECS_ENV_VAR: &str = "COMMIT_TIMEOUT_SECS";
pub const STREAM_LIMIT_ENV_VAR: &str = "STREAM_LIMIT";
// pub const STREAM_KEEP_ALIVE_LIMIT_ENV_VAR: &str = "STREAM_KEEP_ALIVE_LIMIT";

macro_rules! from_env {
    (prefix => $PREFIX:expr, postfix => $POSTFIX:expr) => {{
        let mut var_name: String = String::from($PREFIX);
        var_name.push('_');
        var_name.push_str(&$POSTFIX);
        from_env!(var_name.as_str())
    }};
    (postfix => $POSTFIX:expr) => {
        from_env!(
            prefix => $crate::env_vars::NAKADION_PREFIX,
            postfix => $POSTFIX
        )
    };
    ($ENV_VAR_NAME:expr) => {
        match std::env::var($ENV_VAR_NAME) {
            Ok(value) => value.parse().map_err(|err| {
                $crate::GenericError::new(format!(
                    "could not parse env var '{}': {}",
                    $ENV_VAR_NAME, err
                ))
            }),
            Err(std::env::VarError::NotPresent) => Err($crate::GenericError::new(format!(
                "env var '{}' not found",
                $ENV_VAR_NAME
            ))),
            Err(std::env::VarError::NotUnicode(_)) => Err($crate::GenericError::new(format!(
                "env var '{}' is not unicode",
                $ENV_VAR_NAME
            ))),
        }
    };
}

macro_rules! from_env_maybe {
    (prefix => $PREFIX:expr, postfix => $POSTFIX:expr) => {{
        let mut var_name: String = String::from($PREFIX);
        var_name.push('_');
        var_name.push_str(&$POSTFIX);
        from_env_maybe!(var_name.as_str())
    }};
    (postfix => $POSTFIX:expr) => {
        from_env_maybe!(
            prefix => $crate::env_vars::NAKADION_PREFIX,
            postfix => $POSTFIX
        )
    };
    ($ENV_VAR_NAME:expr) => {
        match std::env::var($ENV_VAR_NAME) {
            Ok(value) => value.parse().map(Some).map_err(|err| {
                $crate::GenericError::new(format!(
                    "could not parse env var '{}': {}",
                    $ENV_VAR_NAME, err
                ))
            }),
            Err(std::env::VarError::NotPresent) => Ok(None),
            Err(std::env::VarError::NotUnicode(_)) => Err($crate::GenericError::new(format!(
                "env var '{}' is not unicode",
                $ENV_VAR_NAME
            ))),
        }
    };
}
