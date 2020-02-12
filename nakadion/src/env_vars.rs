#![macro_use]

pub const NAKADION_PREFIX: &str = "NAKADION";

pub const TOKEN_PATH_ENV_VAR: &str = "ACCESS_TOKEN_PATH";
pub const TOKEN_FIXED_ENV_VAR: &str = "ACCESS_TOKEN_FIXED";
pub const ALLOW_NO_TOKEN_ENV_VAR: &str = "ACCESS_TOKEN_ALLOW_NONE";

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
                $crate::Error::new(format!(
                    "could not parse env var '{}': {}",
                    $ENV_VAR_NAME, err
                ))
            }),
            Err(std::env::VarError::NotPresent) => Err($crate::Error::new(format!(
                "env var '{}' not found",
                $ENV_VAR_NAME
            ))),
            Err(std::env::VarError::NotUnicode(_)) => Err($crate::Error::new(format!(
                "env var '{}' is not unicode",
                $ENV_VAR_NAME
            ))),
        }
    };
}
