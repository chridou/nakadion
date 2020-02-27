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
            prefix => $crate::helpers::NAKADION_PREFIX,
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

macro_rules! env_funs {
    ($var:expr) => {
        #[doc="Initialize from the environment.\n"]
        #[doc="Returns `None` if the value was not found and fails if the value could not be parsed.\n"]
        #[doc="The name of the environment variable is \"NAKADION_"]
        #[doc=$var]
        #[doc="\""]
        pub fn try_from_env() -> Result<Option<Self>, $crate::Error> {
            Self::try_from_env_prefixed($crate::helpers::NAKADION_PREFIX)
        }

        #[doc="Initialize from the environment.\n"]
        #[doc="Returns `None` if the value was not found and fails if the value could not be parsed.\n"]
        #[doc="The name of the environment variable is \"`prefix`_"]
        #[doc=$var]
        #[doc="\""]
        pub fn try_from_env_prefixed<T: Into<String>>(
            prefix: T,
        ) -> Result<Option<Self>, $crate::Error> {
            let mut var_name: String = prefix.into();
            var_name.push('_');
            var_name.push_str(&$var);
            Self::try_from_env_named(var_name)
        }

        #[doc="Initialize from the environment.\n"]
        #[doc="Returns `None` if the value was not found and fails if the value could not be parsed.\n"]
        #[doc="The name of the environment variable is `var_name`."]
         pub fn try_from_env_named<T: AsRef<str>>(
            var_name: T,
        ) -> Result<Option<Self>, $crate::Error> {
            match std::env::var(var_name.as_ref()) {
                Ok(value) => value.parse().map(Some).map_err(|err| {
                    $crate::Error::new(format!(
                        "could not parse env var '{}': {}",
                        var_name.as_ref(),
                        err
                    ))
                }),
                Err(std::env::VarError::NotPresent) => Ok(None),
                Err(std::env::VarError::NotUnicode(_)) => Err($crate::Error::new(format!(
                    "env var '{}' is not unicode",
                    var_name.as_ref()
                ))),
            }
        }

        #[doc="Initialize from the environment.\n"]
        #[doc="Fails if the value was not found or if the value could not be parsed.\n"]
        #[doc="The name of the environment variable is \"NAKADION_"]
        #[doc=$var]
        #[doc="\""]
         pub fn from_env() -> Result<Self, $crate::Error> {
            Self::from_env_prefixed($crate::helpers::NAKADION_PREFIX)
        }

        #[doc="Initialize from the environment.\n"]
        #[doc="Fails if the value was not found or if the value could not be parsed.\n"]
        #[doc="The name of the environment variable is \"`prefix`_"]
        #[doc=$var]
        #[doc="\""]
         pub fn from_env_prefixed<T: Into<String>>(prefix: T) -> Result<Self, $crate::Error> {
            let mut var_name: String = prefix.into();
            var_name.push('_');
            var_name.push_str(&$var);
            Self::from_env_named(var_name)
        }

        #[doc="Initialize from the environment.\n"]
        #[doc="Fails if the value was not found or if the value could not be parsed.\n"]
        #[doc="The name of the environment variable is `var_name`."]
        pub fn from_env_named<T: AsRef<str>>(var_name: T) -> Result<Self, $crate::Error> {
            Self::try_from_env_named(var_name.as_ref()).and_then(|v| {
                v.map(Ok).unwrap_or_else(|| {
                    Err($crate::Error::new(format!(
                        "env var '{}' not found",
                        var_name.as_ref()
                    )))
                })
            })
        }

        #[doc="Initialize from the environment.\n"]
        #[doc="Returns `None` if the value could not be read for any reason.\n"]
        #[doc="The name of the environment variable is \"NAKADION_"]
        #[doc=$var]
        #[doc="\""]
         pub fn from_env_opt() -> Option<Self> {
            Self::from_env_prefixed($crate::helpers::NAKADION_PREFIX).ok()
        }

        #[doc="Initialize from the environment.\n"]
        #[doc="Returns `None` if the value could not be read for any reason.\n"]
        #[doc="The name of the environment variable is \"`prefix`_"]
        #[doc=$var]
        #[doc="\""]
         pub fn from_env_opt_prefixed<T: Into<String>>(prefix: T) -> Option<Self> {
            let mut var_name: String = prefix.into();
            var_name.push('_');
            var_name.push_str(&$var);
            Self::from_env_named(var_name).ok()
        }

        #[doc="Initialize from the environment.\n"]
        #[doc="Returns `None` if the value could not be read for any reason.\n"]
        #[doc="The name of the environment variable is `var_name`."]
        pub fn from_env_opt_named<T: AsRef<str>>(var_name: T) -> Option<Self> {
            Self::from_env_named(var_name.as_ref()).ok()
        }
    };
}

macro_rules! __new_type_base {
    ($(#[$outer:meta])*; $Name:ident; $T:ty) => {
        $(#[$outer])*
        pub struct $Name($T);

        impl $Name {
            pub fn new<T: Into<$T>>(v: T) -> Self {
                Self(v.into())
            }
        }

        impl std::fmt::Display for $Name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                std::write!(f, "{}", self.0)
            }
        }

        impl From<$T> for $Name {
            fn from(v: $T) -> $Name {
                $Name(v)
            }
        }

        impl From<$Name> for $T {
            fn from(v: $Name) -> $T {
                v.0
            }
        }

        impl std::str::FromStr for $Name {
            type Err = $crate::Error;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Ok($Name(s.parse().map_err(|err| {
                    $crate::Error::new(std::format!("could not parse {}: {}", s, err))
                })?))
            }
        }
    }
}

macro_rules! __new_type_base_copy_ext {
    ($Name:ident; $T:ty) => {
        impl $Name {
            pub fn into_inner(self) -> $T {
                self.0
            }
        }
    };
}

macro_rules! __new_type_base_clone_ext {
    ($Name:ident; $T:ty) => {
        impl $Name {
            pub fn into_inner(self) -> $T {
                self.0
            }

            pub fn as_ref(self) -> &$T {
                &self.0
            }
        }
    };
}

macro_rules! __new_type_base_string_ext {
    ($Name:ident) => {
        impl $Name {
            pub fn into_inner(self) -> String {
                self.0
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }

            pub fn as_bytes(&self) -> &[u8] {
                self.0.as_bytes()
            }
        }

        impl From<&str> for $Name {
            fn from(v: &str) -> $Name {
                $Name::new(v)
            }
        }

        impl AsRef<str> for $Name {
            fn as_ref(&self) -> &str {
                self.as_str()
            }
        }

        impl AsRef<[u8]> for $Name {
            fn as_ref(&self) -> &[u8] {
                self.as_bytes()
            }
        }
    };
}

macro_rules! __new_type_base_uuid_ext {
    ($Name:ident) => {
        impl $Name {
            pub fn to_inner(self) -> uuid::Uuid {
                self.0
            }

            pub fn as_bytes(self) -> &[u8] {
                self.0.as_bytes()
            }
        }

        impl AsRef<[u8]> for $Name {
            fn as_ref(&self) -> &[u8] {
                self.as_bytes()
            }
        }
    };
}

macro_rules! new_type {
    ($(#[$outer:meta])* pub struct $Name:ident(String);) => {
        __new_type_base!($(#[$outer])*;$Name;String);
        __new_type_base_string_ext!($Name);
    };
    ($(#[$outer:meta])* pub struct $Name:ident(Uuid);) => {
        __new_type_base!($(#[$outer])*;$Name;Uuid);
        __new_type_base_uuid_ext!($Name);
    };
    ($(#[$outer:meta])* pub struct $Name:ident($T:ty);) => {
        __new_type_base!($(#[$outer])*;$Name;$T);
        __new_type_base_clone_ext!($Name;$T);
    };
    ($(#[$outer:meta])* pub copy struct $Name:ident($T:ty);) => {
        __new_type_base!($(#[$outer])*;$Name;$T);
        __new_type_base_copy_ext!($Name;$T);
    };
    ($(#[$outer:meta])* pub struct $Name:ident(String, env=$env:expr);) => {
        __new_type_base!($(#[$outer])*;$Name;String);
        __new_type_base_string_ext!($Name);
        impl $Name {
            env_funs!($env);
        }
    };
    ($(#[$outer:meta])* pub struct $Name:ident(Uuid, env=$env:expr);) => {
        __new_type_base!($(#[$outer])*;$Name;Uuid);
        __new_type_base_uuid_ext!($Name);
        impl $Name {
            env_funs!($env);
        }
    };
    ($(#[$outer:meta])* pub struct $Name:ident($T:ty, env=$env:expr);) => {
        __new_type_base!($(#[$outer])*;$Name;$T);
        __new_type_base_clone_ext!($Name;$T);
        impl $Name {
            env_funs!($env);
        }
    };
    ($(#[$outer:meta])* pub copy struct $Name:ident($T:ty, env=$env:expr);) => {
        __new_type_base!($(#[$outer])*;$Name;$T);
        __new_type_base_copy_ext!($Name;$T);
        impl $Name {
            env_funs!($env);
        }
    };
    ($(#[$outer:meta])* pub secs struct $Name:ident($T:ty, env=$env:expr);) => {
        __new_type_base!($(#[$outer])*;$Name;$T);
        __new_type_base_copy_ext!($Name;$T);
        impl $Name {
            env_funs!($env);

            pub fn into_duration(self) -> Duration {
                Duration::from_secs(u64::from(self.0))
            }
        }

        impl From<$Name> for Duration {
            fn from(v: $Name) -> Duration {
                v.into_duration()
            }
        }
    };
    ($(#[$outer:meta])* pub millis struct $Name:ident($T:ty, env=$env:expr);) => {
        __new_type_base!($(#[$outer])*;$Name;$T);
        __new_type_base_copy_ext!($Name;$T);
        impl $Name {
            env_funs!($env);

            pub fn into_duration(self) -> Duration {
                Duration::from_millis(u64::from(self.0))
            }
        }

        impl From<$Name> for Duration {
            fn from(v: $Name) -> Duration {
                v.into_duration()
            }
        }
    };
}

pub fn mandatory<T>(v: Option<T>, field_name: &'static str) -> Result<T, crate::Error> {
    if let Some(v) = v {
        Ok(v)
    } else {
        Err(crate::Error::new(format!(
            "field '{}' is mandatory",
            field_name
        )))
    }
}
