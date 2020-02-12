#![macro_use]

pub const NAKADION_PREFIX: &str = "NAKADION";

macro_rules! env_funs {
    ($var:expr) => {
        #[doc="Get value from the environment.\n"]
        #[doc="Returns `None` if the value was not found and fails if the value could not be parsed.\n"]
        #[doc="The name of the environment variable is \"NAKADION_"]
        #[doc=$var]
        #[doc="\""]
        pub fn try_from_env() -> Result<Option<Self>, $crate::Error> {
            Self::try_from_env_prefixed($crate::env_vars::NAKADION_PREFIX)
        }

        #[doc="Get value from the environment.\n"]
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

        #[doc="Get value from the environment.\n"]
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

        #[doc="Get value from the environment.\n"]
        #[doc="Fails if the value was not found or if the value could not be parsed.\n"]
        #[doc="The name of the environment variable is \"NAKADION_"]
        #[doc=$var]
        #[doc="\""]
         pub fn from_env() -> Result<Self, $crate::Error> {
            Self::from_env_prefixed($crate::env_vars::NAKADION_PREFIX)
        }

        #[doc="Get value from the environment.\n"]
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

        #[doc="Get value from the environment.\n"]
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
    };
}

macro_rules! env_type_ext {
    ($name:ident, $tt:ty, $var:expr) => {
        impl $name {
            pub fn new<T: Into<$tt>>(v: T) -> Self {
                Self(v.into())
            }

            env_funs!($var);
        }

        impl std::str::FromStr for $name {
            type Err = $crate::Error;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Ok($name(s.parse().map_err(|err| {
                    $crate::Error::new(format!("could not parse {}: {}", s, err))
                })?))
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.0)
            }
        }
    };
}

macro_rules! env_type_copy {
    ($name:ident, $tt:ty, $var:expr) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
        pub struct $name($tt);

        impl $name {
            pub fn to_inner(self) -> $tt {
                self.0
            }
        }

        impl From<$tt> for $name {
            fn from(v: $tt) -> Self {
                Self(v)
            }
        }

        env_type_ext!($name, $tt, $var);
    };
}

macro_rules! env_extend_string_type {
    ($name:ident, $var:expr) => {
        impl $name {
            pub fn as_str(&self) -> &str {
                &self.0.as_ref()
            }

            pub fn into_inner(self) -> String {
                self.0
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                &self.0
            }
        }

        impl<T> From<T> for $name
        where
            T: Into<String>,
        {
            fn from(v: T) -> Self {
                Self::new(v)
            }
        }

        env_type_ext!($name, String, $var);
    };
}
