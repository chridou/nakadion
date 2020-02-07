use std::error::Error;
use std::fmt;

use http::StatusCode;
use http_api_problem::HttpApiProblem;

use nakadi_types::FlowId;

#[derive(Debug)]
pub struct NakadiApiError {
    message: String,
    cause: Option<Box<dyn Error + Send + 'static>>,
    kind: NakadiApiErrorKind,
    flow_id: Option<FlowId>,
}

impl NakadiApiError {
    pub fn new<T: Into<String>>(kind: NakadiApiErrorKind, message: T) -> Self {
        Self {
            message: message.into(),
            cause: None,
            kind,
            flow_id: None,
        }
    }

    pub fn from_problem<T: Into<HttpApiProblem>>(prob: T) -> Self {
        let prob = prob.into();
        if let Some(status) = prob.status {
            Self::new(
                status.into(),
                format!("An HTTP error with status {} occurred", status),
            )
        } else {
            Self::new(
                NakadiApiErrorKind::Other,
                "An HTTP error with unknown status occurred",
            )
        }
        .caused_by(prob)
    }

    pub fn caused_by<E>(mut self, err: E) -> Self
    where
        E: Error + Send + 'static,
    {
        self.cause = Some(Box::new(err));
        self
    }

    pub fn with_flow_id(mut self, flow_id: FlowId) -> Self {
        self.flow_id = Some(flow_id);
        self
    }

    pub fn with_maybe_flow_id(mut self, flow_id: Option<FlowId>) -> Self {
        self.flow_id = flow_id;
        self
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn kind(&self) -> NakadiApiErrorKind {
        self.kind
    }

    pub fn flow_id(&self) -> Option<&FlowId> {
        self.flow_id.as_ref()
    }

    pub fn problem(&self) -> Option<&HttpApiProblem> {
        if let Some(cause) = self.cause.as_ref() {
            cause.downcast_ref::<HttpApiProblem>()
        } else {
            None
        }
    }

    pub fn try_into_problem(mut self) -> Result<HttpApiProblem, Self> {
        if let Some(cause) = self.cause.take() {
            match cause.downcast::<HttpApiProblem>() {
                Ok(prob) => Ok(*prob),
                Err(the_box) => {
                    self.cause = Some(the_box);
                    Err(self)
                }
            }
        } else {
            Err(self)
        }
    }

    pub fn status(&self) -> Option<StatusCode> {
        self.problem().and_then(|p| p.status)
    }

    pub fn is_client_error(&self) -> bool {
        match self.kind {
            NakadiApiErrorKind::ClientError => true,
            _ => false,
        }
    }

    pub fn is_server_error(&self) -> bool {
        match self.kind {
            NakadiApiErrorKind::ServerError => true,
            _ => false,
        }
    }
}

impl Error for NakadiApiError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.cause.as_ref().map(|p| &**p as &dyn Error)
    }
}

impl fmt::Display for NakadiApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)?;

        Ok(())
    }
}

impl From<NakadiApiErrorKind> for NakadiApiError {
    fn from(kind: NakadiApiErrorKind) -> Self {
        Self::new(kind, kind.to_string())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NakadiApiErrorKind {
    ClientError,
    ServerError,
    Io,
    Other,
}

impl fmt::Display for NakadiApiErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NakadiApiErrorKind::ClientError => {
                write!(f, "client error")?;
            }
            NakadiApiErrorKind::ServerError => {
                write!(f, "server error")?;
            }
            NakadiApiErrorKind::Io => {
                write!(f, "io error")?;
            }
            NakadiApiErrorKind::Other => {
                write!(f, "other error")?;
            }
        }

        Ok(())
    }
}

impl From<StatusCode> for NakadiApiErrorKind {
    fn from(status: StatusCode) -> Self {
        if status.is_client_error() {
            NakadiApiErrorKind::ClientError
        } else if status.is_server_error() {
            NakadiApiErrorKind::ServerError
        } else {
            NakadiApiErrorKind::Other
        }
    }
}

#[derive(Debug)]
pub struct IoError(pub String);

impl IoError {
    pub fn new<T: Into<String>>(s: T) -> Self {
        Self(s.into())
    }
}

impl fmt::Display for IoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)?;

        Ok(())
    }
}

impl Error for IoError {
    fn cause(&self) -> Option<&dyn Error> {
        None
    }
}
