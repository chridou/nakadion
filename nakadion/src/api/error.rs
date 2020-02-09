use std::error::Error;
use std::fmt;

use http::StatusCode;
use http_api_problem::HttpApiProblem;

use nakadi_types::FlowId;

#[derive(Debug)]
pub struct NakadiApiError {
    context: Option<String>,
    cause: Option<Box<dyn Error + Send + 'static>>,
    kind: NakadiApiErrorKind,
    flow_id: Option<FlowId>,
}

impl NakadiApiError {
    pub fn http<T: Into<StatusCode>>(status: T) -> Self {
        Self::create(status.into())
    }

    pub fn io() -> Self {
        Self::create(NakadiApiErrorKind::Io)
    }

    pub fn other() -> Self {
        Self::create(NakadiApiErrorKind::Other(None))
    }

    pub fn http_problem<T: Into<HttpApiProblem>>(prob: T) -> Self {
        let prob = prob.into();
        if let Some(status) = prob.status {
            Self::http(status)
        } else {
            Self::create(NakadiApiErrorKind::Other(None))
        }
        .caused_by(prob)
    }

    pub fn create<T: Into<NakadiApiErrorKind>>(kind: T) -> Self {
        Self {
            context: None,
            cause: None,
            kind: kind.into(),
            flow_id: None,
        }
    }

    pub fn caused_by<E>(mut self, err: E) -> Self
    where
        E: Error + Send + 'static,
    {
        self.cause = Some(Box::new(err));
        self
    }

    pub fn with_context<T: Into<String>>(mut self, context: T) -> Self {
        self.context = Some(context.into());
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
        self.kind.status()
    }

    pub fn is_client_error(&self) -> bool {
        match self.kind {
            NakadiApiErrorKind::ClientError(_) => true,
            _ => false,
        }
    }

    pub fn is_auth_error(&self) -> bool {
        match self.kind {
            NakadiApiErrorKind::ClientError(StatusCode::FORBIDDEN)
            | NakadiApiErrorKind::ClientError(StatusCode::UNAUTHORIZED) => true,
            _ => false,
        }
    }

    pub fn is_server_error(&self) -> bool {
        match self.kind {
            NakadiApiErrorKind::ServerError(_) => true,
            _ => false,
        }
    }

    pub fn is_other_error(&self) -> bool {
        match self.kind {
            NakadiApiErrorKind::Other(_) => true,
            _ => false,
        }
    }

    pub fn is_io_error(&self) -> bool {
        match self.kind {
            NakadiApiErrorKind::Io => true,
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
        if let Some(context) = self.context.as_ref() {
            write!(f, "{}", context)?;
            if let Some(source) = self.source() {
                add_causes(source, f)?;
            } else {
                write!(f, " - {}", self.kind)?;
            }
        } else {
            write!(f, "{}", self.kind)?;
            if let Some(source) = self.source() {
                add_causes(source, f)?;
            }
        }

        Ok(())
    }
}

fn add_causes(err: &dyn Error, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, " - Caused by: {}", err)?;
    if let Some(source) = err.source() {
        add_causes(source, f)?
    }
    Ok(())
}

impl From<NakadiApiErrorKind> for NakadiApiError {
    fn from(kind: NakadiApiErrorKind) -> Self {
        Self::create(kind)
    }
}

impl From<IoError> for NakadiApiError {
    fn from(err: IoError) -> Self {
        Self::other().with_context(err.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NakadiApiErrorKind {
    ClientError(StatusCode),
    ServerError(StatusCode),
    Io,
    Other(Option<StatusCode>),
}

impl NakadiApiErrorKind {
    pub fn status(&self) -> Option<StatusCode> {
        match *self {
            NakadiApiErrorKind::ClientError(status) => Some(status),
            NakadiApiErrorKind::ServerError(status) => Some(status),
            NakadiApiErrorKind::Io => None,
            NakadiApiErrorKind::Other(status) => status,
        }
    }
}

impl fmt::Display for NakadiApiErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NakadiApiErrorKind::ClientError(status) => {
                write!(f, "{}", status)?;
            }
            NakadiApiErrorKind::ServerError(status) => {
                write!(f, "{}", status)?;
            }
            NakadiApiErrorKind::Io => {
                write!(f, "io error")?;
            }
            NakadiApiErrorKind::Other(Some(status)) => {
                write!(f, "{}", status)?;
            }
            NakadiApiErrorKind::Other(None) => {
                write!(f, "other error")?;
            }
        }

        Ok(())
    }
}

impl From<StatusCode> for NakadiApiErrorKind {
    fn from(status: StatusCode) -> Self {
        if status.is_client_error() {
            NakadiApiErrorKind::ClientError(status)
        } else if status.is_server_error() {
            NakadiApiErrorKind::ServerError(status)
        } else {
            NakadiApiErrorKind::Other(Some(status))
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
