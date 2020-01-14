use std::error::Error;
use std::fmt;

macro_rules! try_env {
    () => {};
}

#[derive(Debug)]
pub struct MessageError(pub String);

impl MessageError {
    pub fn new<T: Into<String>>(msg: T) -> Self {
        Self(msg.into())
    }

    pub fn boxed(self) -> Box<dyn Error> {
        Box::new(self)
    }
}

impl fmt::Display for MessageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)?;

        Ok(())
    }
}

impl Error for MessageError {
    fn cause(&self) -> Option<&dyn Error> {
        None
    }
}

impl<T> From<T> for MessageError
where
    T: Into<String>,
{
    fn from(msg: T) -> Self {
        Self::new(msg)
    }
}
