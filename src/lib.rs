#![recursion_limit = "1024"]

extern crate uuid;
extern crate url;
#[macro_use]
extern crate hyper;
#[macro_use]
extern crate derive_builder;

extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;

#[macro_use]
extern crate error_chain;

pub mod client;
mod tokenerrors;

pub use tokenerrors::*;

#[derive(Clone, Debug)]
pub struct Token(String);

impl Token {
    pub fn new(bytes: String) -> Token {
        Token(bytes)
    }
}

pub trait ProvidesToken {
    fn get_token(&self) -> TokenResult<Token>;
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EventType(String);

impl EventType {
    pub fn new<T: Into<String>>(value: T) -> EventType {
        EventType(value.into())
    }
}
