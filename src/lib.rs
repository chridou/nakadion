#![recursion_limit = "1024"]

extern crate uuid;
extern crate hyper;
#[macro_use]
extern crate derive_builder;
#[macro_use]
extern crate error_chain;

pub mod client;
mod tokenerrors;

pub use tokenerrors::*;

pub struct Token(Vec<u8>);

impl Token {
    pub fn new(bytes: Vec<u8>) -> Token {
        Token(bytes)
    }

    pub fn bytes(&self) -> &[u8] {
        self.0.as_ref()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }
}

pub trait ProvidesToken {
    fn get_token(&self) -> TokenResult<Token>;
}

pub struct EventType(String);

impl EventType {
    pub fn new<T: Into<String>>(value: T) -> EventType {
        EventType(value.into())
    }
}
