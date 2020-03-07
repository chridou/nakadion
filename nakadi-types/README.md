# Nakadi-Types

`nakadi-types` contains types for interacting with the [Nakadi](https://nakadi.io) Event Broker.

There is no real logic implemented in this crate.
Almost all types in this crate match 1 to 1 to a type of the Nakadi API. Some
types where Nakadi returns collections in a wrapping object are made explicit.
In this case the field of the wrapping object is renamed to `items` for
serialization purposes. 


This crate is a base library for [nakadion](https://crates.io/crates/nakadion).

