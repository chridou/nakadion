use serde::{Deserialize, Serialize};

use crate::helpers::mandatory;
use crate::misc::OwningApplication;
use crate::Error;

use super::*;

/// A struct to create a `Subscription`.
///
/// This struct is intended for creating `Subscription`s. Since the interface
/// for creating `Subscription`s is slightly different from the actual entity this
/// special struct exists.
///
/// ## Subscription
///
/// Subscription is a high level consumption unit.
///
/// Subscriptions allow applications to easily scale the number of clients by managing
/// consumed event offsets and distributing load between instances.
/// The key properties that identify subscription are ‘owning_application’, ‘event_types’ and ‘consumer_group’.
/// It’s not possible to have two different subscriptions with these properties being the same.
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#definition_Subscription)
#[derive(Debug, Clone, Serialize)]
pub struct SubscriptionInput {
    /// Must be set **if and only** if an updating operation is performed(e.g. Auth)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<SubscriptionId>,
    pub owning_application: OwningApplication,
    pub event_types: EventTypeNames,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub consumer_group: Option<ConsumerGroup>,
    /// Position to start reading events from.
    ///
    /// Currently supported values:
    ///
    /// * Begin - read from the oldest available event.
    /// * End - read from the most recent offset.
    /// * Cursors - read from cursors provided in initial_cursors property.
    /// Applied when the client starts reading from a subscription.
    pub read_from: ReadFrom,
    /// List of cursors to start reading from.
    ///
    /// This property is required when `read_from` = `ReadFrom::Cursors`.
    /// The initial cursors should cover all partitions of subscription.
    /// Clients will get events starting from next offset positions.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub initial_cursors: Option<Vec<EventTypeCursor>>,
    pub authorization: SubscriptionAuthorization,
}

impl SubscriptionInput {
    pub fn builder() -> subscription_input::SubscriptionInputBuilder {
        subscription_input::SubscriptionInputBuilder::default()
    }
}
/// Position to start reading events from. Currently supported values:
///
///  * Begin - read from the oldest available event.
///  * End - read from the most recent offset.
///  * Cursors - read from cursors provided in initial_cursors property.
///  Applied when the client starts reading from a subscription.
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#definition_Subscription)
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadFrom {
    Begin,
    End,
    Cursors,
}

/// A builder for creating a `SubscriptionInput`
///
/// ## Subscription
///
/// Subscription is a high level consumption unit.
///
/// Subscriptions allow applications to easily scale the number of clients by managing
/// consumed event offsets and distributing load between instances.
/// The key properties that identify subscription are ‘owning_application’, ‘event_types’ and ‘consumer_group’.
/// It’s not possible to have two different subscriptions with these properties being the same.
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#definition_Subscription)
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct SubscriptionInputBuilder {
    pub id: Option<SubscriptionId>,
    pub owning_application: Option<OwningApplication>,
    pub event_types: Option<EventTypeNames>,
    pub consumer_group: Option<ConsumerGroup>,
    pub read_from: Option<ReadFrom>,
    pub initial_cursors: Option<Vec<EventTypeCursor>>,
    pub authorization: Option<SubscriptionAuthorization>,
}

impl SubscriptionInputBuilder {
    pub fn id<T: Into<SubscriptionId>>(mut self, id: T) -> Self {
        self.id = Some(id.into());
        self
    }

    pub fn owning_application<T: Into<OwningApplication>>(mut self, owning_application: T) -> Self {
        self.owning_application = Some(owning_application.into());
        self
    }

    pub fn event_types<T: Into<EventTypeNames>>(mut self, event_types: T) -> Self {
        self.event_types = Some(event_types.into());
        self
    }

    pub fn consumer_group<T: Into<ConsumerGroup>>(mut self, consumer_group: T) -> Self {
        self.consumer_group = Some(consumer_group.into());
        self
    }

    pub fn read_from(mut self, read_from: ReadFrom) -> Self {
        self.read_from = Some(read_from);
        self
    }

    pub fn initial_cursors(mut self, initial_cursors: Vec<EventTypeCursor>) -> Self {
        self.initial_cursors = Some(initial_cursors);
        self
    }

    pub fn authorization<T: Into<SubscriptionAuthorization>>(mut self, authorization: T) -> Self {
        self.authorization = Some(authorization.into());
        self
    }

    pub fn finish_for_create(self) -> Result<SubscriptionInput, Error> {
        self.make(true)
    }

    pub fn finish_for_update(self) -> Result<SubscriptionInput, Error> {
        self.make(false)
    }

    fn make(self, for_create: bool) -> Result<SubscriptionInput, Error> {
        let id = if let Some(id) = self.id {
            if for_create {
                return Err(Error::new(
                    "a subscription input for creation must not have a subscription id",
                ));
            }

            Some(id)
        } else {
            if !for_create {
                return Err(Error::new(
                    "a subscription input for update must have a subscription id",
                ));
            }
            None
        };

        let owning_application = mandatory(self.owning_application, "owning_application")?;
        let event_types = if let Some(event_types) = self.event_types {
            if event_types.is_empty() {
                return Err(Error::new("event types must not be empty"));
            } else {
                event_types
            }
        } else {
            return Err(Error::new("event types is mandatory"));
        };

        let read_from = mandatory(self.read_from, "read_from")?;

        let initial_cursors = if let Some(initial_cursors) = self.initial_cursors {
            if initial_cursors.is_empty() {
                return Err(Error::new("initial cursors must not be empty if set"));
            } else {
                Some(initial_cursors)
            }
        } else {
            None
        };

        let authorization = mandatory(self.authorization, "authorization")?;

        Ok(SubscriptionInput {
            id,
            owning_application,
            event_types,
            consumer_group: self.consumer_group,
            read_from,
            initial_cursors,
            authorization,
        })
    }
}
