use std::convert::AsRef;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::model::event_type::EventTypeName;
use crate::model::misc::{AuthorizationAttribute, OwningApplication};
use crate::model::partition::{Cursor, PartitionId};
use crate::Error;

use super::*;

#[derive(Default)]
pub struct SubscriptionInputBuilder {
    pub id: Option<SubscriptionId>,
    pub owning_application: Option<OwningApplication>,
    pub event_types: Option<EventTypeNames>,
    pub consumer_group: Option<ConsumerGroup>,
    pub read_from: Option<ReadFrom>,
    pub initial_cursors: Option<Vec<SubscriptionCursorWithoutToken>>,
    pub authorization: Option<SubscriptionAuthorization>,
}

impl SubscriptionInputBuilder {
    pub fn id<T: Into<SubscriptionId>>(mut self, id: T) -> Self {
        self.id = Some(id.into());
        self
    }

    pub fn owning_application(mut self, owning_application: OwningApplication) -> Self {
        self.owning_application = Some(owning_application);
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

    pub fn initial_cursors(mut self, initial_cursors: Vec<SubscriptionCursorWithoutToken>) -> Self {
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

        let owning_application = if let Some(owning_application) = self.owning_application {
            owning_application
        } else {
            return Err(Error::new("owning application is mandatory"));
        };

        let event_types = if let Some(event_types) = self.event_types {
            if event_types.is_empty() {
                return Err(Error::new("event types must not be empty"));
            } else {
                event_types
            }
        } else {
            return Err(Error::new("event types is mandatory"));
        };

        let read_from = if let Some(read_from) = self.read_from {
            read_from
        } else {
            return Err(Error::new("read from is mandatory"));
        };

        let initial_cursors = if let Some(initial_cursors) = self.initial_cursors {
            if initial_cursors.is_empty() {
                return Err(Error::new("initial cursors must not be empty if set"));
            } else {
                Some(initial_cursors)
            }
        } else {
            None
        };

        let authorization = if let Some(authorization) = self.authorization {
            authorization
        } else {
            return Err(Error::new("authorization is mandatory"));
        };

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
