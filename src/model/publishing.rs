use std::error::Error;
use std::fmt;
use std::str::FromStr;
use uuid::Uuid;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::env_vars::*;
use crate::helpers::MessageError;
use crate::model::cursor::CursorOffset;
use crate::model::event_type::OwningApplication;
use crate::model::misc::AuthorizationAttribute;
use crate::model::{EventTypeName, PartitionId, StreamId};

use super::EventId;

/// An aggregation of status items corresponding to each individual Event’s publishing attempt.
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#definition_BatchItemResponse)
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct BatchResponse {
    items: Vec<BatchItemResponse>,
}

/// A status corresponding to one individual Event’s publishing attempt.
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#definition_BatchItemResponse)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchItemResponse {
    /// Eid of the corresponding item. Will be absent if missing on the incoming Event.
    eid: Option<EventId>,
    /// Indicator of the submission of the Event within a Batch.
    publishing_status: PublishingStatus,
    /// Indicator of the step in the publishing process this Event reached.
    step: Option<PublishingStep>,
    /// Human readable information about the failure on this item.
    /// Items that are not “submitted” should have a description.
    detail: Option<String>,
}

/// Indicator of the submission of the Event within a Batch.
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#definition_BatchItemResponse)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PublishingStatus {
    /// Indicates successful submission, including commit on he underlying broker.
    Submitted,
    /// Indicates the message submission was not possible and can
    /// be resubmitted if so desired.
    Failed,
    /// Indicates that the submission of this item was not attempted any further due
    /// to a failure on another item in the batch.
    Aborted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
/// Indicator of the step in the publishing process this Event reached.
///
/// In Items that “failed” means the step of the failure.
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#definition_BatchItemResponse)
pub enum PublishingStep {
    /// Indicates that nothing was yet attempted for the publishing of this Event. Should
    /// be present only in the case of aborting the publishing during the validation of another
    /// (previous) Event.
    None,
    Validation,
    Partitioning,
    Enriching,
    Publishing,
}
