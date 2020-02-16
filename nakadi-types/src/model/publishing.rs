/// Types for publishing events.
///
/// Even though this is part of the event type resources this
/// deserves special attention since publishing events involves
/// special login for recovery in case of a failed publishing attempt.
use std::fmt;

use serde::{Deserialize, Serialize};

use crate::model::event::EventId;
use crate::FlowId;

/// An aggregation of status items corresponding to each individual Event’s publishing attempt.
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#definition_BatchItemResponse)
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct BatchResponse {
    /// Since this struct is usually part of a failure scenario it
    /// can contain a `FlowId`
    #[serde(skip_serializing_if = "Option::is_none")]
    pub flow_id: Option<FlowId>,
    pub batch_items: Vec<BatchItemResponse>,
}

impl fmt::Display for BatchResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BatchResponse")?;

        Ok(())
    }
}

/// A status corresponding to one individual Event’s publishing attempt.
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#definition_BatchItemResponse)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchItemResponse {
    /// Eid of the corresponding item. Will be absent if missing on the incoming Event.
    pub eid: Option<EventId>,
    /// Indicator of the submission of the Event within a Batch.
    pub publishing_status: PublishingStatus,
    /// Indicator of the step in the publishing process this Event reached.
    pub step: Option<PublishingStep>,
    /// Human readable information about the failure on this item.
    /// Items that are not “submitted” should have a description.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
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

/// Indicator of the step in the publishing process this Event reached.
///
/// In Items that “failed” means the step of the failure.
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#definition_BatchItemResponse)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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
