/// Types for publishing events.
///
/// Even though this is part of the event type resources this
/// deserves special attention since publishing events involves
/// special logic for recovery in case of a failed publishing attempt.
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

#[derive(Debug, Default)]
pub struct BatchStats {
    pub n_items: usize,
    pub n_submitted: usize,
    pub n_failed: usize,
    pub n_aborted: usize,
}

impl BatchResponse {
    /// Returns true if there are no `BatchItemResponse`s.
    ///
    /// This means also that no errors occurred.
    pub fn is_empty(&self) -> bool {
        self.batch_items.is_empty()
    }

    /// Returns the amount of `BatchItemResponse`s.
    ///
    /// Usually at least one contains an error.
    pub fn len(&self) -> usize {
        self.batch_items.len()
    }

    /// Iterate over all `BatchItemResponse`s where the publishing status is `PublishingStatus::Failed`
    pub fn failed_response_items(&self) -> impl Iterator<Item = &BatchItemResponse> {
        self.batch_items
            .iter()
            .filter(|item| item.publishing_status == PublishingStatus::Failed)
    }

    /// Iterate over all `BatchItemResponse`s where the publishing status is `PublishingStatus::Aborted`
    pub fn aborted_response_items(&self) -> impl Iterator<Item = &BatchItemResponse> {
        self.batch_items
            .iter()
            .filter(|item| item.publishing_status == PublishingStatus::Aborted)
    }

    /// Iterate over all `BatchItemResponse`s where the publishing status is `PublishingStatus::Submitted`
    pub fn submitted_response_items(&self) -> impl Iterator<Item = &BatchItemResponse> {
        self.batch_items
            .iter()
            .filter(|item| item.publishing_status == PublishingStatus::Submitted)
    }

    /// Iterate over all `BatchItemResponse`s where the publishing status is not `PublishingStatus::Submitted`
    pub fn non_submitted_response_items(&self) -> impl Iterator<Item = &BatchItemResponse> {
        self.batch_items
            .iter()
            .filter(|item| item.publishing_status != PublishingStatus::Submitted)
    }

    /// Iterate over all `BatchItemResponse`s
    pub fn iter(&self) -> impl Iterator<Item = &BatchItemResponse> {
        self.batch_items.iter()
    }

    pub fn stats(&self) -> BatchStats {
        let mut stats = BatchStats::default();

        for item in &self.batch_items {
            stats.n_items += 1;
            match item.publishing_status {
                PublishingStatus::Submitted => stats.n_submitted += 1,
                PublishingStatus::Failed => stats.n_failed += 1,
                PublishingStatus::Aborted => stats.n_aborted += 1,
            }
        }

        stats
    }
}

impl IntoIterator for BatchResponse {
    type Item = BatchItemResponse;
    type IntoIter = std::vec::IntoIter<BatchItemResponse>;

    fn into_iter(self) -> Self::IntoIter {
        self.batch_items.into_iter()
    }
}

impl fmt::Display for BatchResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let num_submitted = self.submitted_response_items().count();
        let num_failed = self.failed_response_items().count();
        let num_aborted = self.aborted_response_items().count();
        write!(
            f,
            "BatchResponse(items:{}, submitted:{}, failed: {}, aborted: {})",
            self.len(),
            num_submitted,
            num_failed,
            num_aborted
        )?;

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
    ///
    /// Items that are not “submitted” should have a description.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

/// Indicator of the submission of the Event within a Batch.
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#definition_BatchItemResponse)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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
#[serde(rename_all = "snake_case")]
pub enum PublishingStep {
    /// Indicates that nothing was yet attempted for the publishing of this Event. Should
    /// be present only in the case of aborting the publishing during the validation of another
    /// (previous) Event.
    None,
    Validating,
    Partitioning,
    Enriching,
    Publishing,
}
