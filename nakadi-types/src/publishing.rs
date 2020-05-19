/// Types for publishing events.
///
/// Even though this is part of the event type resources this
/// deserves special attention since publishing events involves
/// special logic for recovery in case of a failed publishing attempt.
use std::fmt;

use serde::{Deserialize, Serialize};

use crate::event::EventId;
use crate::FlowId;

/// An evaluated response from Nakadi when events were published.
///
/// A `FlowId` is contained since a worth logging state might be contained in this struct
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#/event-types/name/events_post)
#[derive(Debug, Clone)]
pub struct FailedSubmission {
    pub flow_id: Option<FlowId>,
    /// The actual outcome of the submission attempt
    pub failure: SubmissionFailure,
}

impl fmt::Display for FailedSubmission {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(fid) = self.flow_id.as_ref() {
            write!(f, "{}, flow id: {}", self.failure, fid)?;
        } else {
            write!(f, "{}", self.failure)?;
        }

        Ok(())
    }
}

impl FailedSubmission {
    pub fn is_unprocessable(&self) -> bool {
        if let SubmissionFailure::Unprocessable(_) = self.failure {
            true
        } else {
            false
        }
    }
}

/// The outcome of a submission/publishing attempt to Nakadi.
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#/event-types/name/events_post)
#[derive(Debug, Clone)]
pub enum SubmissionFailure {
    /// At least one event failed to be validated, enriched or partitioned. None were submitted.
    ///
    /// Nakadi responded with 422-UNPROCESSABLE.
    Unprocessable(Vec<BatchItemResponse>),
    /// At least one event has failed to be submitted. The batch might be partially submitted.
    ///
    /// Nakadi responded with 207-MULTI STATUS.
    NotAllSubmitted(Vec<BatchItemResponse>),
}

impl SubmissionFailure {
    /// Returns true if there are no `BatchItemResponse`s.
    ///
    /// This means also that no errors occurred.
    pub fn is_empty(&self) -> bool {
        match self {
            SubmissionFailure::Unprocessable(ref items) => items.is_empty(),
            SubmissionFailure::NotAllSubmitted(ref items) => items.is_empty(),
        }
    }

    /// Returns the amount of `BatchItemResponse`s.
    ///
    /// Usually at least one contains an error.
    pub fn len(&self) -> usize {
        match self {
            SubmissionFailure::Unprocessable(ref items) => items.len(),
            SubmissionFailure::NotAllSubmitted(ref items) => items.len(),
        }
    }

    /// Iterate over all `BatchItemResponse`s
    pub fn iter(&self) -> impl Iterator<Item = &BatchItemResponse> {
        match self {
            SubmissionFailure::Unprocessable(ref items) => items.iter(),
            SubmissionFailure::NotAllSubmitted(ref items) => items.iter(),
        }
    }

    /// Iterate over all `BatchItemResponse`s where the publishing status is `PublishingStatus::Failed`
    pub fn failed_response_items(&self) -> impl Iterator<Item = &BatchItemResponse> {
        self.iter()
            .filter(|item| item.publishing_status == PublishingStatus::Failed)
    }

    /// Iterate over all `BatchItemResponse`s where the publishing status is `PublishingStatus::Aborted`
    pub fn aborted_response_items(&self) -> impl Iterator<Item = &BatchItemResponse> {
        self.iter()
            .filter(|item| item.publishing_status == PublishingStatus::Aborted)
    }

    /// Iterate over all `BatchItemResponse`s where the publishing status is `PublishingStatus::Submitted`
    pub fn submitted_response_items(&self) -> impl Iterator<Item = &BatchItemResponse> {
        self.iter()
            .filter(|item| item.publishing_status == PublishingStatus::Submitted)
    }

    /// Iterate over all `BatchItemResponse`s where the publishing status is not `PublishingStatus::Submitted`
    pub fn non_submitted_response_items(&self) -> impl Iterator<Item = &BatchItemResponse> {
        self.iter()
            .filter(|item| item.publishing_status != PublishingStatus::Submitted)
    }

    pub fn stats(&self) -> BatchStats {
        let mut stats = BatchStats::default();

        for item in self.iter() {
            stats.n_items += 1;
            match item.publishing_status {
                PublishingStatus::Submitted => stats.n_submitted += 1,
                PublishingStatus::Failed => {
                    stats.n_failed += 1;
                    stats.n_not_submitted += 1;
                }
                PublishingStatus::Aborted => {
                    stats.n_aborted += 1;
                    stats.n_not_submitted += 1;
                }
            }
        }

        stats
    }
}

impl IntoIterator for SubmissionFailure {
    type Item = BatchItemResponse;
    type IntoIter = std::vec::IntoIter<BatchItemResponse>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            SubmissionFailure::NotAllSubmitted(items) => items.into_iter(),
            SubmissionFailure::Unprocessable(items) => items.into_iter(),
        }
    }
}

impl fmt::Display for SubmissionFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let num_submitted = self.submitted_response_items().count();
        let num_failed = self.failed_response_items().count();
        let num_aborted = self.aborted_response_items().count();
        write!(
            f,
            "items:{}, submitted:{}, failed: {}, aborted: {}",
            self.len(),
            num_submitted,
            num_failed,
            num_aborted
        )?;

        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct BatchStats {
    pub n_items: usize,
    pub n_submitted: usize,
    pub n_failed: usize,
    pub n_aborted: usize,
    pub n_not_submitted: usize,
}

impl BatchStats {
    pub fn all_submitted(n: usize) -> Self {
        let mut me = Self::default();
        me.n_items = n;
        me.n_submitted = n;
        me
    }
    pub fn all_not_submitted(n: usize) -> Self {
        let mut me = Self::default();
        me.n_items = n;
        me.n_not_submitted = n;
        me
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
