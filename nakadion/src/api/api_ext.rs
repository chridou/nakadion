//! Extensions to the standard API

use std::error::Error as StdError;
use std::fmt;

use bytes::Bytes;
use futures::future::FutureExt;

use nakadi_types::model::event_type::*;
use nakadi_types::model::partition::*;
use nakadi_types::model::publishing::*;
use nakadi_types::model::subscription::*;
use nakadi_types::{Error, FlowId};

pub use super::ApiFuture;

pub trait SubscriptionApiExt {
    /// Reset all the offsets for the subscription to "begin"
    fn reset_cursors_to_begin<T: Into<FlowId>>(
        &self,
        id: SubscriptionId,
        flow_id: T,
    ) -> ApiFuture<()>;
}

impl<S> SubscriptionApiExt for S
where
    S: super::SubscriptionApi + Send + Sync + 'static,
{
    /// Resets all cursors of the given subscription to `CursorOffset::Begin`
    fn reset_cursors_to_begin<T: Into<FlowId>>(
        &self,
        id: SubscriptionId,
        flow_id: T,
    ) -> ApiFuture<()> {
        let flow_id = flow_id.into();
        async move {
            let cursors = self
                .get_subscription_cursors(id, flow_id.clone())
                .await?
                .into_iter()
                .map(|c| c.into_without_token_begin())
                .collect::<Vec<_>>();

            self.reset_subscription_cursors(id, &cursors, flow_id)
                .await?;

            Ok(())
        }
        .boxed()
    }
}
