//! Extensions to the standard API
use futures::future::FutureExt;

use nakadi_types::subscription::*;
use nakadi_types::FlowId;

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
                .map(|c| c.into_event_type_cursor_begin())
                .collect::<Vec<_>>();

            self.reset_subscription_cursors(id, &cursors, flow_id)
                .await?;

            Ok(())
        }
        .boxed()
    }
}
