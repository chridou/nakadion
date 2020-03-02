use bytes::Bytes;
use futures::{FutureExt, TryFutureExt};
use http::{
    header::{HeaderName, HeaderValue},
    Method, StatusCode,
};
use serde::{Deserialize, Serialize};

use crate::nakadi_types::FlowId;
use crate::nakadi_types::{
    event_type::*, misc::OwningApplication, partition::*, publishing::*, subscription::*,
};

use super::*;

impl MonitoringApi for ApiClient {
    fn get_cursor_distances<T: Into<FlowId>>(
        &self,
        name: &EventTypeName,
        query: &CursorDistanceQuery,
        flow_id: T,
    ) -> ApiFuture<CursorDistanceResult> {
        let payload_to_send = serde_json::to_vec(query).unwrap();
        self.send_receive_payload(
            self.urls().monitoring_cursor_distances(name),
            Method::POST,
            payload_to_send.into(),
            RequestMode::RetryAndTimeout,
            flow_id.into(),
        )
        .boxed()
    }

    fn get_cursor_lag<T: Into<FlowId>>(
        &self,
        name: &EventTypeName,
        cursors: &[Cursor],
        flow_id: T,
    ) -> ApiFuture<Vec<Partition>> {
        let payload_to_send = serde_json::to_vec(cursors).unwrap();
        self.send_receive_payload(
            self.urls().monitoring_cursor_lag(name),
            Method::POST,
            payload_to_send.into(),
            RequestMode::RetryAndTimeout,
            flow_id.into(),
        )
        .boxed()
    }

    fn get_event_type_partitions<T: Into<FlowId>>(
        &self,
        event_type: &EventTypeName,
        flow_id: T,
    ) -> ApiFuture<Vec<Partition>> {
        let url = self.urls().monitoring_event_type_partitions(event_type);
        self.get(url, RequestMode::RetryAndTimeout, flow_id.into())
            .boxed()
    }
}

impl SchemaRegistryApi for ApiClient {
    /// Returns a list of all registered EventTypes
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/event-types_get)
    fn list_event_types<T: Into<FlowId>>(&self, flow_id: T) -> ApiFuture<Vec<EventType>> {
        self.get(
            self.urls().schema_registry_list_event_types(),
            RequestMode::RetryAndTimeout,
            flow_id.into(),
        )
        .boxed()
    }
    /// Creates a new EventType.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/event-types_post)
    fn create_event_type<T: Into<FlowId>>(
        &self,
        event_type: &EventTypeInput,
        flow_id: T,
    ) -> ApiFuture<()> {
        let payload_to_send = serde_json::to_vec(event_type).unwrap();
        self.send_payload(
            self.urls().schema_registry_create_event_type(),
            Method::POST,
            payload_to_send.into(),
            RequestMode::RetryAndTimeout,
            flow_id.into(),
        )
        .boxed()
    }

    fn get_event_type<T: Into<FlowId>>(
        &self,
        name: &EventTypeName,
        flow_id: T,
    ) -> ApiFuture<EventType> {
        let url = self.urls().schema_registry_get_event_type(name);
        self.get(url, RequestMode::RetryAndTimeout, flow_id.into())
            .boxed()
    }

    fn update_event_type<T: Into<FlowId>>(
        &self,
        name: &EventTypeName,
        event_type: &EventType,
        flow_id: T,
    ) -> ApiFuture<()> {
        self.send_payload(
            self.urls().schema_registry_update_event_type(name),
            Method::PUT,
            serde_json::to_vec(event_type).unwrap().into(),
            RequestMode::RetryAndTimeout,
            flow_id.into(),
        )
        .boxed()
    }

    /// Deletes an EventType identified by its name.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/event-types/name_delete)
    fn delete_event_type<T: Into<FlowId>>(
        &self,
        name: &EventTypeName,
        flow_id: T,
    ) -> ApiFuture<()> {
        self.delete(
            self.urls().schema_registry_delete_event_type(name),
            RequestMode::RetryAndTimeout,
            flow_id.into(),
        )
        .boxed()
    }
}

impl PublishApi for ApiClient {
    fn publish_events_batch<'a, B: Into<Bytes>, T: Into<FlowId>>(
        &'a self,
        event_type: &'a EventTypeName,
        events: B,
        flow_id: T,
    ) -> PublishFuture<'a> {
        let url = self.urls().publish_events(event_type);

        let body_bytes = events.into();
        let flow_id = flow_id.into();
        async move {
            let response = self
                .remote(
                    &url,
                    Method::POST,
                    body_bytes,
                    HeaderMap::default(),
                    flow_id,
                    None,
                )
                .await?;

            let flow_id = match response.headers().get("x-flow-id") {
                Some(header_value) => {
                    let header_bytes = header_value.as_bytes();
                    let header_str = String::from_utf8_lossy(header_bytes);
                    Some(FlowId::new(header_str))
                }
                None => None,
            };

            let status = response.status();
            match status {
                StatusCode::OK => Ok(()),
                StatusCode::MULTI_STATUS | StatusCode::UNPROCESSABLE_ENTITY => {
                    let batch_items = deserialize_stream(response.into_body()).await?;
                    if status == StatusCode::MULTI_STATUS {
                        Err(PublishFailure::PartialFailure(BatchResponse {
                            flow_id,
                            batch_items,
                        }))
                    } else {
                        Err(PublishFailure::Unprocessable(BatchResponse {
                            flow_id,
                            batch_items,
                        }))
                    }
                }
                _ => {
                    evaluate_error_for_problem(response)
                        .map(|err| Err(PublishFailure::Other(err)))
                        .await
                }
            }
        }
        .boxed()
    }
}

impl SubscriptionApi for ApiClient {
    /// This endpoint creates a subscription for EventTypes.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/subscriptions_post)
    fn create_subscription<T: Into<FlowId>>(
        &self,
        input: &SubscriptionInput,
        flow_id: T,
    ) -> ApiFuture<Subscription> {
        if let Some(subscription_id) = input.id {
            return async move {
                Err(NakadiApiError::other().with_context(format!(
                    "to create a subscription `input` must not have a `SubscriptionId`(id={}) set",
                    subscription_id
                )))
            }
            .boxed();
        }
        let url = self.urls().subscriptions_create_subscription().clone();
        let serialized = serde_json::to_vec(input).unwrap();

        let flow_id = flow_id.into();
        async move {
            self.send_receive_payload(
                url,
                Method::POST,
                serialized.into(),
                RequestMode::RetryAndTimeout,
                flow_id,
            )
            .await
        }
        .boxed()
    }

    /// Returns a subscription identified by id.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/subscriptions/subscription_id_get)
    fn get_subscription<T: Into<FlowId>>(
        &self,
        id: SubscriptionId,
        flow_id: T,
    ) -> ApiFuture<Subscription> {
        let url = self.urls().subscriptions_get_subscription(id);
        self.get(url, RequestMode::RetryAndTimeout, flow_id.into())
            .boxed()
    }

    fn list_subscriptions<T: Into<FlowId>>(
        &self,
        event_type: Option<&EventTypeName>,
        owning_application: Option<&OwningApplication>,
        limit: Option<usize>,
        offset: Option<usize>,
        show_status: bool,
        flow_id: T,
    ) -> BoxStream<'static, Result<Subscription, NakadiApiError>> {
        get_subscriptions::paginate_subscriptions(
            self.clone(),
            event_type.cloned(),
            owning_application.cloned(),
            limit,
            offset,
            show_status,
            flow_id.into(),
        )
    }

    /// This endpoint only allows to update the authorization section of a subscription.
    ///
    /// All other properties are immutable.
    /// This operation is restricted to subjects with administrative role.
    /// This call captures the timestamp of the update request.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/subscriptions/subscription_id_put)
    fn update_auth<T: Into<FlowId>>(&self, input: &SubscriptionInput, flow_id: T) -> ApiFuture<()> {
        if let Some(id) = input.id {
            let url = self.urls().subscriptions_update_auth(id);
            self.send_payload(
                url,
                Method::PUT,
                serde_json::to_vec(input).unwrap().into(),
                RequestMode::RetryAndTimeout,
                flow_id.into(),
            )
            .boxed()
        } else {
            async {
                Err(NakadiApiError::other().with_context(
                    "to update the subscription `input` must have a `SubscriptionId`(id) set",
                ))
            }
            .boxed()
        }
    }

    /// Deletes a subscription.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/subscriptions/subscription_id_delete)
    fn delete_subscription<T: Into<FlowId>>(
        &self,
        id: SubscriptionId,
        flow_id: T,
    ) -> ApiFuture<()> {
        let url = self.urls().subscriptions_delete_subscription(id);
        self.delete(url, RequestMode::RetryAndTimeout, flow_id.into())
            .boxed()
    }

    /// Exposes the currently committed offsets of a subscription.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/subscriptions/subscription_id/cursors_get)
    fn get_subscription_cursors<T: Into<FlowId>>(
        &self,
        id: SubscriptionId,
        flow_id: T,
    ) -> ApiFuture<Vec<SubscriptionCursor>> {
        #[derive(Deserialize)]
        struct EntityWrapper {
            #[serde(default)]
            items: Vec<SubscriptionCursor>,
        };

        let url = self.urls().subscriptions_get_committed_offsets(id);
        self.get::<EntityWrapper>(url, RequestMode::RetryAndTimeout, flow_id.into())
            .map_ok(|wrapper| wrapper.items)
            .boxed()
    }

    fn get_subscription_stats<T: Into<FlowId>>(
        &self,
        id: SubscriptionId,
        show_time_lag: bool,
        flow_id: T,
    ) -> ApiFuture<SubscriptionStats> {
        let url = self.urls().subscriptions_stats(id, show_time_lag);
        self.get(url, RequestMode::RetryAndTimeout, flow_id.into())
            .boxed()
    }

    /// Reset subscription offsets to specified values.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/subscriptions/subscription_id/cursors_patch)
    fn reset_subscription_cursors<T: Into<FlowId>>(
        &self,
        id: SubscriptionId,
        cursors: &[SubscriptionCursorWithoutToken],
        flow_id: T,
    ) -> ApiFuture<()> {
        #[derive(Serialize)]
        struct EntityWrapper<'b> {
            items: &'b [SubscriptionCursorWithoutToken],
        };
        let data = EntityWrapper { items: cursors };
        let url = self.urls().subscriptions_reset_subscription_cursors(id);
        self.send_payload(
            url,
            Method::PATCH,
            serde_json::to_vec(&data).unwrap().into(),
            RequestMode::RetryAndTimeout,
            flow_id.into(),
        )
        .boxed()
    }
}

impl SubscriptionStreamApi for ApiClient {
    fn request_stream<'a, T: Into<FlowId>>(
        &'a self,
        subscription_id: SubscriptionId,
        parameters: &StreamParameters,
        flow_id: T,
    ) -> ApiFuture<'a, SubscriptionStreamChunks> {
        let url = self.urls().subscriptions_request_stream(subscription_id);
        let parameters = serde_json::to_vec(parameters).unwrap();
        let flow_id = flow_id.into();
        async move {
            let response = self
                .remote(
                    &url,
                    Method::POST,
                    parameters.into(),
                    Default::default(),
                    flow_id,
                    None,
                )
                .await?;

            let status = response.status();
            if status == StatusCode::OK {
                match response.headers().get("x-nakadi-streamid") {
                    Some(header_value) => {
                        let header_bytes = header_value.as_bytes();
                        let header_str = std::str::from_utf8(header_bytes).map_err(|err| {
                            NakadiApiError::other().with_context(format!(
                                "the bytes of header 'x-nakadi-streamid' \
                                 were not a valid string: {}",
                                err
                            ))
                        })?;
                        let stream_id = header_str.parse().map_err(|err| {
                            NakadiApiError::other().with_context(format!(
                                "the value '{}' of header 'x-nakadi-streamid' \
                                 was not a valid stream id (UUID): {}",
                                header_str, err
                            ))
                        })?;
                        Ok(SubscriptionStreamChunks {
                            stream_id,
                            chunks: response.into_body(),
                        })
                    }
                    None => {
                        return Err(NakadiApiError::other().with_context(
                            "response did not contain the 'x-nakadi-streamid' header",
                        ))
                    }
                }
            } else {
                evaluate_error_for_problem(response).map(Err).await
            }
        }
        .boxed()
    }
}

impl SubscriptionCommitApi for ApiClient {
    fn commit_cursors<T: Into<FlowId>>(
        &self,
        id: SubscriptionId,
        stream: StreamId,
        cursors: &[SubscriptionCursor],
        flow_id: T,
    ) -> ApiFuture<CursorCommitResults> {
        #[derive(Serialize)]
        struct ItemsWrapper<'a> {
            items: &'a [SubscriptionCursor],
        };

        let wrapped = ItemsWrapper { items: cursors };

        let serialized = serde_json::to_vec(&wrapped).unwrap();

        let flow_id = flow_id.into();
        async move {
            let url = self.urls().subscriptions_commit_cursors(id);

            let mut headers = HeaderMap::default();
            headers.append(
                HeaderName::from_static("x-nakadi-streamid"),
                HeaderValue::from_str(stream.to_string().as_ref())?,
            );

            let response = self
                .request(
                    &url,
                    Method::POST,
                    serialized.into(),
                    headers,
                    RequestMode::Simple,
                    flow_id,
                )
                .await?;

            let status = response.status();
            match status {
                StatusCode::NO_CONTENT => Ok(CursorCommitResults::default()),
                StatusCode::OK => {
                    let commit_results = deserialize_stream(response.into_body()).await?;
                    Ok(commit_results)
                }
                _ => evaluate_error_for_problem(response).map(Err).await,
            }
        }
        .boxed()
    }
}
