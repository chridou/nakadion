use futures::StreamExt;
use http::Method;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};

use crate::nakadi_types::FlowId;
use crate::nakadi_types::{event_type::*, misc::OwningApplication, subscription::*};

use super::*;

pub fn paginate_subscriptions(
    api_client: ApiClient,
    event_type: Option<EventTypeName>,
    owning_application: Option<OwningApplication>,
    limit: Option<usize>,
    offset: Option<usize>,
    show_status: bool,
    flow_id: FlowId,
) -> BoxStream<'static, Result<Subscription, NakadiApiError>> {
    let params = PaginationParams {
        event_type,
        owning_application,
        limit: limit.unwrap_or(20),
        offset: offset.unwrap_or(0),
        show_status,
    };

    let (tx, rx) = unbounded_channel::<Result<Subscription, NakadiApiError>>();

    tokio::spawn(paginate(api_client, params, flow_id, tx));

    rx.boxed()
}

#[derive(Serialize)]
struct PaginationParams {
    event_type: Option<EventTypeName>,
    owning_application: Option<OwningApplication>,
    limit: usize,
    offset: usize,
    show_status: bool,
}

#[derive(Deserialize)]
struct PaginationResponse {
    items: Vec<Subscription>,
    #[serde(rename = "_links")]
    links: Option<PaginationLinks>,
}

#[derive(Deserialize)]
struct PaginationLinks {
    next: Option<String>,
}

async fn paginate(
    api_client: ApiClient,
    params: PaginationParams,
    flow_id: FlowId,
    tx: UnboundedSender<Result<Subscription, NakadiApiError>>,
) {
    match get_first_page(&api_client, params, flow_id.clone()).await {
        Err(err) => {
            let _ = tx.send(Err(err));
        }
        Ok(first_page) => {
            loop_pages(&api_client, first_page, tx, flow_id).await;
        }
    }
}

async fn loop_pages(
    api_client: &ApiClient,
    first_page: PaginationResponse,
    tx: UnboundedSender<Result<Subscription, NakadiApiError>>,
    flow_id: FlowId,
) {
    let mut current_page = first_page;
    loop {
        let PaginationResponse { items, links } = current_page;

        for subscription in items {
            if tx.send(Ok(subscription)).is_err() {
                return;
            }
        }

        if let Some(link_to_next) = links.and_then(|l| l.next) {
            match get_next_page(&api_client, link_to_next, flow_id.clone()).await {
                Ok(next_page) => {
                    current_page = next_page;
                    continue;
                }
                Err(err) => {
                    let _ = tx.send(Err(err));
                    return;
                }
            }
        } else {
            return;
        }
    }
}

async fn get_first_page(
    api_client: &ApiClient,
    params: PaginationParams,
    flow_id: FlowId,
) -> Result<PaginationResponse, NakadiApiError> {
    let url = api_client.urls().subscriptions_list_subscriptions();
    let serialized = serde_json::to_vec(&params)?;
    api_client
        .send_receive_payload(url, Method::GET, serialized, flow_id)
        .await
}

async fn get_next_page(
    api_client: &ApiClient,
    url: String,
    flow_id: FlowId,
) -> Result<PaginationResponse, NakadiApiError> {
    let url = url.parse().map_err(|err| {
        NakadiApiError::other()
            .with_context("get subscriptions: failed to parse url from Nakadi")
            .caused_by(err)
    })?;
    api_client.get(url, flow_id).await
}
