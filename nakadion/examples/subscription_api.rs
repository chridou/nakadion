use nakadi_types::model::subscription::*;
use nakadi_types::FlowId;

use nakadion::api::{ApiClient, SubscriptionApi};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = ApiClient::builder().finish_from_env()?;

    let subscription_id = SubscriptionId::from_env()?;

    let subscription = client
        .get_subscription(subscription_id, FlowId::default())
        .await?;

    println!("Subscription:\n {:#?}\n", subscription);

    let subscription = client
        .get_committed_offsets(subscription_id, FlowId::default())
        .await?;

    println!("Committed offsets:\n {:#?}\n", subscription);

    let stats = client
        .get_subscription_stats(subscription_id, false, FlowId::default())
        .await?;

    println!("Stats:\n {:#?}\n", stats);

    Ok(())
}
