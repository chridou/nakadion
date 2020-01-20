use nakadion_types::model::subscription::*;
use nakadion_types::FlowId;

use nakadion::nakadi_api::{ApiClient, SubscriptionApi};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = ApiClient::new_from_env()?;

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
