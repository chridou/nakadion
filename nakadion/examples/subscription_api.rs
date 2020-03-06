use nakadi_types::subscription::*;
use nakadi_types::RandomFlowId;

use nakadion::api::{ApiClient, SubscriptionApi};

#[cfg(feature = "reqwest")]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = ApiClient::builder().finish_from_env()?;

    let subscription_id = SubscriptionId::from_env()?;

    let subscription = client
        .get_subscription(subscription_id, RandomFlowId)
        .await?;

    println!("Subscription:\n {:#?}\n", subscription);

    let subscription = client
        .get_subscription_cursors(subscription_id, RandomFlowId)
        .await?;

    println!("Committed offsets:\n {:#?}\n", subscription);

    let stats = client
        .get_subscription_stats(subscription_id, false, RandomFlowId)
        .await?;

    println!("Stats:\n {:#?}\n", stats);

    Ok(())
}

#[cfg(not(feature = "reqwest"))]
fn main() {
    println!("Please enable the `reqwest` feature which is a default feature");
}
