mod bitvavo;
mod config;

use crate::bitvavo::{BookUpdate, OrderBook, get_deltas, pull_snapshots};
use crate::config::Config;
use anyhow::Context;
use clap::Parser;
use prettytable::{Table, row};
use std::cmp::Reverse;
use std::collections::BTreeMap;
use tokio_util::sync::CancellationToken;
use tracing::{Level, info, span};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let _span = span!(Level::INFO, "main").entered();

    let config = Config::parse();
    let cancellation_token = CancellationToken::new();

    info!("requesting deltas & snapshots...");

    let (all_updates_r, snapshots_r) = tokio::join!(
        get_deltas(&config, cancellation_token.clone()),
        pull_snapshots(&config, cancellation_token),
    );

    let all_updates = all_updates_r?;
    let mut snapshots = snapshots_r?;

    assert!(all_updates.len() >= 2);
    assert!(snapshots.len() >= 2);

    let snapshot_nonces: Vec<u64> = snapshots.keys().cloned().collect();
    for snapshot_nonce in snapshot_nonces {
        if !all_updates.contains_key(&(snapshot_nonce + 1)) {
            snapshots.remove(&snapshot_nonce);
        }
    }

    let first_update_nonce = *snapshots
        .keys()
        .next()
        .context("can't get first_update_nonce: snapshots are empty?")?;
    let last_update_nonce = *snapshots
        .keys()
        .last()
        .context("can't get last_update_nonce: snapshots are empty?")?;

    let relevant_updates: BTreeMap<u64, BookUpdate> = all_updates
        .range(first_update_nonce + 1..=last_update_nonce)
        .map(|(nonce, update)| (*nonce, update.clone()))
        .collect();

    let mut relevant_updates_keys = relevant_updates.keys();
    let mut snapshot_keys = snapshots.keys();

    let first_nonce_updates = *relevant_updates_keys
        .next()
        .context("can't derive first_nonce_updates: relevant_updates_keys is empty?")?;
    let last_nonce_updates = *relevant_updates_keys
        .last()
        .context("can't derive last_nonce_updates")?;
    let first_nonce_snapshots = *snapshot_keys
        .next()
        .context("can't derive first_nonce_snapshots")?;
    let last_nonce_snapshots = *snapshot_keys
        .last()
        .context("can't derive last_nonce_snapshots")?;

    assert_eq!(first_nonce_updates, first_nonce_snapshots + 1);
    assert_eq!(last_nonce_updates, last_nonce_snapshots);

    let mut base_snapshot = snapshots
        .first_key_value()
        .map(|(_, v)| (*v).clone())
        .unwrap();

    let last_snapshot = snapshots
        .last_key_value()
        .map(|(_, v)| v.clone())
        .context("can't derive last_snapshot")?;

    base_snapshot.apply_updates(relevant_updates);

    print_table(base_snapshot, last_snapshot);

    Ok(())
}

fn print_table(first_snapshot: OrderBook, last_snapshot: OrderBook) {
    let mut bids = Table::new();

    bids.add_row(row![
        "Price level index",
        "Price level",
        "Received snapshot bids",
        "Snapshot bids with applied updates",
    ]);

    let empty = String::from("<empty>");

    for (i, r @ Reverse(price)) in first_snapshot.bids.keys().enumerate() {
        bids.add_row(row![
            i,
            price.to_string(),
            last_snapshot
                .bids
                .get(r)
                .map(|p| p.to_string())
                .unwrap_or_else(|| empty.clone()),
            first_snapshot
                .bids
                .get(r)
                .map(|p| p.to_string())
                .unwrap_or_else(|| empty.clone()),
        ]);
    }

    bids.printstd();

    let mut asks = Table::new();

    asks.add_row(row![
        "Price level index",
        "Price level",
        "Received snapshot asks",
        "Snapshot asks with applied updates",
    ]);

    for (i, price) in first_snapshot.asks.keys().enumerate() {
        asks.add_row(row![
            i,
            price.to_string(),
            last_snapshot
                .asks
                .get(price)
                .map(|p| p.to_string())
                .unwrap_or_else(|| empty.clone()),
            first_snapshot
                .asks
                .get(price)
                .map(|p| p.to_string())
                .unwrap_or_else(|| empty.clone()),
        ]);
    }

    asks.printstd();
}
