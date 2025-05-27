#![feature(let_chains)]

mod config;
mod prettytable_ext;
mod bitvavo;

use crate::bitvavo::{get_deltas, pull_snapshots_until, BookUpdate, OrderBook};
use crate::config::Config;
use bigdecimal::{BigDecimal, Zero};
use clap::Parser;
use prettytable::{row, Table};
use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

trait EqZero {
    fn eq_zero(&self) -> bool;
}

impl EqZero for Reverse<BigDecimal> {
    fn eq_zero(&self) -> bool {
        self.0.is_zero()
    }
}

impl EqZero for BigDecimal {
    fn eq_zero(&self) -> bool {
        self.is_zero()
    }
}

fn apply<A: Ord + Clone + EqZero>(
    maybe_price_levels: Option<&BTreeMap<A, BigDecimal>>,
    mut price_level_tree: BTreeMap<A, BigDecimal>,
) -> BTreeMap<A, BigDecimal> {
    if let Some(price_levels) = maybe_price_levels {
        for (price, size) in price_levels {
            if size.eq_zero() {
                price_level_tree.remove(price);
            } else {
                price_level_tree.insert(price.clone(), size.clone());
            }
        }
    }
    price_level_tree
}

fn merge_snapshot_and_update(snapshot: OrderBook, update: &BookUpdate) -> OrderBook {
    if snapshot.nonce >= update.nonce {
        panic!("snapshot is older than the update");
    }
    OrderBook {
        nonce: update.nonce,
        asks: apply(update.asks.as_ref(), snapshot.asks),
        bids: apply(update.bids.as_ref(), snapshot.bids),
    }
}

#[tokio::main]
async fn main() {
    let config = Config::parse();
    let should_run = Arc::new(AtomicBool::new(true));

    let (all_updates, mut snapshots) = tokio::join!(
        get_deltas(&config, Arc::clone(&should_run)),
        pull_snapshots_until(&config, should_run),
    );

    let snapshot_nonces: Vec<u64> = snapshots.keys().cloned().collect();
    for snapshot_nonce in snapshot_nonces {
        if !all_updates.contains_key(&(snapshot_nonce + 1)) {
            snapshots.remove(&snapshot_nonce);
        }
    }

    let first_update_nonce = *snapshots.keys().next().unwrap();
    let last_update_nonce = *snapshots.keys().last().unwrap();

    let relevant_updates: BTreeMap<u64, BookUpdate> = all_updates
        .range(first_update_nonce + 1..=last_update_nonce)
        .map(|(nonce, update)| (*nonce, update.clone()))
        .collect();

    let first_nonce_updates = *relevant_updates.keys().next().unwrap();
    let last_nonce_updates = *relevant_updates.keys().last().unwrap();
    let first_nonce_snapshots = *snapshots.keys().next().unwrap();
    let last_nonce_snapshots = *snapshots.keys().last().unwrap();

    println!(
        "first_nonce_updates={}, \
        last_nonce_updates={}, \
        first_nonce_snapshots={}, \
        last_nonce_snapshots={}, \
        all_updates_nonces = {:?}, \
        all_snapshots_nonces = {:?}",
        first_nonce_updates,
        last_nonce_updates,
        first_nonce_snapshots,
        last_nonce_snapshots,
        relevant_updates.keys(),
        snapshots.keys(),
    );

    let mut first_snapshot = snapshots
        .first_key_value()
        .map(|(_, v)| (*v).clone())
        .unwrap();

    let last_snapshot = snapshots
        .last_key_value()
        .map(|(_, v)| v.clone()).unwrap();

    for (_, update) in relevant_updates.iter() {
        first_snapshot = merge_snapshot_and_update(first_snapshot, update)
    }

    print_table(&first_snapshot, &last_snapshot);
}

fn print_table(
    first_snapshot: &OrderBook,
    last_snapshot: &OrderBook,
) {
    let mut bids = Table::new();

    bids.add_row(row![
        "Price level index",
        "Price level",
        "Received snapshot bids",
        "Snapshot bids with applied updated",
    ]);

    let empty = String::from("<empty>");

    for (i, r @ Reverse(price)) in first_snapshot.bids.keys().enumerate() {
        bids.add_row(row![
            i,
            price.to_string(),
            last_snapshot.bids.get(r).map(|p|p.to_string()).unwrap_or_else(|| empty.clone()),
            first_snapshot.bids.get(r).map(|p|p.to_string()).unwrap_or_else(|| empty.clone()),
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
            last_snapshot.asks.get(price).map(|p|p.to_string()).unwrap_or_else(|| empty.clone()),
            first_snapshot.asks.get(price).map(|p|p.to_string()).unwrap_or_else(|| empty.clone()),
        ]);
    }

    asks.printstd();
}