#![feature(let_chains)]

mod config;
mod prettytable_ext;

use crate::config::Config;
use crate::prettytable_ext::format_price_level;
use clap::Parser;
use futures::{SinkExt, StreamExt};
use prettytable::{row, Table};
use reqwest::Error;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;

#[derive(Debug, Deserialize, Clone)]
pub struct OrderBook {
    pub asks: Vec<[String; 2]>,
    pub bids: Vec<[String; 2]>,
    pub nonce: u64,
}

#[derive(Debug, Deserialize)]
pub struct BookUpdate {
    #[serde(rename = "nonce")]
    pub nonce: u64,
    #[serde(rename = "asks")]
    pub asks: Option<Vec<[String; 2]>>,
    #[serde(rename = "bids")]
    pub bids: Option<Vec<[String; 2]>>,
}

trait OrderBookUpdates {
    fn trim_zeroes(&mut self);
}

fn trim_price_levels(source: &[[String; 2]]) -> Vec<[String; 2]> {
    source.iter().map(|[price, size]| {
        let price = trim_number(price);
        let size = if size == "0" { "0".to_string() } else { trim_number(size) };
        [price, size]
    }).collect()
}

fn trim_number(num: &str) -> String {
    if let Some(_) = num.find('.') {
        let trimmed = num.trim_end_matches('0');
        if trimmed.ends_with('.') {
            trimmed[..trimmed.len() - 1].to_string()
        } else {
            trimmed.to_string()
        }
    } else {
        num.to_string()
    }
}

impl OrderBookUpdates for OrderBook {
    fn trim_zeroes(&mut self) {
        self.asks = trim_price_levels(&self.asks);
        self.bids = trim_price_levels(&self.bids);
        self.bids.reverse();
    }
}

pub async fn get_snapshot(config: &Config) -> Result<OrderBook, Error> {
    let url = format!("{}/v2/{}/book?depth={}", config.api_url, config.market, config.depth);
    let mut raw_book: OrderBook = reqwest::get(url).await?.json().await?;

    raw_book.trim_zeroes();
    Ok(raw_book)
}

fn apply(
    maybe_price_levels: &Option<Vec<[String; 2]>>,
    price_level_tree: &mut BTreeMap<String, String>,
) {
    if let Some(price_levels) = &maybe_price_levels {
        for [price, size] in price_levels {
            if size == "0" {
                price_level_tree.remove(price);
            } else {
                price_level_tree.insert(price.clone(), size.clone());
            }
        }
    }
}

fn merge_snapshot_and_update(snapshot: OrderBook, update: &BookUpdate) -> OrderBook {
    if snapshot.nonce >= update.nonce {
        panic!("snapshot is older than the update");
    }

    let mut asks_tree = BTreeMap::new();
    let mut bids_tree = BTreeMap::new();

    for [price, size] in &snapshot.asks {
        asks_tree.insert(price.clone(), size.clone());
    }
    for [price, size] in &snapshot.bids {
        bids_tree.insert(price.clone(), size.clone());
    }

    apply(&update.asks, &mut asks_tree);
    apply(&update.bids, &mut bids_tree);

    let asks = asks_tree.iter()
        .map(|(p, s)| [p.clone(), s.clone()])
        .collect::<Vec<_>>();

    let mut bids = bids_tree.iter()
        .map(|(p, s)| [p.clone(), s.clone()])
        .collect::<Vec<_>>();
    bids.reverse();

    OrderBook {
        nonce: update.nonce,
        asks,
        bids,
    }
}

fn decode_book_update(raw_update: BookUpdate) -> BookUpdate {
    let mut asks = None;
    let mut bids = None;

    if let Some(u_asks) = raw_update.asks {
        asks = Some(trim_price_levels(&u_asks));
    }
    if let Some(u_bids) = raw_update.bids {
        bids = Some(trim_price_levels(&u_bids));
    }
    BookUpdate { asks, bids, nonce: raw_update.nonce }
}

#[tokio::main]
async fn main() {
    let config = Config::parse();
    let url = format!("{}/v2/", config.ws_url)
        .into_client_request()
        .expect("invalid URL");

    let (ws_stream, _) = connect_async(url)
        .await
        .expect("failed to connect");

    println!("connected to Bitvavo WebSocket @ {}", config.ws_url);

    let (mut write, mut read) = ws_stream.split();

    let should_run = Arc::new(AtomicBool::new(true));
    let snapshots_arc = Arc::new(RwLock::new(BTreeMap::new()));

    let cp = Arc::clone(&should_run);
    let snapshots_arc_cp = Arc::clone(&snapshots_arc);
    let config_clone = config.clone();

    tokio::spawn(async move {
        while cp.fetch_and(true, Ordering::Relaxed) {
            let snapshot = get_snapshot(&config_clone).await.unwrap();
            snapshots_arc_cp.write().await.entry(snapshot.nonce).or_insert(snapshot);
        }
    });

    // Subscribe to book updates
    let sub_msg = serde_json::json!({
        "action": "subscribe",
        "channels": [{
            "name": "book",
            "markets": [config.market],
        }]
    });

    write.send(Message::Text(sub_msg.to_string().into()))
        .await
        .expect("failed to send subscribe to the events");

    let mut updates = BTreeMap::new();

    println!("starting polling events...");

    while let Some(Ok(msg)) = read.next().await {
        if let Message::Text(text) = msg {
            // Try to parse as a book update
            if let Ok(raw_update) = serde_json::from_str::<BookUpdate>(&text) {
                let update = decode_book_update(raw_update);
                updates.insert(update.nonce, update);
            }
            if updates.len() == config.num_snapshots {
                should_run.store(false, Ordering::Relaxed);
                break;
            }
        }
    }

    let snapshots = snapshots_arc.read().await;
    let keys = updates.keys().cloned().collect::<Vec<_>>();

    // let's clean the updates older the earliest snapshot
    for key in &keys {
        if *key <= *snapshots.first_key_value().unwrap().0 {
            updates.remove(&key);
        } else {
            break;
        }
    }

    // let's clean updates that are younger than the latest snapshot
    for key in &keys {
        if *key > *snapshots.last_key_value().unwrap().0 {
            updates.remove(&key);
        }
    }

    let mut first_snapshot = snapshots
        .first_key_value()
        .map(|(_, v)| (*v).clone())
        .unwrap();

    let last_snapshot = snapshots
        .last_key_value()
        .map(|(_, v)| v.clone()).unwrap();

    for (_, update) in updates.iter() {
        first_snapshot = merge_snapshot_and_update(first_snapshot, update)
    }

    let mut bids = Table::new();

    assert_eq!(first_snapshot.nonce == last_snapshot.nonce, true);

    bids.add_row(row![
        "Price level index",
        "Received snapshot bids",
        "Snapshot with applied updates bids",
    ]);

    for i in 0..first_snapshot.bids.len() {
        bids.add_row(row![
            i.to_string(),
            format_price_level(last_snapshot.bids.get(i)),
            format_price_level(first_snapshot.bids.get(i))
        ]);
    }

    bids.printstd();

    let mut asks = Table::new();

    asks.add_row(row![
        "Price level index",
        "Received snapshot asks",
        "Snapshot with applied updates asks",
    ]);

    for i in 0..first_snapshot.asks.len() {
        asks.add_row(row![
            i.to_string(),
            format_price_level(last_snapshot.asks.get(i)),
            format_price_level(first_snapshot.asks.get(i))
        ]);
    }

    asks.printstd();
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn test_snapshot() {}

    #[test]
    fn test_trim_zeroes_0() {
        let src = vec![["1300".to_string(), "1.234000".to_string()]];
        let dst = trim_price_levels(&src);

        assert_eq!(
            dst,
            vec![["1300".to_string(), "1.234".to_string()]],
        )
    }
}
