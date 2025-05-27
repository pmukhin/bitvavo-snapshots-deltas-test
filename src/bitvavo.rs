use crate::config::Config;
use bigdecimal::BigDecimal;
use futures::{SinkExt, StreamExt};
use reqwest::Error;
use serde::Deserialize;
use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;

fn parse_big_decimal(decimal_str: &str) -> BigDecimal {
    BigDecimal::from_str(decimal_str)
        .unwrap_or_else(|_| panic!("unexpected decimal: {}", decimal_str))
        .normalized()
}

fn decode_asks(asks: &[[String; 2]]) -> BTreeMap<BigDecimal, BigDecimal> {
    asks.iter()
        .map(|b| (
            parse_big_decimal(&b[0]),
            parse_big_decimal(&b[1]))).collect()
}

fn decode_bids(bids: &[[String; 2]]) -> BTreeMap<Reverse<BigDecimal>, BigDecimal> {
    bids.iter()
        .map(|b| (
            Reverse(parse_big_decimal(&b[0])),
            parse_big_decimal(&b[1]))).collect()
}

impl From<OrderBookDto> for OrderBook {
    fn from(dto: OrderBookDto) -> Self {
        let asks = decode_asks(&dto.asks);
        let bids = decode_bids(&dto.bids);
        OrderBook { asks, bids, nonce: 0 }
    }
}

impl From<BookUpdateDto> for BookUpdate {
    fn from(dto: BookUpdateDto) -> BookUpdate {
        let mut asks = None;
        let mut bids = None;
        if let Some(_asks) = dto.asks {
            asks = Some(decode_asks(&_asks));
        }
        if let Some(_bids) = dto.bids {
            bids = Some(decode_bids(&_bids));
        }
        BookUpdate { asks, bids, nonce: dto.nonce }
    }
}

#[derive(Clone, Debug)]
pub struct OrderBook {
    pub asks: BTreeMap<BigDecimal, BigDecimal>,
    pub bids: BTreeMap<Reverse<BigDecimal>, BigDecimal>,
    pub nonce: u64,
}

#[derive(Clone, Debug)]
pub struct BookUpdate {
    pub asks: Option<BTreeMap<BigDecimal, BigDecimal>>,
    pub bids: Option<BTreeMap<Reverse<BigDecimal>, BigDecimal>>,
    pub nonce: u64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct OrderBookDto {
    pub asks: Vec<[String; 2]>,
    pub bids: Vec<[String; 2]>,
    pub nonce: u64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct BookUpdateDto {
    pub nonce: u64,
    pub asks: Option<Vec<[String; 2]>>,
    pub bids: Option<Vec<[String; 2]>>,
}

async fn get_snapshot(config: &Config) -> Result<OrderBookDto, Error> {
    let url = format!("{}/v2/{}/book?depth={}", config.api_url, config.market, config.depth);
    let raw_book: OrderBookDto = reqwest::get(url).await?.json().await?;
    Ok(raw_book)
}

pub async fn pull_snapshots_until(
    config: &Config,
    should_run: Arc<AtomicBool>,
) -> BTreeMap<u64, OrderBook> {
    let mut snapshots = BTreeMap::new();
    while should_run.fetch_and(true, Ordering::Relaxed) {
        let raw_snapshot = get_snapshot(config).await.unwrap();
        snapshots.entry(raw_snapshot.nonce).or_insert(raw_snapshot.into());
    }
    snapshots
}

pub async fn get_deltas(
    config: &Config,
    stop_signal: Arc<AtomicBool>,
) -> BTreeMap<u64, BookUpdate> {
    let url = format!("{}/v2/", config.ws_url)
        .into_client_request()
        .expect("invalid URL");

    let (ws_stream, _) = connect_async(url)
        .await
        .expect("failed to connect");

    println!("connected to Bitvavo WebSocket @ {}", config.ws_url);

    let (mut write, mut read) = ws_stream.split();

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

    let mut updates: BTreeMap<u64, BookUpdate> = BTreeMap::new();

    println!("starting polling events...");

    while let Some(Ok(msg)) = read.next().await {
        if let Message::Text(text) = msg {
            // Try to parse as a book update
            if let Ok(raw_update) = serde_json::from_str::<BookUpdateDto>(&text) {
                updates.insert(raw_update.nonce, raw_update.into());
            }
            if updates.len() == config.num_snapshots {
                stop_signal.store(false, Ordering::Relaxed);
                break;
            }
        }
    }

    updates
}
