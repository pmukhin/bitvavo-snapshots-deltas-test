use crate::config::Config;
use bigdecimal::{BigDecimal, Zero};
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
use tracing::info;

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
        OrderBook { asks, bids, nonce: dto.nonce }
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

impl OrderBook {
    fn apply<A: Ord + Clone + EqZero>(
        maybe_price_levels: Option<&BTreeMap<A, BigDecimal>>,
        price_level_tree: &mut BTreeMap<A, BigDecimal>,
    ) {
        if let Some(price_levels) = maybe_price_levels {
            for (price, size) in price_levels {
                if size.eq_zero() {
                    price_level_tree.remove(price);
                } else {
                    price_level_tree.insert(price.clone(), size.clone());
                }
            }
        }
    }

    pub fn apply_updates(&mut self, updates: BTreeMap<u64, BookUpdate>) {
        for update in updates.values() {
            if self.nonce >= update.nonce {
                eprintln!("snapshot.nonce = {}, update.nonce={}", self.nonce, update.nonce);
                panic!("update nonce is smaller or equal to the snapshot's update")
            }
            Self::apply(update.bids.as_ref(), &mut self.bids);
            Self::apply(update.asks.as_ref(), &mut self.asks);
            self.nonce = update.nonce;
        }
    }
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
    let _span = tracing::info_span!("pull_snapshots_until").entered();
    let mut snapshots = BTreeMap::new();

    info!("fetching snapshots...");

    while should_run.fetch_and(true, Ordering::Relaxed) {
        let raw_snapshot = get_snapshot(config).await.unwrap();
        snapshots.entry(raw_snapshot.nonce).or_insert(raw_snapshot.into());
    }

    info!("done fetching snapshots");
    snapshots
}

pub async fn get_deltas(
    config: &Config,
    stop_signal: Arc<AtomicBool>,
) -> BTreeMap<u64, BookUpdate> {
    let _span = tracing::info_span!("get_deltas").entered();

    let url = format!("{}/v2/", config.ws_url)
        .into_client_request()
        .expect("invalid URL");

    let (ws_stream, _) = connect_async(url)
        .await
        .expect("failed to connect");

    info!("connected to Bitvavo WebSocket @ {}", config.ws_url);

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

    info!("starting polling events...");

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

    info!("done fetching events");

    updates
}

#[cfg(test)]
mod tests {
    use super::*;
    use bigdecimal::BigDecimal;
    use std::collections::{BTreeMap};
    use std::cmp::Reverse;
    use std::str::FromStr;

    #[test]
    fn test_order_book_apply_updates() {
        // given
        let mut initial_asks = BTreeMap::new();
        initial_asks.insert(BigDecimal::from_str("100.0").unwrap(), BigDecimal::from_str("1.0").unwrap());
        initial_asks.insert(BigDecimal::from_str("101.0").unwrap(), BigDecimal::from_str("2.0").unwrap());

        let mut initial_bids = BTreeMap::new();
        initial_bids.insert(Reverse(BigDecimal::from_str("99.0").unwrap()), BigDecimal::from_str("1.5").unwrap());

        let mut order_book = OrderBook {
            asks: initial_asks,
            bids: initial_bids,
            nonce: 1,
        };

        let mut update_asks = BTreeMap::new();
        update_asks.insert(BigDecimal::from_str("100.0").unwrap(), BigDecimal::from_str("1.2").unwrap()); // update
        update_asks.insert(BigDecimal::from_str("101.0").unwrap(), BigDecimal::from_str("0.0").unwrap()); // remove
        update_asks.insert(BigDecimal::from_str("102.0").unwrap(), BigDecimal::from_str("3.0").unwrap()); // add

        let mut update_bids = BTreeMap::new();
        update_bids.insert(Reverse(BigDecimal::from_str("98.0").unwrap()), BigDecimal::from_str("0.0").unwrap()); // ignore - not present
        update_bids.insert(Reverse(BigDecimal::from_str("99.0").unwrap()), BigDecimal::from_str("0.0").unwrap()); // remove
        update_bids.insert(Reverse(BigDecimal::from_str("97.0").unwrap()), BigDecimal::from_str("2.5").unwrap()); // add

        let mut updates = BTreeMap::new();
        updates.insert(2, BookUpdate {
            asks: Some(update_asks),
            bids: Some(update_bids),
            nonce: 2,
        });

        // when
        order_book.apply_updates(updates);

        // then
        assert_eq!(order_book.nonce, 2);

        assert_eq!(order_book.asks.len(), 2);
        assert_eq!(order_book.asks.get(&BigDecimal::from_str("100.0").unwrap()).unwrap(), &BigDecimal::from_str("1.2").unwrap());
        assert_eq!(order_book.asks.get(&BigDecimal::from_str("102.0").unwrap()).unwrap(), &BigDecimal::from_str("3.0").unwrap());
        assert!(!order_book.asks.contains_key(&BigDecimal::from_str("101.0").unwrap()));

        assert_eq!(order_book.bids.len(), 1);
        assert_eq!(order_book.bids.get(&Reverse(BigDecimal::from_str("97.0").unwrap())).unwrap(), &BigDecimal::from_str("2.5").unwrap());
        assert!(!order_book.bids.contains_key(&Reverse(BigDecimal::from_str("99.0").unwrap())));
    }

    #[test]
    fn test_parse_big_decimal_valid() {
        let input = "123.45000";
        let result = parse_big_decimal(input);
        let expected = BigDecimal::from_str("123.45").unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    #[should_panic(expected = "unexpected decimal: abc")]
    fn test_parse_big_decimal_invalid() {
        parse_big_decimal("abc"); // Should panic
    }

    #[test]
    fn test_decode_asks() {
        let input = vec![
            ["100.000".to_string(), "1.0000".to_string()],
            ["101.0".to_string(), "0.000000".to_string()],
        ];
        let result = decode_asks(&input);

        let mut expected = BTreeMap::new();
        expected.insert(BigDecimal::from_str("100").unwrap(), BigDecimal::from_str("1").unwrap());
        expected.insert(BigDecimal::from_str("101").unwrap(), BigDecimal::from_str("0").unwrap());

        assert_eq!(result, expected);
    }

    #[test]
    fn test_decode_bids() {
        let input = vec![
            ["99.9900".to_string(), "5.500".to_string()],
            ["98.0000".to_string(), "0.000".to_string()],
        ];
        let result = decode_bids(&input);

        let mut expected = BTreeMap::new();
        expected.insert(Reverse(BigDecimal::from_str("99.99").unwrap()), BigDecimal::from_str("5.5").unwrap());
        expected.insert(Reverse(BigDecimal::from_str("98").unwrap()), BigDecimal::from_str("0").unwrap());

        assert_eq!(result, expected);
    }
}
