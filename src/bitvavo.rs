use crate::config::Config;
use bigdecimal::{BigDecimal, Zero};
use futures::{SinkExt, StreamExt};
use reqwest::Error;
use serde::Deserialize;
use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::fmt::Display;
use std::str::FromStr;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_util::sync::CancellationToken;
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

fn try_decode_asks(asks: &[[String; 2]]) -> anyhow::Result<BTreeMap<PriceLevel, BigDecimal>> {
    asks.iter()
        .map(|b| (PriceLevel::from_str(&b[0]), BigDecimal::from_str(&b[1])))
        .map(|(p, q)| Ok((p?, q?)))
        .collect()
}

fn try_decode_bids(
    bids: &[[String; 2]],
) -> anyhow::Result<BTreeMap<Reverse<PriceLevel>, BigDecimal>> {
    bids.iter()
        .map(|b| (PriceLevel::from_str(&b[0]), BigDecimal::from_str(&b[1])))
        .map(|(p, q)| Ok((Reverse(p?), q?)))
        .collect()
}

impl TryFrom<OrderBookDto> for OrderBook {
    type Error = anyhow::Error;

    fn try_from(dto: OrderBookDto) -> anyhow::Result<Self> {
        let asks = try_decode_asks(&dto.asks)?;
        let bids = try_decode_bids(&dto.bids)?;
        Ok(OrderBook {
            asks,
            bids,
            nonce: dto.nonce,
        })
    }
}

impl TryFrom<BookUpdateDto> for BookUpdate {
    type Error = anyhow::Error;

    fn try_from(dto: BookUpdateDto) -> anyhow::Result<BookUpdate> {
        let mut asks = None;
        let mut bids = None;
        if let Some(_asks) = dto.asks {
            asks = Some(try_decode_asks(&_asks)?);
        }
        if let Some(_bids) = dto.bids {
            bids = Some(try_decode_bids(&_bids)?);
        }
        Ok(BookUpdate {
            asks,
            bids,
            nonce: dto.nonce,
        })
    }
}

#[derive(Clone, Debug)]
pub struct OrderBook {
    pub asks: BTreeMap<PriceLevel, BigDecimal>,
    pub bids: BTreeMap<Reverse<PriceLevel>, BigDecimal>,
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
                panic!(
                    "update nonce is smaller or equal to the snapshot's update: snapshot.nonce = {}, update.nonce={}",
                    self.nonce, update.nonce
                )
            }
            Self::apply(update.bids.as_ref(), &mut self.bids);
            Self::apply(update.asks.as_ref(), &mut self.asks);
            self.nonce = update.nonce;
        }
    }
}

#[derive(Clone, Debug, Copy, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct PriceLevel {
    price_bps: u64,
}

impl PriceLevel {
    fn from_str(price_str: &str) -> anyhow::Result<Self> {
        let mut r: u64 = 0;
        price_str.bytes().take_while(|b| *b != b'.').for_each(|b| {
            r *= 10;
            r += (b as u64) - 48;
        });
        Ok(PriceLevel { price_bps: r })
    }
}

impl Display for PriceLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.price_bps.to_string())
    }
}

impl EqZero for Reverse<PriceLevel> {
    fn eq_zero(&self) -> bool {
        self.0.eq_zero()
    }
}

impl EqZero for PriceLevel {
    fn eq_zero(&self) -> bool {
        self.price_bps.is_zero()
    }
}

// this is strictly not cloneable
#[derive(Debug)]
pub struct BookUpdate {
    pub asks: Option<BTreeMap<PriceLevel, BigDecimal>>,
    pub bids: Option<BTreeMap<Reverse<PriceLevel>, BigDecimal>>,
    pub nonce: u64,
}

// this is strictly not cloneable
#[derive(Debug, Deserialize)]
pub struct OrderBookDto {
    pub asks: Vec<[String; 2]>,
    pub bids: Vec<[String; 2]>,
    pub nonce: u64,
}

// this is strictly not cloneable
#[derive(Debug, Deserialize)]
pub struct BookUpdateDto {
    pub nonce: u64,
    pub asks: Option<Vec<[String; 2]>>,
    pub bids: Option<Vec<[String; 2]>>,
}

async fn get_snapshot(config: &Config) -> Result<OrderBookDto, Error> {
    let url = format!(
        "{}/v2/{}/book?depth={}",
        config.api_url, config.market, config.depth
    );
    let raw_book: OrderBookDto = reqwest::get(url).await?.json().await?;
    Ok(raw_book)
}

pub async fn pull_snapshots(
    config: &Config,
    cancellation_token: CancellationToken,
) -> anyhow::Result<BTreeMap<u64, OrderBook>> {
    let _span = tracing::info_span!("pull_snapshots_until").entered();
    let mut snapshots = BTreeMap::new();

    info!("fetching snapshots...");

    while !cancellation_token.is_cancelled() {
        let raw_snapshot = get_snapshot(config).await?;
        snapshots
            .entry(raw_snapshot.nonce)
            .or_insert(raw_snapshot.try_into()?);
    }

    info!("done fetching snapshots");
    Ok(snapshots)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bigdecimal::BigDecimal;
    use std::cmp::Reverse;
    use std::collections::BTreeMap;
    use std::str::FromStr;

    #[test]
    fn test_order_book_apply_updates() {
        // given
        let mut initial_asks = BTreeMap::new();
        initial_asks.insert(
            PriceLevel::from_str("100.0").unwrap(),
            BigDecimal::from_str("1.0").unwrap(),
        );
        initial_asks.insert(
            PriceLevel::from_str("101.0").unwrap(),
            BigDecimal::from_str("2.0").unwrap(),
        );

        let mut initial_bids = BTreeMap::new();
        initial_bids.insert(
            Reverse(PriceLevel::from_str("99.0").unwrap()),
            BigDecimal::from_str("1.5").unwrap(),
        );

        let mut order_book = OrderBook {
            asks: initial_asks,
            bids: initial_bids,
            nonce: 1,
        };

        let mut update_asks = BTreeMap::new();
        update_asks.insert(
            PriceLevel::from_str("100.0").unwrap(),
            BigDecimal::from_str("1.2").unwrap(),
        ); // update
        update_asks.insert(
            PriceLevel::from_str("101.0").unwrap(),
            BigDecimal::from_str("0.0").unwrap(),
        ); // remove
        update_asks.insert(
            PriceLevel::from_str("102.0").unwrap(),
            BigDecimal::from_str("3.0").unwrap(),
        ); // add

        let mut update_bids = BTreeMap::new();
        update_bids.insert(
            Reverse(PriceLevel::from_str("98.0").unwrap()),
            BigDecimal::from_str("0.0").unwrap(),
        ); // ignore - not present
        update_bids.insert(
            Reverse(PriceLevel::from_str("99.0").unwrap()),
            BigDecimal::from_str("0.0").unwrap(),
        ); // remove
        update_bids.insert(
            Reverse(PriceLevel::from_str("97.0").unwrap()),
            BigDecimal::from_str("2.5").unwrap(),
        ); // add

        let mut updates = BTreeMap::new();
        updates.insert(
            2,
            BookUpdate {
                asks: Some(update_asks),
                bids: Some(update_bids),
                nonce: 2,
            },
        );

        // when
        order_book.apply_updates(updates);

        // then
        assert_eq!(order_book.nonce, 2);

        assert_eq!(order_book.asks.len(), 2);
        assert_eq!(
            order_book
                .asks
                .get(&PriceLevel::from_str("100.0").unwrap())
                .unwrap(),
            &BigDecimal::from_str("1.2").unwrap()
        );
        assert_eq!(
            order_book
                .asks
                .get(&PriceLevel::from_str("102.0").unwrap())
                .unwrap(),
            &BigDecimal::from_str("3.0").unwrap()
        );
        assert!(
            !order_book
                .asks
                .contains_key(&PriceLevel::from_str("101.0").unwrap())
        );

        assert_eq!(order_book.bids.len(), 1);
        assert_eq!(
            order_book
                .bids
                .get(&Reverse(PriceLevel::from_str("97.0").unwrap()))
                .unwrap(),
            &BigDecimal::from_str("2.5").unwrap()
        );
        assert!(
            !order_book
                .bids
                .contains_key(&Reverse(PriceLevel::from_str("99.0").unwrap()))
        );
    }

    #[test]
    fn test_decode_asks() -> anyhow::Result<()> {
        let input = vec![
            ["100.000".to_string(), "1.0000".to_string()],
            ["101.0".to_string(), "0.000000".to_string()],
        ];
        let result = try_decode_asks(&input)?;

        let mut expected = BTreeMap::new();
        expected.insert(PriceLevel::from_str("100")?, BigDecimal::from_str("1")?);
        expected.insert(PriceLevel::from_str("101")?, BigDecimal::from_str("0")?);

        assert_eq!(result, expected);

        Ok(())
    }

    #[test]
    fn test_decode_bids() -> anyhow::Result<()> {
        let input = vec![
            ["99.9900".to_string(), "5.500".to_string()],
            ["98.0000".to_string(), "0.000".to_string()],
        ];
        let result = try_decode_bids(&input)?;

        let mut expected = BTreeMap::new();
        expected.insert(
            Reverse(PriceLevel::from_str("99.99")?),
            BigDecimal::from_str("5.5")?,
        );
        expected.insert(
            Reverse(PriceLevel::from_str("98")?),
            BigDecimal::from_str("0")?,
        );

        assert_eq!(result, expected);

        Ok(())
    }
}

pub struct TokenWrapper(CancellationToken);

impl Drop for TokenWrapper {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

pub async fn get_deltas(
    config: &Config,
    cancellation_token: CancellationToken,
) -> anyhow::Result<BTreeMap<u64, BookUpdate>> {
    let _span = tracing::info_span!("get_deltas").entered();
    let _token_wrapper = TokenWrapper(cancellation_token);

    let url = format!("{}/v2/", config.ws_url)
        .into_client_request()
        .expect("invalid URL");

    let (ws_stream, _) = connect_async(url).await.expect("failed to connect");

    info!("connected to Bitvavo WebSocket @ {}", config.ws_url);

    let (mut write, mut read) = ws_stream.split();

    let sub_msg = serde_json::json!({
        "action": "subscribe",
        "channels": [{
            "name": "book",
            "markets": [config.market],
        }]
    });

    write
        .send(Message::Text(sub_msg.to_string().into()))
        .await
        .expect("failed to send subscribe to the events");

    let mut updates: BTreeMap<u64, BookUpdate> = BTreeMap::new();

    info!("starting polling events...");

    while let Some(Ok(msg)) = read.next().await {
        if let Message::Text(text) = msg {
            // Try to parse as a book update
            if let Ok(raw_update) = serde_json::from_str::<BookUpdateDto>(&text) {
                updates.insert(raw_update.nonce, raw_update.try_into()?);
            }
            if updates.len() == config.num_snapshots {
                break;
            }
        }
    }

    info!("done fetching events");

    Ok(updates)
}
