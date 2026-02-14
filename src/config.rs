use clap::Parser;

#[derive(Debug, Parser)]
pub struct Config {
    #[arg(long, default_value = "BTC-EUR")]
    pub market: String,
    #[arg(long, default_value = "10")]
    pub depth: u8,
    #[arg(long, default_value = "wss://ws.bitvavo.com")]
    pub ws_url: String,
    #[arg(long, default_value = "https://api.bitvavo.com")]
    pub api_url: String,
    #[arg(long, default_value = "20")]
    pub num_snapshots: usize,
}
