mod helpers;

use async_std::prelude::*;
use async_std::stream;
use chrono::prelude::*;
use clap::Parser;
use futures::future;
use std::io::{Error, ErrorKind};
use std::time::Duration;
use yahoo_finance_api as yahoo;
use async_trait::async_trait;
use xactor::*;
use helpers::*;

#[derive(Parser, Debug)]
#[clap(
    version = "1.0",
    author = "Claus Matzinger",
    about = "A Manning LiveProject: async Rust"
)]
struct Opts {
    #[clap(short, long, default_value = "AAPL,MSFT,UBER,GOOG")]
    symbols: String,
    #[clap(short, long)]
    from: String,
}

#[message(result = "()")]
#[derive(Clone)]
struct DownloadSymbolMessage {
    pub symbol: String,
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
}

struct DownloadActor;

#[async_trait]
impl Actor for DownloadActor {
    async fn started(&mut self, ctx: &mut Context<Self>) -> Result<()> {
        dbg!("Starting download actor");
        ctx.subscribe::<DownloadSymbolMessage>().await;

        Ok(())
    }
}


#[async_trait]
impl Handler<DownloadSymbolMessage> for DownloadActor {
    async fn handle(&mut self, _ctx: &mut Context<Self>, msg: DownloadSymbolMessage) {
        dbg!("Handling Download Symbol");
        let closes = fetch_closing_data(msg.symbol.as_str(), &msg.from, &msg.to).await.unwrap();

        let processSymbolMsg = ProcessSymbolSignalsMessage {
            symbol: msg.symbol,
            from: msg.from,
            to: msg.to,
            closes
        };

        Broker::from_registry().await.unwrap().publish(processSymbolMsg);
        dbg!("Published Process Symbol Message");
    }
}


#[async_std::main]
async fn main() -> std::io::Result<()> {
    let opts = Opts::parse();
    let from: DateTime<Utc> = opts.from.parse().expect("Couldn't parse 'from' date");
    let to = Utc::now();

    let mut interval = stream::interval(Duration::from_secs(2));

    DownloadActor.start().await.unwrap();
    ProcessSymbolActor.start().await.unwrap();
    PrintSymbolSignalsActor.start().await.unwrap();

    // a simple way to output a CSV header
    println!("period start,symbol,price,change %,min,max,30d avg");
    let symbols: Vec<&str> = opts.symbols.split(',').collect();
    while let Some(_) = interval.next().await {
        for symbol in &symbols {
            Broker::from_registry().await.unwrap().publish(DownloadSymbolMessage {
                symbol: symbol.to_string().clone(),
                from: from.clone(),
                to: to.clone()
            });
        }
    }
    Ok(())
}

