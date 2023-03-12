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
    author = "Kofi Oghenerukevwe H.",
    about = "A Manning LiveProject: async Rust"
)]
struct Opts {
    #[clap(short, long, default_value = "AAPL,MSFT,UBER,GOOG")]
    symbols: String,
    #[clap(short, long)]
    from: String,
}

#[message]
#[derive(Debug, Clone)]
struct DownloadSymbolMessage {
    symbol: String,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
}

struct DownloadActor;

#[async_trait]
impl Actor for DownloadActor {
    async fn started(&mut self, ctx: &mut Context<Self>) -> Result<()> {
        dbg!("Starting download actor");
        ctx.subscribe::<DownloadSymbolMessage>().await?;

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



#[xactor::main]
async fn main() -> std::io::Result<()> {
    let opts = Opts::parse();
    let from: DateTime<Utc> = opts.from.parse().expect("Couldn't parse 'from' date");
    let to = Utc::now();

    let mut interval = stream::interval(Duration::from_secs(1));

    let _downloader = Supervisor::start(|| DownloadActor).await;
    let _processor = Supervisor::start(|| ProcessSymbolActor).await;
    let _sink = Supervisor::start(|| PrintSymbolSignalsActor).await;

    // DownloadActor.start().await.unwrap();
    // ProcessSymbolActor.start().await.unwrap();
    // PrintSymbolSignalsActor.start().await.unwrap();

    // a simple way to output a CSV header
    println!("period start,symbol,price,change %,min,max,30d avg");
    let symbols: Vec<&str> = opts.symbols.split(',').collect();
    while let Some(_) = interval.next().await {
        for symbol in &symbols {
            let request = DownloadSymbolMessage {
                symbol: symbol.to_string().clone(),
                from: from.clone(),
                to: to.clone()
            };

            Broker::from_registry().await.unwrap().publish(request).unwrap();
        }
    }
    Ok(())
}

