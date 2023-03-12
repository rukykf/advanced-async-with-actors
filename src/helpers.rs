use yahoo_finance_api as yahoo;
use async_trait::async_trait;
use std::io::{Error, ErrorKind};
use chrono::prelude::*;
use xactor::*;

///
/// A trait to provide a common interface for all signal calculations.
///
#[async_trait]
trait AsyncStockSignal {

    ///
    /// The signal's data type.
    ///
    type SignalType;

    ///
    /// Calculate the signal on the provided series.
    ///
    /// # Returns
    ///
    /// The signal (using the provided type) or `None` on error/invalid data.
    ///
    async fn calculate(&self, series: &[f64]) -> Option<Self::SignalType>;
}

///
/// Calculates the absolute and relative difference between the beginning and ending of an f64 series.
/// The relative difference is relative to the beginning.
///
struct PriceDifference {}

#[async_trait]
impl AsyncStockSignal for PriceDifference {
    ///
    /// A tuple `(absolute, relative)` to represent a price difference.
    ///
    type SignalType = (f64, f64);

    async fn calculate(&self, series: &[f64]) -> Option<Self::SignalType> {
        if !series.is_empty() {
            // unwrap is safe here even if first == last
            let (first, last) = (series.first().unwrap(), series.last().unwrap());
            let abs_diff = last - first;
            let first = if *first == 0.0 { 1.0 } else { *first };
            let rel_diff = abs_diff / first;
            Some((abs_diff, rel_diff))
        } else {
            None
        }
    }
}

///
/// Window function to create a simple moving average
///
struct WindowedSMA {
    pub window_size: usize,
}

#[async_trait]
impl AsyncStockSignal for WindowedSMA {
    type SignalType = Vec<f64>;

    async fn calculate(&self, series: &[f64]) -> Option<Self::SignalType> {
        if !series.is_empty() && self.window_size > 1 {
            Some(
                series
                    .windows(self.window_size)
                    .map(|w| w.iter().sum::<f64>() / w.len() as f64)
                    .collect(),
            )
        } else {
            None
        }
    }
}

///
/// Find the maximum in a series of f64
///
struct MaxPrice {}

#[async_trait]
impl AsyncStockSignal for MaxPrice {
    type SignalType = f64;

    async fn calculate(&self, series: &[f64]) -> Option<Self::SignalType> {
        if series.is_empty() {
            None
        } else {
            Some(series.iter().fold(f64::MIN, |acc, q| acc.max(*q)))
        }
    }
}

///
/// Find the maximum in a series of f64
///
struct MinPrice {}

#[async_trait]
impl AsyncStockSignal for MinPrice {
    type SignalType = f64;

    async fn calculate(&self, series: &[f64]) -> Option<Self::SignalType> {
        if series.is_empty() {
            None
        } else {
            Some(series.iter().fold(f64::MAX, |acc, q| acc.min(*q)))
        }
    }
}

///
/// Retrieve data from a data source and extract the closing prices. Errors during download are mapped onto io::Errors as InvalidData.
///
pub async fn fetch_closing_data(
    symbol: &str,
    beginning: &DateTime<Utc>,
    end: &DateTime<Utc>,
) -> std::io::Result<Vec<f64>> {
    let provider = yahoo::YahooConnector::new();
    let response = provider
        .get_quote_history(symbol, *beginning, *end)
        .await
        .map_err(|_| Error::from(ErrorKind::InvalidData))?;
    let mut quotes = response
        .quotes()
        .map_err(|_| Error::from(ErrorKind::InvalidData))?;
    if !quotes.is_empty() {
        quotes.sort_by_cached_key(|k| k.timestamp);
        Ok(quotes.iter().map(|q| q.adjclose as f64).collect())
    } else {
        Ok(vec![])
    }
}


#[message(result = "()")]
#[derive(Clone, Debug)]
pub struct ProcessSymbolSignalsMessage{
    pub symbol: String,
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
    pub closes: Vec<f64>
}

pub struct ProcessSymbolActor;

#[async_trait]
impl Actor for ProcessSymbolActor{
    async fn started(&mut self, ctx: &mut Context<Self>) -> Result<()> {
        dbg!("Starting ProcessSymbol actor");

        ctx.subscribe::<ProcessSymbolSignalsMessage>().await?;

        Ok(())
    }
}

#[async_trait]
impl Handler<ProcessSymbolSignalsMessage> for ProcessSymbolActor {
    async fn handle(&mut self, _ctx: &mut Context<Self>, msg: ProcessSymbolSignalsMessage) {
        if !msg.closes.is_empty() {
            let diff = PriceDifference {};
            let min = MinPrice {};
            let max = MaxPrice {};
            let sma = WindowedSMA { window_size: 30 };
    
            let period_max: f64 = max.calculate(&msg.closes).await.unwrap();
            let period_min: f64 = min.calculate(&msg.closes).await.unwrap();
    
            let last_price = msg.closes.last().unwrap();
            let (_, pct_change) = diff.calculate(&msg.closes).await.unwrap();
            let sma = sma.calculate(&msg.closes).await.unwrap();
    
            // a simple way to output CSV data
            let symbol_message = format!(
                "{},{},${:.2},{:.2}%,${:.2},${:.2},${:.2}",
                msg.from.to_rfc3339(),
                msg.symbol,
                last_price,
                pct_change * 100.0,
                period_min,
                period_max,
                sma.last().unwrap_or(&0.0)
            );

            let print_signal_msg = PrintSymbolSignalsMessage {
                symbol: msg.symbol,
                message: symbol_message
            };

            Broker::from_registry().await.unwrap().publish(print_signal_msg).unwrap();
        }
       
    }
}

#[message(result = "()")]
#[derive(Debug, Default, Clone)]
struct PrintSymbolSignalsMessage{
    symbol: String,
    message: String
}

pub struct PrintSymbolSignalsActor;

#[async_trait]
impl Actor for PrintSymbolSignalsActor{
    async fn started(&mut self, ctx: &mut Context<Self>) -> Result<()> {
        dbg!("Starting Print symbol actor");

        ctx.subscribe::<PrintSymbolSignalsMessage>().await?;

        Ok(())
    }
}

#[async_trait]
impl Handler<PrintSymbolSignalsMessage> for PrintSymbolSignalsActor {
    async fn handle(&mut self, _ctx: &mut Context<Self>, msg: PrintSymbolSignalsMessage) {
       println!("{}", msg.message);
    }
}


#[cfg(test)]
mod tests {
    #![allow(non_snake_case)]
    use super::*;

    #[async_std::test]
    async fn test_PriceDifference_calculate() {
        let signal = PriceDifference {};
        assert_eq!(signal.calculate(&[]).await, None);
        assert_eq!(signal.calculate(&[1.0]).await, Some((0.0, 0.0)));
        assert_eq!(signal.calculate(&[1.0, 0.0]).await, Some((-1.0, -1.0)));
        assert_eq!(
            signal.calculate(&[2.0, 3.0, 5.0, 6.0, 1.0, 2.0, 10.0]).await,
            Some((8.0, 4.0))
        );
        assert_eq!(
            signal.calculate(&[0.0, 3.0, 5.0, 6.0, 1.0, 2.0, 1.0]).await,
            Some((1.0, 1.0))
        );
    }

    #[async_std::test]
    async fn test_MinPrice_calculate() {
        let signal = MinPrice {};
        assert_eq!(signal.calculate(&[]).await, None);
        assert_eq!(signal.calculate(&[1.0]).await, Some(1.0));
        assert_eq!(signal.calculate(&[1.0, 0.0]).await, Some(0.0));
        assert_eq!(
            signal.calculate(&[2.0, 3.0, 5.0, 6.0, 1.0, 2.0, 10.0]).await,
            Some(1.0)
        );
        assert_eq!(
            signal.calculate(&[0.0, 3.0, 5.0, 6.0, 1.0, 2.0, 1.0]).await,
            Some(0.0)
        );
    }

    #[async_std::test]
    async fn test_MaxPrice_calculate() {
        let signal = MaxPrice {};
        assert_eq!(signal.calculate(&[]).await, None);
        assert_eq!(signal.calculate(&[1.0]).await, Some(1.0));
        assert_eq!(signal.calculate(&[1.0, 0.0]).await, Some(1.0));
        assert_eq!(
            signal.calculate(&[2.0, 3.0, 5.0, 6.0, 1.0, 2.0, 10.0]).await,
            Some(10.0)
        );
        assert_eq!(
            signal.calculate(&[0.0, 3.0, 5.0, 6.0, 1.0, 2.0, 1.0]).await,
            Some(6.0)
        );
    }

    #[async_std::test]
    async fn test_WindowedSMA_calculate() {
        let series = vec![2.0, 4.5, 5.3, 6.5, 4.7];

        let signal = WindowedSMA { window_size: 3 };
        assert_eq!(
            signal.calculate(&series).await,
            Some(vec![3.9333333333333336, 5.433333333333334, 5.5])
        );

        let signal = WindowedSMA { window_size: 5 };
        assert_eq!(signal.calculate(&series).await, Some(vec![4.6]));

        let signal = WindowedSMA { window_size: 10 };
        assert_eq!(signal.calculate(&series).await, Some(vec![]));
    }
}
