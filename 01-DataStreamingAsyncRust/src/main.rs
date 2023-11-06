use async_std::task;
use async_trait::async_trait;
use chrono::prelude::*;
use clap::Parser;
use std::io::{Error, ErrorKind};
use yahoo_finance_api as yahoo;
use yahoo::{YResponse,YahooError, Quote};

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
    #[clap(short, long, default_value = "now")]
    to: String,
}

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
/// Calculates the absolute and relative difference between the beginning and ending of an f64 series. The relative difference is relative to the beginning.
///
/// # Returns
///
/// A tuple `(absolute, relative)` difference.
///
fn price_diff(a: &[f64]) -> Option<(f64, f64)> {
    if !a.is_empty() {
        // unwrap is safe here even if first == last
        let (first, last) = (a.first().unwrap(), a.last().unwrap());
        let abs_diff = last - first;
        let first = if *first == 0.0 { 1.0 } else { *first };
        let rel_diff = abs_diff / first;
        Some((abs_diff, rel_diff))
    } else {
        None
    }
}

///
/// Window function to create a simple moving average
///
fn n_window_sma(n: usize, series: &[f64]) -> Option<Vec<f64>> {
    if !series.is_empty() && n > 1 {
        Some(
            series
                .windows(n)
                .map(|w| w.iter().sum::<f64>() / w.len() as f64)
                .collect(),
        )
    } else {
        None
    }
}

///
/// Find the maximum in a series of f64
///
fn max(series: &[f64]) -> Option<f64> {
    if series.is_empty() {
        None
    } else {
        Some(series.iter().fold(f64::MIN, |acc, q| acc.max(*q)))
    }
}

///
/// Find the minimum in a series of f64
///
fn min(series: &[f64]) -> Option<f64> {
    if series.is_empty() {
        None
    } else {
        Some(series.iter().fold(f64::MAX, |acc, q| acc.min(*q)))
    }
}

///
/// Retrieve data from a data source and extract the closing prices. Errors during download are mapped onto io::Errors as InvalidData.
///
async fn fetch_closing_data(
    symbol: &str,
    beginning: &DateTime<Utc>,
    end: &DateTime<Utc>,
) -> Result<YResponse, YahooError> {
    let provider = yahoo::YahooConnector::new();

    let response = provider
        .get_quote_history(symbol, *beginning, *end)
        .await;
    response
}

async fn process_yahoo_info(opts: String, from: DateTime<Utc>, to: DateTime<Utc>) {
    let mut closes_future_vec = vec![]; 
    for symbol in opts.split(',') {
        let fresult = fetch_closing_data(&symbol, &from, &to).await;
        closes_future_vec.push((fresult, symbol));
    }

    for closes_future in &closes_future_vec {
        match &closes_future.0 {
            Ok(closes_ok) => {
                // The Whole puprose of Futures is to initiate then and ask for their result
                // ONLY when it's needed, so all the awaits are in the println at the end
                let mut quotes: Vec<Quote> = closes_ok.quotes().map_err(|quotes_err| {
                    println!("[ERROR]: Got Quotes Retrieval Error on stock symbol {}: {}", &closes_future.1, quotes_err);
                    Error::from(ErrorKind::InvalidData)
                }).unwrap_or_default();

                if !quotes.is_empty() {
                    quotes.sort_by_cached_key(|k| k.timestamp);
                } else {
                    continue;
                }

                let sorted_quotes_as_vec_f64: Vec<f64> = quotes.iter().map(|q| q.adjclose as f64).collect();
                // min/max of the period. unwrap() because those are Option types
                let pmax_async = MaxPrice {};
                let pmin_async = MinPrice {};
                let period_max = pmax_async.calculate(&sorted_quotes_as_vec_f64);
                let period_min = pmin_async.calculate(&sorted_quotes_as_vec_f64);
                let last_price = *sorted_quotes_as_vec_f64.last().unwrap_or(&0.0);

                // Price Difference Async
                let pdiff_async = PriceDifference {};
                let (_, pct_change) = pdiff_async
                    .calculate(&sorted_quotes_as_vec_f64)
                    .await
                    .unwrap_or((0.0, 0.0));

                // SMA Async
                let sma_async = WindowedSMA { window_size: 30 };
                let sma = sma_async.calculate(&sorted_quotes_as_vec_f64);
                // a simple way to output CSV data
                println!(
                    "{},{},${:.2},{:.2}%,${:.2},${:.2},${:.2}",
                    from.to_rfc3339(),
                    closes_future.1,
                    last_price,
                    pct_change * 100.0,
                    period_min.await.unwrap(),
                    period_max.await.unwrap(),
                    sma.await.unwrap_or_default().last().unwrap_or(&0.0)
                );
            }
            Err(fetch_err) => println!("[ERROR] Error on fetching closing data for stock ticker {}: {}", closes_future.1, fetch_err),
        }
    }
}

fn main() -> std::io::Result<()> {
    let opts = Opts::parse();
    let from: DateTime<Utc> = opts.from.parse().expect("Couldn't parse 'from' date");
    let to_parsed = opts.to.parse();
    let to: DateTime<Utc> = match to_parsed {
        Ok(result) => result,
        Err(_) => Utc::now(),
    };

    let ticker_symbols = opts.symbols;
    for _ in 0..3 {
        let from_date = from.clone();
        let to_date = to.clone();
        let future_symbols = ticker_symbols.clone();

        // a simple way to output a CSV header
        println!("period start,symbol,price,change %,min,max,30d avg");

        let yahoo_info_task = task::spawn(process_yahoo_info(future_symbols, from_date, to_date));
        task::block_on(yahoo_info_task);
    }
    Ok(())
}

// ------------------------------------------------------------------------------------------- //
// -------------------------------- My source code ------------------------------------------- //

struct PriceDifference {}

#[async_trait]
impl AsyncStockSignal for PriceDifference {
    type SignalType = (f64, f64);
    async fn calculate(&self, series: &[f64]) -> Option<Self::SignalType> {
        price_diff(&series)
    }
}

// MIN PRICE
struct MinPrice {}

#[async_trait]
impl AsyncStockSignal for MinPrice {
    type SignalType = f64;
    async fn calculate(&self, series: &[f64]) -> Option<Self::SignalType> {
        min(&series)
    }
}

// MAX PRICE
struct MaxPrice {}

#[async_trait]
impl AsyncStockSignal for MaxPrice {
    type SignalType = f64;
    async fn calculate(&self, series: &[f64]) -> Option<Self::SignalType> {
        max(&series)
    }
}

// WINDOWED SIMPLE MOVING AVERAGE
struct WindowedSMA {
    window_size: usize,
}

#[async_trait]
impl AsyncStockSignal for WindowedSMA {
    type SignalType = Vec<f64>;
    async fn calculate(&self, series: &[f64]) -> Option<Self::SignalType> {
        // usize is a copy type, otherswise I'd be stressing over a move here
        n_window_sma(self.window_size, &series)
    }
}

// ------------------------------------------------------------------------------------------- //

#[cfg(test)]
mod tests {
    #![allow(non_snake_case)]
    use super::*;

    #[async_std::test]
    async fn test_PriceDifference_calculate() {
        task::block_on(async {
            let signal = PriceDifference {};
            assert_eq!(signal.calculate(&[]).await, None);
            assert_eq!(signal.calculate(&[1.0]).await, Some((0.0, 0.0)));
            assert_eq!(signal.calculate(&[1.0, 0.0]).await, Some((-1.0, -1.0)));
            assert_eq!(
                signal
                    .calculate(&[2.0, 3.0, 5.0, 6.0, 1.0, 2.0, 10.0])
                    .await,
                Some((8.0, 4.0))
            );
            assert_eq!(
                signal.calculate(&[0.0, 3.0, 5.0, 6.0, 1.0, 2.0, 1.0]).await,
                Some((1.0, 1.0))
            );
        });
    }

    #[async_std::test]
    async fn test_MinPrice_calculate() {
        let signal = MinPrice {};
        assert_eq!(signal.calculate(&[]).await, None);
        assert_eq!(signal.calculate(&[1.0]).await, Some(1.0));
        assert_eq!(signal.calculate(&[1.0, 0.0]).await, Some(0.0));
        assert_eq!(
            signal
                .calculate(&[2.0, 3.0, 5.0, 6.0, 1.0, 2.0, 10.0])
                .await,
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
            signal
                .calculate(&[2.0, 3.0, 5.0, 6.0, 1.0, 2.0, 10.0])
                .await,
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
