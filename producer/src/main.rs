use std::io::Write;

use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use rdkafka::ClientConfig;

pub use crate::error::{Error, Result};

mod error;

const KAFKA_HOST_URL: &'static str = "localhost:9092";
const KAFKA_TOPIC: &'static str = "chat";

fn create_producer(bootstrap_server: &str) -> Result<FutureProducer> {
  let cfg = ClientConfig::new()
    .set("bootstrap.servers", bootstrap_server)
    .set("queue.buffering.max.ms", "0")
    .create()?;

  Ok(cfg)
}

#[tokio::main]
async fn main() -> Result<()> {
  let producer = create_producer(KAFKA_HOST_URL)?;

  loop {
    let mut input = String::new();

    print!("Type: ");
    std::io::stdin().read_line(&mut input)?;
    std::io::stdout().flush()?;

    let record = FutureRecord::to(KAFKA_TOPIC)
      .key("1")
      .payload(input.as_bytes());

    producer
      .send(record, Timeout::Never)
      .await
      .expect("Failed to produce");
  }
}
