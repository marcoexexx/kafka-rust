use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::{self, ClientConfig, Message};
use uuid::Uuid;

pub use crate::error::{Error, Result};

mod error;

const KAFKA_HOST_URL: &'static str = "localhost:9092";
const KAFKA_TOPIC: &'static str = "chat";

fn create_consumer(bootstrap_server: &str) -> Result<StreamConsumer> {
  let cfg = ClientConfig::new()
    .set("bootstrap.servers", bootstrap_server)
    .set("enable.partition.eof", "false")
    .set("group.id", format!("chat-{}", Uuid::new_v4()))
    .create()?;

  Ok(cfg)
}

async fn listen(consumer: &StreamConsumer) -> Result<()> {
  loop {
    let msg = consumer.recv().await?.detach();
    let key = msg.key().ok_or("No key")?;

    let payload = msg.payload().ok_or("No payload")?;

    let key_str = String::from_utf8_lossy(key);
    let payload_str = String::from_utf8_lossy(payload);

    println!("[{}] Recv: {}", key_str, payload_str);
  }
}

#[tokio::main]
async fn main() -> Result<()> {
  let consumer = create_consumer(KAFKA_HOST_URL)?;

  consumer.subscribe(&[KAFKA_TOPIC])?;

  listen(&consumer).await?;

  Ok(())
}
