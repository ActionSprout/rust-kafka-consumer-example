use crate::model;
use serde::Deserialize;

const POLL_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);

#[derive(Deserialize, Debug)]
struct DebeziumPayload {
    op: String,
    ts_ms: u64,
    before: Option<model::PersonRecord>,
    after: Option<model::PersonRecord>,
}

#[derive(Deserialize, Debug)]
struct DebeziumMessage {
    payload: DebeziumPayload,
}

#[derive(Debug)]
pub struct KafkaOptions {
    pub topic: String,
    pub host: String,
}

pub fn init(options: &KafkaOptions) -> Result<kafka::consumer::Consumer, kafka::Error> {
    log::info!("Connecting to kafka {:?}", options);

    // TODO: Support multiple hosts
    kafka::consumer::Consumer::from_hosts(vec![options.host.clone()])
        .with_topic(options.topic.clone())
        .with_fallback_offset(kafka::consumer::FetchOffset::Earliest)
        .with_group(String::from("people-etl"))
        .with_offset_storage(kafka::consumer::GroupOffsetStorage::Kafka)
        .create()
}

pub fn start_polling(
    consumer: kafka::consumer::Consumer,
    sender: tokio::sync::mpsc::Sender<model::Event>,
) -> tokio::task::JoinHandle<anyhow::Result<()>> {
    tokio::spawn(async move {
        let mut consumer = consumer;
        log::debug!("Starting kafka consumer");

        loop {
            for ms in consumer.poll() {
                let mut mq = vec![];

                for messages in ms.iter() {
                    for message in messages.messages() {
                        match serde_json::from_slice::<DebeziumMessage>(message.value) {
                            Ok(message) => mq.push(message.to_event()),
                            Err(error @ serde_json::Error { .. }) if error.is_eof() => {}
                            Err(error) => anyhow::bail!("Could not parse message {}", error),
                        }
                    }
                    if let Err(_) = consumer.consume_messageset(messages) {
                        log::error!("Could not mark message set as consumed");
                    }
                }

                for m in mq {
                    sender.send(m.clone()).await?;
                }

                if ms.is_empty() {
                    tokio::time::sleep(POLL_INTERVAL).await;
                } else {
                    // Feel free to remove this if we are using a kafka client that supports the tokio runtime.
                    tokio::task::yield_now().await;
                }
            }

            if let Err(_) = consumer.commit_consumed() {
                log::error!("Could not commit_consumed");
            }
        }
    })
}

impl DebeziumMessage {
    fn to_event(self) -> model::Event {
        match self.payload {
            DebeziumPayload {
                op,
                before: None,
                after: Some(after),
                ..
            } if op.as_str() == "c" => model::Event::Create(after),

            DebeziumPayload {
                op,
                before: Some(before),
                after: Some(after),
                ..
            } if op.as_str() == "u" => model::Event::Update(before, after),

            DebeziumPayload {
                op,
                before: Some(before),
                after: None,
                ..
            } if op.as_str() == "d" => model::Event::Delete(before),

            DebeziumPayload { .. } => model::Event::Unknown,
        }
    }
}
