use crate::model;
use serde::Deserialize;

const POLL_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);

#[derive(Deserialize, Debug)]
struct DebeziumPayload {
    op: String,
    before: Option<model::PersonRecord>,
    after: Option<model::PersonRecord>,
}

#[derive(Deserialize, Debug)]
struct DebeziumMessage {
    payload: DebeziumPayload,
}

pub fn start_polling<T>(
    consumer: kafka::consumer::Consumer,
    handle_message: T,
) -> anyhow::Result<()>
where
    T: Fn(model::Event) -> anyhow::Result<()>,
{
    let mut consumer = consumer;

    loop {
        for ms in consumer.poll() {
            for messages in ms.iter() {
                for message in messages.messages() {
                    match serde_json::from_slice::<DebeziumMessage>(message.value) {
                        Ok(message) => handle_message(convert_to_event(message))?,
                        Err(error @ serde_json::Error { .. }) if error.is_eof() => {}
                        Err(error) => anyhow::bail!("Could not parse message {}", error),
                    }
                }
            }

            if ms.is_empty() {
                std::thread::sleep(POLL_INTERVAL);
            }
        }
    }
}

fn convert_to_event(message: DebeziumMessage) -> model::Event {
    match message.payload {
        DebeziumPayload {
            op,
            before: None,
            after: Some(after),
        } if op.as_str() == "c" => model::Event::Create(after),

        DebeziumPayload {
            op,
            before: Some(before),
            after: Some(after),
        } if op.as_str() == "u" => model::Event::Update(before, after),

        DebeziumPayload {
            op,
            before: Some(before),
            after: None,
        } if op.as_str() == "d" => model::Event::Delete(before),

        DebeziumPayload { .. } => model::Event::Unknown,
    }
}
