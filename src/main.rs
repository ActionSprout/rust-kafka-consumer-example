use serde::Deserialize;

const POLL_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);

#[derive(Deserialize, Debug)]
struct PersonRecord {
    id: u32,
    email: Option<String>,
    name: Option<String>,
}

#[derive(Deserialize, Debug)]
struct DebeziumPayload {
    op: String,
    before: Option<PersonRecord>,
    after: Option<PersonRecord>,
}

#[derive(Deserialize, Debug)]
struct DebeziumMessage {
    payload: DebeziumPayload,
}

fn main() -> anyhow::Result<()> {
    println!("Starting kafka consumer");

    let host = String::from("localhost:9094");
    let consumer = kafka::consumer::Consumer::from_hosts(vec![host])
        .with_topic(String::from("fernpeople.public.people"))
        .with_fallback_offset(kafka::consumer::FetchOffset::Earliest)
        .create();

    match consumer {
        Ok(consumer) => start_polling(consumer),
        Err(error) => anyhow::bail!("OH NO {}", error),
    }
}

fn start_polling(consumer: kafka::consumer::Consumer) -> anyhow::Result<()> {
    let mut consumer = consumer;

    loop {
        for ms in consumer.poll() {
            for messages in ms.iter() {
                for message in messages.messages() {
                    match serde_json::from_slice::<DebeziumMessage>(message.value) {
                        Ok(message) => handle_message(message)?,
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

fn handle_message(message: DebeziumMessage) -> anyhow::Result<()> {
    println!("Got message {:?}", message);

    Ok(())
}
