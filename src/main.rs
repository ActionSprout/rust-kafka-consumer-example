extern crate anyhow;
extern crate kafka;

fn main() -> anyhow::Result<()> {
    println!("Hello, world!");

    let host = String::from("localhost:9094");
    let consumer = kafka::consumer::Consumer::from_hosts(vec![host])
        .with_topic(String::from("fernpeople.public.people"))
        .with_fallback_offset(kafka::consumer::FetchOffset::Earliest)
        .create();

    return match consumer {
        Ok(consumer) => start_polling(consumer),
        Err(error) => anyhow::bail!("OH NO {}", error),
    };
}

fn start_polling(consumer: kafka::consumer::Consumer) -> anyhow::Result<()> {
    let mut consumer = consumer;

    loop {
        for ms in consumer.poll() {
            for messages in ms.iter() {
                for message in messages.messages() {
                    let json = String::from_utf8(message.value.to_vec())?;

                    println!("Got message {}", json);
                }
            }
        }
    }
}
