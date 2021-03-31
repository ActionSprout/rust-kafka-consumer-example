mod consumer;
mod model;
mod sink;

fn main() -> anyhow::Result<()> {
    println!("Starting kafka consumer");

    let host = String::from("localhost:9094");
    let consumer = kafka::consumer::Consumer::from_hosts(vec![host])
        .with_topic(String::from("fernpeople.public.people"))
        .with_fallback_offset(kafka::consumer::FetchOffset::Earliest)
        .create();

    let (sender, receiver) = std::sync::mpsc::channel();

    let consumer_handle = match consumer {
        Ok(consumer) => consumer::start_polling(consumer, sender),
        Err(error) => anyhow::bail!("OH NO {}", error),
    };

    for event in receiver.iter() {
        sink::handle_event(event)?;
    }

    consumer_handle.join().unwrap()
}
