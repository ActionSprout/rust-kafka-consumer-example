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

    match consumer {
        Ok(consumer) => consumer::start_polling(consumer, handle_event),
        Err(error) => anyhow::bail!("OH NO {}", error),
    }
}

fn handle_event(event: model::Event) -> anyhow::Result<()> {
    println!("Got message {:?}", event);

    Ok(())
}
