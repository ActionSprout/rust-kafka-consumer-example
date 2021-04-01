mod cli;
mod consumer;
mod model;
mod sink;

fn main() -> anyhow::Result<()> {
    pretty_env_logger::init_timed();

    let args = cli::build_cli().get_matches();

    let (sender, receiver) = std::sync::mpsc::channel();

    let postgres_url = args.value_of("postgres-url").unwrap();
    let kafka_url = args.value_of("kafka-url").unwrap();

    let mut sink = sink::init(postgres_url)?;
    let consumer_conn = consumer::init(kafka_url);

    let consumer_handle = match consumer_conn {
        Ok(consumer_conn) => consumer::start_polling(consumer_conn, sender),
        Err(error) => anyhow::bail!("Could not connect to kafka {}", error),
    };

    for event in receiver.iter() {
        sink.handle_event(event)?;
    }

    consumer_handle.join().unwrap()
}
