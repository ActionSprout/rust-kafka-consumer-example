mod consumer;
mod model;
mod sink;

fn main() -> anyhow::Result<()> {
    pretty_env_logger::init_timed();

    let args = clap::App::new("Rust Kafka Consumer Example")
        .version("1.0")
        .about("Polls kafka for debezium changes and upserts those changes to postgres.")
        .arg(
            clap::Arg::with_name("kafka-url")
                // TODO: Support multiple broker urls
                .long("kafka")
                .env("KAFKA_URL")
                .help("Kafka broker host and port (example: localhost:8082)")
                .takes_value(true)
                .required(true)
                .default_value("localhost:9092"),
        )
        .arg(
            clap::Arg::with_name("postgres-url")
                .long("postgres")
                .env("POSTGRES_URL")
                .help("Postgres url (example: postgres://user:pass@localhost:5432/database)")
                .takes_value(true)
                .required(true),
        )
        .get_matches();

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
