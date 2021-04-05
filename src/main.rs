mod cli;
mod consumer;
mod model;
mod sink;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    pretty_env_logger::init_timed();

    let args = cli::build_cli().get_matches();

    let (sender, mut receiver) = tokio::sync::mpsc::channel(1);

    let postgres_url = args.value_of("postgres-url").unwrap();
    let kafka_url = args.value_of("kafka-url").unwrap();
    let topic = args.value_of("topic").unwrap();

    let mut sink = sink::init(postgres_url).await?;
    let consumer_conn = consumer::init(&consumer::KafkaOptions {
        host: String::from(kafka_url),
        topic: String::from(topic),
    });

    let consumer_handle = match consumer_conn {
        Ok(consumer_conn) => consumer::start_polling(consumer_conn, sender),
        Err(error) => anyhow::bail!("Could not connect to kafka {}", error),
    };

    let sink_handle: tokio::task::JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
        while let Some(event) = receiver.recv().await {
            sink.handle_event(event.clone()).await?;
        }

        Ok(())
    });

    if let Err(error) = tokio::try_join!(sink_handle, consumer_handle) {
        panic!("Join failed; error = {}", error);
    }

    Ok(())
}
