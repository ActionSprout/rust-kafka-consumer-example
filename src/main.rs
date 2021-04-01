mod consumer;
mod model;
mod sink;

fn main() -> anyhow::Result<()> {
    pretty_env_logger::init_timed();
    println!("Started logger with max_level {}", log::max_level());

    let (sender, receiver) = std::sync::mpsc::channel();

    let mut sink = sink::init()?;
    let consumer_conn = consumer::init();

    let consumer_handle = match consumer_conn {
        Ok(consumer_conn) => consumer::start_polling(consumer_conn, sender),
        Err(error) => anyhow::bail!("OH NO {}", error),
    };

    for event in receiver.iter() {
        sink.handle_event(event)?;
    }

    consumer_handle.join().unwrap()
}
