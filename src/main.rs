mod consumer;
mod model;
mod sink;

fn main() -> anyhow::Result<()> {
    let (sender, receiver) = std::sync::mpsc::channel();

    let mut pg_conn = sink::init()?;
    let consumer_conn = consumer::init();

    let consumer_handle = match consumer_conn {
        Ok(consumer_conn) => consumer::start_polling(consumer_conn, sender),
        Err(error) => anyhow::bail!("OH NO {}", error),
    };

    for event in receiver.iter() {
        sink::handle_event(&mut pg_conn, event)?;
    }

    consumer_handle.join().unwrap()
}
