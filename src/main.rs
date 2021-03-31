extern crate kafka;

fn main() {
    println!("Hello, world!");

    let host = String::from("localhost:9094");
    let consumer = kafka::consumer::Consumer::from_hosts(vec![host])
        .with_topic(String::from("fernpeople.public.people"))
        .with_fallback_offset(kafka::consumer::FetchOffset::Earliest)
        .create();

    match consumer {
        Ok(consumer) => start_polling(consumer),
        Err(error) => panic!("OH NO {}", error),
    }
}

fn start_polling(consumer: kafka::consumer::Consumer) {
    let mut consumer = consumer;
    loop {
        for ms in consumer.poll() {
            for messages in ms.iter() {
                for message in messages.messages() {
                    println!("Got message {:?}", message);
                }
            }
        }
    }
}
