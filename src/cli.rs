use clap::App;
use clap::{app_from_crate, crate_authors, crate_description, crate_name, crate_version};

pub fn build_cli() -> App<'static, 'static> {
    app_from_crate!()
        .arg(
            clap::Arg::with_name("kafka-url")
                // TODO: Support multiple broker urls
                .long("kafka")
                .env("KAFKA_URL")
                .help("Kafka broker host and port [example: localhost:9092]")
                .takes_value(true)
                .required(true),
        )
        .arg(
            clap::Arg::with_name("postgres-url")
                .long("postgres")
                .env("POSTGRES_URL")
                .help("Postgres url [example: postgres://user:pass@localhost:5432/database]")
                .takes_value(true)
                .required(true),
        )
}
