use crate::model;

pub fn init() -> Result<postgres::Client, postgres::Error> {
    let url = "postgres://whatcom:whatcom@localhost:52996/whatcom";
    println!("Connecting to postgres: {}", url);

    postgres::Client::connect(url, postgres::NoTls)
}

const UPSERT_QUERY: &str = "
    INSERT INTO people (email) VALUES ($1)
    ON CONFLICT (email) DO UPDATE SET email = EXCLUDED.email
";

const DELETE_QUERY: &str = "
    DELETE FROM people WHERE email = $1
";

pub fn handle_event(
    client: &mut postgres::Client,
    event: model::Event,
) -> anyhow::Result<u64, postgres::Error> {
    println!("Got message {:?}", event);

    match event {
        model::Event::Create(person) | model::Event::Update(_, person) => upsert(client, person),
        model::Event::Delete(person) => delete(client, person),
        model::Event::Unknown => Ok(0),
    }
}

fn upsert(
    client: &mut postgres::Client,
    person: model::PersonRecord,
) -> anyhow::Result<u64, postgres::Error> {
    run_query(client, UPSERT_QUERY, person)
}

fn delete(
    client: &mut postgres::Client,
    person: model::PersonRecord,
) -> anyhow::Result<u64, postgres::Error> {
    run_query(client, DELETE_QUERY, person)
}

fn run_query(
    client: &mut postgres::Client,
    query: &str,
    person: model::PersonRecord,
) -> anyhow::Result<u64, postgres::Error> {
    if let model::PersonRecord { email: None, .. } = person {
        return Ok(0);
    }

    let email = person.email;

    client.execute(query, &[&email])
}
