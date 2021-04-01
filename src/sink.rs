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

    let (query, person) = match event {
        model::Event::Create(person) | model::Event::Update(_, person) => (UPSERT_QUERY, person),
        model::Event::Delete(person) => (DELETE_QUERY, person),
        model::Event::Unknown => panic!("Oh no, got an unknown message"),
    };

    if let model::PersonRecord { email: None, .. } = person {
        return Ok(0);
    }

    let email = person.email;

    client.execute(query, &[&email])
}
