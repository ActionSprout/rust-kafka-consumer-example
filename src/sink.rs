use crate::model;

pub struct Sink {
    upsert_statement: postgres::Statement,
    delete_statement: postgres::Statement,
}

const UPSERT_QUERY: &str = "
    INSERT INTO people (email) VALUES ($1)
    ON CONFLICT (email) DO UPDATE SET email = EXCLUDED.email
";

const DELETE_QUERY: &str = "
    DELETE FROM people WHERE email = $1
";

pub fn init() -> Result<(postgres::Client, Sink), postgres::Error> {
    let url = "postgres://whatcom:whatcom@localhost:52996/whatcom";
    println!("Connecting to postgres: {}", url);

    let mut client = postgres::Client::connect(url, postgres::NoTls)?;

    let sink = Sink {
        upsert_statement: client.prepare(UPSERT_QUERY)?,
        delete_statement: client.prepare(DELETE_QUERY)?,
    };

    Ok((client, sink))
}

impl Sink {
    pub fn handle_event(
        &self,
        client: &mut postgres::Client,
        event: model::Event,
    ) -> anyhow::Result<u64, postgres::Error> {
        println!("Got message {:?}", event);

        match event {
            model::Event::Create(person) | model::Event::Update(_, person) => {
                self.upsert(client, person)
            }
            model::Event::Delete(person) => self.delete(client, person),
            model::Event::Unknown => Ok(0),
        }
    }

    fn upsert(
        &self,
        client: &mut postgres::Client,
        person: model::PersonRecord,
    ) -> anyhow::Result<u64, postgres::Error> {
        self.run_query(client, &self.upsert_statement, person)
    }

    fn delete(
        &self,
        client: &mut postgres::Client,
        person: model::PersonRecord,
    ) -> anyhow::Result<u64, postgres::Error> {
        self.run_query(client, &self.delete_statement, person)
    }

    fn run_query(
        &self,
        client: &mut postgres::Client,
        statement: &postgres::Statement,
        person: model::PersonRecord,
    ) -> anyhow::Result<u64, postgres::Error> {
        if let model::PersonRecord { email: None, .. } = person {
            return Ok(0);
        }

        let email = person.email;

        client.execute(statement, &[&email])
    }
}
