use crate::model;

pub struct Sink {
    client: postgres::Client,
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

pub fn init(url: &str) -> Result<Sink, postgres::Error> {
    log::info!("Connecting to postgres: {}", url);

    let mut client = postgres::Client::connect(url, postgres::NoTls)?;
    let upsert_statement = client.prepare(UPSERT_QUERY)?;
    let delete_statement = client.prepare(DELETE_QUERY)?;

    Ok(Sink {
        client,
        upsert_statement,
        delete_statement,
    })
}

impl Sink {
    pub fn handle_event(&mut self, event: model::Event) -> anyhow::Result<u64, postgres::Error> {
        log::info!("Got message {:?}", event);

        match event {
            model::Event::Create(person) | model::Event::Update(_, person) => self.upsert(person),
            model::Event::Delete(person) => self.delete(person),
            model::Event::Unknown => Ok(0),
        }
    }

    fn upsert(&mut self, person: model::PersonRecord) -> anyhow::Result<u64, postgres::Error> {
        self.run_query(self.upsert_statement.clone(), person)
    }

    fn delete(&mut self, person: model::PersonRecord) -> anyhow::Result<u64, postgres::Error> {
        self.run_query(self.delete_statement.clone(), person)
    }

    fn run_query(
        &mut self,
        statement: postgres::Statement,
        person: model::PersonRecord,
    ) -> anyhow::Result<u64, postgres::Error> {
        if let model::PersonRecord { email: None, .. } = person {
            return Ok(0);
        }

        let email = person.email;

        self.client.execute(&statement, &[&email])
    }
}
