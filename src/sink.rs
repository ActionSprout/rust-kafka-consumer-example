use crate::model;

pub struct Sink {
    client: tokio_postgres::Client,
    upsert_statement: tokio_postgres::Statement,
    delete_statement: tokio_postgres::Statement,
}

const UPSERT_QUERY: &str = "
    INSERT INTO people (email) VALUES ($1)
    ON CONFLICT (email) DO UPDATE SET email = EXCLUDED.email
";

const DELETE_QUERY: &str = "
    DELETE FROM people WHERE email = $1
";

pub async fn init(url: &str) -> anyhow::Result<Sink> {
    log::info!("Connecting to postgres: {}", url);

    let mut store = rustls::RootCertStore::empty();
    let cert = include_bytes!("../digitalocean-ca.crt");
    let slice: &[u8] = &cert[..];

    let mut cert_buf = std::io::BufReader::new(slice);
    store.add_pem_file(&mut cert_buf);
    let mut config = rustls::ClientConfig::new();
    config.root_store = store;
    let tls = tokio_postgres_rustls::MakeRustlsConnect::new(config);

    let (client, conn) = tokio_postgres::connect(url, tls).await?;

    tokio::spawn(async move {
        if let Err(e) = conn.await {
            // TODO: Not panic
            panic!("Panic connecting to postgres {}", e);
        }
    });

    let upsert_statement = client.prepare(UPSERT_QUERY).await?;
    let delete_statement = client.prepare(DELETE_QUERY).await?;

    Ok(Sink {
        client,
        upsert_statement,
        delete_statement,
    })
}

impl Sink {
    pub async fn handle_event(
        &mut self,
        event: model::Event,
    ) -> anyhow::Result<u64, tokio_postgres::Error> {
        log::info!("Got message {:?}", event);

        match event {
            model::Event::Create(person) | model::Event::Update(_, person) => {
                self.upsert(person).await
            }
            model::Event::Delete(person) => self.delete(person).await,
            model::Event::Unknown => Ok(0),
        }
    }

    async fn upsert(
        &mut self,
        person: model::PersonRecord,
    ) -> anyhow::Result<u64, tokio_postgres::Error> {
        self.run_query(self.upsert_statement.clone(), person).await
    }

    async fn delete(
        &mut self,
        person: model::PersonRecord,
    ) -> anyhow::Result<u64, tokio_postgres::Error> {
        self.run_query(self.delete_statement.clone(), person).await
    }

    async fn run_query(
        &mut self,
        statement: tokio_postgres::Statement,
        person: model::PersonRecord,
    ) -> anyhow::Result<u64, tokio_postgres::Error> {
        if let model::PersonRecord { email: None, .. } = person {
            return Ok(0);
        }

        let email = person.email;

        self.client.execute(&statement, &[&email]).await
    }
}
