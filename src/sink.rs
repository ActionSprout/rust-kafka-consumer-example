pub fn init() -> Result<postgres::Client, postgres::Error> {
    let url = "postgres://noodlejam:noodlejam@localhost:5435/fern";
    println!("Connecting to postgres: {}", url);

    postgres::Client::connect(url, postgres::NoTls)
}

pub fn handle_event(_conn: &postgres::Client, event: crate::model::Event) -> anyhow::Result<()> {
    println!("Got message {:?}", event);

    Ok(())
}
