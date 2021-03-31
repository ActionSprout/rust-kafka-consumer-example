pub fn handle_event(event: crate::model::Event) -> anyhow::Result<()> {
    println!("Got message {:?}", event);

    Ok(())
}
