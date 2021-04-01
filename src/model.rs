use serde::Deserialize;

#[derive(Debug)]
pub enum Event {
    Create(PersonRecord),
    Update(PersonRecord, PersonRecord),
    Delete(PersonRecord),
    Unknown,
}

#[derive(Deserialize, Debug)]
pub struct PersonRecord {
    id: u32,
    pub email: Option<String>,
    name: Option<String>,
}
