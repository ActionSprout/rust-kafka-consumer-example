use serde::Deserialize;

#[derive(Debug, Clone)]
pub enum Event {
    Create(PersonRecord),
    Update(PersonRecord, PersonRecord),
    Delete(PersonRecord),
    Unknown,
}

#[derive(Deserialize, Debug, Clone)]
pub struct PersonRecord {
    id: u32,
    pub email: Option<String>,
    name: Option<String>,
}
