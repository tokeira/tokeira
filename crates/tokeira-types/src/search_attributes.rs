use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

/// Search attribute value types intentionally mirror the kinds we expect to be
/// indexable in a SQL-native visibility store.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum SearchAttrValue {
    Keyword(String),
    KeywordList(Vec<String>),
    Int(i64),
    Double(f64),
    Bool(bool),
    Datetime(OffsetDateTime),
    Text(String),
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct SearchAttributes(pub BTreeMap<String, SearchAttrValue>);
