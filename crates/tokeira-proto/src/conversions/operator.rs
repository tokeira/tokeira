//! Operator-service-specific helpers.
//!
//! The most important operator-facing conversion today is the mapping between Tokeira's typed
//! search-attribute registry and the protobuf enum used by the public compatibility surface.

use crate::public::enums;
use tokeira_types::SearchAttrType;

pub fn indexed_value_type_to_proto(value: SearchAttrType) -> i32 {
    use enums::IndexedValueType as Proto;
    match value {
        SearchAttrType::Keyword => Proto::Keyword as i32,
        SearchAttrType::KeywordList => Proto::KeywordList as i32,
        SearchAttrType::Int => Proto::Int as i32,
        SearchAttrType::Bool => Proto::Bool as i32,
        SearchAttrType::Double => Proto::Double as i32,
        SearchAttrType::Datetime => Proto::Datetime as i32,
        SearchAttrType::Text => Proto::Text as i32,
    }
}

pub fn indexed_value_type_from_proto(value: i32) -> Option<SearchAttrType> {
    use enums::IndexedValueType as Proto;
    match Proto::from_i32(value)? {
        Proto::Unspecified => None,
        Proto::Keyword => Some(SearchAttrType::Keyword),
        Proto::KeywordList => Some(SearchAttrType::KeywordList),
        Proto::Int => Some(SearchAttrType::Int),
        Proto::Bool => Some(SearchAttrType::Bool),
        Proto::Double => Some(SearchAttrType::Double),
        Proto::Datetime => Some(SearchAttrType::Datetime),
        Proto::Text => Some(SearchAttrType::Text),
    }
}
