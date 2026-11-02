//! Cross-language contracts generated from the repository JSON Schemas.

use serde::{Deserialize, Deserializer};
use serde_json::{Map, Number, Value};
use sha2::{Digest, Sha256};

pub mod generated;

pub fn deserialize_required_nullable<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

pub fn canonical_json(value: &Value) -> String {
    serde_json::to_string(&normalize_value(value)).expect("JSON Value is serializable")
}

pub fn sha256_hex(value: &str) -> String {
    format!("{:x}", Sha256::digest(value.as_bytes()))
}

pub fn content_addressed_id(prefix: &str, value: &Value, id_field: &str) -> String {
    let mut payload = value.clone();
    payload
        .as_object_mut()
        .expect("contract payload must be an object")
        .remove(id_field);
    format!("{prefix}{}", &sha256_hex(&canonical_json(&payload))[..32])
}

fn normalize_value(value: &Value) -> Value {
    match value {
        Value::Array(items) => Value::Array(items.iter().map(normalize_value).collect()),
        Value::Object(entries) => {
            let mut keys = entries.keys().collect::<Vec<_>>();
            keys.sort();
            let mut result = Map::new();
            for key in keys {
                result.insert(key.clone(), normalize_value(&entries[key]));
            }
            Value::Object(result)
        }
        Value::Number(number) => normalize_number(number),
        _ => value.clone(),
    }
}

fn normalize_number(number: &Number) -> Value {
    if number.is_f64() {
        let float = number.as_f64().expect("f64 number");
        if float.fract() == 0.0 && float >= i64::MIN as f64 && float <= i64::MAX as f64 {
            return Value::Number(Number::from(float as i64));
        }
    }
    Value::Number(number.clone())
}
