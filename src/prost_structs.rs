use crate::proto::{
    opentelemetry::proto::logs::v1::*,
    opentelemetry::proto::common::v1::*,
};
use crate::proto::opentelemetry::proto::common::v1::any_value;
use crate::{LogsView, ResourceLogsView, ScopeLogsView, LogRecordView, AttributeView, AnyValueView, ValueType};

// Implementations for the generated protobuf types
impl<'a> LogsView<'a> for LogsData {
    type ResourceLogs = ResourceLogs;
    type ResourcesIter = std::slice::Iter<'a, ResourceLogs>;
    
    fn resources(&'a self) -> Self::ResourcesIter {
        self.resource_logs.iter()
    }
}

impl<'a> ResourceLogsView<'a> for ResourceLogs {
    type ScopeLogs = ScopeLogs;
    type ScopesIter = std::slice::Iter<'a, ScopeLogs>;
    
    fn resource(&self) -> &str {
        // Extract resource name from the resource field
        self.resource
            .as_ref()
            .and_then(|r| r.attributes.iter().find(|attr| attr.key == "service.name"))
            .and_then(|attr| attr.value.as_ref())
            .and_then(|v| v.value.as_ref())
            .map(|v| match v {
                any_value::Value::StringValue(s) => s.as_str(),
                _ => "unknown",
            })
            .unwrap_or("unknown")
    }
    
    fn scopes(&'a self) -> Self::ScopesIter {
        self.scope_logs.iter()
    }
}

impl<'a> ScopeLogsView<'a> for ScopeLogs {
    type LogRecord = LogRecord;
    type LogRecordsIter = std::slice::Iter<'a, LogRecord>;
    
    fn scope(&self) -> &str {
        self.scope.as_ref()
            .map(|s| s.name.as_str())
            .unwrap_or("unknown")
    }
    
    fn version(&self) -> Option<&str> {
        self.scope.as_ref()
            .and_then(|s| Some(s.version.as_ref()))
    }
    
    fn log_records(&'a self) -> Self::LogRecordsIter {
        self.log_records.iter()
    }
}

impl<'a> LogRecordView<'a> for LogRecord {
    type Attribute = KeyValue;
    type AttributesIter = std::slice::Iter<'a, KeyValue>;
    
    fn name(&self) -> &str {
        // LogRecord doesn't have a "name" field in OTLP, 
        // you might want to extract from body or attributes
        "log_record" // or extract from body/attributes
    }
    
    fn timestamp(&self) -> Option<u64> {
        Some(self.time_unix_nano)
    }
    
    fn attributes(&'a self) -> Self::AttributesIter {
        self.attributes.iter()
    }
}

impl AttributeView for KeyValue {
    type AnyValue = AnyValue;
    
    fn key(&self) -> &str {
        &self.key
    }
    
    fn value(&self) -> Option<&Self::AnyValue> {
        self.value.as_ref()
    }
}

impl AnyValueView for AnyValue {
    type KeyValue = KeyValue;
    
    fn value_type(&self) -> ValueType {
        match &self.value {
            Some(any_value::Value::StringValue(_)) => ValueType::String,
            Some(any_value::Value::BoolValue(_)) => ValueType::Bool,
            Some(any_value::Value::IntValue(_)) => ValueType::Int64,
            Some(any_value::Value::DoubleValue(_)) => ValueType::Double,
            Some(any_value::Value::BytesValue(_)) => ValueType::Bytes,
            Some(any_value::Value::ArrayValue(_)) => ValueType::Array,
            Some(any_value::Value::KvlistValue(_)) => ValueType::KeyValueList,
            None => ValueType::String, // default
        }
    }
    
    fn as_string(&self) -> Option<&str> {
        match &self.value {
            Some(any_value::Value::StringValue(s)) => Some(s),
            _ => None,
        }
    }
    
    fn as_bool(&self) -> Option<bool> {
        match &self.value {
            Some(any_value::Value::BoolValue(b)) => Some(*b),
            _ => None,
        }
    }
    
    fn as_int64(&self) -> Option<i64> {
        match &self.value {
            Some(any_value::Value::IntValue(i)) => Some(*i),
            _ => None,
        }
    }
    
    fn as_double(&self) -> Option<f64> {
        match &self.value {
            Some(any_value::Value::DoubleValue(d)) => Some(*d),
            _ => None,
        }
    }
    
    fn as_bytes(&self) -> Option<&[u8]> {
        match &self.value {
            Some(any_value::Value::BytesValue(b)) => Some(b),
            _ => None,
        }
    }
    
    fn as_array(&self) -> Option<&[Self]> where Self: Sized {
        match &self.value {
            Some(any_value::Value::ArrayValue(arr)) => Some(&arr.values),
            _ => None,
        }
    }
    
    fn as_kvlist(&self) -> Option<&[Self::KeyValue]> {
        match &self.value {
            Some(any_value::Value::KvlistValue(kvlist)) => Some(&kvlist.values),
            _ => None,
        }
    }
}