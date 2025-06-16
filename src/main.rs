pub mod proto {
    pub mod opentelemetry {
        pub mod proto {
            pub mod common {
                pub mod v1 {
                    include!("proto/opentelemetry.proto.common.v1.rs");
                }
            }
            pub mod logs {
                pub mod v1 {
                    include!("proto/opentelemetry.proto.logs.v1.rs");
                }
            }
            pub mod resource {
                pub mod v1 {
                    include!("proto/opentelemetry.proto.resource.v1.rs");
                }
            }
        }
    }
}

mod prost_structs;
// mod bytes_view;
pub mod otlp_bytes;
pub mod otlp_bytes_lazy;

use crate::proto::opentelemetry::proto::{common::v1::*, logs::v1::*, resource::v1::*};
// use crate::bytes_view::LogsDataBytes;
use prost::Message;

// View traits for each hierarchy level (zero-cost iterator-based)
pub trait LogsView<'a> {
    type ResourceLogs: ResourceLogsView<'a>;
    // Iterator yielding borrowed references that must live as long as the input lifetime 'a
    type ResourcesIter: Iterator<Item = &'a Self::ResourceLogs> where Self::ResourceLogs: 'a;
    
    fn resources(&'a self) -> Self::ResourcesIter;
}

pub trait ResourceLogsView<'a> {
    type ScopeLogs: ScopeLogsView<'a>;
    type ScopesIter: Iterator<Item = &'a Self::ScopeLogs> where Self::ScopeLogs: 'a;
    
    fn resource(&self) -> &str;
    fn scopes(&'a self) -> Self::ScopesIter;
}

pub trait ScopeLogsView<'a> {
    type LogRecord: LogRecordView<'a>;
    type LogRecordsIter: Iterator<Item = &'a Self::LogRecord> where Self::LogRecord: 'a;
    
    fn scope(&self) -> &str;
    fn version(&self) -> Option<&str>;
    fn log_records(&'a self) -> Self::LogRecordsIter;
}

pub trait LogRecordView<'a> {
    type Attribute: AttributeView;
    type AttributesIter: Iterator<Item = &'a Self::Attribute> where Self::Attribute: 'a;
    
    fn name(&self) -> &str;
    fn timestamp(&self) -> Option<u64>;
    fn attributes(&'a self) -> Self::AttributesIter;
}

pub trait AttributeView {
    type AnyValue: AnyValueView;    
    fn key(&self) -> &str;
    fn value(&self) -> Option<&Self::AnyValue>;
}

pub trait AnyValueView {
    type KeyValue: AttributeView;
    
    fn value_type(&self) -> ValueType;
    fn as_string(&self) -> Option<&str>;
    fn as_bool(&self) -> Option<bool>;
    fn as_int64(&self) -> Option<i64>;
    fn as_double(&self) -> Option<f64>;
    fn as_bytes(&self) -> Option<&[u8]>;
    fn as_array(&self) -> Option<&[Self]> where Self: Sized;
    fn as_kvlist(&self) -> Option<&[Self::KeyValue]>;
}

#[derive(Debug, Clone, PartialEq)]
pub enum ValueType {
    String,
    Bool,
    Int64,
    Double,
    Bytes,
    Array,
    KeyValueList,
}

// Function to inspect logs data using the traits
pub fn inspect_logs<'a, L: LogsView<'a>>(logs: &'a L) {
    println!("🔍 Inspecting Logs Data");
    println!("========================");
    
    for (resource_idx, resource) in logs.resources().enumerate() {
        println!("📦 Resource {}: {}", resource_idx + 1, resource.resource());
        
        for (scope_idx, scope) in resource.scopes().enumerate() {
            println!("  🔧 Scope {}: {} (version: {:?})", 
                     scope_idx + 1, scope.scope(), scope.version());
            
            for (record_idx, record) in scope.log_records().enumerate() {
                println!("    📝 Log Record {}: {}", record_idx + 1, record.name());
                
                if let Some(ts) = record.timestamp() {
                    println!("       ⏰ Timestamp: {}", ts);
                }
                
                for attr in record.attributes() {
                    print!("       🏷️  {}: ", attr.key());
                    
                    let value = attr.value();
                    if let Some(value) = value {
                        match value.value_type() {
                            ValueType::String => println!("\"{}\"", value.as_string().unwrap_or("N/A")),
                            ValueType::Bool => println!("{}", value.as_bool().unwrap_or(false)),
                            ValueType::Int64 => println!("{}", value.as_int64().unwrap_or(0)),
                            ValueType::Double => println!("{}", value.as_double().unwrap_or(0.0)),
                            ValueType::Bytes => println!("bytes[{}]", value.as_bytes().map(|b| b.len()).unwrap_or(0)),
                            ValueType::Array => println!("array[{}]", value.as_array().map(|a| a.len()).unwrap_or(0)),
                            ValueType::KeyValueList => println!("kvlist[{}]", value.as_kvlist().map(|kv| kv.len()).unwrap_or(0)),
                        }
                    }
                }
                println!();
            }
        }
    }
}

pub fn create_test_logs() -> LogsData {
    LogsData {
        resource_logs: vec![
            // First Resource - Web Server
            ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![
                        KeyValue {
                            key: "service.name".to_string(),
                            value: Some(AnyValue {
                                value: Some(any_value::Value::StringValue("web-server".to_string())),
                            }),
                        },
                        KeyValue {
                            key: "service.version".to_string(),
                            value: Some(AnyValue {
                                value: Some(any_value::Value::StringValue("1.2.3".to_string())),
                            }),
                        },
                        KeyValue {
                            key: "deployment.environment".to_string(),
                            value: Some(AnyValue {
                                value: Some(any_value::Value::StringValue("production".to_string())),
                            }),
                        },
                    ],
                    dropped_attributes_count: 0,
                    entity_refs: vec![],
                }),
                scope_logs: vec![
                    // HTTP Handler Scope
                    ScopeLogs {
                        scope: Some(InstrumentationScope {
                            name: "http-handler".to_string(),
                            version: "1.0.0".to_string(),
                            attributes: vec![],
                            dropped_attributes_count: 0,
                        }),
                        log_records: vec![
                            LogRecord {
                                time_unix_nano: 1718380800000000000,
                                observed_time_unix_nano: 1718380800000000000,
                                severity_number: 9,
                                severity_text: "INFO".to_string(),
                                body: Some(AnyValue {
                                    value: Some(any_value::Value::StringValue("request_received".to_string())),
                                }),
                                attributes: vec![
                                    KeyValue {
                                        key: "method".to_string(),
                                        value: Some(AnyValue {
                                            value: Some(any_value::Value::StringValue("GET".to_string())),
                                        }),
                                    },
                                    KeyValue {
                                        key: "status_code".to_string(),
                                        value: Some(AnyValue {
                                            value: Some(any_value::Value::IntValue(200)),
                                        }),
                                    },
                                    KeyValue {
                                        key: "response_time_ms".to_string(),
                                        value: Some(AnyValue {
                                            value: Some(any_value::Value::DoubleValue(45.7)),
                                        }),
                                    },
                                    KeyValue {
                                        key: "success".to_string(),
                                        value: Some(AnyValue {
                                            value: Some(any_value::Value::BoolValue(true)),
                                        }),
                                    },
                                ],
                                event_name: "HTTP Request".to_string(),
                                dropped_attributes_count: 0,
                                flags: 0,
                                trace_id: vec![],
                                span_id: vec![],
                            },
                            LogRecord {
                                time_unix_nano: 1718380801000000000,
                                observed_time_unix_nano: 1718380801000000000,
                                severity_number: 13,
                                severity_text: "ERROR".to_string(),
                                body: Some(AnyValue {
                                    value: Some(any_value::Value::StringValue("request_failed".to_string())),
                                }),
                                attributes: vec![
                                    KeyValue {
                                        key: "method".to_string(),
                                        value: Some(AnyValue {
                                            value: Some(any_value::Value::StringValue("POST".to_string())),
                                        }),
                                    },
                                    KeyValue {
                                        key: "status_code".to_string(),
                                        value: Some(AnyValue {
                                            value: Some(any_value::Value::IntValue(500)),
                                        }),
                                    },
                                    KeyValue {
                                        key: "error_message".to_string(),
                                        value: Some(AnyValue {
                                            value: Some(any_value::Value::StringValue("Database connection failed".to_string())),
                                        }),
                                    },
                                ],
                                event_name: "HTTP Error".to_string(),
                                dropped_attributes_count: 0,
                                flags: 0,
                                trace_id: vec![],
                                span_id: vec![],
                            },
                        ],
                        schema_url: "".to_string(),
                    },
                    // Database Connection Scope
                    ScopeLogs {
                        scope: Some(InstrumentationScope {
                            name: "database-connector".to_string(),
                            version: "2.1.0".to_string(),
                            attributes: vec![],
                            dropped_attributes_count: 0,
                        }),
                        log_records: vec![
                            LogRecord {
                                time_unix_nano: 1718380802000000000,
                                observed_time_unix_nano: 1718380802000000000,
                                severity_number: 5,
                                severity_text: "DEBUG".to_string(),
                                body: Some(AnyValue {
                                    value: Some(any_value::Value::StringValue("connection_established".to_string())),
                                }),
                                attributes: vec![
                                    KeyValue {
                                        key: "db.name".to_string(),
                                        value: Some(AnyValue {
                                            value: Some(any_value::Value::StringValue("users_db".to_string())),
                                        }),
                                    },
                                    KeyValue {
                                        key: "connection_pool_size".to_string(),
                                        value: Some(AnyValue {
                                            value: Some(any_value::Value::IntValue(10)),
                                        }),
                                    },
                                    KeyValue {
                                        key: "connection_timeout_ms".to_string(),
                                        value: Some(AnyValue {
                                            value: Some(any_value::Value::DoubleValue(5000.0)),
                                        }),
                                    },
                                ],
                                event_name: "DB Connection".to_string(),
                                dropped_attributes_count: 0,
                                flags: 0,
                                trace_id: vec![],
                                span_id: vec![],
                            },
                        ],
                        schema_url: "".to_string(),
                    },
                ],
                schema_url: "".to_string(),
            },
            // Second Resource - Background Service
            ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![
                        KeyValue {
                            key: "service.name".to_string(),
                            value: Some(AnyValue {
                                value: Some(any_value::Value::StringValue("background-worker".to_string())),
                            }),
                        },
                        KeyValue {
                            key: "service.version".to_string(),
                            value: Some(AnyValue {
                                value: Some(any_value::Value::StringValue("0.9.1".to_string())),
                            }),
                        },
                        KeyValue {
                            key: "worker.id".to_string(),
                            value: Some(AnyValue {
                                value: Some(any_value::Value::IntValue(42)),
                            }),
                        },
                    ],
                    dropped_attributes_count: 0,
                    entity_refs: vec![],
                }),
                scope_logs: vec![
                    // Job Processor Scope
                    ScopeLogs {
                        scope: Some(InstrumentationScope {
                            name: "job-processor".to_string(),
                            version: "3.0.0".to_string(),
                            attributes: vec![],
                            dropped_attributes_count: 0,
                        }),
                        log_records: vec![
                            LogRecord {
                                time_unix_nano: 1718380803000000000,
                                observed_time_unix_nano: 1718380803000000000,
                                severity_number: 9,
                                severity_text: "INFO".to_string(),
                                body: Some(AnyValue {
                                    value: Some(any_value::Value::StringValue("job_started".to_string())),
                                }),
                                attributes: vec![
                                    KeyValue {
                                        key: "job.id".to_string(),
                                        value: Some(AnyValue {
                                            value: Some(any_value::Value::StringValue("job-12345".to_string())),
                                        }),
                                    },
                                    KeyValue {
                                        key: "job.type".to_string(),
                                        value: Some(AnyValue {
                                            value: Some(any_value::Value::StringValue("email-batch".to_string())),
                                        }),
                                    },
                                    KeyValue {
                                        key: "batch_size".to_string(),
                                        value: Some(AnyValue {
                                            value: Some(any_value::Value::IntValue(1000)),
                                        }),
                                    },
                                ],
                                event_name: "Job Processing".to_string(),
                                dropped_attributes_count: 0,
                                flags: 0,
                                trace_id: vec![],
                                span_id: vec![],
                            },
                        ],
                        schema_url: "".to_string(),
                    },
                ],
                schema_url: "".to_string(),
            },
        ],
    }
}

// Helper function to encode LogsData to bytes
pub fn encode_logs_data(logs: &LogsData) -> Vec<u8> {
    let mut buf = Vec::new();
    logs.encode(&mut buf).expect("Failed to encode logs data");
    buf
}

fn traverse_otlp_bytes_lazy_logs(logs: &otlp_bytes_lazy::LogsDataParser) {
    println!("🔍 Inspecting Logs Data (Lazy");
    println!("=======================================");
    
    // Get resource logs iterator/count
    let mut resource_count = 0;
    let mut scope_count = 0;
    let mut record_count = 0;
    
    for resource in logs.resource_logs() {
        resource_count += 1;
        
        // Process attributes
        if let Some(resource_attrs) = resource.attributes() {
            for attr in resource_attrs {
                if attr.key() == Some("service.name") {
                    if let Some(value) = attr.value() {
                        println!("📦 Resource {}: {}", resource_count, value.to_display_string());
                    }
                }
            }
        }

        for scope in resource.scope_logs() {
            scope_count += 1;
            println!("  🔧 Scope {}: {} (version: {:?})", 
                     scope_count, scope.scope_name(), scope.scope_version());
            
            for record in scope.log_records() {
                record_count += 1;
                println!("    📝 Log Record {}", record_count);
                
                println!("       ⏰ Timestamp: {}", record.observed_time_unix_nano());

                // Print log record attributes
                for attr in record.attributes() {
                    if let Some(key) = attr.key() {
                        print!("       🏷️  {}: ", key);
                        
                        if let Some(value) = attr.value() {
                            println!("{}", value.to_display_string());
                        } else {
                            println!("N/A");
                        }
                    }
                }
                
                // Add a blank line between log records for better readability
                println!();
            }
        }
    }
    
    println!("Total: {} resources, {} scopes, {} records", resource_count, scope_count, record_count);
}

// Keep the original main function for demonstration
pub fn main() {
    let sample_logs = create_test_logs();
    
    println!("=== Testing Decoded Logs ===");
    let encoded_logs = encode_logs_data(&sample_logs);
    let sample_logs = LogsData::decode(&encoded_logs[..]).expect("Failed to decode logs data");
    inspect_logs(&sample_logs);
    
    println!("\n=== Testing Bytes-based Logs ===");
    let encoded_logs = encode_logs_data(&sample_logs);
    let mut logs_data = otlp_bytes::LogsData::new();

    logs_data.parse(&encoded_logs);

    inspect_logs(&logs_data);

    println!("\n=== Testing Bytes-based Logs (Lazy) ===");
    let logs_data_lazy = otlp_bytes_lazy::LogsDataParser::new(&encoded_logs);
    traverse_otlp_bytes_lazy_logs(&logs_data_lazy);

}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::opentelemetry::proto::common::v1::any_value;

    #[test]
    fn test_logs_view_iteration() {
        let logs = create_test_logs();
        let mut resource_count = 0;
        
        for resource in logs.resources() {
            resource_count += 1;
            assert!(!resource.resource().is_empty());
        }
        
        assert_eq!(resource_count, 2);
    }

    #[test]
    fn test_resource_logs_view() {
        let logs = create_test_logs();
        let resources: Vec<_> = logs.resources().collect();
        
        // Test first resource
        assert_eq!(resources[0].resource(), "web-server");
        let scopes: Vec<_> = resources[0].scopes().collect();
        assert_eq!(scopes.len(), 2);
        
        // Test second resource
        assert_eq!(resources[1].resource(), "background-worker");
        let scopes: Vec<_> = resources[1].scopes().collect();
        assert_eq!(scopes.len(), 1);
    }

    #[test]
    fn test_scope_logs_view() {
        let logs = create_test_logs();
        let resources: Vec<_> = logs.resources().collect();
        let scopes: Vec<_> = resources[0].scopes().collect();
        
        // Test first scope
        assert_eq!(scopes[0].scope(), "http-handler");
        assert_eq!(scopes[0].version(), Some("1.0.0"));
        let records: Vec<_> = scopes[0].log_records().collect();
        assert_eq!(records.len(), 2);
        
        // Test second scope
        assert_eq!(scopes[1].scope(), "database-connector");
        assert_eq!(scopes[1].version(), Some("2.1.0"));
        let records: Vec<_> = scopes[1].log_records().collect();
        assert_eq!(records.len(), 1);
    }

    #[test]
    fn test_log_record_view() {
        let logs = create_test_logs();
        let resources: Vec<_> = logs.resources().collect();
        let scopes: Vec<_> = resources[0].scopes().collect();
        let records: Vec<_> = scopes[0].log_records().collect();
        
        // Test first log record
        assert_eq!(records[0].name(), "log_record");
        assert_eq!(records[0].timestamp(), Some(1718380800000000000));
        
        let attributes: Vec<_> = records[0].attributes().collect();
        assert_eq!(attributes.len(), 4);
        
        // Test second log record
        assert_eq!(records[1].name(), "log_record");
        assert_eq!(records[1].timestamp(), Some(1718380801000000000));
    }

    #[test]
    fn test_attribute_values() {
        let logs = create_test_logs();
        let resources: Vec<_> = logs.resources().collect();
        let scopes: Vec<_> = resources[0].scopes().collect();
        let records: Vec<_> = scopes[0].log_records().collect();
        let attributes: Vec<_> = records[0].attributes().collect();
        
        // Find and test each attribute type
        for attr in &attributes {
            match attr.key() {
                "method" => {
                    if let Some(value) = attr.value() {
                        assert_eq!(value.value_type(), ValueType::String);
                        assert_eq!(value.as_string(), Some("GET"));
                    }
                },
                "status_code" => {
                    if let Some(value) = attr.value() {
                        assert_eq!(value.value_type(), ValueType::Int64);
                        assert_eq!(value.as_int64(), Some(200));
                    }
                },
                "response_time_ms" => {
                    if let Some(value) = attr.value() {
                        assert_eq!(value.value_type(), ValueType::Double);
                        assert_eq!(value.as_double(), Some(45.7));
                    }
                },
                "success" => {
                    if let Some(value) = attr.value() {
                        assert_eq!(value.value_type(), ValueType::Bool);
                        assert_eq!(value.as_bool(), Some(true));
                    }
                },
                _ => {}
            }
        }
    }

    #[test]
    fn test_nested_iteration_complete() {
        let logs = create_test_logs();
        let mut total_records = 0;
        let mut total_attributes = 0;
        
        for resource in logs.resources() {
            for scope in resource.scopes() {
                for record in scope.log_records() {
                    total_records += 1;
                    for _attr in record.attributes() {
                        total_attributes += 1;
                    }
                }
            }
        }
        
        assert_eq!(total_records, 4); // 2 HTTP + 1 DB + 1 background worker record 
        assert_eq!(total_attributes, 13); // 4 + 3 + 3 + 3 (simplified count)
    }

    #[test]
    fn test_value_type_detection() {
        let logs = create_test_logs();
        let resources: Vec<_> = logs.resources().collect();
        let scopes: Vec<_> = resources[0].scopes().collect();
        let records: Vec<_> = scopes[0].log_records().collect();
        
        // Test error record attributes
        let error_attributes: Vec<_> = records[1].attributes().collect();
        let mut found_string = false;
        let mut found_int = false;
        
        for attr in &error_attributes {
            if let Some(value) = attr.value() {
                match value.value_type() {
                    ValueType::String => found_string = true,
                    ValueType::Int64 => found_int = true,
                    _ => {}
                }
            }
        }
        
        assert!(found_string);
        assert!(found_int);
    }

    #[test]
    fn test_background_worker_resource() {
        let logs = create_test_logs();
        let resources: Vec<_> = logs.resources().collect();
        
        assert_eq!(resources[1].resource(), "background-worker");
        let scopes: Vec<_> = resources[1].scopes().collect();
        assert_eq!(scopes[0].scope(), "job-processor");
        assert_eq!(scopes[0].version(), Some("3.0.0"));
        
        let records: Vec<_> = scopes[0].log_records().collect();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].name(), "log_record");
    }

    #[test]
    fn test_empty_logs() {
        let empty_logs = LogsData {
            resource_logs: vec![],
        };
        
        let resources: Vec<_> = empty_logs.resources().collect();
        assert_eq!(resources.len(), 0);
    }

    #[test]
    fn test_resource_with_no_scopes() {
        let logs_no_scopes = LogsData {
            resource_logs: vec![
                ResourceLogs {
                    resource: Some(Resource {
                        attributes: vec![
                            KeyValue {
                                key: "service.name".to_string(),
                                value: Some(AnyValue {
                                    value: Some(any_value::Value::StringValue("empty-service".to_string())),
                                }),
                            },
                        ],
                        dropped_attributes_count: 0,
                        entity_refs: vec![],
                    }),
                    scope_logs: vec![],
                    schema_url: "".to_string(),
                },
            ],
        };
        
        let resources: Vec<_> = logs_no_scopes.resources().collect();
        assert_eq!(resources.len(), 1);
        assert_eq!(resources[0].resource(), "empty-service");
        
        let scopes: Vec<_> = resources[0].scopes().collect();
        assert_eq!(scopes.len(), 0);
    }

    #[test]
    fn test_bytes_logs_view_iteration() {
        let logs = create_test_logs();
        let encoded = encode_logs_data(&logs);
        let mut bytes_logs = otlp_bytes::LogsData::new();
        bytes_logs.parse(&encoded);
        
        let mut resource_count = 0;
        
        for resource in bytes_logs.resources() {
            resource_count += 1;
            assert!(!resource.resource().is_empty());
        }
        
        assert_eq!(resource_count, 2);
    }

    #[test]
    fn test_bytes_resource_logs_view() {
        let logs = create_test_logs();
        let encoded = encode_logs_data(&logs);
        let mut bytes_logs = otlp_bytes::LogsData::new();
        bytes_logs.parse(&encoded);
        let resources: Vec<_> = bytes_logs.resources().collect();
        
        // Test first resource
        assert_eq!(resources[0].resource(), "web-server");
        let scopes: Vec<_> = resources[0].scopes().collect();
        assert_eq!(scopes.len(), 2);
        
        // Test second resource
        assert_eq!(resources[1].resource(), "background-worker");
        let scopes: Vec<_> = resources[1].scopes().collect();
        assert_eq!(scopes.len(), 1);
    }

    #[test]
    fn test_bytes_scope_logs_view() {
        let logs = create_test_logs();
        let encoded = encode_logs_data(&logs);
        let mut bytes_logs = otlp_bytes::LogsData::new();
        bytes_logs.parse(&encoded);
        let resources: Vec<_> = bytes_logs.resources().collect();
        let scopes: Vec<_> = resources[0].scopes().collect();
        
        // Test first scope
        assert_eq!(scopes[0].scope(), "http-handler");
        assert_eq!(scopes[0].version(), Some("1.0.0"));
        let records: Vec<_> = scopes[0].log_records().collect();
        assert_eq!(records.len(), 2);
        
        // Test second scope
        assert_eq!(scopes[1].scope(), "database-connector");
        assert_eq!(scopes[1].version(), Some("2.1.0"));
        let records: Vec<_> = scopes[1].log_records().collect();
        assert_eq!(records.len(), 1);
    }

    #[test]
    fn test_bytes_log_record_view() {
        let logs = create_test_logs();
        let encoded = encode_logs_data(&logs);
        let mut bytes_logs = otlp_bytes::LogsData::new();
        bytes_logs.parse(&encoded);
        let resources: Vec<_> = bytes_logs.resources().collect();
        let scopes: Vec<_> = resources[0].scopes().collect();
        let records: Vec<_> = scopes[0].log_records().collect();
        
        // Test first log record
        assert_eq!(records[0].name(), "log_record");
        assert_eq!(records[0].timestamp(), Some(1718380800000000000));
        
        let attributes: Vec<_> = records[0].attributes().collect();
        assert_eq!(attributes.len(), 4);
        
        // Test second log record
        assert_eq!(records[1].name(), "log_record");
        assert_eq!(records[1].timestamp(), Some(1718380801000000000));
    }

    #[test]
    fn test_bytes_attribute_values() {
        let logs = create_test_logs();
        let encoded = encode_logs_data(&logs);
        let mut bytes_logs = otlp_bytes::LogsData::new();
        bytes_logs.parse(&encoded);
        let resources: Vec<_> = bytes_logs.resources().collect();
        let scopes: Vec<_> = resources[0].scopes().collect();
        let records: Vec<_> = scopes[0].log_records().collect();
        let attributes: Vec<_> = records[0].attributes().collect();
        
        // Find and test each attribute type
        for attr in &attributes {
            match attr.key() {
                "method" => {
                    if let Some(value) = attr.value() {
                        assert_eq!(value.value_type(), otlp_bytes::AnyValueType::String);
                        assert_eq!(value.as_string(), Some("GET"));
                    }
                },
                "status_code" => {
                    if let Some(value) = attr.value() {
                        assert_eq!(value.value_type(), otlp_bytes::AnyValueType::Int);
                        assert_eq!(value.as_int64(), Some(200));
                    }
                },
                "response_time_ms" => {
                    if let Some(value) = attr.value() {
                        assert_eq!(value.value_type(), otlp_bytes::AnyValueType::Double);
                        assert_eq!(value.as_double(), Some(45.7));
                    }
                },
                "success" => {
                    if let Some(value) = attr.value() {
                        assert_eq!(value.value_type(), otlp_bytes::AnyValueType::Bool);
                        assert_eq!(value.as_bool(), Some(true));
                    }
                },
                _ => {}
            }
        }
    }

    #[test]
    fn test_bytes_nested_iteration_complete() {
        let logs = create_test_logs();
        let encoded = encode_logs_data(&logs);
        let mut bytes_logs = otlp_bytes::LogsData::new();
        bytes_logs.parse(&encoded);
        
        let mut total_records = 0;
        let mut total_attributes = 0;
        
        for resource in bytes_logs.resources() {
            for scope in resource.scopes() {
                for record in scope.log_records() {
                    total_records += 1;
                    for _attr in record.attributes() {
                        total_attributes += 1;
                    }
                }
            }
        }
        
        assert_eq!(total_records, 4); // 2 HTTP + 1 DB + 1 background worker record 
        assert_eq!(total_attributes, 13); // 4 + 3 + 3 + 3 (simplified count)
    }
}