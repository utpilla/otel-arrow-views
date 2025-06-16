use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rust_logs_sample::*;
use rust_logs_sample::otlp_bytes;
use rust_logs_sample::otlp_bytes_lazy;
use rust_logs_sample::proto::opentelemetry::proto::logs::v1::*;
use prost::Message;

// Helper function to traverse logs without printing
fn traverse_logs<'a, L: LogsView<'a>>(logs: &'a L) -> (usize, usize, usize, usize) {
    let mut resource_count = 0;
    let mut scope_count = 0;
    let mut record_count = 0;
    let mut attribute_count = 0;
    
    for resource in logs.resources() {
        resource_count += 1;
        black_box(resource.resource()); // Prevent optimization
        
        for scope in resource.scopes() {
            scope_count += 1;
            black_box(scope.scope());
            black_box(scope.version());
            
            for record in scope.log_records() {
                record_count += 1;
                black_box(record.name());
                black_box(record.timestamp());
                
                for attr in record.attributes() {
                    attribute_count += 1;
                    black_box(attr.key());
                    
                    if let Some(value) = attr.value() {
                        black_box(value.value_type());
                        
                        // Touch all possible value types to ensure fair comparison
                        // black_box(value.as_string());
                        // black_box(value.as_bool());
                        // black_box(value.as_int64());
                        // black_box(value.as_double());
                        // black_box(value.as_bytes());
                        // black_box(value.as_array());
                        // black_box(value.as_kvlist());
                    }
                }
            }
        }
    }
    
    (resource_count, scope_count, record_count, attribute_count)
}

fn traverse_otlp_bytes_lazy_logs(logs: &otlp_bytes_lazy::LogsDataParser) -> (usize, usize, usize, usize) {
    let mut resource_count = 0;
    let mut scope_count = 0;
    let mut record_count = 0;
    let mut attribute_count = 0;
    
    for resource in logs.resource_logs() {
        resource_count += 1;
        black_box(resource.resource()); // Prevent optimization
        
        for scope in resource.scope_logs() {
            scope_count += 1;
            black_box(scope.scope_name());
            black_box(scope.scope_version());
            
            for record in scope.log_records() {
                record_count += 1;
                black_box(record.event_name());
                black_box(record.observed_time_unix_nano());
                
                for attr in record.attributes() {
                    attribute_count += 1;
                    black_box(attr.key());
                    
                    if let Some(value) = attr.value() {
                        // Access value based on available methods
                        black_box(value);
                    }
                }
            }
        }
    }
    
    (resource_count, scope_count, record_count, attribute_count)
}

fn bench_parsing_only_comparison(c: &mut Criterion) {
    let logs = create_test_logs();
    let encoded = encode_logs_data(&logs);
    
    let mut group = c.benchmark_group("parsing_only_comparison");
    
    group.bench_function("prost_decode", |b| {
        b.iter(|| {
            LogsData::decode(black_box(&encoded[..])).expect("Failed to decode");
        })
    });
    
    group.bench_function("otlp_bytes_parse", |b| {
        b.iter(|| {
            let mut bytes_logs = otlp_bytes::LogsData::new();
            bytes_logs.parse(black_box(&encoded));
        })
    });

    group.bench_function("otlp_bytes_lazy_parse", |b| {
        b.iter(|| {
            otlp_bytes_lazy::LogsDataParser::new(black_box(&encoded));
        })
    });
    
    group.finish();
}

fn bench_traversal_only_comparison(c: &mut Criterion) {
    let logs = create_test_logs();
    let encoded = encode_logs_data(&logs);
    let mut bytes_logs = otlp_bytes::LogsData::new();
    bytes_logs.parse(&encoded);

    let bytes_logs_lazy = otlp_bytes_lazy::LogsDataParser::new(&encoded);
    
    let mut group = c.benchmark_group("traversal_only_comparison");
    
    group.bench_function("prost_structs", |b| {
        b.iter(|| {
            traverse_logs(black_box(&logs))
        })
    });
    
    group.bench_function("otlp_bytes", |b| {
        b.iter(|| {
            traverse_logs(black_box(&bytes_logs))
        })
    });

    group.bench_function("otlp_bytes_lazy", |b| {
        b.iter(|| {
            traverse_otlp_bytes_lazy_logs(black_box(&bytes_logs_lazy))
        })
    });
    
    group.finish();
}

fn bench_parse_and_traversal_comparison(c: &mut Criterion) {
    let logs = create_test_logs();
    let encoded = encode_logs_data(&logs);
    
    let mut group = c.benchmark_group("parse_and_traversal_comparison");
    
    group.bench_function("prost_parse_and_traversal", |b| {
        b.iter(|| {
            // Decode bytes back to prost structs
            let decoded_logs = LogsData::decode(black_box(&encoded[..])).expect("Failed to decode");
            traverse_logs(black_box(&decoded_logs))
        })
    });
    
    let mut bytes_logs = otlp_bytes::LogsData::new();
    group.bench_function("otlp_bytes_parse_and_traversal", |b| {
        b.iter(|| {
            bytes_logs.parse(black_box(&encoded));
            traverse_logs(black_box(&bytes_logs))
        })
    });

    group.bench_function("otlp_bytes_lazy_parse_and_traversal", |b| {
        b.iter(|| {
            let bytes_logs = otlp_bytes_lazy::LogsDataParser::new(black_box(&encoded));
            traverse_otlp_bytes_lazy_logs(black_box(&bytes_logs))
        })
    });
    
    group.finish();
}
// Create a larger dataset for more meaningful benchmarks
pub fn create_large_test_logs() -> LogsData {
    let mut logs: LogsData = create_test_logs();
    
    // Duplicate the resource logs multiple times to create a larger dataset
    let original_resources = logs.resource_logs.clone();
    for _ in 0..100 { // Increased multiplier for more significant difference
        logs.resource_logs.extend(original_resources.clone());
    }
    
    logs
}

criterion_group!(
    benches,
    bench_parsing_only_comparison,
    bench_traversal_only_comparison,
    bench_parse_and_traversal_comparison
);
criterion_main!(benches);