use crate::{LogsView, ResourceLogsView, ScopeLogsView, LogRecordView, AttributeView, AnyValueView, ValueType};

/// Base protobuf parser with common functionality
pub struct ProtobufParser<'a> {
    data: &'a [u8],
}

impl<'a> ProtobufParser<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data }
    }

    /// Optimized varint parsing with fast path for single-byte values
    fn parse_varint(&self, mut pos: usize) -> Option<(u64, usize)> {
        if pos >= self.data.len() {
            return None;
        }
        
        let byte = self.data[pos];
        pos += 1;
        
        // Fast path for single-byte varints (values 0-127, most common case)
        if byte & 0x80 == 0 {
            return Some((byte as u64, pos));
        }
        
        // Multi-byte varint - unrolled for common 2-3 byte cases
        let mut result = (byte & 0x7F) as u64;
        
        if pos >= self.data.len() { return None; }
        let byte = self.data[pos];
        pos += 1;
        result |= ((byte & 0x7F) as u64) << 7;
        if byte & 0x80 == 0 { return Some((result, pos)); }
        
        if pos >= self.data.len() { return None; }
        let byte = self.data[pos];
        pos += 1;
        result |= ((byte & 0x7F) as u64) << 14;
        if byte & 0x80 == 0 { return Some((result, pos)); }
        
        if pos >= self.data.len() { return None; }
        let byte = self.data[pos];
        pos += 1;
        result |= ((byte & 0x7F) as u64) << 21;
        if byte & 0x80 == 0 { return Some((result, pos)); }
        
        // Fall back to loop for remaining bytes (4+ bytes, rare)
        let mut shift = 28;
        while pos < self.data.len() && shift < 64 {
            let byte = self.data[pos];
            pos += 1;
            
            result |= ((byte & 0x7F) as u64) << shift;
            
            if byte & 0x80 == 0 {
                return Some((result, pos));
            }
            
            shift += 7;
        }
        
        None
    }

    /// Parse a length-delimited field
    fn parse_length_delimited(&self, mut pos: usize) -> Option<(&'a [u8], usize)> {
        let (length, new_pos) = self.parse_varint(pos)?;
        pos = new_pos;
        
        let end = pos + length as usize;
        if end <= self.data.len() {
            Some((&self.data[pos..end], end))
        } else {
            None
        }
    }

    /// Parse a fixed32 field
    fn parse_fixed32(&self, pos: usize) -> Option<(u32, usize)> {
        if pos + 4 <= self.data.len() {
            let value = u32::from_le_bytes([
                self.data[pos],
                self.data[pos + 1],
                self.data[pos + 2],
                self.data[pos + 3],
            ]);
            Some((value, pos + 4))
        } else {
            None
        }
    }

    /// Parse a fixed64 field
    fn parse_fixed64(&self, pos: usize) -> Option<(u64, usize)> {
        if pos + 8 <= self.data.len() {
            let value = u64::from_le_bytes([
                self.data[pos],
                self.data[pos + 1],
                self.data[pos + 2],
                self.data[pos + 3],
                self.data[pos + 4],
                self.data[pos + 5],
                self.data[pos + 6],
                self.data[pos + 7],
            ]);
            Some((value, pos + 8))
        } else {
            None
        }
    }

    /// Parse all occurrences of a field
    fn parse_all_fields(&self, target_tag: u32) -> Vec<(u8, usize)> {
        let mut results = Vec::new();
        let mut pos = 0;
        
        while pos < self.data.len() {
            if let Some((tag_and_wire, new_pos)) = self.parse_varint(pos) {
                pos = new_pos;
                
                let tag = (tag_and_wire >> 3) as u32;
                let wire_type = (tag_and_wire & 0x7) as u8;
                
                if tag == target_tag {
                    results.push((wire_type, pos));
                }
                
                // Skip field based on wire type
                pos = match wire_type {
                    0 => {
                        if let Some((_, new_pos)) = self.parse_varint(pos) {
                            new_pos
                        } else {
                            break;
                        }
                    },
                    1 => {
                        if pos + 8 <= self.data.len() { pos + 8 } else { break; }
                    },
                    2 => {
                        if let Some((_, new_pos)) = self.parse_length_delimited(pos) {
                            new_pos
                        } else {
                            break;
                        }
                    },
                    5 => {
                        if pos + 4 <= self.data.len() { pos + 4 } else { break; }
                    },
                    _ => break,
                };
            } else {
                break;
            }
        }
        
        results
    }

    /// Find first occurrence of a field by tag number
    fn find_field(&self, target_tag: u32) -> Option<(u8, usize)> {
        self.parse_all_fields(target_tag).into_iter().next()
    }
}

/// Reusable eagerly parsed LogsData
pub struct LogsData<'a> {
    pub resource_logs: Vec<ResourceLogs<'a>>,
    pub used_count: usize,
}

impl<'a> LogsData<'a> {
    pub fn new() -> Self {
        Self {
            resource_logs: Vec::new(),
            used_count: 0,
        }
    }

    pub fn clear(&mut self) {
        // Clear nested structures while preserving their capacity
        // for resource_log in &mut self.resource_logs[..self.used_count] {
        //     resource_log.clear();
        // }
        self.used_count = 0;
    }

    pub fn parse(&mut self, data: &'a [u8]) -> bool {
        self.clear();
        
        let parser = ProtobufParser::new(data);
        let mut pos = 0;
        
        while pos < data.len() {
            if let Some((tag_and_wire, new_pos)) = parser.parse_varint(pos) {
                pos = new_pos;
                
                let tag = (tag_and_wire >> 3) as u32;
                let wire_type = (tag_and_wire & 0x7) as u8;
                
                if tag == 1 && wire_type == 2 {
                    if let Some((bytes, end_pos)) = parser.parse_length_delimited(pos) {
                        // Reuse existing ResourceLogs if available
                        let resource_log = if self.used_count < self.resource_logs.len() {
                            &mut self.resource_logs[self.used_count]
                        } else {
                            self.resource_logs.push(ResourceLogs::new());
                            self.resource_logs.last_mut().unwrap()
                        };
                        
                        if resource_log.parse(bytes) {
                            self.used_count += 1;
                        }
                        pos = end_pos;
                    } else {
                        break;
                    }
                } else {
                    // Skip unknown fields
                    pos = match wire_type {
                        0 => parser.parse_varint(pos).map(|(_, p)| p).unwrap_or(data.len()),
                        1 => pos + 8,
                        2 => parser.parse_length_delimited(pos).map(|(_, p)| p).unwrap_or(data.len()),
                        5 => pos + 4,
                        _ => break,
                    };
                }
            } else {
                break;
            }
        }

        self.used_count > 0
    }
}

/// Reusable eagerly parsed ResourceLogs
pub struct ResourceLogs<'a> {
    pub resource: Option<Resource<'a>>,
    pub scope_logs: Vec<ScopeLogs<'a>>,
    pub scope_logs_used: usize,
    pub schema_url: Option<&'a str>,
}

impl<'a> ResourceLogs<'a> {
    pub fn new() -> Self {
        Self {
            resource: None,
            scope_logs: Vec::new(),
            scope_logs_used: 0,
            schema_url: None,
        }
    }

    pub fn clear(&mut self) {
        self.resource = None;
        // Clear nested structures while preserving capacity
        // for scope_log in &mut self.scope_logs[..self.scope_logs_used] {
        //     scope_log.clear();
        // }
        self.scope_logs_used = 0;
        self.schema_url = None;
    }

    pub fn parse(&mut self, data: &'a [u8]) -> bool {
        self.clear();
        
        let parser = ProtobufParser::new(data);
        
        self.resource = parser.find_field(1).and_then(|(wire_type, pos)| {
            if wire_type == 2 {
                parser.parse_length_delimited(pos)
                    .and_then(|(bytes, _)| {
                        let mut resource = Resource::new();
                        if resource.parse(bytes) {
                            Some(resource)
                        } else {
                            None
                        }
                    })
            } else {
                None
            }
        });

        for (wire_type, pos) in parser.parse_all_fields(2) {
            if wire_type == 2 {
                if let Some((bytes, _)) = parser.parse_length_delimited(pos) {
                    // Reuse existing ScopeLogs if available
                    let scope_log = if self.scope_logs_used < self.scope_logs.len() {
                        &mut self.scope_logs[self.scope_logs_used]
                    } else {
                        self.scope_logs.push(ScopeLogs::new());
                        self.scope_logs.last_mut().unwrap()
                    };
                    
                    if scope_log.parse(bytes) {
                        self.scope_logs_used += 1;
                    }
                }
            }
        }

        self.schema_url = parser.find_field(3).and_then(|(wire_type, pos)| {
            if wire_type == 2 {
                parser.parse_length_delimited(pos)
                    .and_then(|(bytes, _)| std::str::from_utf8(bytes).ok())
            } else {
                None
            }
        });

        true
    }
}

/// Reusable eagerly parsed ScopeLogs
pub struct ScopeLogs<'a> {
    pub scope: Option<InstrumentationScope<'a>>,
    pub log_records: Vec<LogRecord<'a>>,
    pub log_records_used: usize,
    pub schema_url: Option<&'a str>,
}

impl<'a> ScopeLogs<'a> {
    pub fn new() -> Self {
        Self {
            scope: None,
            log_records: Vec::new(),
            log_records_used: 0,
            schema_url: None,
        }
    }

    pub fn clear(&mut self) {
        self.scope = None;
        // Clear nested structures while preserving capacity
        // for log_record in &mut self.log_records[..self.log_records_used] {
        //     log_record.clear();
        // }
        self.log_records_used = 0;
        self.schema_url = None;
    }

    pub fn parse(&mut self, data: &'a [u8]) -> bool {
        self.clear();
        
        let parser = ProtobufParser::new(data);
        
        self.scope = parser.find_field(1).and_then(|(wire_type, pos)| {
            if wire_type == 2 {
                parser.parse_length_delimited(pos)
                    .and_then(|(bytes, _)| {
                        let mut scope = InstrumentationScope::new();
                        if scope.parse(bytes) {
                            Some(scope)
                        } else {
                            None
                        }
                    })
            } else {
                None
            }
        });

        for (wire_type, pos) in parser.parse_all_fields(2) {
            if wire_type == 2 {
                if let Some((bytes, _)) = parser.parse_length_delimited(pos) {
                    // Reuse existing LogRecord if available
                    let log_record = if self.log_records_used < self.log_records.len() {
                        &mut self.log_records[self.log_records_used]
                    } else {
                        self.log_records.push(LogRecord::new());
                        self.log_records.last_mut().unwrap()
                    };
                    
                    if log_record.parse(bytes) {
                        self.log_records_used += 1;
                    }
                }
            }
        }

        self.schema_url = parser.find_field(3).and_then(|(wire_type, pos)| {
            if wire_type == 2 {
                parser.parse_length_delimited(pos)
                    .and_then(|(bytes, _)| std::str::from_utf8(bytes).ok())
            } else {
                None
            }
        });

        true
    }
}

/// Reusable eagerly parsed LogRecord
pub struct LogRecord<'a> {
    pub time_unix_nano: Option<u64>,
    pub observed_time_unix_nano: u64,
    pub severity_number: i32,
    pub severity_text: Option<&'a str>,
    pub body: Option<AnyValue<'a>>,
    pub attributes: Vec<KeyValue<'a>>,
    pub attributes_used: usize,
    pub dropped_attributes_count: Option<u32>,
    pub flags: Option<u32>,
    pub trace_id: Option<&'a [u8]>,
    pub span_id: Option<&'a [u8]>,
    pub event_name: Option<&'a str>,
}

impl<'a> LogRecord<'a> {
    pub fn new() -> Self {
        Self {
            time_unix_nano: None,
            observed_time_unix_nano: 0,
            severity_number: 0,
            severity_text: None,
            body: None,
            attributes: Vec::new(),
            attributes_used: 0,
            dropped_attributes_count: None,
            flags: None,
            trace_id: None,
            span_id: None,
            event_name: None,
        }
    }

    pub fn clear(&mut self) {
        self.time_unix_nano = None;
        self.observed_time_unix_nano = 0;
        self.severity_number = 0;
        self.severity_text = None;
        self.body = None;
        // Clear nested structures while preserving capacity
        // for attr in &mut self.attributes[..self.attributes_used] {
        //     attr.clear();
        // }
        self.attributes_used = 0;
        self.dropped_attributes_count = None;
        self.flags = None;
        self.trace_id = None;
        self.span_id = None;
        self.event_name = None;
    }

    pub fn parse(&mut self, data: &'a [u8]) -> bool {
        self.clear();
        
        let parser = ProtobufParser::new(data);

        self.time_unix_nano = parser.find_field(1).and_then(|(wire_type, pos)| {
            if wire_type == 1 {
                parser.parse_fixed64(pos).map(|(value, _)| value)
            } else {
                None
            }
        });

        // observed_time_unix_nano is required - parse with default 0
        self.observed_time_unix_nano = parser.find_field(11).and_then(|(wire_type, pos)| {
            if wire_type == 1 {
                parser.parse_fixed64(pos).map(|(value, _)| value)
            } else {
                None
            }
        }).unwrap_or(0);

        // severity_number is required enum - parse with default 0 (UNSPECIFIED)
        self.severity_number = parser.find_field(2).and_then(|(wire_type, pos)| {
            if wire_type == 0 {
                parser.parse_varint(pos).map(|(value, _)| value as i32)
            } else {
                None
            }
        }).unwrap_or(0);

        self.severity_text = parser.find_field(3).and_then(|(wire_type, pos)| {
            if wire_type == 2 {
                parser.parse_length_delimited(pos)
                    .and_then(|(bytes, _)| std::str::from_utf8(bytes).ok())
            } else {
                None
            }
        });

        self.body = parser.find_field(5).and_then(|(wire_type, pos)| {
            if wire_type == 2 {
                parser.parse_length_delimited(pos)
                    .and_then(|(bytes, _)| {
                        let mut any_value = AnyValue::new();
                        if any_value.parse(bytes) {
                            Some(any_value)
                        } else {
                            None
                        }
                    })
            } else {
                None
            }
        });

        for (wire_type, pos) in parser.parse_all_fields(6) {
            if wire_type == 2 {
                if let Some((bytes, _)) = parser.parse_length_delimited(pos) {
                    // Reuse existing KeyValue if available
                    let kv = if self.attributes_used < self.attributes.len() {
                        &mut self.attributes[self.attributes_used]
                    } else {
                        self.attributes.push(KeyValue::new());
                        self.attributes.last_mut().unwrap()
                    };
                    
                    if kv.parse(bytes) {
                        self.attributes_used += 1;
                    }
                }
            }
        }

        self.dropped_attributes_count = parser.find_field(7).and_then(|(wire_type, pos)| {
            if wire_type == 0 {
                parser.parse_varint(pos).map(|(value, _)| value as u32)
            } else {
                None
            }
        });

        self.flags = parser.find_field(8).and_then(|(wire_type, pos)| {
            if wire_type == 5 {
                parser.parse_fixed32(pos).map(|(value, _)| value)
            } else {
                None
            }
        });

        self.trace_id = parser.find_field(9).and_then(|(wire_type, pos)| {
            if wire_type == 2 {
                parser.parse_length_delimited(pos).map(|(bytes, _)| bytes)
            } else {
                None
            }
        });

        self.span_id = parser.find_field(10).and_then(|(wire_type, pos)| {
            if wire_type == 2 {
                parser.parse_length_delimited(pos).map(|(bytes, _)| bytes)
            } else {
                None
            }
        });

        self.event_name = parser.find_field(12).and_then(|(wire_type, pos)| {
            if wire_type == 2 {
                parser.parse_length_delimited(pos)
                    .and_then(|(bytes, _)| std::str::from_utf8(bytes).ok())
            } else {
                None
            }
        });

        true
    }

    /// Check if trace_id is valid (16 bytes, not all zeros)
    pub fn is_trace_id_valid(&self) -> bool {
        if let Some(trace_id) = &self.trace_id {
            trace_id.len() == 16 && !trace_id.iter().all(|&b| b == 0)
        } else {
            false
        }
    }

    /// Check if span_id is valid (8 bytes, not all zeros)
    pub fn is_span_id_valid(&self) -> bool {
        if let Some(span_id) = &self.span_id {
            span_id.len() == 8 && !span_id.iter().all(|&b| b == 0)
        } else {
            false
        }
    }

    /// Extract trace flags from the flags field (lower 8 bits)
    pub fn trace_flags(&self) -> Option<u8> {
        self.flags.map(|flags| (flags & 0xFF) as u8)
    }
}

/// Reusable eagerly parsed KeyValue
#[derive(Debug, Clone)]
pub struct KeyValue<'a> {
    pub key: &'a str,
    pub value: Option<AnyValue<'a>>,
}

impl<'a> KeyValue<'a> {
    pub fn new() -> Self {
        Self {
            key: "",
            value: None,
        }
    }

    pub fn clear(&mut self) {
        self.key = "";
        self.value = None;
    }

    pub fn parse(&mut self, data: &'a [u8]) -> bool {
        self.clear();
        
        let parser = ProtobufParser::new(data);

        self.key = parser.find_field(1).and_then(|(wire_type, pos)| {
            if wire_type == 2 {
                parser.parse_length_delimited(pos)
                    .and_then(|(bytes, _)| std::str::from_utf8(bytes).ok())
            } else {
                None
            }
        }).unwrap_or("");

        if self.key.is_empty() {
            return false;
        }

        self.value = parser.find_field(2).and_then(|(wire_type, pos)| {
            if wire_type == 2 {
                parser.parse_length_delimited(pos)
                    .and_then(|(bytes, _)| {
                        let mut any_value = AnyValue::new();
                        if any_value.parse(bytes) {
                            Some(any_value)
                        } else {
                            None
                        }
                    })
            } else {
                None
            }
        });

        true
    }
}

/// Reusable eagerly parsed AnyValue
#[derive(Debug, Clone)]
pub struct AnyValue<'a> {
    pub value: AnyValueData<'a>,
}

#[derive(Debug, Clone)]
pub enum AnyValueData<'a> {
    String(&'a str),
    Bool(bool),
    Int(i64),
    Double(f64),
    Array(Vec<AnyValue<'a>>),
    KvList(Vec<KeyValue<'a>>),
    Bytes(&'a [u8]),
}

impl<'a> AnyValue<'a> {
    pub fn new() -> Self {
        Self {
            value: AnyValueData::String(""),
        }
    }

    pub fn clear(&mut self) {
        self.value = AnyValueData::String("");
    }

    pub fn parse(&mut self, data: &'a [u8]) -> bool {
        self.clear();
        
        let parser = ProtobufParser::new(data);

        // Check each field type in order
        if let Some((wire_type, pos)) = parser.find_field(1) {
            if wire_type == 2 {
                if let Some((bytes, _)) = parser.parse_length_delimited(pos) {
                    if let Ok(s) = std::str::from_utf8(bytes) {
                        self.value = AnyValueData::String(s);
                        return true;
                    }
                }
            }
        }

        if let Some((wire_type, pos)) = parser.find_field(2) {
            if wire_type == 0 {
                if let Some((value, _)) = parser.parse_varint(pos) {
                    self.value = AnyValueData::Bool(value != 0);
                    return true;
                }
            }
        }

        if let Some((wire_type, pos)) = parser.find_field(3) {
            if wire_type == 0 {
                if let Some((value, _)) = parser.parse_varint(pos) {
                    self.value = AnyValueData::Int(value as i64);
                    return true;
                }
            }
        }

        if let Some((wire_type, pos)) = parser.find_field(4) {
            if wire_type == 1 {
                if let Some((value, _)) = parser.parse_fixed64(pos) {
                    self.value = AnyValueData::Double(f64::from_bits(value));
                    return true;
                }
            }
        }

        if parser.find_field(5).is_some() {
            // Array field - need to parse all array values
            let mut array_values = Vec::new();
            for (wire_type, pos) in parser.parse_all_fields(5) {
                if wire_type == 2 {
                    if let Some((bytes, _)) = parser.parse_length_delimited(pos) {
                        let mut any_value = AnyValue::new();
                        if any_value.parse(bytes) {
                            array_values.push(any_value);
                        }
                    }
                }
            }
            self.value = AnyValueData::Array(array_values);
            return true;
        }

        if parser.find_field(6).is_some() {
            // KvList field - need to parse all key-value pairs
            let mut kv_values = Vec::new();
            for (wire_type, pos) in parser.parse_all_fields(6) {
                if wire_type == 2 {
                    if let Some((bytes, _)) = parser.parse_length_delimited(pos) {
                        let mut kv = KeyValue::new();
                        if kv.parse(bytes) {
                            kv_values.push(kv);
                        }
                    }
                }
            }
            self.value = AnyValueData::KvList(kv_values);
            return true;
        }

        if let Some((wire_type, pos)) = parser.find_field(7) {
            if wire_type == 2 {
                if let Some((bytes, _)) = parser.parse_length_delimited(pos) {
                    self.value = AnyValueData::Bytes(bytes);
                    return true;
                }
            }
        }

        false
    }

    pub fn value_type(&self) -> AnyValueType {
        match &self.value {
            AnyValueData::String(_) => AnyValueType::String,
            AnyValueData::Bool(_) => AnyValueType::Bool,
            AnyValueData::Int(_) => AnyValueType::Int,
            AnyValueData::Double(_) => AnyValueType::Double,
            AnyValueData::Array(_) => AnyValueType::Array,
            AnyValueData::KvList(_) => AnyValueType::KvList,
            AnyValueData::Bytes(_) => AnyValueType::Bytes,
        }
    }

    pub fn string_value(&self) -> Option<&str> {
        match &self.value {
            AnyValueData::String(s) => Some(s),
            _ => None,
        }
    }

    pub fn bool_value(&self) -> Option<bool> {
        match &self.value {
            AnyValueData::Bool(b) => Some(*b),
            _ => None,
        }
    }

    pub fn int_value(&self) -> Option<i64> {
        match &self.value {
            AnyValueData::Int(i) => Some(*i),
            _ => None,
        }
    }

    pub fn double_value(&self) -> Option<f64> {
        match &self.value {
            AnyValueData::Double(d) => Some(*d),
            _ => None,
        }
    }

    pub fn array_value(&self) -> Option<&[AnyValue<'a>]> {
        match &self.value {
            AnyValueData::Array(arr) => Some(arr),
            _ => None,
        }
    }

    pub fn kvlist_value(&self) -> Option<&[KeyValue<'a>]> {
        match &self.value {
            AnyValueData::KvList(kv) => Some(kv),
            _ => None,
        }
    }

    pub fn bytes_value(&self) -> Option<&[u8]> {
        match &self.value {
            AnyValueData::Bytes(b) => Some(b),
            _ => None,
        }
    }

    pub fn to_display_string(&self) -> String {
        match &self.value {
            AnyValueData::String(s) => format!("\"{}\"", s),
            AnyValueData::Bool(b) => format!("{}", b),
            AnyValueData::Int(i) => format!("{}", i),
            AnyValueData::Double(d) => format!("{}", d),
            AnyValueData::Bytes(b) => format!("bytes[{}]", b.len()),
            AnyValueData::Array(arr) => format!("array[{}]", arr.len()),
            AnyValueData::KvList(kv) => format!("kvlist[{}]", kv.len()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AnyValueType {
    String,
    Bool,
    Int,
    Double,
    Array,
    KvList,
    Bytes,
}

/// Reusable eagerly parsed Resource
pub struct Resource<'a> {
    pub attributes: Vec<KeyValue<'a>>,
    pub attributes_used: usize,
    pub dropped_attributes_count: Option<u32>,
}

impl<'a> Resource<'a> {
    pub fn new() -> Self {
        Self {
            attributes: Vec::new(),
            attributes_used: 0,
            dropped_attributes_count: None,
        }
    }

    pub fn clear(&mut self) {
        // Clear nested structures while preserving capacity
        // for attr in &mut self.attributes[..self.attributes_used] {
        //     attr.clear();
        // }
        self.attributes_used = 0;
        self.dropped_attributes_count = None;
    }

    pub fn parse(&mut self, data: &'a [u8]) -> bool {
        self.clear();
        
        let parser = ProtobufParser::new(data);

        for (wire_type, pos) in parser.parse_all_fields(1) {
            if wire_type == 2 {
                if let Some((bytes, _)) = parser.parse_length_delimited(pos) {
                    // Reuse existing KeyValue if available
                    let kv = if self.attributes_used < self.attributes.len() {
                        &mut self.attributes[self.attributes_used]
                    } else {
                        self.attributes.push(KeyValue::new());
                        self.attributes.last_mut().unwrap()
                    };
                    
                    if kv.parse(bytes) {
                        self.attributes_used += 1;
                    }
                }
            }
        }

        self.dropped_attributes_count = parser.find_field(2).and_then(|(wire_type, pos)| {
            if wire_type == 0 {
                parser.parse_varint(pos).map(|(value, _)| value as u32)
            } else {
                None
            }
        });

        true
    }

    pub fn get_service_name(&self) -> Option<&str> {
        self.attributes[..self.attributes_used]
            .iter()
            .find(|attr| attr.key == "service.name")
            .and_then(|attr| attr.value.as_ref())
            .and_then(|val| val.string_value())
    }
}

/// Reusable eagerly parsed InstrumentationScope
pub struct InstrumentationScope<'a> {
    pub name: Option<&'a str>,
    pub version: Option<&'a str>,
    pub attributes: Vec<KeyValue<'a>>,
    pub attributes_used: usize,
    pub dropped_attributes_count: Option<u32>,
}

impl<'a> InstrumentationScope<'a> {
    pub fn new() -> Self {
        Self {
            name: None,
            version: None,
            attributes: Vec::new(),
            attributes_used: 0,
            dropped_attributes_count: None,
        }
    }

    pub fn clear(&mut self) {
        self.name = None;
        self.version = None;
        // Clear nested structures while preserving capacity
        // for attr in &mut self.attributes[..self.attributes_used] {
        //     attr.clear();
        // }
        self.attributes_used = 0;
        self.dropped_attributes_count = None;
    }

    pub fn parse(&mut self, data: &'a [u8]) -> bool {
        self.clear();
        
        let parser = ProtobufParser::new(data);

        self.name = parser.find_field(1).and_then(|(wire_type, pos)| {
            if wire_type == 2 {
                parser.parse_length_delimited(pos)
                    .and_then(|(bytes, _)| std::str::from_utf8(bytes).ok())
            } else {
                None
            }
        });

        self.version = parser.find_field(2).and_then(|(wire_type, pos)| {
            if wire_type == 2 {
                parser.parse_length_delimited(pos)
                    .and_then(|(bytes, _)| std::str::from_utf8(bytes).ok())
            } else {
                None
            }
        });

        for (wire_type, pos) in parser.parse_all_fields(3) {
            if wire_type == 2 {
                if let Some((bytes, _)) = parser.parse_length_delimited(pos) {
                    // Reuse existing KeyValue if available
                    let kv = if self.attributes_used < self.attributes.len() {
                        &mut self.attributes[self.attributes_used]
                    } else {
                        self.attributes.push(KeyValue::new());
                        self.attributes.last_mut().unwrap()
                    };
                    
                    if kv.parse(bytes) {
                        self.attributes_used += 1;
                    }
                }
            }
        }

        self.dropped_attributes_count = parser.find_field(4).and_then(|(wire_type, pos)| {
            if wire_type == 0 {
                parser.parse_varint(pos).map(|(value, _)| value as u32)
            } else {
                None
            }
        });

        true
    }
}

/// Custom iterator that only iterates over used elements
pub struct UsedSliceIter<'a, T> {
    slice: &'a [T],
    index: usize,
}

impl<'a, T> UsedSliceIter<'a, T> {
    fn new(slice: &'a [T]) -> Self {
        Self { slice, index: 0 }
    }
}

impl<'a, T> Iterator for UsedSliceIter<'a, T> {
    type Item = &'a T;
    
    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.slice.len() {
            let item = &self.slice[self.index];
            self.index += 1;
            Some(item)
        } else {
            None
        }
    }
}

// Update trait implementations
// Implement LogsView for LogsData
impl<'a> LogsView<'a> for LogsData<'a> {
    type ResourceLogs = ResourceLogs<'a>;
    type ResourcesIter = UsedSliceIter<'a, ResourceLogs<'a>>;
    
    fn resources(&'a self) -> Self::ResourcesIter {
        UsedSliceIter::new(&self.resource_logs[..self.used_count])
    }
}

// Implement ResourceLogsView for ResourceLogs
impl<'a> ResourceLogsView<'a> for ResourceLogs<'a> {
    type ScopeLogs = ScopeLogs<'a>;
    type ScopesIter = UsedSliceIter<'a, ScopeLogs<'a>>;
    
    fn resource(&self) -> &str {
        self.resource
            .as_ref()
            .and_then(|r| r.get_service_name())
            .unwrap_or("unknown-service")
    }
    
    fn scopes(&'a self) -> Self::ScopesIter {
        UsedSliceIter::new(&self.scope_logs[..self.scope_logs_used])
    }
}

// Implement ScopeLogsView for ScopeLogs
impl<'a> ScopeLogsView<'a> for ScopeLogs<'a> {
    type LogRecord = LogRecord<'a>;
    type LogRecordsIter = UsedSliceIter<'a, LogRecord<'a>>;
    
    fn scope(&self) -> &str {
        self.scope
            .as_ref()
            .and_then(|s| s.name)
            .unwrap_or("unknown-scope")
    }
    
    fn version(&self) -> Option<&str> {
        self.scope
            .as_ref()
            .and_then(|s| s.version)
    }
    
    fn log_records(&'a self) -> Self::LogRecordsIter {
        UsedSliceIter::new(&self.log_records[..self.log_records_used])
    }
}

// Implement LogRecordView for LogRecord
impl<'a> LogRecordView<'a> for LogRecord<'a> {
    type Attribute = KeyValue<'a>;
    type AttributesIter = std::slice::Iter<'a, KeyValue<'a>>;
    
    fn name(&self) -> &str {
        "log_record" // LogRecord doesn't have a name field in the protobuf, use constant
    }
    
    fn timestamp(&self) -> Option<u64> {
        self.time_unix_nano.or_else(|| {
            if self.observed_time_unix_nano != 0 {
                Some(self.observed_time_unix_nano)
            } else {
                None
            }
        })
    }
    
    fn attributes(&'a self) -> Self::AttributesIter {
        self.attributes[..self.attributes_used].iter()
    }
}

// Implement AttributeView for KeyValue
impl<'a> AttributeView for KeyValue<'a> {
    type AnyValue = AnyValue<'a>;
    
    fn key(&self) -> &str {
        self.key
    }
    
    fn value(&self) -> Option<&Self::AnyValue> {
        self.value.as_ref()
    }
}

// Implement AnyValueView for AnyValue
impl<'a> AnyValueView for AnyValue<'a> {
    type KeyValue = KeyValue<'a>;
    
    fn value_type(&self) -> ValueType {
        match self.value_type() {
            AnyValueType::String => ValueType::String,
            AnyValueType::Bool => ValueType::Bool,
            AnyValueType::Int => ValueType::Int64,
            AnyValueType::Double => ValueType::Double,
            AnyValueType::Bytes => ValueType::Bytes,
            AnyValueType::Array => ValueType::Array,
            AnyValueType::KvList => ValueType::KeyValueList,
        }
    }
    
    fn as_string(&self) -> Option<&str> {
        self.string_value()
    }
    
    fn as_bool(&self) -> Option<bool> {
        self.bool_value()
    }
    
    fn as_int64(&self) -> Option<i64> {
        self.int_value()
    }
    
    fn as_double(&self) -> Option<f64> {
        self.double_value()
    }
    
    fn as_bytes(&self) -> Option<&[u8]> {
        self.bytes_value()
    }
    
    fn as_array(&self) -> Option<&[Self]> where Self: Sized {
        self.array_value()
    }
    
    fn as_kvlist(&self) -> Option<&[Self::KeyValue]> {
        self.kvlist_value()
    }
}