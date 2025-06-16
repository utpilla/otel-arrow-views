// use crate::{LogsView, ResourceLogsView, ScopeLogsView, LogRecordView, AttributeView, AnyValueView, ValueType};

/// Base protobuf parser with common functionality
pub struct ProtobufParser<'a> {
    data: &'a [u8],
}

impl<'a> ProtobufParser<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data }
    }

    /// Parse a varint from the current position
    #[inline]
    fn parse_varint(&self, mut pos: usize) -> Option<(u64, usize)> {
        let mut result;
        let mut shift;
        
        // Bounds check once at the start
        if pos >= self.data.len() {
            return None;
        }
        
        // Unroll first few iterations for common cases (most varints are 1-2 bytes)
        let byte = self.data[pos];
        pos += 1;
        result = (byte & 0x7F) as u64;
        
        if byte & 0x80 == 0 {
            return Some((result, pos));
        }
        
        // Second byte (handles most remaining cases)
        if pos < self.data.len() {
            let byte = self.data[pos];
            pos += 1;
            result |= ((byte & 0x7F) as u64) << 7;
            
            if byte & 0x80 == 0 {
                return Some((result, pos));
            }
        } else {
            return None;
        }
        
        // Handle remaining bytes with loop
        shift = 14;
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
    #[inline]
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
    #[inline]
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
    #[inline]
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

    /// Find a field by tag number, returns (wire_type, position_after_tag)
    #[inline]
    fn find_field(&self, target_tag: u32) -> Option<(u8, usize)> {
        let mut pos = 0;
        
        while pos < self.data.len() {
            let (tag_and_wire, new_pos) = self.parse_varint(pos)?;
            pos = new_pos;
            
            let tag = (tag_and_wire >> 3) as u32;
            let wire_type = (tag_and_wire & 0x7) as u8;
            
            if tag == target_tag {
                return Some((wire_type, pos));
            }
            
            // Skip field based on wire type
            pos = match wire_type {
                0 => {
                    let (_, new_pos) = self.parse_varint(pos)?;
                    new_pos
                },
                1 => {
                    if pos + 8 <= self.data.len() { pos + 8 } else { return None; }
                },
                2 => {
                    let (_, new_pos) = self.parse_length_delimited(pos)?;
                    new_pos
                },
                5 => {
                    if pos + 4 <= self.data.len() { pos + 4 } else { return None; }
                },
                _ => return None,
            };
        }
        
        None
    }
}

/// Zero-allocation parser for LogsData
pub struct LogsDataParser<'a> {
    parser: ProtobufParser<'a>,
}

impl<'a> LogsDataParser<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self {
            parser: ProtobufParser::new(data),
        }
    }

    /// Get iterator over ResourceLogs (tag 1, repeated message)
    pub fn resource_logs(&'a self) -> ResourceLogsIterator<'a> {
        ResourceLogsIterator {
            parser: &self.parser,
            pos: 0,
        }
    }
}

/// Iterator over ResourceLogs messages
pub struct ResourceLogsIterator<'a> {
    parser: &'a ProtobufParser<'a>,
    pos: usize,
}

impl<'a> Iterator for ResourceLogsIterator<'a> {
    type Item = ResourceLogsParser<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.pos < self.parser.data.len() {
            let (tag_and_wire, new_pos) = self.parser.parse_varint(self.pos)?;
            self.pos = new_pos;

            let tag = (tag_and_wire >> 3) as u32;
            let wire_type = (tag_and_wire & 0x7) as u8;

            if tag == 1 && wire_type == 2 {
                let (bytes, end_pos) = self.parser.parse_length_delimited(self.pos)?;
                self.pos = end_pos;
                return Some(ResourceLogsParser::new(bytes));
            } else {
                // Skip field
                self.pos = match wire_type {
                    0 => self.parser.parse_varint(self.pos)?.1,
                    1 => self.pos + 8,
                    2 => self.parser.parse_length_delimited(self.pos)?.1,
                    5 => self.pos + 4,
                    _ => return None,
                };
            }
        }
        None
    }
}

/// Zero-allocation parser for ResourceLogs
pub struct ResourceLogsParser<'a> {
    parser: ProtobufParser<'a>,
}

impl<'a> ResourceLogsParser<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self {
            parser: ProtobufParser::new(data),
        }
    }

    /// Get the resource field (tag 1, optional message) - returns raw bytes
    pub fn resource(&self) -> Option<&'a [u8]> {
        self.parser.find_field(1).and_then(|(wire_type, pos)| {
            if wire_type == 2 {
                self.parser.parse_length_delimited(pos).map(|(bytes, _)| bytes)
            } else {
                None
            }
        })
    }

    /// Get iterator over resource attributes
    pub fn attributes(&'a self) -> Option<ResourceAttributeIterator<'a>> {
        // First get the resource field bytes
        if let Some(resource_bytes) = self.resource() {
            Some(ResourceAttributeIterator {
                parser: ProtobufParser::new(resource_bytes),
                pos: 0,
            })
        } else {
            None
        }
    }
    
    /// Get iterator over ScopeLogs (tag 2, repeated message)
    pub fn scope_logs(&'a self) -> ScopeLogsIterator<'a> {
        ScopeLogsIterator {
            parser: &self.parser,
            pos: 0,
        }
    }

    /// Get the schema_url field (tag 3, string)
    pub fn schema_url(&self) -> Option<&'a str> {
        self.parser.find_field(3).and_then(|(wire_type, pos)| {
            if wire_type == 2 {
                self.parser.parse_length_delimited(pos)
                    .and_then(|(bytes, _)| std::str::from_utf8(bytes).ok())
            } else {
                None
            }
        })
    }
}

/// Iterator over resource attribute KeyValue messages
pub struct ResourceAttributeIterator<'a> {
    parser: ProtobufParser<'a>,
    pos: usize,
}

impl<'a> Iterator for ResourceAttributeIterator<'a> {
    type Item = KeyValueParser<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.pos < self.parser.data.len() {
            let (tag_and_wire, new_pos) = self.parser.parse_varint(self.pos)?;
            self.pos = new_pos;

            let tag = (tag_and_wire >> 3) as u32;
            let wire_type = (tag_and_wire & 0x7) as u8;

            // Resource attributes are at tag 1 in the Resource message
            if tag == 1 && wire_type == 2 {
                let (bytes, end_pos) = self.parser.parse_length_delimited(self.pos)?;
                self.pos = end_pos;
                return Some(KeyValueParser::new(bytes));
            } else {
                // Skip field
                self.pos = match wire_type {
                    0 => self.parser.parse_varint(self.pos)?.1,
                    1 => self.pos + 8,
                    2 => self.parser.parse_length_delimited(self.pos)?.1,
                    5 => self.pos + 4,
                    _ => return None,
                };
            }
        }
        None
    }
}

/// Iterator over ScopeLogs messages
pub struct ScopeLogsIterator<'a> {
    parser: &'a ProtobufParser<'a>,
    pos: usize,
}

impl<'a> Iterator for ScopeLogsIterator<'a> {
    type Item = ScopeLogsParser<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.pos < self.parser.data.len() {
            let (tag_and_wire, new_pos) = self.parser.parse_varint(self.pos)?;
            self.pos = new_pos;

            let tag = (tag_and_wire >> 3) as u32;
            let wire_type = (tag_and_wire & 0x7) as u8;

            if tag == 2 && wire_type == 2 {
                let (bytes, end_pos) = self.parser.parse_length_delimited(self.pos)?;
                self.pos = end_pos;
                return Some(ScopeLogsParser::new(bytes));
            } else {
                // Skip field
                self.pos = match wire_type {
                    0 => self.parser.parse_varint(self.pos)?.1,
                    1 => self.pos + 8,
                    2 => self.parser.parse_length_delimited(self.pos)?.1,
                    5 => self.pos + 4,
                    _ => return None,
                };
            }
        }
        None
    }
}

/// Zero-allocation parser for ScopeLogs
pub struct ScopeLogsParser<'a> {
    parser: ProtobufParser<'a>,
}

impl<'a> ScopeLogsParser<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self {
            parser: ProtobufParser::new(data),
        }
    }

    /// Get the scope field (tag 1, optional message) - returns raw bytes
    pub fn scope(&self) -> Option<&'a [u8]> {
        self.parser.find_field(1).and_then(|(wire_type, pos)| {
            if wire_type == 2 {
                self.parser.parse_length_delimited(pos).map(|(bytes, _)| bytes)
            } else {
                None
            }
        })
    }

    /// Get iterator over LogRecord (tag 2, repeated message)
    pub fn log_records(&'a self) -> LogRecordIterator<'a> {
        LogRecordIterator {
            parser: &self.parser,
            pos: 0,
        }
    }

    /// Get the schema_url field (tag 3, string)
    pub fn schema_url(&self) -> Option<&'a str> {
        self.parser.find_field(3).and_then(|(wire_type, pos)| {
            if wire_type == 2 {
                self.parser.parse_length_delimited(pos)
                    .and_then(|(bytes, _)| std::str::from_utf8(bytes).ok())
            } else {
                None
            }
        })
    }

    /// Get the scope name as a readable string
    pub fn scope_name(&self) -> &'a str {
        if let Some(scope_bytes) = self.scope() {
            let scope_parser = ProtobufParser::new(scope_bytes);
            // Field 1 in InstrumentationScope is the name (string)
            if let Some((wire_type, pos)) = scope_parser.find_field(1) {
                if wire_type == 2 {
                    if let Some((bytes, _)) = scope_parser.parse_length_delimited(pos) {
                        return std::str::from_utf8(bytes).unwrap_or("");
                    }
                }
            }
        }
        ""
    }

    /// Get the scope version as a readable string
    pub fn scope_version(&self) -> Option<&'a str> {
        if let Some(scope_bytes) = self.scope() {
            let scope_parser = ProtobufParser::new(scope_bytes);
            // Field 2 in InstrumentationScope is the version (string)
            if let Some((wire_type, pos)) = scope_parser.find_field(2) {
                if wire_type == 2 {
                    if let Some((bytes, _)) = scope_parser.parse_length_delimited(pos) {
                        let version = std::str::from_utf8(bytes).unwrap_or("");
                        return if version.is_empty() { None } else { Some(version) };
                    }
                }
            }
        }
        None
    }
}

/// Iterator over LogRecord messages
pub struct LogRecordIterator<'a> {
    parser: &'a ProtobufParser<'a>,
    pos: usize,
}

impl<'a> Iterator for LogRecordIterator<'a> {
    type Item = LogRecordParser<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.pos < self.parser.data.len() {
            let (tag_and_wire, new_pos) = self.parser.parse_varint(self.pos)?;
            self.pos = new_pos;

            let tag = (tag_and_wire >> 3) as u32;
            let wire_type = (tag_and_wire & 0x7) as u8;

            if tag == 2 && wire_type == 2 {
                let (bytes, end_pos) = self.parser.parse_length_delimited(self.pos)?;
                self.pos = end_pos;
                return Some(LogRecordParser::new(bytes));
            } else {
                // Skip field
                self.pos = match wire_type {
                    0 => self.parser.parse_varint(self.pos)?.1,
                    1 => self.pos + 8,
                    2 => self.parser.parse_length_delimited(self.pos)?.1,
                    5 => self.pos + 4,
                    _ => return None,
                };
            }
        }
        None
    }
}

/// Zero-allocation parser for LogRecord
pub struct LogRecordParser<'a> {
    parser: ProtobufParser<'a>,
    cache: std::cell::OnceCell<FieldCache>, // Add this field
}

impl<'a> LogRecordParser<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self {
            parser: ProtobufParser::new(data),
            cache: std::cell::OnceCell::new(), // Initialize the cache
        }
    }

    /// Parse all fields once and cache their positions
    fn get_cache(&self) -> &FieldCache {
        self.cache.get_or_init(|| {
            let mut cache = FieldCache::default();
            let mut pos = 0;
            
            while pos < self.parser.data.len() {
                if let Some((tag_and_wire, new_pos)) = self.parser.parse_varint(pos) {
                    pos = new_pos;
                    let tag = (tag_and_wire >> 3) as u32;
                    let wire_type = (tag_and_wire & 0x7) as u8;
                    
                    // Cache field positions based on tag
                    match tag {
                        1 => cache.time_unix_nano = Some((wire_type, pos)),
                        2 => cache.severity_number = Some((wire_type, pos)),
                        3 => cache.severity_text = Some((wire_type, pos)),
                        5 => cache.body = Some((wire_type, pos)),
                        6 => cache.attributes.push((wire_type, pos)),
                        7 => cache.dropped_attributes_count = Some((wire_type, pos)),
                        8 => cache.flags = Some((wire_type, pos)),
                        9 => cache.trace_id = Some((wire_type, pos)),
                        10 => cache.span_id = Some((wire_type, pos)),
                        11 => cache.observed_time_unix_nano = Some((wire_type, pos)),
                        12 => cache.event_name = Some((wire_type, pos)),
                        _ => {} // Skip unknown fields
                    }
                    
                    // Skip to next field based on wire type
                    pos = match wire_type {
                        0 => {
                            if let Some((_, new_pos)) = self.parser.parse_varint(pos) {
                                new_pos
                            } else {
                                break;
                            }
                        },
                        1 => {
                            if pos + 8 <= self.parser.data.len() { 
                                pos + 8 
                            } else { 
                                break; 
                            }
                        },
                        2 => {
                            if let Some((_, new_pos)) = self.parser.parse_length_delimited(pos) {
                                new_pos
                            } else {
                                break;
                            }
                        },
                        5 => {
                            if pos + 4 <= self.parser.data.len() { 
                                pos + 4 
                            } else { 
                                break; 
                            }
                        },
                        _ => break, // Unknown wire type
                    };
                } else {
                    break;
                }
            }
            cache
        })
    }

    pub fn time_unix_nano(&self) -> u64 {
        if let Some((wire_type, pos)) = self.get_cache().time_unix_nano {
            if wire_type == 1 {
                return self.parser.parse_fixed64(pos).map(|(value, _)| value).unwrap_or(0);
            }
        }
        0
    }

    pub fn observed_time_unix_nano(&self) -> u64 {
        if let Some((wire_type, pos)) = self.get_cache().observed_time_unix_nano {
            if wire_type == 1 {
                return self.parser.parse_fixed64(pos).map(|(value, _)| value).unwrap_or(0);
            }
        }
        0
    }

    pub fn severity_number(&self) -> i32 {
        if let Some((wire_type, pos)) = self.get_cache().severity_number {
            if wire_type == 0 {
                return self.parser.parse_varint(pos).map(|(value, _)| value as i32).unwrap_or(0);
            }
        }
        0
    }

    /// Get the severity_text field (tag 3, string)
    pub fn severity_text(&self) -> Option<&'a str> {
        if let Some((wire_type, pos)) = self.get_cache().severity_text {
            if wire_type == 2 {
                return self.parser.parse_length_delimited(pos)
                    .and_then(|(bytes, _)| std::str::from_utf8(bytes).ok());
            }
        }
        None
    }

    /// Get the body field (tag 5, message) - returns raw bytes
    pub fn body(&self) -> Option<&'a [u8]> {
        if let Some((wire_type, pos)) = self.get_cache().body {
            if wire_type == 2 {
                return self.parser.parse_length_delimited(pos).map(|(bytes, _)| bytes);
            }
        }
        None
    }

    /// Get iterator over attributes (tag 6, repeated message)
    pub fn attributes(&'a self) -> CachedAttributeIterator<'a> {
        CachedAttributeIterator {
            parser: &self.parser,
            positions: &self.get_cache().attributes,
            index: 0,
        }
    }

    /// Get the dropped_attributes_count field (tag 7, uint32)
    pub fn dropped_attributes_count(&self) -> Option<u32> {
        if let Some((wire_type, pos)) = self.get_cache().dropped_attributes_count {
            if wire_type == 0 {
                return self.parser.parse_varint(pos).map(|(value, _)| value as u32);
            }
        }
        None
    }

    /// Get the flags field (tag 8, fixed32)
    pub fn flags(&self) -> Option<u32> {
        if let Some((wire_type, pos)) = self.get_cache().flags {
            if wire_type == 5 {
                return self.parser.parse_fixed32(pos).map(|(value, _)| value);
            }
        }
        None
    }

    /// Get the trace_id field (tag 9, bytes)
    pub fn trace_id(&self) -> Option<&'a [u8]> {
        if let Some((wire_type, pos)) = self.get_cache().trace_id {
            if wire_type == 2 {
                return self.parser.parse_length_delimited(pos).map(|(bytes, _)| bytes);
            }
        }
        None
    }

    /// Get the span_id field (tag 10, bytes)
    pub fn span_id(&self) -> Option<&'a [u8]> {
        if let Some((wire_type, pos)) = self.get_cache().span_id {
            if wire_type == 2 {
                return self.parser.parse_length_delimited(pos).map(|(bytes, _)| bytes);
            }
        }
        None
    }

    /// Get the event_name field (tag 12, string)
    pub fn event_name(&self) -> Option<&'a str> {
        if let Some((wire_type, pos)) = self.get_cache().event_name {
            if wire_type == 2 {
                return self.parser.parse_length_delimited(pos)
                    .and_then(|(bytes, _)| std::str::from_utf8(bytes).ok());
            }
        }
        None
    }

    /// Check if trace_id is valid (16 bytes, not all zeros)
    pub fn is_trace_id_valid(&self) -> bool {
        if let Some(trace_id) = self.trace_id() {
            trace_id.len() == 16 && !trace_id.iter().all(|&b| b == 0)
        } else {
            false
        }
    }

    /// Check if span_id is valid (8 bytes, not all zeros)
    pub fn is_span_id_valid(&self) -> bool {
        if let Some(span_id) = self.span_id() {
            span_id.len() == 8 && !span_id.iter().all(|&b| b == 0)
        } else {
            false
        }
    }

    /// Extract trace flags from the flags field (lower 8 bits)
    pub fn trace_flags(&self) -> Option<u8> {
        self.flags().map(|flags| (flags & 0xFF) as u8)
    }
}

/// Iterator over attribute KeyValue messages
pub struct AttributeIterator<'a> {
    parser: &'a ProtobufParser<'a>,
    pos: usize,
}

impl<'a> Iterator for AttributeIterator<'a> {
    type Item = KeyValueParser<'a>; // Changed from &'a [u8] to KeyValueParser<'a>

    fn next(&mut self) -> Option<Self::Item> {
        while self.pos < self.parser.data.len() {
            let (tag_and_wire, new_pos) = self.parser.parse_varint(self.pos)?;
            self.pos = new_pos;

            let tag = (tag_and_wire >> 3) as u32;
            let wire_type = (tag_and_wire & 0x7) as u8;

            if tag == 6 && wire_type == 2 {
                let (bytes, end_pos) = self.parser.parse_length_delimited(self.pos)?;
                self.pos = end_pos;
                return Some(KeyValueParser::new(bytes)); // Return parsed KeyValueParser
            } else {
                // Skip field
                self.pos = match wire_type {
                    0 => self.parser.parse_varint(self.pos)?.1,
                    1 => self.pos + 8,
                    2 => self.parser.parse_length_delimited(self.pos)?.1,
                    5 => self.pos + 4,
                    _ => return None,
                };
            }
        }
        None
    }
}

/// Zero-allocation parser for KeyValue (attributes)
pub struct KeyValueParser<'a> {
    parser: ProtobufParser<'a>,
}

impl<'a> KeyValueParser<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self {
            parser: ProtobufParser::new(data),
        }
    }

    /// Get the key field (tag 1, string)
    pub fn key(&self) -> Option<&'a str> {
        self.parser.find_field(1).and_then(|(wire_type, pos)| {
            if wire_type == 2 {
                self.parser.parse_length_delimited(pos)
                    .and_then(|(bytes, _)| std::str::from_utf8(bytes).ok())
            } else {
                None
            }
        })
    }

    /// Get the value field (tag 2, message) - returns raw AnyValue bytes
    pub fn value(&self) -> Option<AnyValueParser<'a>> {
        self.parser.find_field(2).and_then(|(wire_type, pos)| {
            if wire_type == 2 {
                self.parser.parse_length_delimited(pos)
                    .map(|(bytes, _)| AnyValueParser::new(bytes))
            } else {
                None
            }
        })
    }
}

/// Zero-allocation parser for AnyValue
pub struct AnyValueParser<'a> {
    parser: ProtobufParser<'a>,
}

impl<'a> AnyValueParser<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self {
            parser: ProtobufParser::new(data),
        }
    }

    /// Get string value (tag 1, string)
    pub fn string_value(&self) -> Option<&'a str> {
        self.parser.find_field(1).and_then(|(wire_type, pos)| {
            if wire_type == 2 {
                self.parser.parse_length_delimited(pos)
                    .and_then(|(bytes, _)| std::str::from_utf8(bytes).ok())
            } else {
                None
            }
        })
    }

    /// Get bool value (tag 2, bool)
    pub fn bool_value(&self) -> Option<bool> {
        self.parser.find_field(2).and_then(|(wire_type, pos)| {
            if wire_type == 0 {
                self.parser.parse_varint(pos).map(|(value, _)| value != 0)
            } else {
                None
            }
        })
    }

    /// Get int value (tag 3, int64)
    pub fn int_value(&self) -> Option<i64> {
        self.parser.find_field(3).and_then(|(wire_type, pos)| {
            if wire_type == 0 {
                self.parser.parse_varint(pos).map(|(value, _)| value as i64)
            } else {
                None
            }
        })
    }

    /// Get double value (tag 4, double)
    pub fn double_value(&self) -> Option<f64> {
        self.parser.find_field(4).and_then(|(wire_type, pos)| {
            if wire_type == 1 {
                self.parser.parse_fixed64(pos).map(|(value, _)| f64::from_bits(value))
            } else {
                None
            }
        })
    }

    /// Get array value (tag 5, repeated AnyValue)
    pub fn array_value(&'a self) -> Option<ArrayValueIterator<'a>> {
        // Check if field 5 exists
        if self.parser.find_field(5).is_some() {
            Some(ArrayValueIterator {
                parser: &self.parser,
                pos: 0,
            })
        } else {
            None
        }
    }

    /// Get kvlist value (tag 6, repeated KeyValue)
    pub fn kvlist_value(&'a self) -> Option<KvListIterator<'a>> {
        // Check if field 6 exists
        if self.parser.find_field(6).is_some() {
            Some(KvListIterator {
                parser: &self.parser,
                pos: 0,
            })
        } else {
            None
        }
    }

    /// Get bytes value (tag 7, bytes)
    pub fn bytes_value(&self) -> Option<&'a [u8]> {
        self.parser.find_field(7).and_then(|(wire_type, pos)| {
            if wire_type == 2 {
                self.parser.parse_length_delimited(pos).map(|(bytes, _)| bytes)
            } else {
                None
            }
        })
    }

    /// Determine the value type by checking which field is present
    pub fn value_type(&self) -> AnyValueType {
        if self.parser.find_field(1).is_some() {
            AnyValueType::String
        } else if self.parser.find_field(2).is_some() {
            AnyValueType::Bool
        } else if self.parser.find_field(3).is_some() {
            AnyValueType::Int
        } else if self.parser.find_field(4).is_some() {
            AnyValueType::Double
        } else if self.parser.find_field(5).is_some() {
            AnyValueType::Array
        } else if self.parser.find_field(6).is_some() {
            AnyValueType::KvList
        } else if self.parser.find_field(7).is_some() {
            AnyValueType::Bytes
        } else {
            AnyValueType::Unknown
        }
    }

    /// Get a string representation of the value for easy printing
    pub fn to_display_string(&self) -> String {
        match self.value_type() {
            AnyValueType::String => {
                format!("\"{}\"", self.string_value().unwrap_or("N/A"))
            },
            AnyValueType::Bool => {
                format!("{}", self.bool_value().unwrap_or(false))
            },
            AnyValueType::Int => {
                format!("{}", self.int_value().unwrap_or(0))
            },
            AnyValueType::Double => {
                format!("{}", self.double_value().unwrap_or(0.0))
            },
            AnyValueType::Bytes => {
                format!("bytes[{}]", self.bytes_value().map(|b| b.len()).unwrap_or(0))
            },
            AnyValueType::Array => {
                if let Some(array) = self.array_value() {
                    format!("array[{}]", array.count())
                } else {
                    "array[0]".to_string()
                }
            },
            AnyValueType::KvList => {
                if let Some(kvlist) = self.kvlist_value() {
                    format!("kvlist[{}]", kvlist.count())
                } else {
                    "kvlist[0]".to_string()
                }
            },
            AnyValueType::Unknown => "unknown".to_string(),
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
    Unknown,
}

/// Iterator over array values
pub struct ArrayValueIterator<'a> {
    parser: &'a ProtobufParser<'a>,
    pos: usize,
}

impl<'a> Iterator for ArrayValueIterator<'a> {
    type Item = AnyValueParser<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.pos < self.parser.data.len() {
            let (tag_and_wire, new_pos) = self.parser.parse_varint(self.pos)?;
            self.pos = new_pos;

            let tag = (tag_and_wire >> 3) as u32;
            let wire_type = (tag_and_wire & 0x7) as u8;

            if tag == 5 && wire_type == 2 {
                let (bytes, end_pos) = self.parser.parse_length_delimited(self.pos)?;
                self.pos = end_pos;
                return Some(AnyValueParser::new(bytes));
            } else {
                // Skip field
                self.pos = match wire_type {
                    0 => self.parser.parse_varint(self.pos)?.1,
                    1 => self.pos + 8,
                    2 => self.parser.parse_length_delimited(self.pos)?.1,
                    5 => self.pos + 4,
                    _ => return None,
                };
            }
        }
        None
    }
}

/// Iterator over KeyValue list
pub struct KvListIterator<'a> {
    parser: &'a ProtobufParser<'a>,
    pos: usize,
}

impl<'a> Iterator for KvListIterator<'a> {
    type Item = KeyValueParser<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.pos < self.parser.data.len() {
            let (tag_and_wire, new_pos) = self.parser.parse_varint(self.pos)?;
            self.pos = new_pos;

            let tag = (tag_and_wire >> 3) as u32;
            let wire_type = (tag_and_wire & 0x7) as u8;

            if tag == 6 && wire_type == 2 {
                let (bytes, end_pos) = self.parser.parse_length_delimited(self.pos)?;
                self.pos = end_pos;
                return Some(KeyValueParser::new(bytes));
            } else {
                // Skip field
                self.pos = match wire_type {
                    0 => self.parser.parse_varint(self.pos)?.1,
                    1 => self.pos + 8,
                    2 => self.parser.parse_length_delimited(self.pos)?.1,
                    5 => self.pos + 4,
                    _ => return None,
                };
            }
        }
        None
    }
}

/// Cache for field positions to avoid repeated scanning
#[derive(Default)]
struct FieldCache {
    time_unix_nano: Option<(u8, usize)>,
    observed_time_unix_nano: Option<(u8, usize)>,
    severity_number: Option<(u8, usize)>,
    severity_text: Option<(u8, usize)>,
    body: Option<(u8, usize)>,
    attributes: Vec<(u8, usize)>,
    dropped_attributes_count: Option<(u8, usize)>,
    flags: Option<(u8, usize)>,
    trace_id: Option<(u8, usize)>,
    span_id: Option<(u8, usize)>,
    event_name: Option<(u8, usize)>,
}

/// Cached iterator over attribute KeyValue messages
pub struct CachedAttributeIterator<'a> {
    parser: &'a ProtobufParser<'a>,
    positions: &'a [(u8, usize)],
    index: usize,
}

impl<'a> Iterator for CachedAttributeIterator<'a> {
    type Item = KeyValueParser<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.positions.len() {
            let (wire_type, pos) = self.positions[self.index];
            self.index += 1;
            
            if wire_type == 2 {
                if let Some((bytes, _)) = self.parser.parse_length_delimited(pos) {
                    return Some(KeyValueParser::new(bytes));
                }
            }
        }
        None
    }
}