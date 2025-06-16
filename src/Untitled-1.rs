/// Base protobuf parser with common functionality
pub struct ProtobufParser<'a> {
    data: &'a [u8],
}

impl<'a> ProtobufParser<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data }
    }

    /// Parse a varint from the current position
    fn parse_varint(&self, mut pos: usize) -> Option<(u64, usize)> {
        let mut result = 0u64;
        let mut shift = 0;
        
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

    /// Find a field by tag number, returns (wire_type, position_after_tag)
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
}

impl<'a> LogRecordParser<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self {
            parser: ProtobufParser::new(data),
        }
    }

    /// Get the time_unix_nano field (tag 1, fixed64)
    pub fn time_unix_nano(&self) -> Option<u64> {
        self.parser.find_field(1).and_then(|(wire_type, pos)| {
            if wire_type == 1 {
                self.parser.parse_fixed64(pos).map(|(value, _)| value)
            } else {
                None
            }
        })
    }

    /// Get the observed_time_unix_nano field (tag 11, fixed64)
    pub fn observed_time_unix_nano(&self) -> Option<u64> {
        self.parser.find_field(11).and_then(|(wire_type, pos)| {
            if wire_type == 1 {
                self.parser.parse_fixed64(pos).map(|(value, _)| value)
            } else {
                None
            }
        })
    }

    /// Get the severity_number field (tag 2, varint)
    pub fn severity_number(&self) -> Option<i32> {
        self.parser.find_field(2).and_then(|(wire_type, pos)| {
            if wire_type == 0 {
                self.parser.parse_varint(pos).map(|(value, _)| value as i32)
            } else {
                None
            }
        })
    }

    /// Get the severity_text field (tag 3, string)
    pub fn severity_text(&self) -> Option<&'a str> {
        self.parser.find_field(3).and_then(|(wire_type, pos)| {
            if wire_type == 2 {
                self.parser.parse_length_delimited(pos)
                    .and_then(|(bytes, _)| std::str::from_utf8(bytes).ok())
            } else {
                None
            }
        })
    }

    /// Get the body field (tag 5, message) - returns raw bytes
    pub fn body(&self) -> Option<&'a [u8]> {
        self.parser.find_field(5).and_then(|(wire_type, pos)| {
            if wire_type == 2 {
                self.parser.parse_length_delimited(pos).map(|(bytes, _)| bytes)
            } else {
                None
            }
        })
    }

    /// Get iterator over attributes (tag 6, repeated message)
    pub fn attributes(&'a self) -> AttributeIterator<'a> {
        AttributeIterator {
            parser: &self.parser,
            pos: 0,
        }
    }

    /// Get the dropped_attributes_count field (tag 7, uint32)
    pub fn dropped_attributes_count(&self) -> Option<u32> {
        self.parser.find_field(7).and_then(|(wire_type, pos)| {
            if wire_type == 0 {
                self.parser.parse_varint(pos).map(|(value, _)| value as u32)
            } else {
                None
            }
        })
    }

    /// Get the flags field (tag 8, fixed32)
    pub fn flags(&self) -> Option<u32> {
        self.parser.find_field(8).and_then(|(wire_type, pos)| {
            if wire_type == 5 {
                self.parser.parse_fixed32(pos).map(|(value, _)| value)
            } else {
                None
            }
        })
    }

    /// Get the trace_id field (tag 9, bytes)
    pub fn trace_id(&self) -> Option<&'a [u8]> {
        self.parser.find_field(9).and_then(|(wire_type, pos)| {
            if wire_type == 2 {
                self.parser.parse_length_delimited(pos).map(|(bytes, _)| bytes)
            } else {
                None
            }
        })
    }

    /// Get the span_id field (tag 10, bytes)
    pub fn span_id(&self) -> Option<&'a [u8]> {
        self.parser.find_field(10).and_then(|(wire_type, pos)| {
            if wire_type == 2 {
                self.parser.parse_length_delimited(pos).map(|(bytes, _)| bytes)
            } else {
                None
            }
        })
    }

    /// Get the event_name field (tag 12, string)
    pub fn event_name(&self) -> Option<&'a str> {
        self.parser.find_field(12).and_then(|(wire_type, pos)| {
            if wire_type == 2 {
                self.parser.parse_length_delimited(pos)
                    .and_then(|(bytes, _)| std::str::from_utf8(bytes).ok())
            } else {
                None
            }
        })
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