//! PostgreSQL logical replication protocol message parser
//! Handles parsing of various message types from the replication stream

use crate::buffer::BufferReader;
use crate::errors::{ReplicationError, Result};
use crate::types::*;
use tracing::{debug, error, warn};

/// Parse logical replication messages from a buffer
pub struct MessageParser;

impl MessageParser {
    /// Parse a WAL message from the given buffer
    /// Returns a ReplicationMessage on success
    /// Errors with ReplicationError on failure
    /// please refer to https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html#PROTOCOL-LOGICALREP-MESSAGE-FORMATS
    pub fn parse_wal_message(buffer: &[u8], in_streaming_txn: bool) -> Result<ReplicationMessage> {
        let mut reader = BufferReader::new(buffer);
        let message_type = reader.skip_message_type()?;

        debug!("Parsing message type: {}, streaming: {}", message_type, in_streaming_txn);

        match message_type {
            'B' => Self::parse_begin_message(&mut reader),
            'C' => Self::parse_commit_message(&mut reader),
            'R' => Self::parse_relation_message(&mut reader, in_streaming_txn),
            'I' => Self::parse_insert_message(&mut reader),
            'U' => Self::parse_update_message(&mut reader),
            'D' => Self::parse_delete_message(&mut reader),
            'T' => Self::parse_truncate_message(&mut reader),
            'S' => Self::parse_stream_start_message(&mut reader),
            'E' => Self::parse_stream_stop_message(&mut reader),
            'c' => Self::parse_stream_commit_message(&mut reader),
            'A' => Self::parse_stream_abort_message(&mut reader),
            _ => {
                warn!("Unknown message type: {}", message_type);
                Err(ReplicationError::parse_with_context(
                    "Unknown message type",
                    format!("Message type: {}", message_type),
                ))
            }
        }
    }

    fn parse_begin_message(reader: &mut BufferReader) -> Result<ReplicationMessage> {
        // BEGIN message: final_lsn (8) + timestamp (8) + xid (4) = 20 bytes + 1 for type
        if !reader.has_bytes(20) {
            return Err(ReplicationError::parse("Begin message too short"));
        }

        let final_lsn = reader.read_u64()?;
        let timestamp = reader.read_i64()?;
        let xid = reader.read_u32()?;

        Ok(ReplicationMessage::Begin {
            final_lsn,
            timestamp,
            xid,
        })
    }

    fn parse_commit_message(reader: &mut BufferReader) -> Result<ReplicationMessage> {
        // COMMIT message: flags (1) + commit_lsn (8) + end_lsn (8) + timestamp (8) = 25 bytes + 1 for type
        if !reader.has_bytes(25) {
            return Err(ReplicationError::parse("Commit message too short"));
        }

        let flags = reader.read_u8()?;
        let commit_lsn = reader.read_u64()?;
        let end_lsn = reader.read_u64()?;
        let timestamp = reader.read_i64()?;

        Ok(ReplicationMessage::Commit {
            flags,
            commit_lsn,
            end_lsn,
            timestamp,
        })
    }

    fn parse_relation_message(reader: &mut BufferReader, in_streaming_txn: bool) -> Result<ReplicationMessage> {
        // RELATION message in streaming mode: xid (4) + oid (4) + namespace (null-terminated) + relation_name (null-terminated) + replica_identity (1) + column_count (2) + columns
        // RELATION message in non-streaming mode: oid (4) + namespace (null-terminated) + relation_name (null-terminated) + replica_identity (1) + column_count (2) + columns
        let min_bytes = if in_streaming_txn { 11 } else { 7 };
        if !reader.has_bytes(min_bytes) {
            return Err(ReplicationError::parse("Relation message too short"));
        }

        // Read transaction ID if in streaming mode
        let _xid = if in_streaming_txn {
            Some(reader.read_u32()?)
        } else {
            None
        };

        let oid = reader.read_u32()?;
        let namespace = reader.read_null_terminated_string()?;
        let relation_name = reader.read_null_terminated_string()?;
        let replica_identity = reader.read_u8()? as char;
        let column_count = reader.read_i16()?;

        let mut columns = Vec::with_capacity(column_count as usize);
        for i in 0..column_count {
            if !reader.has_bytes(9) {
                // Minimum: key_flag (1) + column_name (1) + column_type (4) + atttypmod (4)
                return Err(ReplicationError::parse_with_context(
                    "Column data truncated",
                    format!("Column {} of {}", i + 1, column_count),
                ));
            }

            let key_flag = reader.read_u8()? as i8;
            let column_name = reader.read_null_terminated_string()?;
            let column_type = reader.read_u32()?;
            let atttypmod = reader.read_i32()?;

            columns.push(ColumnInfo {
                key_flag,
                column_name,
                column_type,
                atttypmod,
            });
        }

        let relation = RelationInfo {
            oid,
            namespace,
            relation_name,
            replica_identity,
            column_count,
            columns,
        };

        Ok(ReplicationMessage::Relation { relation })
    }

    fn parse_insert_message(reader: &mut BufferReader) -> Result<ReplicationMessage> {
        // INSERT message: first u32 could be relation_id or transaction_id depending on streaming
        if !reader.has_bytes(5) {
            // Minimum: transaction_id_or_oid (4) + 'N' marker (1)
            return Err(ReplicationError::parse("Insert message too short"));
        }

        let transaction_id_or_oid = reader.read_u32()?;

        // Determine if this is a streaming transaction by checking what comes next
        let (relation_id, is_stream, xid) = if reader.peek_u8()? == b'N' {
            // Not a streaming transaction
            (transaction_id_or_oid, false, None)
        } else {
            // Streaming transaction: read the actual relation_id
            let relation_id = reader.read_u32()?;
            (relation_id, true, Some(transaction_id_or_oid))
        };

        // Expect 'N' marker for new tuple
        let marker = reader.read_u8()?;
        if marker != b'N' {
            return Err(ReplicationError::parse_with_context(
                "Expected 'N' marker in insert message",
                format!("Found: {}", marker as char),
            ));
        }

        let tuple_data = Self::parse_tuple_data(reader)?;

        Ok(ReplicationMessage::Insert {
            relation_id,
            tuple_data,
            is_stream,
            xid,
        })
    }

    fn parse_update_message(reader: &mut BufferReader) -> Result<ReplicationMessage> {
        // UPDATE message: first u32 could be relation_id or transaction_id depending on streaming
        if !reader.has_bytes(5) {
            // Minimum: transaction_id_or_oid (4) + marker (1)
            return Err(ReplicationError::parse("Update message too short"));
        }

        let transaction_id_or_oid = reader.read_u32()?;

        // Check if this is a streaming transaction by examining the next byte
        let next_byte = reader.peek_u8()?;
        let (relation_id, is_stream, xid) =
            if next_byte == b'K' || next_byte == b'O' || next_byte == b'N' {
                // Not a streaming transaction
                (transaction_id_or_oid, false, None)
            } else {
                // Streaming transaction: read the actual relation_id
                let relation_id = reader.read_u32()?;
                (relation_id, true, Some(transaction_id_or_oid))
            };

        // Read the tuple marker
        let marker = reader.read_u8()? as char;

        let (key_type, old_tuple_data) = match marker {
            'K' | 'O' => {
                // Parse old tuple data
                let tuple_data = Self::parse_tuple_data(reader)?;

                // Expect 'N' marker for new tuple data
                let new_marker = reader.read_u8()?;
                if new_marker != b'N' {
                    return Err(ReplicationError::parse_with_context(
                        "Expected 'N' marker after old tuple data",
                        format!("Found: {}", new_marker as char),
                    ));
                }

                (Some(marker), Some(tuple_data))
            }
            'N' => (None, None),
            _ => {
                return Err(ReplicationError::parse_with_context(
                    "Invalid marker in update message",
                    format!("Marker: {}", marker),
                ))
            }
        };

        let new_tuple_data = Self::parse_tuple_data(reader)?;

        Ok(ReplicationMessage::Update {
            relation_id,
            key_type,
            old_tuple_data,
            new_tuple_data,
            is_stream,
            xid,
        })
    }

    fn parse_delete_message(reader: &mut BufferReader) -> Result<ReplicationMessage> {
        // DELETE message: first u32 could be relation_id or transaction_id depending on streaming
        if !reader.has_bytes(5) {
            // Minimum: transaction_id_or_oid (4) + key_type (1)
            return Err(ReplicationError::parse("Delete message too short"));
        }

        let transaction_id_or_oid = reader.read_u32()?;

        // Check if this is a streaming transaction by examining the next byte
        let next_byte = reader.peek_u8()?;
        let (relation_id, is_stream, xid, key_type) = if next_byte == b'K' || next_byte == b'O' {
            // Not a streaming transaction
            let key_type = reader.read_u8()? as char;
            (transaction_id_or_oid, false, None, key_type)
        } else {
            // Streaming transaction: read the actual relation_id
            let relation_id = reader.read_u32()?;
            let key_type = reader.read_u8()? as char;
            (relation_id, true, Some(transaction_id_or_oid), key_type)
        };

        let tuple_data = Self::parse_tuple_data(reader)?;

        Ok(ReplicationMessage::Delete {
            relation_id,
            key_type,
            tuple_data,
            is_stream,
            xid,
        })
    }

    fn parse_truncate_message(reader: &mut BufferReader) -> Result<ReplicationMessage> {
        // TRUNCATE message: Complex logic to determine if streaming or not
        if !reader.has_bytes(9) {
            // Minimum: first_u32 (4) + second_u32 (4) + flags (1)
            return Err(ReplicationError::parse("Truncate message too short"));
        }

        let first_u32 = reader.read_u32()?;
        let second_u32 = reader.read_u32()?;

        // Estimate remaining bytes needed based on second_u32 as potential relation count
        let remaining_bytes = reader.remaining();
        let expected_for_streaming = 1 + (second_u32 as usize * 4); // flags + relation IDs

        let (is_stream, xid, num_relations) = if remaining_bytes == expected_for_streaming {
            // Streaming transaction: first_u32 is xid, second_u32 is num_relations
            (true, Some(first_u32), second_u32)
        } else {
            // Not streaming: first_u32 is num_relations, rewind to read second_u32 as flags later
            let current_pos = reader.position();
            reader.set_position(current_pos - 4)?; // Go back 4 bytes to re-read second_u32 as flags
            (false, None, first_u32)
        };

        let flags = reader.read_u8()? as i8;

        let mut relation_ids = Vec::with_capacity(num_relations as usize);
        for i in 0..num_relations {
            if !reader.has_bytes(4) {
                return Err(ReplicationError::parse_with_context(
                    "Truncate relation IDs truncated",
                    format!("Relation {} of {}", i + 1, num_relations),
                ));
            }
            let relation_id = reader.read_u32()?;
            relation_ids.push(relation_id);
        }

        Ok(ReplicationMessage::Truncate {
            relation_ids,
            flags,
            is_stream,
            xid,
        })
    }

    fn parse_stream_start_message(reader: &mut BufferReader) -> Result<ReplicationMessage> {
        // STREAM START message: xid (4) + optional first_segment (1)
        if !reader.has_bytes(4) {
            return Err(ReplicationError::parse("Stream start message too short"));
        }

        let xid = reader.read_u32()?;

        let first_segment = if reader.has_bytes(1) {
            reader.read_u8()? == 1
        } else {
            false
        };

        Ok(ReplicationMessage::StreamStart { xid, first_segment })
    }

    fn parse_stream_stop_message(_reader: &mut BufferReader) -> Result<ReplicationMessage> {
        // STREAM STOP message has no additional data
        Ok(ReplicationMessage::StreamStop)
    }

    fn parse_stream_commit_message(reader: &mut BufferReader) -> Result<ReplicationMessage> {
        // STREAM COMMIT message: xid (4) + flags (1) + commit_lsn (8) + end_lsn (8) + timestamp (8) = 29 bytes
        if !reader.has_bytes(29) {
            return Err(ReplicationError::parse("Stream commit message too short"));
        }

        let xid = reader.read_u32()?;
        let flags = reader.read_u8()?;
        let commit_lsn = reader.read_u64()?;
        let end_lsn = reader.read_u64()?;
        let timestamp = reader.read_i64()?;

        Ok(ReplicationMessage::StreamCommit {
            xid,
            flags,
            commit_lsn,
            end_lsn,
            timestamp,
        })
    }

    fn parse_stream_abort_message(reader: &mut BufferReader) -> Result<ReplicationMessage> {
        // STREAM ABORT message: xid (4) + subtransaction_xid (4) = 8 bytes
        if !reader.has_bytes(8) {
            return Err(ReplicationError::parse("Stream abort message too short"));
        }

        let xid = reader.read_u32()?;
        let subtransaction_xid = reader.read_u32()?;

        Ok(ReplicationMessage::StreamAbort {
            xid,
            subtransaction_xid,
        })
    }

    fn parse_tuple_data(reader: &mut BufferReader) -> Result<TupleData> {
        // TUPLE DATA: column_count (2) + columns
        if !reader.has_bytes(2) {
            return Err(ReplicationError::parse("Tuple data too short"));
        }

        let start_position = reader.position();
        let column_count = reader.read_i16()?;

        let mut columns = Vec::with_capacity(column_count as usize);

        for i in 0..column_count {
            if !reader.has_bytes(1) {
                return Err(ReplicationError::parse_with_context(
                    "Tuple data truncated",
                    format!("Column {} of {}", i + 1, column_count),
                ));
            }

            let data_type = reader.read_u8()? as char;

            let column_data = match data_type {
                'n' => {
                    // NULL value
                    ColumnData {
                        data_type: 'n',
                        length: 0,
                        data: String::new(),
                    }
                }
                'u' => {
                    // Unchanged TOAST value
                    debug!("Unchanged TOAST value encountered");
                    ColumnData {
                        data_type: 'u',
                        length: 0,
                        data: String::new(),
                    }
                }
                't' => {
                    // Text data with length prefix
                    let text_data = reader.read_length_prefixed_string()?;
                    ColumnData {
                        data_type: 't',
                        length: text_data.len() as i32,
                        data: text_data,
                    }
                }
                _ => {
                    error!("Unknown tuple data type: {}", data_type);
                    return Err(ReplicationError::parse_with_context(
                        "Unknown tuple data type",
                        format!("Data type: {}", data_type),
                    ));
                }
            };

            columns.push(column_data);
        }

        let processed_length = reader.position() - start_position;

        Ok(TupleData {
            column_count,
            columns,
            processed_length,
        })
    }
}
