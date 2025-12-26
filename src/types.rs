//! Data structures for PostgreSQL logical replication
//! Contains types for representing relation information, tuple data, and messages

use crate::utils::{Oid, Xid};
use std::collections::HashMap;

/// Information about a table column
#[derive(Debug)]
pub struct ColumnInfo {
    pub key_flag: i8,
    pub column_name: String,
    pub column_type: Oid,
    pub atttypmod: i32,
}

/// Information about a relation (table)
#[derive(Debug)]
pub struct RelationInfo {
    pub oid: Oid,
    pub namespace: String,
    pub relation_name: String,
    pub replica_identity: char,
    pub column_count: i16,
    pub columns: Vec<ColumnInfo>,
}

/// Data for a single column in a tuple
#[derive(Debug)]
pub struct ColumnData {
    pub data_type: char, // 'n' for null, 't' for text, 'u' for unchanged
    pub length: i32,
    pub data: String,
}

/// Data for a complete row/tuple
#[derive(Debug)]
pub struct TupleData {
    pub column_count: i16,
    pub columns: Vec<ColumnData>,
    pub processed_length: usize, // How many bytes were processed
}

/// Types of logical replication messages
#[derive(Debug)]
pub enum ReplicationMessage {
    Begin {
        final_lsn: u64,
        timestamp: i64,
        xid: Xid,
    },
    Commit {
        flags: u8,
        commit_lsn: u64,
        end_lsn: u64,
        timestamp: i64,
    },
    Relation {
        relation: RelationInfo,
    },
    Insert {
        relation_id: Oid,
        tuple_data: TupleData,
        is_stream: bool,
        xid: Option<Xid>,
    },
    Update {
        relation_id: Oid,
        key_type: Option<char>, // 'K' for replica identity, 'O' for old tuple
        old_tuple_data: Option<TupleData>,
        new_tuple_data: TupleData,
        is_stream: bool,
        xid: Option<Xid>,
    },
    Delete {
        relation_id: Oid,
        key_type: char, // 'K' for replica identity, 'O' for old tuple
        tuple_data: TupleData,
        is_stream: bool,
        xid: Option<Xid>,
    },
    Truncate {
        relation_ids: Vec<Oid>,
        flags: i8,
        is_stream: bool,
        xid: Option<Xid>,
    },
    StreamStart {
        xid: Xid,
        first_segment: bool,
    },
    StreamStop,
    StreamCommit {
        xid: Xid,
        flags: u8,
        commit_lsn: u64,
        end_lsn: u64,
        timestamp: i64,
    },
    StreamAbort {
        xid: Xid,
        subtransaction_xid: Xid,
    },
}

/// State for managing logical replication
#[derive(Debug)]
pub struct ReplicationState {
    pub relations: HashMap<Oid, RelationInfo>,
    pub received_lsn: u64,
    pub flushed_lsn: u64,
    pub last_feedback_time: std::time::Instant,
    pub in_streaming_txn: bool,
    pub streaming_xid: Option<Xid>,
}

impl ReplicationState {
    pub fn new() -> Self {
        Self {
            relations: HashMap::new(),
            received_lsn: 0,
            flushed_lsn: 0,
            last_feedback_time: std::time::Instant::now(),
            in_streaming_txn: false,
            streaming_xid: None,
        }
    }

    pub fn start_streaming(&mut self, xid: Xid) {
        self.in_streaming_txn = true;
        self.streaming_xid = Some(xid);
    }

    pub fn stop_streaming(&mut self) {
        self.in_streaming_txn = false;
        self.streaming_xid = None;
    }

    pub fn add_relation(&mut self, relation: RelationInfo) {
        self.relations.insert(relation.oid, relation);
    }

    pub fn get_relation(&self, oid: Oid) -> Option<&RelationInfo> {
        self.relations.get(&oid)
    }

    pub fn update_lsn(&mut self, lsn: u64) {
        if lsn > 0 {
            self.received_lsn = std::cmp::max(self.received_lsn, lsn);
        }
    }
}

impl Default for ReplicationState {
    fn default() -> Self {
        Self::new()
    }
}

/// Configuration for the replication checker with validation
#[derive(Debug)]
pub struct ReplicationConfig {
    pub connection_string: String,
    pub publication_name: String,
    pub slot_name: String,
    pub feedback_interval_secs: u64,
}

impl ReplicationConfig {
    /// Create a new ReplicationConfig with validation
    pub fn new(
        connection_string: String,
        publication_name: String,
        slot_name: String,
    ) -> crate::errors::Result<Self> {
        // Basic validation
        if connection_string.trim().is_empty() {
            return Err(crate::errors::ReplicationError::config(
                "Connection string cannot be empty",
            ));
        }

        if publication_name.trim().is_empty() {
            return Err(crate::errors::ReplicationError::config(
                "Publication name cannot be empty",
            ));
        }

        if slot_name.trim().is_empty() {
            return Err(crate::errors::ReplicationError::config(
                "Slot name cannot be empty",
            ));
        }

        // Validate slot name format (PostgreSQL naming rules)
        if !slot_name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_')
        {
            return Err(crate::errors::ReplicationError::config(
                "Slot name can only contain alphanumeric characters and underscores",
            ));
        }

        if slot_name.len() > 63 {
            // PostgreSQL identifier length limit
            return Err(crate::errors::ReplicationError::config(
                "Slot name cannot be longer than 63 characters",
            ));
        }

        Ok(Self {
            connection_string,
            publication_name,
            slot_name,
            feedback_interval_secs: 1, // Send feedback every second
        })
    }
}
