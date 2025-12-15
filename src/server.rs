//! PostgreSQL replication server implementation
//! Main server that handles connection, replication slot management, and message processing

use crate::buffer::{BufferReader, BufferWriter};
use crate::errors::Result;
use crate::parser::MessageParser;
use crate::types::*;
use crate::utils::{format_timestamp_from_pg, system_time_to_postgres_timestamp, PGConnection, INVALID_XLOG_REC_PTR};
use std::time::{Duration, Instant, SystemTime};
use tracing::{debug, error, info, warn};

pub struct ReplicationServer {
    connection: PGConnection,
    config: ReplicationConfig,
    state: ReplicationState,
}

impl ReplicationServer {
    pub fn new(config: ReplicationConfig) -> Result<Self> {
        let connection = PGConnection::connect(&config.connection_string)?;
        info!("Successfully connected to database server");

        Ok(Self {
            connection,
            config,
            state: ReplicationState::new(),
        })
    }

    pub fn identify_system(&self) -> Result<()> {
        debug!("Identifying system");
        match self.connection.exec("IDENTIFY_SYSTEM") {
            Ok(result) => {
                let status = result.status();
                if result.is_ok() && result.ntuples() > 0 {
                    let system_id = result.getvalue(0, 0);
                    let timeline = result.getvalue(0, 1); 
                    let xlogpos = result.getvalue(0, 2);
                    let dbname = result.getvalue(0, 3);
                    info!("IDENTIFY_SYSTEM succeeded: status: {:?}, system_id: {:?}, timeline: {:?}, xlogpos: {:?}, dbname: {:?}", 
                        status, system_id, timeline, xlogpos, dbname);
                } else {
                    return Err(crate::errors::ReplicationError::protocol(format!(
                        "IDENTIFY_SYSTEM failed: status: {:?}, rows: {}, columns: {}. This usually means the connection is not in replication mode or lacks replication privileges.",
                        status, result.ntuples(), result.nfields()
                    )));
                }
            }
            Err(err) => {
                return Err(crate::errors::ReplicationError::protocol(format!(
                    "IDENTIFY_SYSTEM command failed: {}",
                    err
                )));
            }
        }

        info!("System identification successful");
        Ok(())
    }

    pub async fn create_replication_slot_and_start(&mut self) -> Result<()> {
        self.create_replication_slot()?;
        self.start_replication().await?;
        Ok(())
    }

    fn create_replication_slot(&self) -> Result<()> {
        // https://www.postgresql.org/docs/14/protocol-replication.html
        let create_slot_sql = format!(
            "CREATE_REPLICATION_SLOT \"{}\" LOGICAL pgoutput NOEXPORT_SNAPSHOT;",
            self.config.slot_name
        );

        info!("Creating replication slot: {}", self.config.slot_name);
        let result = self.connection.exec(&create_slot_sql)?;

        if !result.is_ok() {
            warn!("Replication slot creation may have failed, but continuing");
        } else {
            info!("Replication slot created successfully");
        }

        Ok(())
    }

    async fn start_replication(&mut self) -> Result<()> {
        /*
        proto_version
            Protocol version. Currently versions 1, 2, 3, and 4 are supported. A valid version is required.
            Version 2 is supported only for server version 14 and above, and it allows streaming of large in-progress transactions.
            Version 3 is supported only for server version 15 and above, and it allows streaming of two-phase commits.
            Version 4 is supported only for server version 16 and above, and it allows streams of large in-progress transactions to be applied in parallel.
        https://www.postgresql.org/docs/current/protocol-logical-replication.html#PROTOCOL-LOGICAL-REPLICATION-PARAMS
        */
        let start_replication_sql = format!(
            "START_REPLICATION SLOT \"{}\" LOGICAL 0/0 (proto_version '2', streaming 'on', publication_names '\"{}\"');",
            self.config.slot_name,
            self.config.publication_name
        );

        info!(
            "Starting replication with publication: {}, executing SQL: {}",
            self.config.publication_name, start_replication_sql
        );
        let _ = self.connection.exec(&start_replication_sql)?;

        info!("Started receiving data from database server");
        self.replication_loop().await?;
        Ok(())
    }

    async fn replication_loop(&mut self) -> Result<()> {
        loop {
            self.check_and_send_feedback()?;

            match self.connection.get_copy_data(0)? {
                None => {
                    info!("No data received, continuing");
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    continue;
                }
                Some(data) => {
                    if data.is_empty() {
                        continue;
                    }
                    
                    // please refer to https://www.postgresql.org/docs/current/protocol-replication.html#PROTOCOL-REPLICATION-XLOGDATA
                    match data[0] as char {
                        'k' => {
                            self.process_keepalive_message(&data)?;
                        }
                        'w' => {
                            self.process_wal_message(&data)?;
                        }
                        _ => {
                            warn!("Received unknown message type: {}", data[0] as char);
                        }
                    }
                }
            }
        }
    }

    fn process_keepalive_message(&mut self, data: &[u8]) -> Result<()> {
        if data.len() < 18 {
            // 'k' + 8 bytes LSN + 8 bytes timestamp + 1 byte reply flag
            return Err(crate::errors::ReplicationError::protocol(
                "Keepalive message too short",
            ));
        }

        debug!("Processing keepalive message");

        let mut reader = BufferReader::new(data);
        let _msg_type = reader.skip_message_type()?; // Skip 'k'
        let log_pos = reader.read_u64()?;

        self.state.update_lsn(log_pos);

        self.send_feedback()?;
        Ok(())
    }

    fn process_wal_message(&mut self, data: &[u8]) -> Result<()> {
        if data.len() < 25 {
            // 'w' + 8 + 8 + 8 + at least 1 byte data
            return Err(crate::errors::ReplicationError::protocol(
                "WAL message too short",
            ));
        }

        let mut reader = BufferReader::new(data);
        let _msg_type = reader.skip_message_type()?; // Skip 'w'

        // Parse WAL message header
        let data_start = reader.read_u64()?;
        let _wal_end = reader.read_u64()?;
        let _send_time = reader.read_i64()?;

        if data_start > 0 {
            self.state.update_lsn(data_start);
        }

        if reader.remaining() == 0 {
            return Err(crate::errors::ReplicationError::protocol(
                "WAL message has no data",
            ));
        }

        // Parse the actual logical replication message
        let message_data = &data[reader.position()..];
        match MessageParser::parse_wal_message(message_data, self.state.in_streaming_txn) {
            Ok(message) => {
                self.process_replication_message(message)?;
            }
            Err(e) => {
                error!("Failed to parse replication message: {}", e);
                return Err(e);
            }
        }

        self.send_feedback()?;
        Ok(())
    }

    fn process_replication_message(&mut self, message: ReplicationMessage) -> Result<()> {
        match message {
            ReplicationMessage::Begin { xid, .. } => {
                info!("BEGIN: Xid {}", xid);
            }

            ReplicationMessage::Commit { 
                flags,
                commit_lsn,
                end_lsn,
                timestamp,
             } => {
                info!("COMMIT: flags: {}, lsn: {}, end_lsn: {}, commit_time: {}", flags, commit_lsn, end_lsn, format_timestamp_from_pg(timestamp));
            }

            ReplicationMessage::Relation { relation } => {
                // info!(
                //     "Received relation info for {}.{}",
                //     relation.namespace, relation.relation_name
                // );
                self.state.add_relation(relation);
            }

            ReplicationMessage::Insert {
                relation_id,
                tuple_data,
                is_stream,
                xid,
            } => {
                if let Some(relation) = self.state.get_relation(relation_id) {
                    if is_stream {
                        if let Some(xid) = xid {
                            info!("Streaming, Xid: {} ", xid);
                        }
                    }
                    info!(
                        "table {}.{}: INSERT: ",
                        relation.namespace, relation.relation_name
                    );
                    self.info_tuple_data(relation, &tuple_data)?;
                } else {
                    error!("Received INSERT for unknown relation: {}", relation_id);
                }
            }

            ReplicationMessage::Update {
                relation_id,
                key_type,
                old_tuple_data,
                new_tuple_data,
                is_stream,
                xid,
            } => {
                if let Some(relation) = self.state.get_relation(relation_id) {
                    if is_stream {
                        if let Some(xid) = xid {
                            info!("Streaming, Xid: {} ", xid);
                        }
                    }
                    info!(
                        "table {}.{} UPDATE ",
                        relation.namespace, relation.relation_name
                    );

                    if let Some(old_data) = old_tuple_data {
                        let key_info = match key_type {
                            Some('K') => "INDEX: ",
                            Some('O') => "REPLICA IDENTITY: ",
                            _ => "",
                        };
                        info!("Old {}: ", key_info);
                        self.info_tuple_data(relation, &old_data)?;
                    } 

                    info!("New Row: ");
                    self.info_tuple_data(relation, &new_tuple_data)?;
                } else {
                    error!("Received UPDATE for unknown relation: {}", relation_id);
                }
            }

            ReplicationMessage::Delete {
                relation_id,
                key_type,
                tuple_data,
                is_stream,
                xid,
            } => {
                if let Some(relation) = self.state.get_relation(relation_id) {
                    if is_stream {
                        if let Some(xid) = xid {
                            info!("Streaming, Xid: {} ", xid);
                        }
                    }
                    let key_info = match key_type {
                        'K' => "INDEX",
                        'O' => "REPLICA IDENTITY",
                        _ => "UNKNOWN",
                    };
                    info!(
                        "table {}.{}: DELETE: ({}): ",
                        relation.namespace, relation.relation_name, key_info
                    );
                    self.info_tuple_data(relation, &tuple_data)?;
                } else {
                    error!("Received DELETE for unknown relation: {}", relation_id);
                }
            }

            ReplicationMessage::Truncate {
                relation_ids,
                flags,
                is_stream,
                xid,
            } => {
                if is_stream {
                    if let Some(xid) = xid {
                        info!("Streaming, Xid: {} ", xid);
                    }
                }

                let flag_info = match flags {
                    1 => "CASCADE ",
                    2 => "RESTART IDENTITY ",
                    _ => "",
                };

                info!("TRUNCATE {}", flag_info);
                for relation_id in relation_ids {
                    if let Some(relation) = self.state.get_relation(relation_id) {
                        info!("{}.{} ", relation.namespace, relation.relation_name);
                    } else {
                        info!("UNKNOWN_RELATION({}) ", relation_id);
                    }
                }
            }

            ReplicationMessage::StreamStart { xid, .. } => {
                info!("Opening a streamed block for transaction {}", xid);
                self.state.start_streaming(xid);
            }

            ReplicationMessage::StreamStop => {
                info!("Stream Stop");
                self.state.stop_streaming();
            }

            ReplicationMessage::StreamCommit { xid, .. } => {
                info!("Committing streamed transaction {}\n", xid);
                self.state.stop_streaming();
            }

            ReplicationMessage::StreamAbort { xid, .. } => {
                info!("Aborting streamed transaction {}", xid);
                self.state.stop_streaming();
            }
        }

        Ok(())
    }

    fn info_tuple_data(&self, relation: &RelationInfo, tuple_data: &TupleData) -> Result<()> {
        let line: String = tuple_data
            .columns
            .iter()
            .enumerate()
            .filter_map(|(i, column_data)| {
                if column_data.data_type == 'n' || i >= relation.columns.len() {
                    None
                } else {
                    Some(format!("{}: {}", relation.columns[i].column_name, column_data.data))
                }
            })
            .collect::<Vec<_>>()
            .join(", ");

        info!("[{}]", line);
        Ok(())
    }

    fn send_feedback(&mut self) -> Result<()> {
        if self.state.received_lsn == 0 {
            return Ok(());
        }

        let now = SystemTime::now();
        let timestamp = system_time_to_postgres_timestamp(now);
        let mut reply_buf = [0u8; 34]; // 1 + 8 + 8 + 8 + 8 + 1
        let bytes_written = {
            let mut writer = BufferWriter::new(&mut reply_buf);

            writer.write_u8(b'r')?;
            writer.write_u64(self.state.received_lsn)?; // Received LSN
            writer.write_u64(self.state.received_lsn)?; // Flushed LSN (same as received)
            writer.write_u64(INVALID_XLOG_REC_PTR)?; // Applied LSN (not tracking)
            writer.write_i64(timestamp)?; // Timestamp
            writer.write_u8(0)?; // Don't request reply

            writer.bytes_written()
        };

        self.connection.put_copy_data(&reply_buf[..bytes_written])?;
        self.connection.flush()?;

        debug!("Sent feedback with LSN: {}", self.state.received_lsn);
        Ok(())
    }

    fn check_and_send_feedback(&mut self) -> Result<()> {
        let now = Instant::now();
        if now.duration_since(self.state.last_feedback_time)
            > Duration::from_secs(self.config.feedback_interval_secs)
        {
            self.send_feedback()?;
            self.state.last_feedback_time = now;
        }
        Ok(())
    }
}
