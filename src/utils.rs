//! Utility functions for PostgreSQL replication
//! Contains helper functions for byte manipulation, timestamp conversion, and other utilities

use crate::errors::Result;
use chrono::DateTime;
use libpq_sys::*;
use std::ffi::{CStr, CString};
use std::ptr;
use std::time::{ SystemTime, UNIX_EPOCH};
use tracing::warn;

// PostgreSQL epoch constants
const PG_EPOCH_OFFSET_SECS: i64 = 946_684_800; // Seconds from 1970 to 2000

// Type aliases to match PostgreSQL types
pub type XLogRecPtr = u64;
pub type Xid = u32;
pub type Oid = u32;
pub type TimestampTz = i64;

pub const INVALID_XLOG_REC_PTR: XLogRecPtr = 0;

/// Convert SystemTime to PostgreSQL timestamp format
pub fn system_time_to_postgres_timestamp(time: SystemTime) -> TimestampTz {
    let duration_since_unix = time
        .duration_since(UNIX_EPOCH)
        .expect("SystemTime is before Unix epoch");

    let unix_secs = duration_since_unix.as_secs() as i64;
    let unix_micros =
        unix_secs * 1_000_000 + (duration_since_unix.subsec_micros() as i64);

    // Shift Unix epoch to PostgreSQL epoch
    unix_micros - PG_EPOCH_OFFSET_SECS * 1_000_000
}

/// Read a value from buffer with proper endianness handling
pub fn buf_recv<T>(buf: &[u8]) -> T
where
    T: Copy,
{
    assert!(buf.len() >= std::mem::size_of::<T>());

    unsafe {
        let mut val: T = std::mem::zeroed();
        std::ptr::copy_nonoverlapping(
            buf.as_ptr(),
            &mut val as *mut T as *mut u8,
            std::mem::size_of::<T>(),
        );
        val
    }
}

/// Specialized function for reading network byte order integers
pub fn buf_recv_u16(buf: &[u8]) -> u16 {
    assert!(buf.len() >= 2);
    u16::from_be_bytes(buf[..2].try_into().unwrap())
}

pub fn buf_recv_u32(buf: &[u8]) -> u32 {
    assert!(buf.len() >= 4);
    u32::from_be_bytes(buf[..4].try_into().unwrap())
}

pub fn buf_recv_u64(buf: &[u8]) -> u64 {
    assert!(buf.len() >= 8);
    u64::from_be_bytes(buf[..8].try_into().unwrap())
}

pub fn buf_recv_i16(buf: &[u8]) -> i16 {
    assert!(buf.len() >= 2);
    i16::from_be_bytes(buf[..2].try_into().unwrap())
}

pub fn buf_recv_i32(buf: &[u8]) -> i32 {
    assert!(buf.len() >= 4);
    i32::from_be_bytes(buf[..4].try_into().unwrap())
}

pub fn buf_recv_i64(buf: &[u8]) -> i64 {
    assert!(buf.len() >= 8);
    i64::from_be_bytes(buf[..8].try_into().unwrap())
}

/// Write a value to buffer with proper endianness handling
pub fn buf_send<T>(val: T, buf: &mut [u8])
where
    T: Copy,
{
    assert!(buf.len() >= std::mem::size_of::<T>());

    unsafe {
        std::ptr::copy_nonoverlapping(
            &val as *const T as *const u8,
            buf.as_mut_ptr(),
            std::mem::size_of::<T>(),
        );
    }
}

/// Specialized functions for writing network byte order integers
pub fn buf_send_u16(val: u16, buf: &mut [u8]) {
    assert!(buf.len() >= 2);
    let bytes = val.to_be_bytes();
    buf[0] = bytes[0];
    buf[1] = bytes[1];
}

pub fn buf_send_u32(val: u32, buf: &mut [u8]) {
    assert!(buf.len() >= 4);
    let bytes = val.to_be_bytes();
    buf[..4].copy_from_slice(&bytes);
}

pub fn buf_send_u64(val: u64, buf: &mut [u8]) {
    assert!(buf.len() >= 8);
    let bytes = val.to_be_bytes();
    buf[..8].copy_from_slice(&bytes);
}

pub fn buf_send_i16(val: i16, buf: &mut [u8]) {
    assert!(buf.len() >= 2);
    let bytes = val.to_be_bytes();
    buf[0] = bytes[0];
    buf[1] = bytes[1];
}

pub fn buf_send_i32(val: i32, buf: &mut [u8]) {
    assert!(buf.len() >= 4);
    let bytes = val.to_be_bytes();
    buf[..4].copy_from_slice(&bytes);
}

pub fn buf_send_i64(val: i64, buf: &mut [u8]) {
    assert!(buf.len() >= 8);
    let bytes = val.to_be_bytes();
    buf[..8].copy_from_slice(&bytes);
}


/// Safe wrapper for PostgreSQL connection
pub struct PGConnection {
    conn: *mut PGconn,
}

impl PGConnection {
    pub fn connect(conninfo: &str) -> Result<Self> {
        let c_conninfo = CString::new(conninfo)?;
        let conn = unsafe { PQconnectdb(c_conninfo.as_ptr()) };

        if conn.is_null() {
            return Err(crate::errors::ReplicationError::connection(
                "Failed to allocate connection object",
            ));
        }

        let status = unsafe { PQstatus(conn) };
        if status != ConnStatusType::CONNECTION_OK {
            let error_msg = unsafe {
                let error_ptr = PQerrorMessage(conn);
                if error_ptr.is_null() {
                    "Unknown connection error".to_string()
                } else {
                    CStr::from_ptr(error_ptr).to_string_lossy().into_owned()
                }
            };
            unsafe { PQfinish(conn) };
            return Err(crate::errors::ReplicationError::connection(format!(
                "Connection failed: {}",
                error_msg
            )));
        }

        Ok(Self { conn })
    }

    pub fn exec(&self, query: &str) -> Result<PGResult> {
        let c_query = CString::new(query)?;
        let result = unsafe { PQexec(self.conn, c_query.as_ptr()) };

        if result.is_null() {
            return Err(crate::errors::ReplicationError::protocol(
                "Query execution failed",
            ));
        }

        Ok(PGResult { result })
    }

    fn get_error_message(&self) -> String {
        unsafe {
            let error_ptr = PQerrorMessage(self.conn);
            if error_ptr.is_null() {
                "Unknown error".to_string()
            } else {
                CStr::from_ptr(error_ptr)
                    .to_string_lossy()
                    .into_owned()
                    .trim()
                    .to_string()
            }
        }
    }

    pub fn get_copy_data(&self, timeout: i32) -> Result<Option<Vec<u8>>> {
        let mut buffer: *mut std::os::raw::c_char = ptr::null_mut();
        let result = unsafe { PQgetCopyData(self.conn, &mut buffer, timeout) };

        match result {
            -2 => {
                let error_msg = self.get_error_message();
                Err(crate::errors::ReplicationError::protocol(format!(
                    "Copy operation failed: {}",
                    error_msg
                )))
            }
            -1 => Ok(None), // No more data
            0 => Ok(None),  // Timeout or no data available
            len => {
                if buffer.is_null() {
                    return Err(crate::errors::ReplicationError::buffer(
                        "Received null buffer",
                    ));
                }

                let data = unsafe {
                    std::slice::from_raw_parts(buffer as *const u8, len as usize).to_vec()
                };

                unsafe { PQfreemem(buffer as *mut std::os::raw::c_void) };
                Ok(Some(data))
            }
        }
    }

    pub fn put_copy_data(&self, data: &[u8]) -> Result<()> {
        let result = unsafe {
            PQputCopyData(
                self.conn,
                data.as_ptr() as *const std::os::raw::c_char,
                data.len() as i32,
            )
        };

        if result != 1 {
            return Err(crate::errors::ReplicationError::protocol(
                "Failed to send copy data",
            ));
        }

        Ok(())
    }

    pub fn flush(&self) -> Result<()> {
        let result = unsafe { PQflush(self.conn) };
        match result {
            0 => Ok(()), // Success or send queue is empty
            1 => {
                // Unable to send all data yet - this is normal for large transactions, the data is queued and will be sent later
                // This happens when the send buffer is full, data will be sent as the buffer drains
                Ok(())
            }
            -1 => {
                // Actual error occurred - get detailed error message
                let error_msg = self.get_error_message();
                Err(crate::errors::ReplicationError::protocol(format!(
                    "Failed to flush connection: {}",
                    error_msg
                )))
            }
            _ => {
                // Unexpected return value
                warn!("PQflush returned unexpected value: {}", result);
                Ok(())
            }
        }
    }
}

impl Drop for PGConnection {
    fn drop(&mut self) {
        if !self.conn.is_null() {
            unsafe { PQfinish(self.conn) };
        }
    }
}

/// Safe wrapper for PostgreSQL result
pub struct PGResult {
    result: *mut PGresult,
}

impl PGResult {
    pub fn status(&self) -> ExecStatusType {
        unsafe { PQresultStatus(self.result) }
    }

    pub fn is_ok(&self) -> bool {
        matches!(
            self.status(),
            ExecStatusType::PGRES_TUPLES_OK | ExecStatusType::PGRES_COMMAND_OK
        )
    }

    pub fn ntuples(&self) -> i32 {
        unsafe { PQntuples(self.result) }
    }

    pub fn nfields(&self) -> i32 {
        unsafe { PQnfields(self.result) }
    }

    pub fn getvalue(&self, row: i32, col: i32) -> Option<String> {
        let value_ptr = unsafe { PQgetvalue(self.result, row, col) };
        if value_ptr.is_null() {
            None
        } else {
            unsafe { Some(CStr::from_ptr(value_ptr).to_string_lossy().into_owned()) }
        }
    }
}

impl Drop for PGResult {
    fn drop(&mut self) {
        if !self.result.is_null() {
            unsafe { PQclear(self.result) };
        }
    }
}


/// Convert a microsecond or nanosecond timestamp to a formatted UTC date string.
///
/// # Arguments
/// * `ts` - The timestamp value for microseconds
///
/// # Returns
/// A `String` in "YYYY-MM-DD HH:MM:SS.sss UTC" format.
pub fn format_timestamp_from_pg(ts: i64) -> String {

    let secs = ts / 1_000_000 + PG_EPOCH_OFFSET_SECS;
    let nsecs = (ts % 1_000_000) * 1_000;
    
    let datetime = DateTime::from_timestamp(secs, nsecs as u32)
        .expect("Invalid timestamp");

    datetime.format("%Y-%m-%d %H:%M:%S%.3f UTC").to_string()
}
