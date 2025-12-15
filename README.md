# PostgreSQL Replication Checker - Rust Edition

A Rust implementation of a PostgreSQL logical replication client that connects to a database, creates replication slots, and displays changes in real-time. This is a Rust port of the original C++ implementation from [https://github.com/fkfk000/replication_checker](https://github.com/fkfk000/replication_checker).

>　https://www.postgresql.org/docs/current/protocol-replication.html

## Features

- **Logical Replication**: Connects to PostgreSQL as a replication client using the logical replication protocol
- **Real-time Change Display**: Shows INSERT, UPDATE, DELETE, TRUNCATE operations as they happen
- **Streaming Transaction Support**: Handles both regular and streaming (large) transactions
- **Built with libpq-sys**: Uses low-level PostgreSQL libpq bindings for maximum performance and control
- **Comprehensive Logging**: Uses tracing for structured logging and debugging

## Prerequisites

- PostgreSQL server with logical replication enabled (`wal_level = logical`)
- A publication created on the source database
- libpq development libraries installed on your system
- Rust 1.70+ with Cargo

### PostgreSQL Setup

1. Enable logical replication in your PostgreSQL configuration:
   ```sql
   ALTER SYSTEM SET wal_level = logical;
   -- Restart PostgreSQL server after this change
   ```

2. Create a publication for the tables you want to replicate:
   ```sql
   CREATE PUBLICATION my_publication FOR TABLE table1, table2;
   -- Or for all tables:
   CREATE PUBLICATION my_publication FOR ALL TABLES;
   ```

3. Create a user with replication privileges:
   ```sql
   CREATE USER replicator WITH REPLICATION LOGIN PASSWORD 'password';
   GRANT SELECT ON ALL TABLES IN SCHEMA public TO replicator;
   ```

## Installation

> [!WARNING]
> Please note : PostgreSQL DB version must equal or higher to version 14, more information refer to below link. 
> * https://www.postgresql.org/docs/14/protocol-replication.html
> * https://www.postgresql.org/docs/current/protocol-logical-replication.html#PROTOCOL-LOGICAL-REPLICATION-PARAMS


### From Source

```bash
git clone https://github.com/isdaniel/replication_checker_rs.git
cd replication_checker_rs
cargo build --release
```

### Using Docker

```bash
docker build -t pg_replica_rs .
```

### System Dependencies

Make sure you have libpq development libraries installed:

**Ubuntu/Debian:**
```bash
sudo apt-get install libpq-dev \
    clang \
    libclang-dev 
```

**CentOS/RHEL/Fedora:**
```bash
sudo yum install postgresql-devel
# or
sudo dnf install postgresql-devel
```

**macOS:**
```bash
brew install postgresql
```

## Usage

### Basic Usage

The replication checker now uses environment variables for all configuration. You must set the `DB_CONNECTION_STRING` environment variable with a PostgreSQL connection string:

```bash
# Using environment variables (recommended approach)
export DB_CONNECTION_STRING="postgresql://username:password@host:port/database?replication=database"
export slot_name="my_slot"
export pub_name="my_publication"
./target/release/pg_replica_rs

# Example with full connection string
export DB_CONNECTION_STRING="postgresql://postgres:test.123@127.0.0.1:5432/postgres?replication=database"
export slot_name="cdc_slot1"
export pub_name="cdc_pub"
./target/release/pg_replica_rs

# Using a .env file for configuration
set -a; source .env; set +a
./target/release/pg_replica_rs
```

### Connection String Format

The `DB_CONNECTION_STRING` must be a valid PostgreSQL connection string with **the `replication=database` parameter for logical replication:**

```bash
# Basic format
DB_CONNECTION_STRING="postgresql://username:password@host:port/database?replication=database"

# With SSL settings  
DB_CONNECTION_STRING="postgresql://username:password@host:port/database?replication=database&sslmode=require"

# With connection timeout
DB_CONNECTION_STRING="postgresql://username:password@host:port/database?replication=database&connect_timeout=10"

# Examples
DB_CONNECTION_STRING="postgresql://replicator:secret@localhost:5432/mydb?replication=database"
DB_CONNECTION_STRING="postgresql://postgres:test.123@127.0.0.1:5432/postgres?replication=database&sslmode=prefer"
```

### Docker Usage

```bash
# Basic run with environment variables
docker run \
  -e DB_CONNECTION_STRING="postgresql://postgres:secret@host.docker.internal:5432/mydb?replication=database" \
  -e slot_name=my_slot \
  -e pub_name=my_pub \
  pg_replica_rs

# With custom logging configuration
docker run \
  -e DB_CONNECTION_STRING="postgresql://postgres:secret@host.docker.internal:5432/mydb?replication=database" \
  -e slot_name=my_slot \
  -e pub_name=my_pub \
  -e LOG_OUTPUT=all \
  -e LOG_JSON_FORMAT=true \
  -v $(pwd)/logs:/app/logs \
  pg_replica_rs
```

### Environment Variables Configuration

**Required Environment Variables:**
- `DB_CONNECTION_STRING`: PostgreSQL connection string with `replication=database` parameter (required)

**Replication Configuration:**
- `slot_name`: Name of the replication slot to create/use (default: "sub")
- `pub_name`: Name of the publication to subscribe to (default: "pub")

**Logging Configuration:**
- `LOG_OUTPUT`: Where to send logs - `console`, `file`, or `all` (default: console)
- `LOG_DIRECTORY`: Directory for log files (default: "./logs")
- `LOG_FILE_PREFIX`: Prefix for log file names (default: "replication")
- `LOG_ROTATION`: Log rotation policy - `never`, `hourly`, `daily`, `weekly` (default: daily)
- `LOG_CONSOLE_LEVEL`: Console log level (default: info)
- `LOG_FILE_LEVEL`: File log level (default: debug)
- `LOG_JSON_FORMAT`: Enable JSON format for file logs - `true`/`false` (default: false)
- `LOG_ANSI_ENABLED`: Enable colors in console output - `true`/`false` (default: true)

**Legacy Logging Control:**
- `RUST_LOG`: Traditional Rust log level control (overrides other settings if used)

### Advanced Logging

The application supports sophisticated logging configurations:

```bash
# Console only with info level
LOG_OUTPUT=console LOG_CONSOLE_LEVEL=info ./target/release/pg_replica_rs ...

# File only with JSON format for analysis
LOG_OUTPUT=file LOG_JSON_FORMAT=true LOG_FILE_LEVEL=debug ./target/release/pg_replica_rs ...

# Both console and file with different levels
LOG_OUTPUT=all LOG_CONSOLE_LEVEL=info LOG_FILE_LEVEL=debug ./target/release/pg_replica_rs ...

# Custom log directory and file prefix
LOG_OUTPUT=file LOG_DIRECTORY=/var/log/postgres LOG_FILE_PREFIX=replication ./target/release/pg_replica_rs ...
```

## Example Output

### Console Output

When running, you'll see structured output like:

```
2024-09-07T10:30:00.123Z INFO pg_replica_rs: Logging initialized: output=Console, json=false
2024-09-07T10:30:00.124Z INFO pg_replica_rs: Slot name: my_slot  
2024-09-07T10:30:00.124Z INFO pg_replica_rs: Publication name: my_publication
2024-09-07T10:30:00.125Z INFO pg_replica_rs: Connection string: user=postgres password=*** host=localhost port=5432 dbname=testdb
2024-09-07T10:30:00.125Z INFO pg_replica_rs: Successfully connected to database server
2024-09-07T10:30:00.126Z INFO pg_replica_rs: System identification successful
2024-09-07T10:30:00.127Z INFO pg_replica_rs: Creating replication slot: my_slot
2024-09-07T10:30:00.128Z INFO pg_replica_rs: Started receiving data from database server

2024-09-07T10:30:01.200Z INFO pg_replica_rs: BEGIN: Xid 12345
2024-09-07T10:30:01.201Z INFO pg_replica_rs: table public.users: INSERT: id: 100 name: John Doe email: john@example.com 
2024-09-07T10:30:01.202Z INFO pg_replica_rs: table public.orders: INSERT: id: 50 user_id: 100 amount: 99.99 
2024-09-07T10:30:01.203Z INFO pg_replica_rs: COMMIT: flags: 0, lsn: 0/1A2B3C4D, end_lsn: 0/1A2B3C5D, commit_time: 2024-09-07T10:30:01.203Z

2024-09-07T10:30:02.300Z INFO pg_replica_rs: BEGIN: Xid 12346
2024-09-07T10:30:02.301Z INFO pg_replica_rs: table public.users UPDATE Old REPLICA IDENTITY: id: 100 New Row: id: 100 name: John Smith email: john.smith@example.com 
2024-09-07T10:30:02.302Z INFO pg_replica_rs: COMMIT: flags: 0, lsn: 0/1A2B3C6D, end_lsn: 0/1A2B3C7D, commit_time: 2024-09-07T10:30:02.302Z
```

### JSON Log Output

When `LOG_JSON_FORMAT=true` is set, structured JSON logs are generated:

```json
{
  "timestamp": "2024-09-07T10:30:01.201Z",
  "level": "INFO",
  "fields": {
    "event_type": "table_operation",
    "operation": "INSERT",
    "table_schema": "public",
    "table_name": "users",
    "data": {
      "id": "100",
      "name": "John Doe",
      "email": "john@example.com"
    },
    "message": "Table operation: INSERT on public.users"
  },
  "target": "pg_replica_rs"
}

{
  "timestamp": "2024-09-07T10:30:01.203Z",
  "level": "INFO", 
  "fields": {
    "event_type": "transaction_commit",
    "flags": 0,
    "commit_lsn": "0/1A2B3C4D",
    "end_lsn": "0/1A2B3C5D",
    "commit_time": "2024-09-07T10:30:01.203Z",
    "message": "Transaction committed with details"
  },
  "target": "pg_replica_rs"
}
```

## Architecture

The implementation consists of several well-organized modules:

- **`main.rs`**: Application entry point with clap-based argument parsing and async runtime setup
- **`server.rs`**: Main replication server that manages PostgreSQL connection and message processing
- **`parser.rs`**: Protocol message parser for PostgreSQL logical replication messages
- **`types.rs`**: Data structures for relations, tuples, and replication messages
- **`utils.rs`**: Utility functions for connection management, byte manipulation, and PostgreSQL integration
- **`logging.rs`**: Advanced logging configuration with support for console, file, and JSON output
- **`errors.rs`**: Comprehensive error handling with detailed error types
- **`buffer.rs`**: Efficient buffer reading and writing for PostgreSQL protocol messages

### Key Components

1. **Connection Management**: Safe wrapper around libpq connections with proper resource cleanup
2. **Protocol Parsing**: Complete implementation of PostgreSQL logical replication protocol v2
3. **Message Processing**: Handlers for all logical replication message types (BEGIN, COMMIT, INSERT, UPDATE, DELETE, TRUNCATE, streaming transactions)
4. **Feedback System**: Implements the feedback protocol to acknowledge processed WAL positions
5. **Logging System**: Structured logging with JSON support for monitoring and analytics
6. **CLI Interface**: Modern command-line interface with proper help and validation

## Supported Operations

- **BEGIN** - Transaction start with structured logging
- **COMMIT** - Transaction commit with LSN tracking and timestamps
- **INSERT** - Row insertions with JSON data logging
- **UPDATE** - Row updates (with old/new value support and change tracking)
- **DELETE** - Row deletions with replica identity handling
- **TRUNCATE** - Table truncation with cascade and restart identity support
- **RELATION** - Table schema information with column metadata
- **Streaming Transactions** - Large transaction support (protocol v2)
- **Stream Start/Stop/Commit/Abort** - Streaming transaction lifecycle
- **Keep-alive** - Connection health monitoring with automatic feedback

### Enhanced Logging System
- **Flexible Output**: Choose between console, file, or both
- **JSON Logging**: Structured logs for monitoring and analytics integration
- **Log Rotation**: Daily, hourly, or custom rotation policies
- **Configurable Levels**: Different log levels for console vs. file output
- **Structured Events**: Dedicated macros for transaction and table operation logging

### Better CLI Experience
- **Modern Interface**: Built with clap for better help and validation
- **Flexible Parameters**: Support for all PostgreSQL connection parameters
- **Environment Integration**: Easy configuration via environment variables

## Limitations

- Currently displays changes in human-readable format (console) or structured JSON (file logging)
- Text data type display optimization (binary types show as raw data)
- Basic replication slot management (creates slot, manual cleanup required on exit)
- Minimal error recovery (will exit on critical errors, but with detailed error context)

## Troubleshooting

### Connection Issues

1. **"Connection failed"**: Check your PostgreSQL server is running and accessible
2. **"Permission denied"**: Ensure your user has REPLICATION privilege
3. **"Replication slot creation failed"**: The slot might already exist, or you lack privileges

### Compilation Issues

1. **"libpq not found"**: Install PostgreSQL development libraries
2. **"linking failed"**: Ensure libpq is in your library path

### Runtime Issues

1. **No data received**: Check that your publication includes the tables being modified
2. **"Unknown relation"**: The replication stream may be out of sync; restart the application
3. **Log files not created**: Check LOG_DIRECTORY permissions and path validity

### Performance Issues

1. **High memory usage**: Check LOG_ROTATION settings and ensure log files are being rotated
2. **Slow startup**: Database connection might be slow; check network connectivity and timeouts

### Common Configuration Mistakes

```bash
# Wrong: Using old environment variable format
RUST_LOG=debug ./target/release/pg_replica_rs ...

# Right: Using new logging configuration
LOG_OUTPUT=all LOG_CONSOLE_LEVEL=info LOG_FILE_LEVEL=debug ./target/release/pg_replica_rs ...

# Wrong: Forgetting to set publication and slot names
./target/release/pg_replica_rs user postgres ...

# Right: Setting required environment variables
export slot_name="my_slot"
export pub_name="my_publication"  
./target/release/pg_replica_rs user postgres ...
```

## Testing & Verification

### Quick Test Setup

1. **Start a PostgreSQL instance with logical replication:**
```sql
-- Enable logical replication
ALTER SYSTEM SET wal_level = logical;
-- Restart PostgreSQL after this change

-- Create test database and user
CREATE DATABASE test_replication;
CREATE USER repl_user WITH REPLICATION LOGIN PASSWORD 'repl_pass';
GRANT CONNECT ON DATABASE test_replication TO repl_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO repl_user;
```

2. **Create test table and publication:**
```sql
\c test_replication

CREATE TABLE test_table (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE PUBLICATION test_pub FOR ALL TABLES;
```

3. **Run the replication checker:**
```bash
export slot_name="test_slot"
export pub_name="test_pub"
export LOG_OUTPUT=console
export LOG_CONSOLE_LEVEL=debug

./target/release/pg_replica_rs user repl_user password repl_pass \
  host localhost port 5432 dbname test_replication
```

4. **Generate test data in another terminal:**
```sql
-- Connect to the test database
\c test_replication

-- Insert some data
INSERT INTO test_table (name) VALUES ('Alice'), ('Bob'), ('Charlie');

-- Update data
UPDATE test_table SET name = 'Alice Smith' WHERE id = 1;

-- Delete data  
DELETE FROM test_table WHERE id = 3;

-- Truncate table
TRUNCATE test_table;
```

### Verifying Logs

Check that you see output similar to:
```
INFO pg_replica_rs: BEGIN: Xid 12345
INFO pg_replica_rs: table public.test_table: INSERT: id: 1 name: Alice ...
INFO pg_replica_rs: COMMIT: flags: 0, lsn: 0/1A2B3C4D ...
```

## Dependencies

The project uses the following key dependencies:

- **libpq-sys** (0.8): Low-level PostgreSQL libpq bindings
- **tokio** (1.47.1): Async runtime with full features
- **tracing** (0.1): Structured logging and tracing
- **tracing-subscriber** (0.3): Log formatting and filtering with chrono and JSON support
- **tracing-appender** (0.2.3): Log file rotation and management
- **chrono** (0.4): DateTime handling with serde support
- **thiserror** (2.0.12): Ergonomic error handling
- **anyhow** (1.0): Error context and chaining

## License

This project is licensed under the same terms as the original C++ implementation.

## Acknowledgments

- Based on the original C++ implementation by [fkfk000](https://github.com/fkfk000/replication_checker)
- Uses the excellent [libpq-sys](https://crates.io/crates/libpq-sys) crate for PostgreSQL integration
- Built with the [Tokio](https://tokio.rs/) async runtime
- Logging powered by [tracing](https://tracing.rs/) ecosystem for structured observability
