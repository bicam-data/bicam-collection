# Bicam Collection - Amendment Processing

This project processes JSON amendment data from the Congress.gov API into SQL tables defined in the staging schema (`staging_congressional`).

## Overview

The system reads from raw tables in the `bicam_raw_congressional` schema and processes them into structured tables in the `staging_congressional` schema. It supports processing:

- **Amendments** (`bicam_raw_congressional.amendments_raw`)
- **Amendment Actions** (`bicam_raw_congressional.amendments_actions_raw`)

## Setup

1. **Environment Variables**: Create a `.env` file with your database credentials:
   ```
   POSTGRESQL_HOST=your_host
   POSTGRESQL_PORT=5432
   POSTGRESQL_DATABASE=your_database
   POSTGRESQL_USERNAME=your_username
   POSTGRESQL_PASSWORD=your_password
   ```

2. **Database Schema**: Run the staging schema creation script:
   ```bash
   psql -h your_host -U your_username -d your_database -f build_staging_congressional.sql
   ```

3. **SQL Functions**: Load the processing functions:
   ```bash
   python process_amendments.py --table-type amendments --reload-functions
   ```

## Usage

### Generic Processor

The main script `process_amendments.py` is now a generic processor that can handle different table types:

```bash
# Process amendments
python process_amendments.py --table-type amendments --process-all

# Process amendment actions
python process_amendments.py --table-type amendment_actions --process-all

# Test with a single record
python process_amendments.py --table-type amendments --test
python process_amendments.py --table-type amendment_actions --test

# Get statistics
python process_amendments.py --table-type amendments --unprocessed-stats
python process_amendments.py --table-type amendment_actions --unprocessed-stats
```

### Command Line Options

- `--table-type`: Type of table to process (`amendments` or `amendment_actions`)
- `--process-all`: Process all records from the raw table
- `--from-raw-table`: Process from raw table with custom parameters
- `--batch-size`: Number of records to process in each batch (default: 1000)
- `--start-offset`: Starting offset for processing (default: 0)
- `--max-records`: Maximum number of records to process
- `--test`: Test with a single record
- `--unprocessed-stats`: Show unprocessed record statistics

### Examples

**Process all amendments:**
```bash
python process_amendments.py --table-type amendments --process-all
```

**Process amendment actions in batches:**
```bash
python process_amendments.py --table-type amendment_actions --from-raw-table --batch-size 500
```

**Test amendment processing:**
```bash
python process_amendments.py --table-type amendments --test
```

**Get statistics:**
```bash
python process_amendments.py --table-type amendments --unprocessed-stats
```

## Table Structure

### Amendments

The system creates the following tables for amendments:

- `staging_congressional.amendments` - Main amendment records
- `staging_congressional.amendments_sponsors` - Amendment sponsors
- `staging_congressional.amendments_amended_bills` - Bills amended by amendments
- `staging_congressional.amendments_amended_amendments` - Amendments amended by amendments
- `staging_congressional.amendments_amended_treaties` - Treaties amended by amendments

### Amendment Actions

The system creates the following tables for amendment actions:

- `staging_congressional.amendments_actions` - Main action records
- `staging_congressional.amendments_actions_recordedvotes` - Recorded votes for actions

## ID Formats

According to `formats.md`, the following ID formats are used:

- **Amendments**: `{lower(type)}{number}-{congress}` (e.g., `samdt2686-118`)
- **Bills**: `{lower(type)}{number}-{congress}` (e.g., `s4638-118`)
- **Treaties**: `td{congressreceived}-{number}` (e.g., `td118-123`)

## Error Handling

The system includes comprehensive error handling:

- Batch processing with rollback on errors
- Detailed logging of processing statistics
- Conflict resolution for duplicate records
- Graceful handling of malformed JSON data

## Development

### Testing

Test individual components:

```bash
# Test amendment processing
python test_single_amendment.py

# Test amendment actions processing
python process_amendments.py --table-type amendment_actions --test
```

### Debugging

Use the diagnostic scripts for troubleshooting:

```bash
# Debug specific errors
python debug_specific_errors.py

# Get detailed diagnostics
python debug_amendments.py
```

## File Structure

- `process_amendments.py` - Generic processor for all table types
- `convert_amendments_json_to_sql.sql` - SQL functions and procedures
- `build_staging_congressional.sql` - Database schema creation
- `test_single_amendment.py` - Single amendment testing
- `debug_amendments.py` - Diagnostic tools
- `formats.md` - ID format specifications
