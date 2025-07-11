# Amendment JSON to SQL Processing

This directory contains scripts to convert JSON amendment data from the Congress.gov API into structured SQL tables in the `staging_congressional` schema.

## Files

- `convert_amendments_json_to_sql.sql` - SQL functions for processing amendment JSON data
- `process_amendments.py` - Python script for processing amendments from various sources
- `test_amendment_processing.py` - Test script demonstrating usage with example data
- `README_amendments.md` - This documentation file

## Overview

The scripts handle the conversion of JSON amendment data into the following tables:

1. `staging_congressional.amendments` - Main amendment records
2. `staging_congressional.amendments_sponsors` - Amendment sponsors
3. `staging_congressional.amendments_amended_bills` - Bills that amendments modify
4. `staging_congressional.amendments_amended_amendments` - Amendments that modify other amendments
5. `staging_congressional.amendments_amended_treaties` - Treaties that amendments modify

## Data Source

The primary data source is `bicam_raw_congressional.amendments_raw` table, which contains JSON payloads from the Congress.gov API.

The scripts automatically filter out amendments that have already been processed by checking against the `bicam.amendments` table, ensuring no duplicate processing.

## Usage

### 1. Load SQL Functions

First, load the SQL functions into your database:

```sql
\i convert_amendments_json_to_sql.sql
```

### 2. Process from Raw Table

#### Using Python Script

```bash
# Process all amendments
python process_amendments.py \
    --db-name your_database \
    --db-user your_username \
    --db-password your_password \
    --process-all

# Process with custom batch size and limits
python process_amendments.py \
    --db-name your_database \
    --db-user your_username \
    --db-password your_password \
    --from-raw-table \
    --batch-size 500 \
    --start-offset 0 \
    --max-records 10000

# Show statistics after processing
python process_amendments.py \
    --db-name your_database \
    --db-user your_username \
    --db-password your_password \
    --process-all \
    --stats

# Check unprocessed amendment statistics
python process_amendments.py \
    --db-name your_database \
    --db-user your_username \
    --db-password your_password \
    --unprocessed-stats
```

#### Using SQL Directly

```sql
-- Process all amendments
SELECT * FROM process_all_amendments();

-- Process with custom parameters
SELECT * FROM process_amendments_from_raw_table(
    batch_size := 1000,
    start_offset := 0,
    max_records := 5000
);

-- Check unprocessed amendment statistics
SELECT * FROM get_unprocessed_amendment_stats();
```

### 3. Process from JSON File

```bash
python process_amendments.py \
    --db-name your_database \
    --db-user your_username \
    --db-password your_password \
    --json-file amendments.json \
    --batch-size 100
```

### 4. Process JSON Strings

```bash
python process_amendments.py \
    --db-name your_database \
    --db-user your_username \
    --db-password your_password \
    --json-strings '{"type":"SAMDT","number":"584",...}' '{"type":"SAMDT","number":"96",...}'
```

## Data Coverage

The scripts handle various amendment types and scenarios:

### Amendment Types
- `SAMDT` - Senate Amendment
- `HAMDT` - House Amendment
- `S` - Senate Bill Amendment
- `H` - House Bill Amendment
- And other variations

### Data Relationships
- **Sponsors**: Extracts bioguide IDs from sponsor arrays
- **Amended Bills**: Links amendments to bills they modify
- **Amended Amendments**: Handles amendments that modify other amendments
- **Amended Treaties**: Links amendments to treaties they modify
- **Counts**: Extracts action counts, cosponsor counts, etc.

### Robust Error Handling
- Validates required fields (type, number, congress, chamber)
- Handles missing or null values gracefully
- Continues processing even if individual records fail
- Provides detailed error reporting

## Database Schema

The scripts create data in the `staging_congressional` schema with the following structure:

### Main Amendment Table
```sql
CREATE TABLE staging_congressional.amendments (
    amendment_id TEXT PRIMARY KEY,
    amendment_type TEXT,
    amendment_number TEXT,
    congress INTEGER,
    chamber TEXT,
    purpose TEXT,
    description TEXT,
    proposed_at TIMESTAMP WITH TIME ZONE,
    submitted_at TIMESTAMP WITH TIME ZONE,
    is_bill_amendment BOOLEAN,
    is_treaty_amendment BOOLEAN,
    is_amendment_amendment BOOLEAN,
    notes TEXT,
    actions_count INTEGER,
    cosponsors_count INTEGER,
    amendments_to_amendment_count INTEGER,
    updated_at TIMESTAMP WITH TIME ZONE
);
```

### Related Tables
- `amendments_sponsors` - Links amendments to member bioguide IDs
- `amendments_amended_bills` - Links amendments to bills they modify
- `amendments_amended_amendments` - Links amendments to other amendments they modify
- `amendments_amended_treaties` - Links amendments to treaties they modify

## Testing

Run the test script to verify functionality:

```bash
python test_amendment_processing.py
```

Update the database configuration in the test script before running.

## Performance Considerations

- **Batch Processing**: Default batch size is 1000 records
- **Error Isolation**: Individual record failures don't stop batch processing
- **Memory Efficiency**: Processes data in chunks to avoid memory issues
- **Transaction Safety**: Uses database transactions for data consistency

## Troubleshooting

### Common Issues

1. **Database Connection**: Ensure database credentials are correct
2. **Schema Permissions**: Verify write access to `staging_congressional` schema
3. **Raw Table Access**: Ensure read access to `bicam_raw_congressional.amendments_raw`
4. **JSON Format**: Verify JSON payloads are valid

### Error Logging

The scripts provide detailed logging:
- Processing progress
- Error counts and details
- Statistics on processed data

### Cleanup

To clear processed data for testing:

```sql
SELECT cleanup_amendment_test_data();
```

## Dependencies

- PostgreSQL 12+ with JSONB support
- Python 3.7+ with psycopg2
- Access to `bicam_raw_congressional.amendments_raw` table
- Write permissions to `staging_congressional` schema 