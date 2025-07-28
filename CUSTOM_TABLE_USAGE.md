# Using Lobbyist Matching with Custom Tables

This guide explains how to run the bill reference extraction and matching system on your own tables instead of the default `relational___lda` tables.

## Prerequisites

Your custom table must provide the following information (either in a single table or through joins):

| Required Field | Description | Example Column Names |
|---|---|---|
| `filing_uuid` | Unique identifier for each filing (UUID format) | `filing_id`, `document_uuid`, `filing_uuid` |
| `section_id` | Unique identifier for each section | `section_id`, `paragraph_id`, `text_block_id` |
| `text_content` | The actual text to analyze for bill references | `issue_text`, `content`, `text`, `description` |
| `filing_year` | Year the filing was submitted (integer) | `filing_year`, `submission_year`, `year` |

## Method 1: Command Line Usage

You can specify a custom table directly from the command line:

```bash
python -m src.bicam_collection.lobbyist_matching.main \
    --custom-table "your_schema.your_table" \
    --filing-uuid-col "your_filing_id_column" \
    --section-id-col "your_section_id_column" \
    --text-col "your_text_column" \
    --year-col "your_year_column" \
    --additional-where "status = 'active' AND category = 'lobbying'" \
    --sample-size 1000 \
    --description "Test run on custom table"
```

### Command Line Arguments

- `--custom-table`: Full table name (required)
- `--filing-uuid-col`: Column name for filing UUID (default: "filing_uuid")
- `--section-id-col`: Column name for section ID (default: "section_id")
- `--text-col`: Column name for text content (default: "issue_text")
- `--year-col`: Column name for filing year (default: "filing_year")
- `--additional-where`: Optional additional WHERE conditions

## Method 2: Programmatic Usage

```python
import asyncio
from src.bicam_collection.lobbyist_matching.main import process_filings

async def run_custom_processing():
    # Database config
    db_config = {
        "host": "your_host",
        "port": 5432,
        "user": "your_user", 
        "password": "your_password",
        "database": "your_database"
    }
    
    # Table configuration
    table_config = {
        "table_name": "your_schema.your_table",
        "filing_uuid_col": "document_id",
        "section_id_col": "paragraph_id", 
        "text_col": "content_text",
        "year_col": "submission_year",
        "additional_where": "status = 'published'"
    }
    
    # Run processing
    run_id = await process_filings(
        db_config=db_config,
        table_config=table_config,
        sample_size=5000,
        description="Custom table processing"
    )
    
    print(f"Completed with run ID: {run_id}")

# Run it
asyncio.run(run_custom_processing())
```

## Example Table Schemas

### Single Table (Simplest)
```sql
CREATE TABLE my_schema.lobbying_filings (
    filing_uuid UUID PRIMARY KEY,
    section_id TEXT,
    issue_text TEXT,
    filing_year INTEGER,
    status TEXT
);
```

Usage:
```python
table_config = {
    "table_name": "my_schema.lobbying_filings",
    "additional_where": "status = 'active'"
}
```

### Joined Tables (More Complex)
```sql
-- Main filings table
CREATE TABLE my_schema.filings (
    filing_id UUID PRIMARY KEY,
    submission_year INTEGER,
    status TEXT
);

-- Sections/paragraphs table  
CREATE TABLE my_schema.filing_sections (
    section_id SERIAL PRIMARY KEY,
    filing_id UUID REFERENCES my_schema.filings(filing_id),
    content_text TEXT,
    section_type TEXT
);
```

Usage with JOIN:
```python
table_config = {
    "table_name": """
        my_schema.filing_sections fs 
        JOIN my_schema.filings f ON fs.filing_id = f.filing_id
    """,
    "filing_uuid_col": "f.filing_id",
    "section_id_col": "fs.section_id", 
    "text_col": "fs.content_text",
    "year_col": "f.submission_year",
    "additional_where": "f.status = 'published' AND fs.section_type = 'issues'"
}
```

## Understanding the Results

The system will create several tables in the `lobbied_bill_matching` schema:

### Key Output Tables

1. **`lobbied_bill_matching.processing_runs`** - Tracks each processing run
2. **`lobbied_bill_matching.extracted_references`** - Raw bill references found in text
3. **`lobbied_bill_matching.reference_matches`** - References matched to actual bills
4. **`lobbied_bill_matching.paragraph_matches`** - Paragraph-level associations

### Sample Query to View Results

```sql
-- Get high-confidence matches with details
SELECT 
    pr.description as run_description,
    er.filing_uuid,
    er.section_id,
    er.full_match as extracted_text,
    rm.match_type,
    rm.confidence_score,
    rm.matched_title,
    rm.bill_id,
    rm.matched_congress
FROM lobbied_bill_matching.processing_runs pr
JOIN lobbied_bill_matching.extracted_references er ON pr.run_id = er.run_id  
JOIN lobbied_bill_matching.reference_matches rm ON er.reference_id = rm.reference_id
WHERE rm.match_type = 'high_confidence_match'
ORDER BY rm.confidence_score DESC;
```

## Performance Tips

1. **Indexing**: Create indexes on your source columns:
   ```sql
   CREATE INDEX idx_filing_year ON your_table(filing_year);
   CREATE INDEX idx_text_length ON your_table(text_col) WHERE length(text_col) > 10;
   ```

2. **Text Quality**: The system works best with:
   - Clean, readable text (not OCR'd documents with errors)
   - Text length > 50 characters per section
   - Proper sentence structure

3. **Batch Size**: Adjust based on your text length:
   - Short texts (< 1000 chars): `batch_size=100`
   - Medium texts (1000-5000 chars): `batch_size=50` (default)
   - Long texts (> 5000 chars): `batch_size=25`

## Troubleshooting

### Common Issues

1. **"No sections found"** - Check that your WHERE conditions aren't too restrictive
2. **UUID format errors** - Ensure filing_uuid column contains valid UUID strings
3. **Performance issues** - Add indexes and reduce batch_size for large texts

### Testing on Small Sample

Always test first with a small sample:

```bash
python -m src.bicam_collection.lobbyist_matching.main \
    --custom-table "your_schema.your_table" \
    --sample-size 100 \
    --description "Test run"
```

### Viewing Progress

Monitor the `lobbied_bill_matching.processing_runs` table:

```sql
SELECT 
    run_id,
    description, 
    status,
    total_sections,
    start_time,
    parameters->'table_config'->>'table_name' as source_table
FROM lobbied_bill_matching.processing_runs 
ORDER BY start_time DESC;
``` 