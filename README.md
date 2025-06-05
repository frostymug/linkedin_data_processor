# LinkedIn Data Processor

This script processes CSV files from LinkedIn data exports and loads them into a SQLite database. It automatically:
- Recursively scans for CSV files
- Normalizes column names
- Infers appropriate data types
- Creates and populates database tables

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Place your LinkedIn data export files in a directory

## Usage

Edit the `linkedin_data_processor.py` file and update the `input_dir` parameter in the `main()` function to point to your LinkedIn data export directory:

```python
processor = LinkedInDataProcessor(
    input_dir="path/to/linkedin/export",  # Replace with your LinkedIn export directory
    db_path="linkedin_data.db"
)
```

Then run the script:
```bash
python linkedin_data_processor.py
```

## Features

- Automatic table creation based on CSV structure
- Column name normalization (removes special characters, makes lowercase)
- Data type inference for SQLite
- Error handling for problematic files
- Recursive directory scanning

## Schema Structure

The database schema includes several key tables with relationships and indexes:

### Core Tables

1. **Email Addresses**
   - Primary key: `email`
   - Contains email addresses of LinkedIn connections
   - Indexed on: `email`

2. **Connections**
   - Primary key: `id`
   - Contains connection information
   - Indexed on: `email`, `first_name`, `last_name`
   - Foreign key relationships:
     - Messages (via email)
     - Invitations (via email)

3. **Messages**
   - Primary key: `id`
   - Contains message history
   - Indexed on: `timestamp`, `from_email`, `to_email`
   - Foreign key relationships:
     - Connections (via email)
     - Email Addresses (via from_email, to_email)

4. **Invitations**
   - Primary key: `id`
   - Contains invitation history
   - Indexed on: `email`, `date_col`
   - Foreign key relationships:
     - Email Addresses (via email)

5. **Endorsements**
   - Split into two tables:
     - Endorsement_Received_Info (indexed on endorser_email)
     - Endorsement_Given_Info (indexed on endorsee_email)

### Index Strategy

Indexes are created on frequently queried columns to improve performance:
- Email addresses (for quick lookups)
- Names (for search functionality)
- Dates (for time-based queries)
- Message metadata (for conversation tracking)

## Output

The script will create a SQLite database (`linkedin_data.db` by default) with tables named after the CSV files (without extensions). Each table will have normalized column names and appropriate data types inferred from the CSV data.
