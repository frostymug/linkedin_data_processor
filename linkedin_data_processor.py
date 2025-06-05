import os
import sqlite3
import pandas as pd
from pathlib import Path
import re

class LinkedInDataProcessor:
    def __init__(self, input_dir, db_path='linkedin_data.db'):
        self.input_dir = Path(input_dir)
        self.db_path = Path(db_path)
        self.connection = None
        self.column_mapping = {}
        self._seen_columns = set()
        
    def _infer_data_type(self, series):
        """Infer appropriate SQLite data type for a pandas series"""
        dtype = series.dtype
        if pd.api.types.is_integer_dtype(dtype):
            return 'INTEGER'
        elif pd.api.types.is_float_dtype(dtype):
            return 'REAL'
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            return 'TIMESTAMP'
        else:
            return 'TEXT'

    def _normalize_column_name(self, col_name):
        """Normalize column names by removing special characters and making them lowercase"""
        # Remove special characters (except underscore)
        normalized = re.sub(r'[^a-zA-Z0-9_]', '_', col_name)
        # Remove leading/trailing underscores
        normalized = normalized.strip('_')
        # Replace multiple underscores with single underscore
        normalized = re.sub(r'_+', '_', normalized)
        # Make lowercase
        normalized = normalized.lower()
        
        # Handle date columns specially
        if normalized == 'date':
            normalized = 'date_col'
        elif normalized in ['from', 'to', 'text']:
            normalized = f'col_{normalized}'
        
        # If the column name contains 'date' but isn't exactly 'date', keep it as is
        if 'date' in normalized and normalized != 'date_col':
            normalized = normalized
        
        # Print debug info for problematic columns
        if normalized == 'text':
            print(f"\nDebug info for column '{col_name}'")
            print(f"Original: {col_name}")
            print(f"Normalized: {normalized}")
        
        return normalized

    def _create_table(self, df, table_name):
        """Create table in SQLite database with appropriate data types"""
        # Replace spaces with underscores in table name and quote it
        table_name = '"' + table_name.replace(' ', '_') + '"'
        
        # Create mapping of original to normalized column names
        self.column_mapping = {}
        columns = []
        for col in df.columns:
            normalized_col = self._normalize_column_name(col)
            self.column_mapping[col] = normalized_col
            dtype = self._infer_data_type(df[col])
            # Always quote column names to avoid SQL syntax errors
            normalized_col = '"' + normalized_col + '"'
            columns.append(f"{normalized_col} {dtype}")
            
            # Print debug info for problematic columns
            if normalized_col.lower() == '"text"':
                print(f"\nDebug info for column '{col}'")
                print(f"Original: {col}")
                print(f"Normalized: {normalized_col}")
                print(f"Data type: {dtype}")
        
        # Print debug info for problematic tables
        if table_name in [""""ads_clicked""", """"coach_messages""", """"education""", """"email_addresses""", 
                          """"endorsement_given_info""", """"endorsement_received_info""", """"events""", 
                          """"hashtag_follows""", """"learning""", """"learning_coach_messages""", 
                          """"learning_role_play_messages""", """"logins""", """"messages"""]:
            print(f"\nDebug info for {table_name}")
            print("Original column names:", df.columns.tolist())
            print("Normalized column names:", [self.column_mapping[col] for col in df.columns])
            
            # Get the actual column names from the database
            cursor = self.connection.cursor()
            cursor.execute(f"PRAGMA table_info({table_name})")
            db_columns = cursor.fetchall()
            print("Database column names:", [col[1] for col in db_columns])
            
            # Print detailed mapping
            print("Detailed mapping:")
            for orig_col, norm_col in self.column_mapping.items():
                print(f"  {orig_col} -> {norm_col}")
        
        columns_str = ', '.join(columns)
        create_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_str})"
        
        cursor = self.connection.cursor()
        try:
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            cursor.execute(create_query)
            self.connection.commit()
        except sqlite3.Error as e:
            print(f"Error creating table {table_name}: {str(e)}")
            raise
    
    def _insert_data(self, df, table_name):
        """Insert data into existing table"""
        # Replace spaces with underscores in table name and quote it
        table_name = '"' + table_name.replace(' ', '_') + '"'
        
        # Get column names and values for the INSERT statement
        normalized_cols = []
        for col in df.columns:
            # If column exists in mapping, use normalized name
            if col in self.column_mapping:
                normalized_cols.append(self.column_mapping[col])
            else:
                # If column wasn't in original mapping (e.g. new column added), normalize it
                normalized_cols.append(self._normalize_column_name(col))
        
        # Get column names from database to ensure they match
        cursor = self.connection.cursor()
        cursor.execute(f"PRAGMA table_info({table_name})")
        db_columns = cursor.fetchall()
        db_column_names = [col[1] for col in db_columns]
        
        # Print debug info for problematic tables
        problematic_tables = [
            "ads_clicked", "coach_messages", "education", "email_addresses",
            "endorsement_given_info", "endorsement_received_info", "events",
            "hashtag_follows", "learning", "learning_coach_messages",
            "learning_role_play_messages", "logins", "messages"
        ]
        if table_name in problematic_tables:
            print(f"\nDebug info for {table_name}")
            print("Database column names:", db_column_names)
            print("Column mapping:", self.column_mapping)
        
        # Create insert query with quoted column names
        columns_str = ', '.join([f'"{col}"' for col in db_column_names])
        placeholders = ', '.join(['?'] * len(db_column_names))
        insert_query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
        
        # Convert DataFrame to list of tuples and insert data
        data = []
        for _, row in df.iterrows():
            row_data = []
            for col in db_column_names:
                # Get the original column name from our mapping
                orig_col = next((k for k, v in self.column_mapping.items() if v == col), col)
                value = row.get(orig_col)
                row_data.append(value)
            data.append(tuple(row_data))
        
        try:
            cursor.executemany(insert_query, data)
            self.connection.commit()
        except sqlite3.Error as e:
            print(f"Error inserting data into {table_name}: {str(e)}")
            print(f"Insert query: {insert_query}")
            print(f"First row of data: {data[0] if data else 'No data'}")
            raise
    
    def process_directory(self):
        """Process all CSV files in the directory and load them into SQLite"""
        # Create database connection
        self.connection = sqlite3.connect(self.db_path)
        
        # Find all CSV files recursively
        csv_files = list(self.input_dir.rglob('*.csv'))
        
        for csv_file in csv_files:
            try:
                # Read CSV file with multiple parsing attempts
                df = None
                try:
                    # Try standard CSV parsing
                    df = pd.read_csv(csv_file, on_bad_lines='warn', encoding='utf-8')
                    print(f"Successfully parsed {csv_file.name} with default settings")
                except pd.errors.ParserError as e:
                    print(f"Warning: Failed to parse {csv_file.name} with default settings: {str(e)}")
                    try:
                        # Try with different delimiter
                        df = pd.read_csv(csv_file, delimiter=';', on_bad_lines='warn', encoding='utf-8')
                        print(f"Successfully parsed {csv_file.name} with semicolon delimiter")
                    except pd.errors.ParserError as e:
                        print(f"Warning: Failed to parse {csv_file.name} with semicolon delimiter: {str(e)}")
                        try:
                            # Try with different quote handling
                            df = pd.read_csv(csv_file, quotechar='"', on_bad_lines='warn', encoding='utf-8')
                            print(f"Successfully parsed {csv_file.name} with quote handling")
                        except pd.errors.ParserError as e:
                            print(f"Warning: Failed to parse {csv_file.name} with quote handling: {str(e)}")
                            try:
                                # Try with different encoding
                                df = pd.read_csv(csv_file, on_bad_lines='warn', encoding='latin1')
                                print(f"Successfully parsed {csv_file.name} with latin1 encoding")
                            except pd.errors.ParserError as e:
                                print(f"Warning: Failed to parse {csv_file.name} with latin1 encoding: {str(e)}")
                                try:
                                    # Try with different quote handling and encoding
                                    df = pd.read_csv(csv_file, quotechar='"', on_bad_lines='warn', encoding='latin1')
                                    print(f"Successfully parsed {csv_file.name} with latin1 encoding and quote handling")
                                except pd.errors.ParserError as e:
                                    print(f"Warning: Failed to parse {csv_file.name} with latin1 encoding and quote handling: {str(e)}")
                                    try:
                                        df = pd.read_csv(csv_file, delimiter=';', on_bad_lines='warn', encoding='latin1')
                                        print(f"Successfully parsed {csv_file.name} with semicolon delimiter and latin1 encoding")
                                    except pd.errors.ParserError as e:
                                        print(f"Warning: Failed to parse {csv_file.name} with semicolon delimiter and latin1 encoding: {str(e)}")
                                        print(f"Warning: Could not parse {csv_file.name} using standard methods")
                                        try:
                                            with open(csv_file, 'r', encoding='utf-8') as f:
                                                lines = f.readlines()
                                            lines = [line.strip() for line in lines if line.strip()]
                                            data = [re.split(r',(?=(?:[^"\']*"[^"\']*"[^"\']*"[^"]*$|[^"]*$))', line) for line in lines]
                                            df = pd.DataFrame(data[1:], columns=data[0])
                                            print(f"Successfully parsed {csv_file.name} using manual method")
                                        except Exception as e:
                                            print(f"Error reading {csv_file.name}: {str(e)}")
                                            continue
                if df is None:
                    raise ValueError(f"Could not parse CSV file {csv_file.name}")
                
                # Drop any rows with missing values in the first column
                if len(df.columns) > 0:
                    df = df.dropna(subset=[df.columns[0]])
                
                table_name = csv_file.stem.lower()
                self._create_table(df, table_name)
                self._insert_data(df, table_name)
                print(f"Successfully processed: {csv_file.name}")
            except Exception as e:
                print(f"Error processing {csv_file.name}: {str(e)}")
                if isinstance(e, pd.errors.ParserError):
                    print(f"  Details: {str(e)}")
                continue
        
        self.connection.close()

def main():
    import argparse
    
    # Create argument parser
    parser = argparse.ArgumentParser(description='Process LinkedIn data export CSV files')
    parser.add_argument('--input-dir', default="C:\\Users\\andyf\\LinkedInExport",
                       help='Directory containing LinkedIn export CSV files (default: C:\\Users\\andyf\\LinkedInExport)')
    parser.add_argument('--db-path', default='linkedin_data.db', 
                       help='Path to SQLite database file (default: linkedin_data.db)')
    
    args = parser.parse_args()
    
    # Create and run processor
    processor = LinkedInDataProcessor(
        input_dir=args.input_dir,
        db_path=args.db_path
    )
    processor.process_directory()
    print("Processing complete!")

if __name__ == "__main__":
    main()
