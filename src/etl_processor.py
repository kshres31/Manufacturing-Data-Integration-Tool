"""
ETL Processor for Manufacturing Data Integration Tool
Handles database connections, data loading, and error logging
"""

import pandas as pd
import pyodbc
import os
import sys
from datetime import datetime
from typing import Tuple, List, Dict
from dataclasses import dataclass

# Add project root to path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.insert(0, project_root)

from src.xml_parser import ConfigParser
from data_validator import DataValidator, ValidationError


class ETLProcessor:
    """Handles database operations and ETL workflow"""
    
    def __init__(self, config_path: str):
        self.config = ConfigParser(config_path)
        self.target_config = self.config.get_target_config()
        self.etl_config = self.config.get_etl_config()
        self.conn = None
        self.cursor = None
        
    def connect(self):
        """Establish SQL Server connection"""
        try:
            print(f"Connecting to SQL Server...")
            self.conn = pyodbc.connect(self.target_config.connection_string)
            self.cursor = self.conn.cursor()
            print("✓ Connected successfully")
            return True
        except Exception as e:
            print(f"✗ Connection failed: {e}")
            return False
    
    def disconnect(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
            print("✓ Disconnected from database")
    
    def load_valid_data(self, df: pd.DataFrame, source_file: str) -> int:
        """
        Load validated records to QualityData table
        Returns: number of rows inserted
        """
        if df.empty:
            print("No valid records to load")
            return 0
        
        target_table = self.target_config.target_table
        rows_inserted = 0
        
        print(f"\nLoading {len(df)} records to {target_table}...")
        
        # Prepare insert statement
        insert_sql = f"""
        INSERT INTO {target_table} (
            RecordTimestamp, ProductionLineID, BatchNumber, ProductCode,
            TemperatureCelsius, PressureKPA, HumidityPercent, OperatorID,
            DefectCount, FileSource, ValidationStatus
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'VALID')
        """
        
        try:
            for idx, row in df.iterrows():
                # Handle missing humidity
                humidity = row.get('humidity_pct') if pd.notna(row.get('humidity_pct')) else None
                
                self.cursor.execute(insert_sql, (
                    row['timestamp'],
                    row['line_id'],
                    row['batch_number'],
                    row['product_code'],
                    row['temperature_c'],
                    row['pressure_kpa'],
                    humidity,
                    row['operator_id'],
                    row['defect_count'],
                    source_file
                ))
                rows_inserted += 1
                
                # Commit in batches
                if rows_inserted % self.etl_config.batch_size == 0:
                    self.conn.commit()
                    print(f"  Committed {rows_inserted} rows...")
            
            # Final commit
            self.conn.commit()
            print(f"✓ Loaded {rows_inserted} records successfully")
            return rows_inserted
            
        except Exception as e:
            self.conn.rollback()
            print(f"✗ Load failed: {e}")
            raise
    
    def log_errors(self, errors: List[ValidationError], source_file: str) -> int:
        """
        Log validation errors to DataValidationErrors table
        Returns: number of errors logged
        """
        if not errors:
            print("No errors to log")
            return 0
        
        print(f"\nLogging {len(errors)} errors...")
        
        insert_sql = """
        INSERT INTO Production.DataValidationErrors (
            FileSource, ErrorType, ErrorMessage, FieldName, FieldValue
        ) VALUES (?, ?, ?, ?, ?)
        """
        
        try:
            for error in errors:
                self.cursor.execute(insert_sql, (
                    source_file,
                    error.error_type,
                    error.error_message,
                    error.field_name,
                    str(error.field_value)[:255]  # Truncate if too long
                ))
            
            self.conn.commit()
            print(f"✓ Logged {len(errors)} errors")
            return len(errors)
            
        except Exception as e:
            self.conn.rollback()
            print(f"✗ Error logging failed: {e}")
            raise
    
    def get_processing_summary(self) -> pd.DataFrame:
        """Get summary of processed records"""
        query = """
        SELECT 
            CAST(ProcessedDate as DATE) as ProcessDate,
            ValidationStatus,
            COUNT(*) as RecordCount
        FROM Production.QualityData
        WHERE ProcessedDate >= DATEADD(day, -7, GETDATE())
        GROUP BY CAST(ProcessedDate as DATE), ValidationStatus
        ORDER BY ProcessDate DESC
        """
        return pd.read_sql(query, self.conn)
    
    def archive_file(self, source_path: str, archive_dir: str = None):
        """Move processed file to archive folder"""
        if not self.etl_config.archive_processed_files:
            return
        
        if archive_dir is None:
            archive_dir = os.path.join(project_root, 'data', 'processed')
        
        os.makedirs(archive_dir, exist_ok=True)
        
        filename = os.path.basename(source_path)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        archive_name = f"{timestamp}_{filename}"
        dest_path = os.path.join(archive_dir, archive_name)
        
        try:
            os.rename(source_path, dest_path)
            print(f"✓ Archived to: {dest_path}")
        except Exception as e:
            print(f"✗ Archive failed: {e}")


def run_etl_pipeline(csv_path: str, config_path: str = None):
    """
    Main ETL pipeline: Validate → Load Valid → Log Errors → Archive
    """
    if config_path is None:
        config_path = os.path.join(project_root, 'config', 'mapping_config.xml')
    
    source_file = os.path.basename(csv_path)
    print(f"\n{'='*60}")
    print(f"ETL PIPELINE: {source_file}")
    print(f"{'='*60}")
    
    # Step 1: Validate
    print("\n[STEP 1] VALIDATION")
    validator = DataValidator(config_path)
    df = pd.read_csv(csv_path)
    valid_df, invalid_df = validator.validate_dataframe(df)
    errors = validator.errors
    
    # Step 2: Connect to database
    print("\n[STEP 2] DATABASE CONNECTION")
    processor = ETLProcessor(config_path)
    if not processor.connect():
        print("ETL aborted - cannot connect to database")
        return
    
    try:
        # Step 3: Load valid data
        print("\n[STEP 3] LOAD VALID DATA")
        rows_loaded = processor.load_valid_data(valid_df, source_file)
        
        # Step 4: Log errors
        print("\n[STEP 4] LOG ERRORS")
        errors_logged = processor.log_errors(errors, source_file)
        
        # Step 5: Archive file
        print("\n[STEP 5] ARCHIVE")
        processor.archive_file(csv_path)
        
        # Summary
        print(f"\n{'='*60}")
        print("ETL SUMMARY")
        print(f"{'='*60}")
        print(f"Source Records:     {len(df)}")
        print(f"Valid Records:      {len(valid_df)}")
        print(f"Invalid Records:    {len(invalid_df)}")
        print(f"Rows Loaded to DB:  {rows_loaded}")
        print(f"Errors Logged:      {errors_logged}")
        print(f"{'='*60}")
        
    finally:
        processor.disconnect()


# Test the ETL pipeline
if __name__ == "__main__":
    # Test file path
    csv_path = os.path.join(project_root, 'data', 'raw', 'production_data_20240215.csv')
    
    if os.path.exists(csv_path):
        run_etl_pipeline(csv_path)
    else:
        print(f"Test file not found: {csv_path}")
        print("Please ensure the CSV file exists in data/raw/")