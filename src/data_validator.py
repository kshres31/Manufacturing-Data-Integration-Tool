"""
Data Validation Engine for Manufacturing Data Integration Tool
Validates CSV data against rules defined in XML configuration
"""

import pandas as pd
import numpy as np
import re
import sys
import os
import traceback

# Add project root to path so imports work
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.insert(0, project_root)

from datetime import datetime
from typing import List, Dict, Tuple, Optional, Any
from dataclasses import dataclass

# Import from same folder (src)
# from xml_parser import ConfigParser, FieldMapping, ValidationRule
from src.xml_parser import ConfigParser, FieldMapping, ValidationRule


@dataclass
class ValidationError:
    """Represents a single validation error"""
    row_index: int
    field_name: str
    field_value: Any
    error_type: str
    error_message: str
    
    def to_dict(self) -> Dict:
        return {
            'row_index': self.row_index,
            'field_name': self.field_name,
            'field_value': str(self.field_value),
            'error_type': self.error_type,
            'error_message': self.error_message
        }


class DataValidator:
    """Validates DataFrame against XML configuration rules"""
    
    def __init__(self, config_path: str):
        print(f"Initializing validator with config: {config_path}")
        self.config = ConfigParser(config_path)
        self.field_mappings = self.config.get_field_mappings()
        self.global_validations = self.config.get_global_validations()
        self.errors: List[ValidationError] = []
        self.valid_rows: List[int] = []
        self.invalid_rows: List[int] = []
        
        # Build lookup maps for referential integrity
        self._lookup_cache = {}
        print(f"Loaded {len(self.field_mappings)} field mappings")
    
    def validate_dataframe(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Main validation method
        Returns: (valid_records_df, invalid_records_df)
        """
        print(f"\nStarting validation of {len(df)} records...")
        self.errors = []
        self.valid_rows = []
        self.invalid_rows = []
        
        # Check required columns exist
        self._validate_columns(df)
        
        # Row-by-row validation
        for idx, row in df.iterrows():
            row_errors = self._validate_row(idx, row)
            
            if row_errors:
                self.invalid_rows.append(idx)
                self.errors.extend(row_errors)
            else:
                self.valid_rows.append(idx)
        
        # Global validations (duplicates across dataset)
        self._run_global_validations(df)
        
        # Split data
        valid_df = df.loc[self.valid_rows].copy() if self.valid_rows else pd.DataFrame()
        invalid_df = df.loc[self.invalid_rows].copy() if self.invalid_rows else pd.DataFrame()
        
        # Print summary
        self._print_summary(len(df), len(valid_df), len(invalid_df))
        
        return valid_df, invalid_df
    
    def _validate_columns(self, df: pd.DataFrame):
        """Verify all required source columns exist in CSV"""
        required_cols = [m.source_field for m in self.field_mappings]
        missing = set(required_cols) - set(df.columns)
        
        if missing:
            raise ValueError(f"Missing required columns in CSV: {missing}")
        
        print(f"✓ All {len(required_cols)} required columns found")
    
    def _validate_row(self, idx: int, row: pd.Series) -> List[ValidationError]:
        """Validate a single row against all field rules"""
        errors = []
        
        for mapping in self.field_mappings:
            field_name = mapping.source_field
            value = row.get(field_name)
            
            # Check required
            if mapping.required and pd.isna(value):
                errors.append(ValidationError(
                    row_index=idx,
                    field_name=field_name,
                    field_value=value,
                    error_type='REQUIRED_FIELD_MISSING',
                    error_message=f"Required field '{field_name}' is empty"
                ))
                continue
            
            # Skip further validation if empty and not required
            if pd.isna(value) and not mapping.required:
                continue
            
            # Run all validation rules for this field
            for rule in mapping.validations:
                error = self._apply_rule(idx, field_name, value, rule)
                if error:
                    errors.append(error)
        
        return errors
    
    def _apply_rule(self, idx: int, field: str, value: Any, rule: ValidationRule) -> Optional[ValidationError]:
        """Apply a single validation rule"""
        
        if rule.rule_type == 'not_null':
            if pd.isna(value) or str(value).strip() == '':
                return ValidationError(idx, field, value, 'NOT_NULL', f"{field} cannot be null")
        
        elif rule.rule_type == 'range':
            try:
                num_val = float(value)
                min_val = rule.parameters.get('min')
                max_val = rule.parameters.get('max')
                
                if min_val is not None and num_val < min_val:
                    return ValidationError(idx, field, value, 'RANGE', 
                        f"{field}={num_val} below minimum {min_val}")
                if max_val is not None and num_val > max_val:
                    return ValidationError(idx, field, value, 'RANGE', 
                        f"{field}={num_val} exceeds maximum {max_val}")
            except (ValueError, TypeError):
                return ValidationError(idx, field, value, 'NUMERIC', 
                    f"{field}='{value}' is not a valid number")
        
        elif rule.rule_type == 'regex':
            pattern = rule.parameters.get('pattern')
            if pattern and not re.match(pattern, str(value)):
                desc = rule.parameters.get('description', f"match pattern {pattern}")
                return ValidationError(idx, field, value, 'REGEX', 
                    f"{field}='{value}' does not match required format: {desc}")
        
        elif rule.rule_type == 'date_range':
            try:
                if isinstance(value, str):
                    dt_val = pd.to_datetime(value)
                else:
                    dt_val = value
                
                min_date = rule.parameters.get('min')
                max_date = rule.parameters.get('max')
                
                if min_date and dt_val < pd.to_datetime(min_date):
                    return ValidationError(idx, field, value, 'DATE_RANGE', 
                        f"{field} date before minimum {min_date}")
                if max_date and dt_val > pd.to_datetime(max_date):
                    return ValidationError(idx, field, value, 'DATE_RANGE', 
                        f"{field} date after maximum {max_date}")
            except Exception:
                return ValidationError(idx, field, value, 'DATE_FORMAT', 
                    f"{field}='{value}' is not a valid date")
        
        elif rule.rule_type == 'lookup':
            # Check value exists in reference table
            lookup_table = rule.parameters.get('table')
            lookup_column = rule.parameters.get('column')
            cache_key = f"{lookup_table}.{lookup_column}"
            
            if cache_key not in self._lookup_cache:
                self._lookup_cache[cache_key] = set()
            
            # For demo, check against known values
            valid_products = {'PROD-A1', 'PROD-B2', 'PROD-C3', 'PROD-D4'}
            if lookup_column == 'ProductCode' and str(value) not in valid_products:
                return ValidationError(idx, field, value, 'LOOKUP', 
                    f"{field}='{value}' not found in {lookup_table}")
        
        return None
    
    def _run_global_validations(self, df: pd.DataFrame):
        """Run validations that require checking entire dataset"""
        
        for rule in self.global_validations:
            if rule['rule_type'] == 'duplicate_check':
                fields = rule['fields'].split(',')
                
                # Find duplicates
                duplicates = df[df.duplicated(subset=fields, keep=False)]
                
                for idx in duplicates.index:
                    if idx not in self.invalid_rows:
                        self.invalid_rows.append(idx)
                        if idx in self.valid_rows:
                            self.valid_rows.remove(idx)
                        
                        self.errors.append(ValidationError(
                            row_index=idx,
                            field_name=','.join(fields),
                            field_value='multiple',
                            error_type='DUPLICATE',
                            error_message=f"Duplicate combination of {fields}"
                        ))
    
    def _print_summary(self, total: int, valid: int, invalid: int):
        """Print validation summary"""
        print(f"\n{'='*50}")
        print("VALIDATION SUMMARY")
        print(f"{'='*50}")
        print(f"Total Records:    {total}")
        print(f"Valid Records:    {valid} ({valid/total*100:.1f}%)")
        print(f"Invalid Records:  {invalid} ({invalid/total*100:.1f}%)")
        print(f"{'='*50}")
        
        if self.errors:
            print(f"\nFirst 5 Errors:")
            for err in self.errors[:5]:
                print(f"  Row {err.row_index}: {err.error_type} - {err.error_message}")
    
    def get_error_report(self) -> pd.DataFrame:
        """Export errors as DataFrame for loading to SQL error table"""
        if not self.errors:
            return pd.DataFrame()
        return pd.DataFrame([e.to_dict() for e in self.errors])


# Test the validator
if __name__ == "__main__":
    try:
        # Get paths
        config_path = os.path.join(project_root, 'config', 'mapping_config.xml')
        csv_path = os.path.join(project_root, 'data', 'raw', 'production_data_20240215.csv')
        
        print(f"Project root: {project_root}")
        print(f"Config path: {config_path}")
        print(f"CSV path: {csv_path}")
        
        # Check files exist
        if not os.path.exists(config_path):
            print(f"ERROR: Config file not found at {config_path}")
            sys.exit(1)
        if not os.path.exists(csv_path):
            print(f"ERROR: CSV file not found at {csv_path}")
            sys.exit(1)
        
        # Load config and validate
        validator = DataValidator(config_path)
        
        # Load test CSV
        print(f"\nLoading CSV...")
        df = pd.read_csv(csv_path)
        
        print("CSV Preview:")
        print(df.head())
        print(f"\nColumns: {list(df.columns)}")
        print(f"Shape: {df.shape}")
        
        # Run validation
        valid_df, invalid_df = validator.validate_dataframe(df)
        
        print(f"\nValid records shape: {valid_df.shape}")
        print(f"Invalid records shape: {invalid_df.shape}")
        
    except Exception as e:
        print(f"\nERROR: {e}")
        traceback.print_exc()