"""
Unit tests for data validation engine
"""

import unittest
import sys
import os
import pandas as pd

# Add src to path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.insert(0, project_root)

from src.data_validator import DataValidator, ValidationError
from src.xml_parser import ConfigParser


class TestDataValidator(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        """Set up test fixtures"""
        cls.config_path = os.path.join(project_root, 'config', 'mapping_config.xml')
        cls.validator = DataValidator(cls.config_path)
    
    def test_01_config_loading(self):
        """Test that XML config loads correctly"""
        print("\nTest 1: Config loading...")
        self.assertEqual(len(self.validator.field_mappings), 9)
        self.assertIsNotNone(self.validator.config)
        print("PASSED: Config loaded with 9 field mappings")
    
    def test_02_required_field_validation(self):
        """Test that empty required fields are caught"""
        print("\nTest 2: Required field validation...")
        test_data = {
            'timestamp': '',
            'line_id': 'LINE001',
            'batch_number': 'BATCH001',
            'product_code': 'PROD-A1',
            'temperature_c': 150.0,
            'pressure_kpa': 400.0,
            'humidity_pct': 50.0,
            'operator_id': 'OP0001',
            'defect_count': 0
        }
        
        df = pd.DataFrame([test_data])
        valid_df, invalid_df = self.validator.validate_dataframe(df)
        
        self.assertEqual(len(invalid_df), 1)
        self.assertTrue(any('REQUIRED_FIELD_MISSING' in str(e.error_type) for e in self.validator.errors))
        print("PASSED: Empty timestamp caught as required field missing")
    
    def test_03_range_validation(self):
        """Test that out of range values are caught"""
        print("\nTest 3: Range validation...")
        test_data = {
            'timestamp': '2024-02-15 10:00:00',
            'line_id': 'LINE001',
            'batch_number': 'BATCH001',
            'product_code': 'PROD-A1',
            'temperature_c': 300.0,
            'pressure_kpa': 400.0,
            'humidity_pct': 50.0,
            'operator_id': 'OP0001',
            'defect_count': 0
        }
        
        df = pd.DataFrame([test_data])
        valid_df, invalid_df = self.validator.validate_dataframe(df)
        
        self.assertEqual(len(invalid_df), 1)
        self.assertTrue(any('RANGE' in str(e.error_type) for e in self.validator.errors))
        print("PASSED: Temperature 300C caught as out of range")
    
    def test_04_regex_validation(self):
        """Test that invalid formats are caught"""
        print("\nTest 4: Regex validation...")
        test_data = {
            'timestamp': '2024-02-15 10:00:00',
            'line_id': 'BADLINE',
            'batch_number': 'BATCH001',
            'product_code': 'PROD-A1',
            'temperature_c': 150.0,
            'pressure_kpa': 400.0,
            'humidity_pct': 50.0,
            'operator_id': 'OP0001',
            'defect_count': 0
        }
        
        df = pd.DataFrame([test_data])
        valid_df, invalid_df = self.validator.validate_dataframe(df)
        
        self.assertEqual(len(invalid_df), 1)
        self.assertTrue(any('REGEX' in str(e.error_type) for e in self.validator.errors))
        print("PASSED: Invalid line_id format caught")
    
    def test_05_valid_record_passes(self):
        """Test that valid records pass all checks"""
        print("\nTest 5: Valid record...")
        test_data = {
            'timestamp': '2024-02-15 10:00:00',
            'line_id': 'LINE001',
            'batch_number': 'BATCH001',
            'product_code': 'PROD-A1',
            'temperature_c': 150.0,
            'pressure_kpa': 400.0,
            'humidity_pct': 50.0,
            'operator_id': 'OP0001',
            'defect_count': 0
        }
        
        df = pd.DataFrame([test_data])
        valid_df, invalid_df = self.validator.validate_dataframe(df)
        
        self.assertEqual(len(valid_df), 1)
        self.assertEqual(len(invalid_df), 0)
        print("PASSED: Valid record accepted")
    
    def test_06_duplicate_detection(self):
        """Test that duplicates are caught"""
        print("\nTest 6: Duplicate detection...")
        test_data = [
            {
                'timestamp': '2024-02-15 10:00:00',
                'line_id': 'LINE001',
                'batch_number': 'BATCH001',
                'product_code': 'PROD-A1',
                'temperature_c': 150.0,
                'pressure_kpa': 400.0,
                'humidity_pct': 50.0,
                'operator_id': 'OP0001',
                'defect_count': 0
            },
            {
                'timestamp': '2024-02-15 10:00:00',
                'line_id': 'LINE001',
                'batch_number': 'BATCH001',
                'product_code': 'PROD-A1',
                'temperature_c': 155.0,
                'pressure_kpa': 405.0,
                'humidity_pct': 51.0,
                'operator_id': 'OP0002',
                'defect_count': 1
            }
        ]
        
        df = pd.DataFrame(test_data)
        valid_df, invalid_df = self.validator.validate_dataframe(df)
        
        self.assertEqual(len(invalid_df), 2)
        self.assertTrue(any('DUPLICATE' in str(e.error_type) for e in self.validator.errors))
        print("PASSED: Duplicate records caught")


class TestXMLParser(unittest.TestCase):
    
    def test_07_config_parsing(self):
        """Test XML config parsing"""
        print("\nTest 7: XML parsing...")
        config_path = os.path.join(project_root, 'config', 'mapping_config.xml')
        parser = ConfigParser(config_path)
        
        source = parser.get_source_config()
        self.assertEqual(source.name, 'ProductionLine')
        self.assertEqual(source.delimiter, ',')
        
        target = parser.get_target_config()
        self.assertEqual(target.name, 'QualityDatabase')
        
        mappings = parser.get_field_mappings()
        self.assertEqual(len(mappings), 9)
        
        etl = parser.get_etl_config()
        self.assertEqual(etl.batch_size, 1000)
        print("PASSED: XML config parsed correctly")


if __name__ == '__main__':
    # Run tests with verbosity
    unittest.main(verbosity=2)