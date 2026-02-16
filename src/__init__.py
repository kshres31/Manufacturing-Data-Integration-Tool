"""
Manufacturing Data Integration Tool
A Python-based ETL system for validating and loading manufacturing data to SQL Server
"""

__version__ = "1.0.0"
__author__ = "Your Name"

from .xml_parser import ConfigParser, FieldMapping, ValidationRule, SourceConfig, TargetConfig, ETLConfig
from .data_validator import DataValidator, ValidationError
from .etl_processor import ETLProcessor, run_etl_pipeline

__all__ = [
    'ConfigParser',
    'FieldMapping', 
    'ValidationRule',
    'SourceConfig',
    'TargetConfig',
    'ETLConfig',
    'DataValidator',
    'ValidationError',
    'ETLProcessor',
    'run_etl_pipeline'
]