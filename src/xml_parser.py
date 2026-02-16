"""
XML Configuration Parser for Manufacturing Data Integration Tool
Parses mapping_config.xml and returns structured configuration objects
"""

import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any
import os


@dataclass
class ValidationRule:
    """Represents a single validation rule from XML config"""
    rule_type: str
    parameters: Dict[str, Any] = field(default_factory=dict)
    
    def __repr__(self):
        return f"ValidationRule({self.rule_type})"


@dataclass
class FieldMapping:
    """Represents field mapping between source CSV and target SQL"""
    source_field: str
    target_field: str
    data_type: str
    required: bool
    validations: List[ValidationRule] = field(default_factory=list)
    
    def get_validation(self, rule_type: str) -> Optional[ValidationRule]:
        """Get specific validation rule by type"""
        for v in self.validations:
            if v.rule_type == rule_type:
                return v
        return None


@dataclass
class SourceConfig:
    """Source system configuration (CSV)"""
    name: str
    file_path: str
    delimiter: str
    has_header: bool


@dataclass
class TargetConfig:
    """Target system configuration (SQL Server)"""
    name: str
    connection_string: str
    target_table: str


@dataclass
class ETLConfig:
    """ETL processing configuration"""
    batch_size: int
    error_handling: str
    log_level: str
    archive_processed_files: bool


class ConfigParser:
    """Parses XML configuration file"""
    
    def __init__(self, config_path: str):
        self.config_path = config_path
        self.tree = None
        self.root = None
        self._parse_xml()
    
    def _parse_xml(self):
        """Load and parse XML file"""
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"Config file not found: {self.config_path}")
        
        self.tree = ET.parse(self.config_path)
        self.root = self.tree.getroot()
        print(f"✓ Loaded config: {self.config_path}")
    
    def get_source_config(self) -> SourceConfig:
        """Extract source system configuration"""
        source = self.root.find('SourceSystem')
        return SourceConfig(
            name=source.get('name'),
            file_path=source.find('FilePath').text,
            delimiter=source.find('Delimiter').text,
            has_header=source.find('HasHeader').text.lower() == 'true'
        )
    
    def get_target_config(self) -> TargetConfig:
        """Extract target system configuration"""
        target = self.root.find('TargetSystem')
        return TargetConfig(
            name=target.get('name'),
            connection_string=target.find('ConnectionString').text,
            target_table=target.find('TargetTable').text
        )
    
    def get_field_mappings(self) -> List[FieldMapping]:
        """Extract all field mappings with validations"""
        mappings = []
        fields = self.root.find('FieldMappings')
        
        for field_elem in fields.findall('Field'):
            # Parse validations
            validations = []
            for val_elem in field_elem.findall('Validation'):
                rule_type = val_elem.get('rule')
                params = {k: v for k, v in val_elem.attrib.items() if k != 'rule'}
                
                # Type conversion for numeric parameters (only for range rules, not dates)
                if rule_type == 'range':
                    if 'min' in params:
                        params['min'] = float(params['min']) if '.' in str(params['min']) else int(params['min'])
                    if 'max' in params:
                        params['max'] = float(params['max']) if '.' in str(params['max']) else int(params['max'])
                if 'threshold' in params:
                    params['threshold'] = float(params['threshold'])
                
                validations.append(ValidationRule(rule_type=rule_type, parameters=params))
            
            mapping = FieldMapping(
                source_field=field_elem.get('source'),
                target_field=field_elem.get('target'),
                data_type=field_elem.get('dataType'),
                required=field_elem.get('required').lower() == 'true',
                validations=validations
            )
            mappings.append(mapping)
        
        return mappings
    
    def get_etl_config(self) -> ETLConfig:
        """Extract ETL configuration"""
        etl = self.root.find('ETLConfig')
        return ETLConfig(
            batch_size=int(etl.find('BatchSize').text),
            error_handling=etl.find('ErrorHandling').text,
            log_level=etl.find('LogLevel').text,
            archive_processed_files=etl.find('ArchiveProcessedFiles').text.lower() == 'true'
        )
    
    def get_global_validations(self) -> List[Dict]:
        """Extract global validation rules"""
        globals_elem = self.root.find('GlobalValidations')
        validations = []
        
        for val in globals_elem.findall('Validation'):
            rule = {'rule_type': val.get('rule')}
            rule.update({k: v for k, v in val.attrib.items() if k != 'rule'})
            validations.append(rule)
        
        return validations


# Test the parser
if __name__ == "__main__":
    # Adjust path relative to project root
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'mapping_config.xml')
    
    parser = ConfigParser(config_path)
    
    print("\n=== Source Config ===")
    source = parser.get_source_config()
    print(f"Name: {source.name}")
    print(f"File Pattern: {source.file_path}")
    print(f"Delimiter: '{source.delimiter}'")
    
    print("\n=== Target Config ===")
    target = parser.get_target_config()
    print(f"Name: {target.name}")
    print(f"Table: {target.target_table}")
    
    print("\n=== Field Mappings ===")
    for mapping in parser.get_field_mappings():
        print(f"\n{mapping.source_field} -> {mapping.target_field} ({mapping.data_type})")
        print(f"  Required: {mapping.required}")
        for v in mapping.validations:
            print(f"  - {v.rule_type}: {v.parameters}")
    
    print("\n=== ETL Config ===")
    etl = parser.get_etl_config()
    print(f"Batch Size: {etl.batch_size}")
    print(f"Error Handling: {etl.error_handling}")