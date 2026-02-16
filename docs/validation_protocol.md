# Validation Protocol

## Purpose
Document describing data validation rules and testing procedures for the Manufacturing Data Integration Tool.

## Validation Rules

### 1. Required Field Check (not_null)
- Field must contain a value
- Empty strings and NULL values are rejected
- Applied to: timestamp, line_id, batch_number, product_code, temperature_c, pressure_kpa, operator_id, defect_count

### 2. Range Validation (range)
- Numeric values must fall within specified min/max bounds
- Temperature: -10.0 to 200.0 Celsius
- Pressure: 0.0 to 1000.0 KPA
- Humidity: 0.0 to 100.0 percent (optional field)
- Defect count: 0 to 9999

### 3. Pattern Matching (regex)
- Values must match specified format patterns
- Line ID: LINE followed by 3 digits (LINE001)
- Operator ID: OP followed by 4 digits (OP0001)

### 4. Date Range (date_range)
- Timestamps must be between 2020-01-01 and 2099-12-31
- Invalid date formats are rejected

### 5. Lookup Validation (lookup)
- Product codes must exist in Production.Products table
- Referential integrity check against database

### 6. Duplicate Detection (unique)
- Combination of timestamp, line_id, and batch_number must be unique
- Duplicate records are flagged as errors

## Testing Procedures

### Unit Testing
1. Run xml_parser.py to verify config loads correctly
2. Run data_validator.py with sample CSV
3. Verify validation catches all intentional errors

### Integration Testing
1. Run etl_processor.py to test database connection
2. Verify valid records load to QualityData table
3. Verify errors log to DataValidationErrors table
4. Check that processed files move to archive folder

### Error Handling
1. Test with missing CSV file - should show clear error
2. Test with invalid database connection - should fail gracefully
3. Test with malformed XML config - should show parse error

## Expected Results

### Valid Records
- Pass all validation rules
- Load to Production.QualityData table
- Status marked as VALID

### Invalid Records
- Fail one or more validation rules
- Do not load to QualityData table
- Error details logged to DataValidationErrors table

## Sample Test Data

File: production_data_20240215.csv contains 9 records:
- 4 valid records (rows 0, 1, 2, 4)
- 5 invalid records with specific error types

## Deployment Checklist

- [ ] Database tables created
- [ ] Stored procedures installed
- [ ] XML config file updated with correct connection string
- [ ] ODBC driver installed
- [ ] Python dependencies installed (pandas, pyodbc)
- [ ] Sample CSV file in data/raw folder
- [ ] Validation runs without errors
- [ ] Database populated with test data