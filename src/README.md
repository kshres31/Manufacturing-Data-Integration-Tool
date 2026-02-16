# Manufacturing Data Integration Tool

A Python tool for validating manufacturing CSV data and loading it into SQL Server.

## What It Does

- Reads production data from CSV files
- Validates data against rules (ranges, formats, duplicates)
- Loads valid records to SQL Server
- Logs errors for invalid records
- Archives processed files

## Project Structure
Manufacturing Data Integration Tool/
├── config/
│   └── mapping_config.xml          # Field mappings and validation rules
├── src/
│   ├── init.py
│   ├── xml_parser.py               # Reads XML config
│   ├── data_validator.py           # Validates CSV data
│   ├── etl_processor.py            # Database operations
│   └── main.py                     # Main script
├── sql/
│   ├── create_tables.sql           # Database setup
│   └── etl_procedures.sql          # SQL stored procedures
├── data/
│   ├── raw/                        # Input CSV files
│   └── processed/                  # Archived files
└── README.md
plain
Copy

## Requirements

- Python 3.8 or higher
- SQL Server (any edition)
- ODBC Driver 17 for SQL Server
- Python packages: pandas, pyodbc

## Setup

1. Install Python packages:
pip install pandas pyodbc
plain
Copy

2. Create the database:
- Open SQL Server Management Studio
- Run `sql/create_tables.sql`
- Run `sql/etl_procedures.sql`

3. Update the connection string in `config/mapping_config.xml` if your SQL Server is not local

## How to Use

Process one file:
cd src
python main.py --input data/raw/production_data_20240215.csv
plain
Copy

Process all files:
python main.py --batch "data/raw/*.csv"
plain
Copy

Process all files in raw folder (default):
python main.py
plain
Copy

## Validation Rules

Rules are defined in `config/mapping_config.xml`:

- **not_null** - Field must have a value
- **range** - Number must be between min and max
- **regex** - Must match a pattern (like LINE001 format)
- **date_range** - Date must be within range
- **lookup** - Value must exist in reference table
- **unique** - No duplicates allowed

## Database Tables

- **Production.Products** - Product codes and names
- **Production.Operators** - Operator IDs and names
- **Production.QualityData** - Main table for production records
- **Production.DataValidationErrors** - Table for error records

## Sample Data

The included CSV file has intentional errors to test validation:
- Row 3: Temperature too high (300°C, max is 200°C)
- Row 5: Duplicate batch number
- Row 6: Invalid product code
- Row 7: Invalid operator ID
- Row 8: Missing timestamp

## Built With

- Python
- SQL Server
- XML for configuration

## License

MIT