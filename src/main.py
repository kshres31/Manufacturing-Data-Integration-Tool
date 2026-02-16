"""
Manufacturing Data Integration Tool - Main Entry Point
Usage: python main.py --input data/raw/production_data_20240215.csv
"""

import argparse
import os
import sys
import glob
from datetime import datetime

# Add project root to path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.insert(0, project_root)

from etl_processor import run_etl_pipeline


def process_single_file(file_path: str, config_path: str):
    """Process one CSV file"""
    if not os.path.exists(file_path):
        print(f"Error: File not found: {file_path}")
        return False
    
    try:
        run_etl_pipeline(file_path, config_path)
        return True
    except Exception as e:
        print(f"Failed to process {file_path}: {e}")
        return False


def process_batch(pattern: str, config_path: str):
    """Process multiple files matching pattern"""
    files = glob.glob(pattern)
    
    if not files:
        print(f"No files found matching: {pattern}")
        return
    
    print(f"\nFound {len(files)} files to process")
    
    success_count = 0
    for i, file_path in enumerate(files, 1):
        print(f"\n{'='*60}")
        print(f"Processing file {i}/{len(files)}: {os.path.basename(file_path)}")
        print(f"{'='*60}")
        
        if process_single_file(file_path, config_path):
            success_count += 1
    
    print(f"\n{'='*60}")
    print(f"BATCH COMPLETE: {success_count}/{len(files)} files processed successfully")
    print(f"{'='*60}")


def main():
    parser = argparse.ArgumentParser(
        description='Manufacturing Data Integration Tool',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py --input data/raw/production_data_20240215.csv
  python main.py --batch "data/raw/*.csv"
  python main.py --batch "data/raw/production_data_*.csv" --dry-run
        """
    )
    
    parser.add_argument('--input', '-i', 
                       help='Single CSV file to process')
    parser.add_argument('--batch', '-b',
                       help='Process multiple files (glob pattern)')
    parser.add_argument('--config', '-c',
                       default=os.path.join(project_root, 'config', 'mapping_config.xml'),
                       help='Path to XML config file')
    parser.add_argument('--dry-run', action='store_true',
                       help='Validate only, do not load to database')
    
    args = parser.parse_args()
    
    if not args.input and not args.batch:
        # Default: process all files in data/raw/
        args.batch = os.path.join(project_root, 'data', 'raw', '*.csv')
        print("No input specified, processing all files in data/raw/")
    
    if args.input:
        process_single_file(args.input, args.config)
    elif args.batch:
        process_batch(args.batch, args.config)


if __name__ == "__main__":
    main()