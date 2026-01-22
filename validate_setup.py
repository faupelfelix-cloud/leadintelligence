#!/usr/bin/env python3
"""
Validation Script - Test Airtable connection and field mappings
Run this before enrichment to catch configuration issues early
"""

import yaml
import sys
from pyairtable import Api
from datetime import datetime

def test_connection(config_path="config.yaml"):
    """Test Airtable connection and validate field names"""
    
    print("="*60)
    print("AIRTABLE VALIDATION TEST")
    print("="*60)
    
    # Load config
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        print("✓ Config file loaded successfully")
    except Exception as e:
        print(f"✗ Failed to load config: {e}")
        return False
    
    # Check API keys
    if not config['airtable']['api_key']:
        print("✗ Airtable API key not configured")
        return False
    print("✓ Airtable API key found")
    
    if config['anthropic']['api_key'] == 'YOUR_ANTHROPIC_API_KEY_HERE':
        print("⚠ Anthropic API key not configured (add it before running enrichment)")
    else:
        print("✓ Anthropic API key configured")
    
    # Connect to Airtable
    try:
        airtable = Api(config['airtable']['api_key'])
        base = airtable.base(config['airtable']['base_id'])
        print("✓ Connected to Airtable")
    except Exception as e:
        print(f"✗ Failed to connect to Airtable: {e}")
        return False
    
    # Test each table
    tables_to_test = {
        'Companies': config['airtable']['tables']['companies'],
        'Leads': config['airtable']['tables']['leads'],
        'Intelligence Log': config['airtable']['tables']['intelligence_log']
    }
    
    expected_fields = {
        'Companies': [
            'Company Name', 'Website', 'LinkedIn Company Page', 'Location/HQ',
            'Company Size', 'Focus Area', 'Technology Platform', 'Funding Stage',
            'Total Funding', 'Latest Funding Round', 'Pipeline Stage', 'Lead Programs',
            'Therapeutic Areas', 'Current CDMO Partnerships', 'Manufacturing Status',
            'Enrichment Status', 'ICP Fit Score', 'Urgency Score', 'Last Intelligence Check',
            'Intelligence Notes'
        ],
        'Leads': [
            'Lead Name', 'CRM Lead ID', 'Title', 'Email', 'LinkedIn URL', 'Company',
            'Status', 'Enrichment Status', 'Enrichment Confidence', 'Intelligence Notes',
            'Last Contacted'
        ],
        'Intelligence Log': [
            'Date', 'Record Type', 'Lead', 'Company', 'Intelligence Type',
            'Summary', 'Source URL', 'Confidence Level'
        ]
    }
    
    print("\n" + "="*60)
    print("TESTING TABLES AND FIELDS")
    print("="*60)
    
    all_valid = True
    
    for table_display_name, table_name in tables_to_test.items():
        print(f"\n{table_display_name} Table:")
        print("-" * 40)
        
        try:
            table = base.table(table_name)
            
            # Try to get schema (fetch one record to see fields)
            records = table.all(max_records=1)
            
            if not records:
                print(f"  ⚠ Table is empty (this is OK for a new setup)")
                print(f"  → Expected fields: {', '.join(expected_fields[table_display_name][:5])}...")
            else:
                actual_fields = set(records[0]['fields'].keys())
                expected = set(expected_fields[table_display_name])
                
                print(f"  ✓ Table accessible")
                print(f"  → Found {len(records)} record(s)")
                
                # Check for missing expected fields
                missing = expected - actual_fields
                if missing:
                    print(f"  ⚠ Missing expected fields: {', '.join(missing)}")
                    print(f"    (These might not be set yet, which is OK)")
                
                # Show sample of actual fields
                print(f"  → Sample fields: {', '.join(list(actual_fields)[:5])}...")
                
        except Exception as e:
            print(f"  ✗ Error accessing table: {e}")
            all_valid = False
    
    # Test field value validation
    print("\n" + "="*60)
    print("TESTING FIELD VALUE VALIDATION")
    print("="*60)
    
    # Test select field options
    test_values = {
        'Company Size': ['1-10', '11-50', '51-200'],
        'Focus Area': ['mAbs', 'Bispecifics', 'ADCs'],
        'Funding Stage': ['Series B', 'Series C', 'Unknown'],
        'Manufacturing Status': ['No Public Partner', 'Has Partner']
    }
    
    print("\nTesting if we can write to select fields...")
    
    try:
        companies_table = base.table(config['airtable']['tables']['companies'])
        
        # Try to create and delete a test record
        test_record = {
            'Company Name': 'TEST_VALIDATION_DELETE_ME',
            'Enrichment Status': 'Not Enriched',
            'Company Size': '11-50',
            'Funding Stage': 'Unknown',
            'Manufacturing Status': 'Unknown',
            'ICP Fit Score': 50,
            'Urgency Score': 50,
            'Last Intelligence Check': datetime.now().strftime('%Y-%m-%d')
        }
        
        created = companies_table.create(test_record)
        print("✓ Successfully created test record")
        
        # Try updating with multiple select
        update_data = {
            'Focus Area': ['mAbs', 'Bispecifics'],
            'Technology Platform': ['Mammalian CHO'],
            'Pipeline Stage': ['Phase 2']
        }
        companies_table.update(created['id'], update_data)
        print("✓ Successfully updated with multiple select fields")
        
        # Clean up
        companies_table.delete(created['id'])
        print("✓ Successfully deleted test record")
        print("\n✓ All field validations passed!")
        
    except Exception as e:
        print(f"\n✗ Field validation failed: {e}")
        print("  This might mean:")
        print("  1. Field names don't match exactly (check capitalization)")
        print("  2. Select field options don't match")
        print("  3. Field types are configured incorrectly")
        all_valid = False
    
    # Final summary
    print("\n" + "="*60)
    if all_valid:
        print("✓ VALIDATION PASSED - Ready to enrich!")
    else:
        print("✗ VALIDATION FAILED - Please fix issues above")
    print("="*60)
    
    return all_valid


if __name__ == "__main__":
    success = test_connection()
    sys.exit(0 if success else 1)
