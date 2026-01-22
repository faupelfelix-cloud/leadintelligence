#!/usr/bin/env python3
"""
First-time setup script - Reads from environment variables or prompts for input
"""

import os
import sys
import yaml

def setup():
    print("="*60)
    print("LEAD INTELLIGENCE SYSTEM - SETUP")
    print("="*60)
    print()
    
    # Check if config.yaml already exists
    if os.path.exists('config.yaml'):
        print("✓ config.yaml already exists")
        print()
    
    # Try to get from environment variables (GitHub Secrets)
    airtable_key = os.environ.get('AIRTABLE_API_KEY')
    airtable_base = os.environ.get('AIRTABLE_BASE_ID')
    anthropic_key = os.environ.get('ANTHROPIC_API_KEY')
    
    if airtable_key and airtable_base and anthropic_key:
        print("✓ Found API keys in environment variables (GitHub Secrets)")
    else:
        print("⚠ API keys not found in environment variables")
        print("Please enter your API credentials:")
        print()
        
        if not airtable_key:
            airtable_key = input("Airtable API key: ").strip()
        if not airtable_base:
            airtable_base = input("Airtable Base ID: ").strip()
        if not anthropic_key:
            anthropic_key = input("Anthropic API key: ").strip()
        print()
    
    # Load example config
    with open('config.example.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    # Update with values
    config['airtable']['api_key'] = airtable_key
    config['airtable']['base_id'] = airtable_base
    config['anthropic']['api_key'] = anthropic_key
    
    # Write config.yaml
    with open('config.yaml', 'w') as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False)
    
    print("✓ config.yaml created successfully!")
    print()
    
    # Install dependencies
    print("Installing dependencies...")
    os.system('pip install -r requirements.txt')
    print()
    
    # Run validation
    print("="*60)
    print("VALIDATING SETUP")
    print("="*60)
    os.system('python validate_setup.py')
    
    print()
    print("="*60)
    print("SETUP COMPLETE!")
    print("="*60)
    print()
    print("Next steps:")
    print("1. Add your leads to Airtable")
    print("2. Test with: python enrich_companies.py --limit 5")
    print()

if __name__ == "__main__":
    try:
        setup()
    except KeyboardInterrupt:
        print("\n\nSetup cancelled.")
        sys.exit(1)
    except Exception as e:
        print(f"\n\nError during setup: {e}")
        sys.exit(1)
