#!/usr/bin/env python3
"""
Conference Intelligence Diagnostics
Check why conferences aren't being monitored
"""

import os
import sys
import yaml
from datetime import datetime, timedelta
from pyairtable import Api

def diagnose():
    """Diagnose conference monitoring issues"""
    
    # Load config
    with open('config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    # Initialize Airtable
    airtable = Api(config['airtable']['api_key'])
    base = airtable.base(config['airtable']['base_id'])
    conferences_table = base.table(config['airtable']['tables']['conferences'])
    
    print("="*70)
    print("CONFERENCE INTELLIGENCE DIAGNOSTICS")
    print("="*70)
    print()
    
    today = datetime.now()
    print(f"Today's date: {today.strftime('%Y-%m-%d')}")
    print()
    
    # Get all conferences
    all_conferences = conferences_table.all()
    print(f"Total conferences in table: {len(all_conferences)}")
    print()
    
    if not all_conferences:
        print("❌ No conferences found in table!")
        print("   → Add conferences to 'Conferences We Attend' table")
        return
    
    print("="*70)
    print("CHECKING EACH CONFERENCE")
    print("="*70)
    print()
    
    for idx, conf in enumerate(all_conferences, 1):
        fields = conf['fields']
        conf_name = fields.get('Conference Name', 'Unnamed')
        conf_date_str = fields.get('Date')
        
        print(f"{idx}. {conf_name}")
        print("   " + "-"*60)
        
        # Check 1: Has date?
        if not conf_date_str:
            print("   ❌ SKIP REASON: No date set")
            print("      DEBUG: 'Date' field is empty or null")
            print()
            continue
        
        print(f"   Conference Date (raw from Airtable): '{conf_date_str}'")
        print(f"   Date type: {type(conf_date_str)}")
        
        # Try to detect the format
        if isinstance(conf_date_str, str):
            if '.' in conf_date_str:
                print(f"   ⚠️  Format detected: European (DD.M.YYYY or similar)")
                print(f"   ⚠️  Expected format: ISO (YYYY-MM-DD)")
                print(f"   ❌ SKIP REASON: Date format not ISO")
                print()
                continue
            elif '/' in conf_date_str:
                print(f"   ⚠️  Format detected: US (M/D/YYYY or similar)")
                print(f"   ⚠️  Expected format: ISO (YYYY-MM-DD)")
                print(f"   ❌ SKIP REASON: Date format not ISO")
                print()
                continue
        
        # Parse date
        try:
            conf_date = datetime.strptime(conf_date_str, '%Y-%m-%d')
        except:
            print(f"   ❌ SKIP REASON: Invalid date format (expected YYYY-MM-DD)")
            print()
            continue
        
        # Calculate monitoring window
        monitoring_start = conf_date - timedelta(days=120)
        days_until = (conf_date - today).days
        days_since_start = (today - monitoring_start).days
        
        print(f"   Monitoring Start: {monitoring_start.strftime('%Y-%m-%d')} (4 months before)")
        print(f"   Days until conference: {days_until}")
        
        # Check 2: Too early?
        if today < monitoring_start:
            print(f"   ❌ SKIP REASON: Too early (starts in {-days_since_start} days)")
            print()
            continue
        
        # Check 3: Already happened?
        if today > conf_date:
            print(f"   ❌ SKIP REASON: Conference already happened ({abs(days_until)} days ago)")
            print()
            continue
        
        print(f"   ✓ Within monitoring window (started {days_since_start} days ago)")
        
        # Check 4: Last monitored
        last_monitored_str = fields.get('Last Monitored')
        if last_monitored_str:
            try:
                last_monitored = datetime.strptime(last_monitored_str, '%Y-%m-%d')
                days_since = (today - last_monitored).days
                print(f"   Last Monitored: {last_monitored_str} ({days_since} days ago)")
                
                if days_since < 14:
                    print(f"   ❌ SKIP REASON: Monitored recently (need 14+ days, only {days_since})")
                    print()
                    continue
                else:
                    print(f"   ✓ Ready for monitoring (14+ days since last check)")
            except:
                print(f"   ⚠ Last Monitored date invalid: {last_monitored_str}")
        else:
            print(f"   Last Monitored: Never")
            print(f"   ✓ Ready for first monitoring run")
        
        # Check 5: ICP Filter
        icp_filter = fields.get('ICP Filter', False)
        focus_areas = fields.get('Focus Areas', [])
        
        print(f"   ICP Filter: {'ON' if icp_filter else 'OFF'}")
        print(f"   Focus Areas: {', '.join(focus_areas) if focus_areas else 'None set'}")
        
        if icp_filter:
            if 'Biologics' not in focus_areas:
                print(f"   ❌ SKIP REASON: ICP filter ON but 'Biologics' not in Focus Areas")
                print()
                continue
            else:
                print(f"   ✓ ICP filter passed (Biologics included)")
        else:
            print(f"   ✓ ICP filter OFF (will monitor all)")
        
        # Passed all checks!
        print()
        print(f"   ✅ SHOULD BE MONITORED!")
        print()
    
    print("="*70)
    print("SUMMARY")
    print("="*70)
    print()
    print("Common issues:")
    print("1. Conference dates not set → Set 'Date' field (format: YYYY-MM-DD)")
    print("2. Conferences too far in future → Monitoring starts 4 months before")
    print("3. Conferences already happened → Remove or update date")
    print("4. Monitored <14 days ago → Wait until 14 days since last check")
    print("5. ICP Filter ON without 'Biologics' → Add 'Biologics' to Focus Areas or turn off filter")
    print()
    print("="*70)
    print("HOW TO FIX DATE FORMAT IN AIRTABLE")
    print("="*70)
    print()
    print("If you see dates like '12.1.2026' or '1/12/2026' above:")
    print()
    print("1. Open Airtable → Conferences We Attend table")
    print("2. Click the 'Date' column header")
    print("3. Click 'Customize field type'")
    print("4. Change 'Date format' to: ISO (YYYY-MM-DD)")
    print("5. Make sure 'Include time' is UNCHECKED")
    print("6. Click 'Save'")
    print()
    print("After this change, dates will show as: 2026-01-12")
    print("And the monitoring system will work!")
    print()
    print("Alternative: If the field format won't change, you may need to:")
    print("1. Create a NEW date field called 'Date ISO'")
    print("2. Use formula: DATETIME_FORMAT({Date}, 'YYYY-MM-DD')")
    print("3. Convert it to a regular date field")
    print("4. Delete old 'Date' field and rename 'Date ISO' to 'Date'")
    print()


if __name__ == "__main__":
    try:
        diagnose()
    except Exception as e:
        print(f"Error running diagnostics: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
