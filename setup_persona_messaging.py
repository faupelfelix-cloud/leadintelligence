#!/usr/bin/env python3
"""
Setup Persona Messaging table and backfill Persona Category field on existing leads.

Usage:
    # Create Persona Messaging table with default data
    python setup_persona_messaging.py --create-table
    
    # Backfill Persona Category on Leads table (existing leads)
    python setup_persona_messaging.py --backfill-leads
    
    # Do everything (create table + backfill leads)
    python setup_persona_messaging.py --all
    
    # Dry run (show what would change, don't write)
    python setup_persona_messaging.py --backfill-leads --dry-run
    
NOTE: Campaign Leads table does NOT need backfill — add a Lookup field
called "Persona Category" that pulls from Linked Lead → Persona Category.
"""

import argparse
import json
import logging
import yaml
from pyairtable import Api

from company_profile_utils import classify_persona, PERSONA_BUCKETS, DEFAULT_PERSONA_MESSAGING

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)


def load_config():
    with open('config.yaml', 'r') as f:
        return yaml.safe_load(f)


def create_persona_messaging_table(base):
    """Create (or populate) the Persona Messaging table with default data.
    
    NOTE: Airtable API cannot create tables — this populates an existing table.
    You need to manually create the table first with these columns:
    
    Table name: "Persona Messaging"
    Columns:
        - Persona (Single line text) — primary field
        - Value Drivers (Long text)
        - Proof Points (Long text)
        - Tone (Long text)
        - What They Dont Want (Long text)
        - Example Angles (Long text)
        - Description (Long text) — what this persona bucket represents
    """
    table = base.table('Persona Messaging')
    
    # Check if already populated
    existing = table.all()
    if existing:
        existing_personas = [r['fields'].get('Persona', '') for r in existing]
        logger.info(f"Persona Messaging table already has {len(existing)} records:")
        for p in existing_personas:
            logger.info(f"  - {p}")
        logger.info("Skipping creation. Delete existing records first if you want to re-populate.")
        return
    
    logger.info("Populating Persona Messaging table...")
    
    for persona_name, messaging in DEFAULT_PERSONA_MESSAGING.items():
        bucket_info = PERSONA_BUCKETS.get(persona_name, {})
        
        record_data = {
            'Persona': persona_name,
            'Value Drivers': messaging.get('Value Drivers', ''),
            'Proof Points': messaging.get('Proof Points', ''),
            'Tone': messaging.get('Tone', ''),
            'What They Dont Want': messaging.get('What They Dont Want', ''),
            'Example Angles': messaging.get('Example Angles', ''),
            'Description': bucket_info.get('description', ''),
        }
        
        try:
            table.create(record_data)
            logger.info(f"  ✓ Created: {persona_name}")
        except Exception as e:
            logger.error(f"  ✗ Failed to create {persona_name}: {e}")
    
    logger.info(f"\n✓ Persona Messaging table populated with {len(DEFAULT_PERSONA_MESSAGING)} personas")
    logger.info("\nKeyword mapping for each persona:")
    for persona_name, bucket_info in PERSONA_BUCKETS.items():
        keywords = bucket_info.get('keywords', [])
        logger.info(f"  {persona_name}: {', '.join(keywords[:8])}...")


def backfill_persona_category(base, config, table_key='leads', dry_run=False):
    """Backfill Persona Category field on existing leads based on their Title.
    
    Args:
        base: Airtable base
        config: Config dict
        table_key: 'leads' or 'campaign_leads'
        dry_run: If True, only report what would change
    """
    if table_key == 'leads':
        table_name = config['airtable']['tables'].get('leads', 'Leads')
    elif table_key == 'campaign_leads':
        table_name = config['airtable']['tables'].get('campaign_leads', 'Campaign Leads')
    else:
        raise ValueError(f"Unknown table key: {table_key}")
    
    table = base.table(table_name)
    
    logger.info(f"\n{'='*60}")
    logger.info(f"Backfilling Persona Category on: {table_name}")
    logger.info(f"{'='*60}")
    
    # Fetch all records
    logger.info("Fetching records...")
    records = table.all()
    logger.info(f"Found {len(records)} total records")
    
    # Filter to those with a title but no persona
    needs_update = []
    already_set = 0
    no_title = 0
    
    for record in records:
        fields = record['fields']
        title = fields.get('Title', '').strip()
        existing_persona = fields.get('Persona Category', '').strip()
        
        if not title:
            no_title += 1
            continue
        
        if existing_persona:
            already_set += 1
            continue
        
        persona = classify_persona(title)
        needs_update.append({
            'id': record['id'],
            'name': fields.get('Lead Name', 'Unknown'),
            'title': title,
            'persona': persona
        })
    
    logger.info(f"\nSummary:")
    logger.info(f"  Already has Persona Category: {already_set}")
    logger.info(f"  No title (can't classify): {no_title}")
    logger.info(f"  Needs update: {len(needs_update)}")
    
    if not needs_update:
        logger.info("Nothing to update!")
        return
    
    # Show distribution
    from collections import Counter
    persona_counts = Counter(item['persona'] for item in needs_update)
    logger.info(f"\nPersona distribution (to be set):")
    for persona, count in sorted(persona_counts.items(), key=lambda x: -x[1]):
        logger.info(f"  {persona}: {count}")
    
    # Show examples
    logger.info(f"\nExamples:")
    for item in needs_update[:15]:
        logger.info(f"  {item['title'][:50]:50s} → {item['persona']}")
    if len(needs_update) > 15:
        logger.info(f"  ... and {len(needs_update) - 15} more")
    
    if dry_run:
        logger.info(f"\n--- DRY RUN — no changes made ---")
        return
    
    # Apply updates
    logger.info(f"\nApplying updates...")
    success = 0
    errors = 0
    
    for idx, item in enumerate(needs_update, 1):
        try:
            table.update(item['id'], {'Persona Category': item['persona']})
            success += 1
            if idx % 50 == 0:
                logger.info(f"  Progress: {idx}/{len(needs_update)}")
        except Exception as e:
            logger.error(f"  Error updating {item['name']}: {e}")
            errors += 1
    
    logger.info(f"\n✓ Backfill complete: {success} updated, {errors} errors")


def main():
    parser = argparse.ArgumentParser(description='Setup Persona Messaging and backfill Persona Category')
    parser.add_argument('--create-table', action='store_true', help='Populate Persona Messaging table')
    parser.add_argument('--backfill-leads', action='store_true', help='Backfill Persona Category on Leads')
    parser.add_argument('--all', action='store_true', help='Create table + backfill leads')
    parser.add_argument('--dry-run', action='store_true', help='Show what would change without writing')
    args = parser.parse_args()
    
    if not any([args.create_table, args.backfill_leads, args.all]):
        parser.print_help()
        return
    
    config = load_config()
    api = Api(config['airtable']['api_key'])
    base = api.base(config['airtable']['base_id'])
    
    if args.create_table or args.all:
        create_persona_messaging_table(base)
    
    if args.backfill_leads or args.all:
        backfill_persona_category(base, config, 'leads', dry_run=args.dry_run)


if __name__ == '__main__':
    main()
