#!/usr/bin/env python3
"""
Cleanup Orphan Triggers
- Identifies triggers without linked leads
- Attempts to auto-link based on name/company matching
- Optionally deletes truly orphan triggers
"""

import os
import sys
import yaml
import logging
import argparse
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from pyairtable import Api

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('trigger_cleanup.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class TriggerCleanup:
    """Clean up orphan triggers in Trigger History table"""
    
    def __init__(self, config_path: str = "config.yaml"):
        """Initialize with configuration"""
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        # Initialize Airtable
        self.airtable = Api(self.config['airtable']['api_key'])
        self.base = self.airtable.base(self.config['airtable']['base_id'])
        
        # Tables
        self.trigger_table = self.base.table('Trigger History')
        self.leads_table = self.base.table(self.config['airtable']['tables']['leads'])
        self.companies_table = self.base.table(self.config['airtable']['tables']['companies'])
        
        # Cache for leads lookup
        self.leads_cache = {}
        self.companies_cache = {}
        
        logger.info("TriggerCleanup initialized")
    
    def load_leads_cache(self):
        """Load all leads into cache for fast lookup"""
        logger.info("Loading leads cache...")
        
        all_leads = self.leads_table.all()
        
        for lead in all_leads:
            fields = lead.get('fields', {})
            record_id = lead.get('id')
            name = fields.get('Lead Name', '').strip().lower()
            email = fields.get('Email', '').strip().lower()
            
            # Get company name (might be linked or text)
            company_field = fields.get('Company', '')
            if isinstance(company_field, list):
                # It's a linked field - we'd need to look up the company name
                company = ''  # Will match by name instead
            else:
                company = str(company_field).strip().lower()
            
            # Store by name
            if name:
                key = name
                if key not in self.leads_cache:
                    self.leads_cache[key] = []
                self.leads_cache[key].append({
                    'id': record_id,
                    'name': fields.get('Lead Name', ''),
                    'email': email,
                    'company': company,
                    'title': fields.get('Title', '')
                })
            
            # Store by email
            if email:
                if email not in self.leads_cache:
                    self.leads_cache[email] = []
                self.leads_cache[email].append({
                    'id': record_id,
                    'name': fields.get('Lead Name', ''),
                    'email': email,
                    'company': company,
                    'title': fields.get('Title', '')
                })
        
        logger.info(f"  Loaded {len(all_leads)} leads into cache")
    
    def find_matching_lead(self, trigger_fields: Dict) -> Optional[str]:
        """Try to find a matching lead for a trigger"""
        
        # Get trigger info
        lead_name = trigger_fields.get('Lead Name', '').strip()
        company_name = trigger_fields.get('Company Name', '').strip()
        
        if not lead_name or lead_name.lower() == 'information not available':
            return None
        
        # Try exact name match
        name_key = lead_name.lower()
        if name_key in self.leads_cache:
            matches = self.leads_cache[name_key]
            
            # If only one match, use it
            if len(matches) == 1:
                return matches[0]['id']
            
            # Multiple matches - try to match by company
            if company_name:
                company_lower = company_name.lower()
                for match in matches:
                    if company_lower in match.get('company', '').lower() or \
                       match.get('company', '').lower() in company_lower:
                        return match['id']
            
            # Return first match if no company match
            return matches[0]['id']
        
        # Try partial name match (first + last name)
        name_parts = lead_name.lower().split()
        if len(name_parts) >= 2:
            for cache_key, matches in self.leads_cache.items():
                if not '@' in cache_key:  # Skip email keys
                    cache_parts = cache_key.split()
                    if len(cache_parts) >= 2:
                        # Match first and last name
                        if name_parts[0] == cache_parts[0] and name_parts[-1] == cache_parts[-1]:
                            return matches[0]['id']
        
        return None
    
    def get_orphan_triggers(self) -> List[Dict]:
        """Get all triggers without linked leads"""
        
        # Formula: Lead field is empty
        formula = "OR({Lead} = '', {Lead} = BLANK())"
        
        try:
            orphans = self.trigger_table.all(formula=formula)
            logger.info(f"Found {len(orphans)} orphan triggers")
            return orphans
        except Exception as e:
            logger.error(f"Error fetching orphan triggers: {str(e)}")
            return []
    
    def analyze_orphans(self, orphans: List[Dict]) -> Dict:
        """Analyze orphan triggers and categorize them"""
        
        analysis = {
            'total': len(orphans),
            'can_link': [],      # Have lead name, can try to link
            'no_name': [],       # No lead name - likely company-level triggers
            'info_not_available': [],  # "Information Not Available" placeholder
            'by_trigger_type': {},
            'by_company': {}
        }
        
        for orphan in orphans:
            fields = orphan.get('fields', {})
            record_id = orphan.get('id')
            lead_name = fields.get('Lead Name', '').strip()
            company = fields.get('Company Name', 'Unknown')
            trigger_type = fields.get('Trigger Type', 'Unknown')
            
            # Categorize
            if not lead_name:
                analysis['no_name'].append({
                    'id': record_id,
                    'company': company,
                    'trigger_type': trigger_type,
                    'description': fields.get('Trigger Description', '')[:100]
                })
            elif lead_name.lower() == 'information not available':
                analysis['info_not_available'].append({
                    'id': record_id,
                    'company': company,
                    'trigger_type': trigger_type,
                    'description': fields.get('Trigger Description', '')[:100]
                })
            else:
                analysis['can_link'].append({
                    'id': record_id,
                    'lead_name': lead_name,
                    'company': company,
                    'trigger_type': trigger_type
                })
            
            # Count by trigger type
            if trigger_type not in analysis['by_trigger_type']:
                analysis['by_trigger_type'][trigger_type] = 0
            analysis['by_trigger_type'][trigger_type] += 1
            
            # Count by company
            if company not in analysis['by_company']:
                analysis['by_company'][company] = 0
            analysis['by_company'][company] += 1
        
        return analysis
    
    def link_orphan_triggers(self, dry_run: bool = True) -> Dict:
        """Attempt to link orphan triggers to existing leads"""
        
        results = {
            'attempted': 0,
            'linked': 0,
            'not_found': 0,
            'errors': 0,
            'linked_details': [],
            'not_found_details': []
        }
        
        # Load leads cache
        self.load_leads_cache()
        
        # Get orphans that can potentially be linked
        orphans = self.get_orphan_triggers()
        analysis = self.analyze_orphans(orphans)
        
        linkable = analysis['can_link']
        logger.info(f"\nAttempting to link {len(linkable)} triggers with lead names...")
        
        for item in linkable:
            results['attempted'] += 1
            record_id = item['id']
            lead_name = item['lead_name']
            company = item['company']
            
            # Try to find matching lead
            matching_lead_id = self.find_matching_lead({
                'Lead Name': lead_name,
                'Company Name': company
            })
            
            if matching_lead_id:
                if dry_run:
                    logger.info(f"  [DRY RUN] Would link: {lead_name} @ {company}")
                    results['linked'] += 1
                    results['linked_details'].append({
                        'trigger_id': record_id,
                        'lead_name': lead_name,
                        'company': company,
                        'lead_id': matching_lead_id
                    })
                else:
                    try:
                        self.trigger_table.update(record_id, {
                            'Lead': [matching_lead_id]
                        })
                        logger.info(f"  ✓ Linked: {lead_name} @ {company}")
                        results['linked'] += 1
                        results['linked_details'].append({
                            'trigger_id': record_id,
                            'lead_name': lead_name,
                            'company': company,
                            'lead_id': matching_lead_id
                        })
                    except Exception as e:
                        logger.error(f"  ✗ Error linking {lead_name}: {str(e)}")
                        results['errors'] += 1
            else:
                results['not_found'] += 1
                results['not_found_details'].append({
                    'trigger_id': record_id,
                    'lead_name': lead_name,
                    'company': company
                })
                logger.info(f"  ○ No match found: {lead_name} @ {company}")
        
        return results
    
    def delete_orphan_triggers(self, delete_type: str = 'all', dry_run: bool = True) -> Dict:
        """Delete orphan triggers
        
        delete_type options:
        - 'all': Delete all orphans
        - 'no_name': Delete only triggers without a lead name
        - 'info_not_available': Delete only "Information Not Available" triggers
        - 'unlinked_after_attempt': Delete triggers that couldn't be linked
        """
        
        results = {
            'type': delete_type,
            'attempted': 0,
            'deleted': 0,
            'errors': 0,
            'deleted_details': []
        }
        
        orphans = self.get_orphan_triggers()
        analysis = self.analyze_orphans(orphans)
        
        # Determine which to delete
        to_delete = []
        
        if delete_type == 'all':
            to_delete = orphans
        elif delete_type == 'no_name':
            to_delete = [{'id': item['id'], **item} for item in analysis['no_name']]
        elif delete_type == 'info_not_available':
            to_delete = [{'id': item['id'], **item} for item in analysis['info_not_available']]
        elif delete_type == 'outreach_generated':
            # Delete orphans that already have outreach generated (not useful without lead)
            for orphan in orphans:
                fields = orphan.get('fields', {})
                if fields.get('Email Body') or fields.get('LinkedIn Message'):
                    to_delete.append(orphan)
        
        logger.info(f"\n{'[DRY RUN] ' if dry_run else ''}Deleting {len(to_delete)} orphan triggers (type: {delete_type})...")
        
        for item in to_delete:
            record_id = item.get('id') or item.get('trigger_id')
            fields = item.get('fields', item)
            lead_name = fields.get('Lead Name', fields.get('lead_name', 'N/A'))
            company = fields.get('Company Name', fields.get('company', 'N/A'))
            trigger_type = fields.get('Trigger Type', fields.get('trigger_type', 'N/A'))
            
            results['attempted'] += 1
            
            if dry_run:
                logger.info(f"  [DRY RUN] Would delete: {trigger_type} - {lead_name} @ {company}")
                results['deleted'] += 1
                results['deleted_details'].append({
                    'id': record_id,
                    'lead_name': lead_name,
                    'company': company,
                    'trigger_type': trigger_type
                })
            else:
                try:
                    self.trigger_table.delete(record_id)
                    logger.info(f"  ✓ Deleted: {trigger_type} - {lead_name} @ {company}")
                    results['deleted'] += 1
                    results['deleted_details'].append({
                        'id': record_id,
                        'lead_name': lead_name,
                        'company': company,
                        'trigger_type': trigger_type
                    })
                except Exception as e:
                    logger.error(f"  ✗ Error deleting: {str(e)}")
                    results['errors'] += 1
        
        return results
    
    def run_report(self):
        """Generate a report of orphan triggers"""
        
        logger.info("="*60)
        logger.info("ORPHAN TRIGGER ANALYSIS REPORT")
        logger.info("="*60)
        
        orphans = self.get_orphan_triggers()
        analysis = self.analyze_orphans(orphans)
        
        logger.info(f"\nTotal orphan triggers: {analysis['total']}")
        logger.info(f"  - With lead name (can try to link): {len(analysis['can_link'])}")
        logger.info(f"  - No lead name: {len(analysis['no_name'])}")
        logger.info(f"  - 'Information Not Available': {len(analysis['info_not_available'])}")
        
        logger.info(f"\nBy Trigger Type:")
        for trigger_type, count in sorted(analysis['by_trigger_type'].items(), key=lambda x: -x[1]):
            logger.info(f"  - {trigger_type}: {count}")
        
        logger.info(f"\nTop Companies with Orphan Triggers:")
        sorted_companies = sorted(analysis['by_company'].items(), key=lambda x: -x[1])[:10]
        for company, count in sorted_companies:
            logger.info(f"  - {company}: {count}")
        
        if analysis['can_link']:
            logger.info(f"\nTriggers that can potentially be linked:")
            for item in analysis['can_link'][:10]:
                logger.info(f"  - {item['lead_name']} @ {item['company']} ({item['trigger_type']})")
            if len(analysis['can_link']) > 10:
                logger.info(f"  ... and {len(analysis['can_link']) - 10} more")
        
        logger.info("\n" + "="*60)
        logger.info("RECOMMENDATIONS")
        logger.info("="*60)
        
        if analysis['can_link']:
            logger.info(f"\n1. Run with --link to auto-link {len(analysis['can_link'])} triggers to existing leads")
        
        if analysis['info_not_available']:
            logger.info(f"\n2. Run with --delete info_not_available to remove {len(analysis['info_not_available'])} placeholder triggers")
        
        if analysis['no_name']:
            logger.info(f"\n3. Review {len(analysis['no_name'])} company-level triggers (no lead name)")
            logger.info("   These might be useful for company intelligence but not for outreach")
        
        logger.info("\n" + "="*60)
        
        return analysis


def main():
    """CLI entry point"""
    parser = argparse.ArgumentParser(description='Cleanup Orphan Triggers')
    parser.add_argument('--config', default='config.yaml', help='Path to config file')
    parser.add_argument('--report', action='store_true', help='Generate analysis report only')
    parser.add_argument('--link', action='store_true', help='Attempt to link orphans to existing leads')
    parser.add_argument('--delete', choices=['all', 'no_name', 'info_not_available', 'outreach_generated'],
                        help='Delete orphan triggers')
    parser.add_argument('--dry-run', action='store_true', default=True,
                        help='Show what would be done without making changes (default)')
    parser.add_argument('--execute', action='store_true',
                        help='Actually execute changes (opposite of --dry-run)')
    
    args = parser.parse_args()
    
    # Determine if dry run
    dry_run = not args.execute
    
    if dry_run and (args.link or args.delete):
        logger.info("="*60)
        logger.info("DRY RUN MODE - No changes will be made")
        logger.info("Add --execute to actually make changes")
        logger.info("="*60 + "\n")
    
    try:
        cleanup = TriggerCleanup(config_path=args.config)
        
        if args.report or (not args.link and not args.delete):
            # Default: just show report
            cleanup.run_report()
        
        if args.link:
            results = cleanup.link_orphan_triggers(dry_run=dry_run)
            logger.info("\n" + "="*60)
            logger.info("LINK RESULTS")
            logger.info("="*60)
            logger.info(f"Attempted: {results['attempted']}")
            logger.info(f"Linked: {results['linked']}")
            logger.info(f"Not found: {results['not_found']}")
            logger.info(f"Errors: {results['errors']}")
            
            if results['not_found_details']:
                logger.info(f"\nLeads not found in database ({len(results['not_found_details'])}):")
                for item in results['not_found_details'][:10]:
                    logger.info(f"  - {item['lead_name']} @ {item['company']}")
                if len(results['not_found_details']) > 10:
                    logger.info(f"  ... and {len(results['not_found_details']) - 10} more")
        
        if args.delete:
            results = cleanup.delete_orphan_triggers(delete_type=args.delete, dry_run=dry_run)
            logger.info("\n" + "="*60)
            logger.info("DELETE RESULTS")
            logger.info("="*60)
            logger.info(f"Type: {results['type']}")
            logger.info(f"Attempted: {results['attempted']}")
            logger.info(f"Deleted: {results['deleted']}")
            logger.info(f"Errors: {results['errors']}")
        
        if dry_run and (args.link or args.delete):
            logger.info("\n" + "="*60)
            logger.info("This was a DRY RUN - no changes were made")
            logger.info("Run with --execute to apply changes")
            logger.info("="*60)
        
    except FileNotFoundError:
        logger.error(f"Config file not found: {args.config}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
