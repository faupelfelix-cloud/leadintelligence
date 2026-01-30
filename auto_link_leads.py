#!/usr/bin/env python3
"""
Auto-Link Leads to Companies
Automatically links leads to companies based on company name matching with fuzzy support
"""

import os
import sys
import yaml
import logging
from typing import Dict, List, Optional
from pyairtable import Api

# Import fuzzy matching utilities
try:
    from fuzzy_match import normalize_company_name, similarity_score
    HAS_FUZZY_MATCH = True
except ImportError:
    HAS_FUZZY_MATCH = False
    normalize_company_name = lambda x: x.lower().strip() if x else ""
    similarity_score = lambda x, y, f: 1.0 if f(x) == f(y) else 0.0

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('auto_link.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class LeadCompanyLinker:
    """Automatically link leads to companies based on name matching with fuzzy support"""
    
    def __init__(self, config_path: str = "config.yaml"):
        """Initialize with configuration"""
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        # Initialize Airtable
        self.airtable = Api(self.config['airtable']['api_key'])
        self.base = self.airtable.base(self.config['airtable']['base_id'])
        self.leads_table = self.base.table(self.config['airtable']['tables']['leads'])
        self.companies_table = self.base.table(self.config['airtable']['tables']['companies'])
        
        # Cache of companies for faster lookup
        self.company_cache = {}
        
        logger.info("LeadCompanyLinker initialized successfully")
        logger.info(f"  Fuzzy matching: {'ENABLED' if HAS_FUZZY_MATCH else 'DISABLED'}")
    
    def build_company_cache(self):
        """Build a cache of all companies for quick lookup"""
        logger.info("Building company cache...")
        
        all_companies = self.companies_table.all()
        
        for company in all_companies:
            company_name = company['fields'].get('Company Name', '').strip()
            if company_name:
                # Store with normalized name as key
                norm_name = normalize_company_name(company_name)
                self.company_cache[norm_name] = {
                    'id': company['id'],
                    'name': company_name,
                    'original_name': company_name
                }
        
        logger.info(f"  ✓ Cached {len(self.company_cache)} companies")
    
    def find_company_by_name(self, company_name: str, threshold: float = 0.85) -> Optional[str]:
        """Find company ID by name with fuzzy matching support"""
        if not company_name:
            return None
        
        norm_query = normalize_company_name(company_name)
        
        # Try exact match first (after normalization)
        if norm_query in self.company_cache:
            return self.company_cache[norm_query]['id']
        
        # Try fuzzy match
        best_match = None
        best_score = 0.0
        
        for norm_name, company_data in self.company_cache.items():
            # Calculate similarity
            score = similarity_score(company_name, company_data['original_name'], normalize_company_name)
            
            if score > best_score:
                best_score = score
                best_match = company_data
        
        # Return if above threshold
        if best_score >= threshold and best_match:
            matched_name = best_match['original_name']
            if best_score < 1.0:
                logger.info(f"    Fuzzy matched '{company_name}' -> '{matched_name}' (score: {best_score:.2f})")
            return best_match['id']
        
        return None
    
    def create_company_if_needed(self, company_name: str) -> str:
        """Create a new company if it doesn't exist"""
        logger.info(f"    Creating new company: {company_name}")
        
        new_company = self.companies_table.create({
            'Company Name': company_name,
            'Enrichment Status': 'Not Enriched'
        })
        
        company_id = new_company['id']
        
        # Add to cache
        self.company_cache[company_name.lower()] = {
            'id': company_id,
            'name': company_name
        }
        
        return company_id
    
    def get_unlinked_leads(self) -> List[Dict]:
        """Get all leads that don't have a company linked"""
        # Leads without Company field filled
        formula = "NOT({Company})"
        leads = self.leads_table.all(formula=formula)
        
        logger.info(f"Found {len(leads)} leads without company links")
        return leads
    
    def extract_company_name_from_lead(self, lead_fields: Dict) -> Optional[str]:
        """Try to extract company name from various fields"""
        
        # Try different field names where company might be stored
        possible_fields = [
            'Company Name',  # If you have this field
            'Account Name',  # CRM export might use this
            'Organization',  # Another common name
            'Employer',      # LinkedIn export
        ]
        
        for field in possible_fields:
            if field in lead_fields and lead_fields[field]:
                return lead_fields[field].strip()
        
        # Try to extract from CRM Lead ID or notes if it contains company name
        crm_id = lead_fields.get('CRM Lead ID', '')
        if crm_id and ' - ' in crm_id:
            # Sometimes CRM IDs are like "LEAD-123 - Acme Corp"
            parts = crm_id.split(' - ')
            if len(parts) > 1:
                return parts[1].strip()
        
        return None
    
    def link_leads_to_companies(self, auto_create: bool = True, dry_run: bool = False):
        """Main workflow: link all unlinked leads to companies"""
        
        logger.info("=" * 60)
        logger.info("AUTO-LINKING LEADS TO COMPANIES")
        if dry_run:
            logger.info("DRY RUN MODE - No changes will be made")
        logger.info("=" * 60)
        logger.info("")
        
        # Build company cache
        self.build_company_cache()
        logger.info("")
        
        # Get unlinked leads
        unlinked_leads = self.get_unlinked_leads()
        
        if not unlinked_leads:
            logger.info("✓ All leads are already linked to companies!")
            return
        
        logger.info("")
        logger.info("Processing leads...")
        logger.info("")
        
        linked_count = 0
        created_count = 0
        skipped_count = 0
        failed_count = 0
        
        for idx, lead in enumerate(unlinked_leads, 1):
            lead_name = lead['fields'].get('Lead Name', 'Unknown')
            record_id = lead['id']
            
            logger.info(f"[{idx}/{len(unlinked_leads)}] {lead_name}")
            
            # Try to find company name
            company_name = self.extract_company_name_from_lead(lead['fields'])
            
            if not company_name:
                logger.info(f"  ⚠ No company name found - skipping")
                skipped_count += 1
                continue
            
            logger.info(f"  Company: {company_name}")
            
            # Find or create company
            company_id = self.find_company_by_name(company_name)
            
            if not company_id:
                if auto_create:
                    if not dry_run:
                        company_id = self.create_company_if_needed(company_name)
                    created_count += 1
                    logger.info(f"  ✓ Created company")
                else:
                    logger.info(f"  ⚠ Company not found and auto-create is off - skipping")
                    skipped_count += 1
                    continue
            else:
                logger.info(f"  ✓ Found existing company")
            
            # Link lead to company
            if not dry_run and company_id:
                try:
                    self.leads_table.update(record_id, {
                        'Company': [company_id]
                    })
                    linked_count += 1
                    logger.info(f"  ✓ Linked!")
                except Exception as e:
                    logger.error(f"  ✗ Failed to link: {str(e)}")
                    failed_count += 1
            elif dry_run:
                linked_count += 1
                logger.info(f"  ✓ Would link (dry run)")
        
        # Summary
        logger.info("")
        logger.info("=" * 60)
        logger.info("LINKING COMPLETE")
        logger.info("=" * 60)
        logger.info(f"Total unlinked leads: {len(unlinked_leads)}")
        logger.info(f"Successfully linked: {linked_count}")
        logger.info(f"Companies created: {created_count}")
        logger.info(f"Skipped: {skipped_count}")
        logger.info(f"Failed: {failed_count}")
        logger.info("=" * 60)


def main():
    """CLI entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Auto-link leads to companies')
    parser.add_argument('--dry-run', action='store_true', help='Show what would happen without making changes')
    parser.add_argument('--no-create', action='store_true', help='Do not create new companies (only link to existing)')
    parser.add_argument('--config', default='config.yaml', help='Path to config file')
    
    args = parser.parse_args()
    
    try:
        linker = LeadCompanyLinker(config_path=args.config)
        linker.link_leads_to_companies(
            auto_create=not args.no_create,
            dry_run=args.dry_run
        )
        sys.exit(0)
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
