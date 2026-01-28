#!/usr/bin/env python3
"""
Campaign Leads Processor - Unified Enrichment and Outreach

Processes leads from Campaign Leads table:
1. Creates company in Companies table if not exists
2. Runs FULL company enrichment (same as enrich_companies.py)
3. Creates lead in Leads table if not exists
4. Runs FULL lead enrichment (same as enrich_leads.py)
5. Links Campaign Lead to both records
6. Generates personalized outreach messages
"""

import os
import sys
import yaml
import json
import time
import logging
import argparse
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any

import anthropic
from pyairtable import Api
from pyairtable.formulas import match

# Import existing enrichers
from enrich_companies import CompanyEnricher
from enrich_leads import LeadEnricher

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('campaign_leads.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class CampaignLeadsProcessor:
    """Process campaign leads with full enrichment and outreach generation"""
    
    def __init__(self, config_path: str = "config.yaml"):
        """Initialize with configuration"""
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        # Initialize Airtable
        self.airtable = Api(self.config['airtable']['api_key'])
        self.base = self.airtable.base(self.config['airtable']['base_id'])
        
        # Tables
        table_name = self.config['airtable']['tables'].get('campaign_leads', 'Campaign Leads')
        self.campaign_leads_table = self.base.table(table_name)
        self.companies_table = self.base.table(self.config['airtable']['tables']['companies'])
        self.leads_table = self.base.table(self.config['airtable']['tables']['leads'])
        
        # Initialize enrichers (they handle their own Airtable connections)
        self.company_enricher = CompanyEnricher(config_path)
        self.lead_enricher = LeadEnricher(config_path)
        
        # Initialize Claude for outreach generation
        self.anthropic_client = anthropic.Anthropic(
            api_key=self.config['anthropic']['api_key']
        )
        
        logger.info("CampaignLeadsProcessor initialized")
    
    # ==================== COMPANY OPERATIONS ====================
    
    def lookup_company(self, company_name: str) -> Tuple[Optional[Dict], Optional[str]]:
        """Look up company in Companies table"""
        try:
            formula = match({"Company Name": company_name})
            records = self.companies_table.all(formula=formula)
            
            if records:
                record = records[0]
                return record['fields'], record['id']
            return None, None
        except Exception as e:
            logger.error(f"Error looking up company: {e}")
            return None, None
    
    def create_minimal_company(self, company_name: str) -> Optional[str]:
        """Create a minimal company record for enrichment"""
        try:
            fields = {
                'Company Name': company_name,
                'Enrichment Status': 'Not Enriched'
            }
            record = self.companies_table.create(fields)
            return record.get('id')
        except Exception as e:
            logger.error(f"Error creating company: {e}")
            return None
    
    def enrich_company_record(self, record_id: str, company_name: str) -> bool:
        """Run full enrichment on a company record using CompanyEnricher"""
        try:
            logger.info(f"    Running full company enrichment...")
            
            # Use the same search method as enrich_companies.py
            enriched_data = self.company_enricher.search_company_info(company_name)
            
            if enriched_data.get('confidence') == 'Failed' or enriched_data.get('error'):
                logger.warning(f"    Company enrichment returned: {enriched_data.get('error', 'Failed')}")
                self.companies_table.update(record_id, {
                    'Enrichment Status': 'Failed',
                    'Intelligence Notes': f"Enrichment failed: {enriched_data.get('error', 'No data found')}"
                })
                return False
            
            # Use the same update method as enrich_companies.py
            self.company_enricher.update_company_record(record_id, enriched_data)
            return True
            
        except Exception as e:
            logger.error(f"    Error enriching company: {e}")
            return False
    
    # ==================== LEAD OPERATIONS ====================
    
    def lookup_lead(self, email: str, name: str, company: str) -> Tuple[Optional[Dict], Optional[str]]:
        """Look up lead in Leads table by email or name+company"""
        try:
            # Try by email first
            if email and '@' in email:
                formula = match({"Email": email})
                records = self.leads_table.all(formula=formula)
                if records:
                    return records[0]['fields'], records[0]['id']
            
            # Try by name
            formula = match({"Lead Name": name})
            records = self.leads_table.all(formula=formula)
            if records:
                return records[0]['fields'], records[0]['id']
            
            return None, None
        except Exception as e:
            logger.error(f"Error looking up lead: {e}")
            return None, None
    
    def create_minimal_lead(self, name: str, title: str, company_record_id: Optional[str]) -> Optional[str]:
        """Create a minimal lead record for enrichment"""
        try:
            fields = {
                'Lead Name': name,
                'Title': title,
                'Enrichment Status': 'Not Enriched'
            }
            if company_record_id:
                fields['Company'] = [company_record_id]
            
            record = self.leads_table.create(fields)
            return record.get('id')
        except Exception as e:
            logger.error(f"Error creating lead: {e}")
            return None
    
    def enrich_lead_record(self, record_id: str, lead_name: str, company_name: str, title: str) -> bool:
        """Run full enrichment on a lead record using LeadEnricher"""
        try:
            logger.info(f"    Running full lead enrichment...")
            
            # Use the same search method as enrich_leads.py
            enriched_data = self.lead_enricher.search_lead_info(
                lead_name=lead_name,
                company_name=company_name,
                current_title=title
            )
            
            if enriched_data.get('overall_confidence') == 'Failed' or enriched_data.get('error'):
                logger.warning(f"    Lead enrichment returned: {enriched_data.get('error', 'Failed')}")
                self.leads_table.update(record_id, {
                    'Enrichment Status': 'Failed',
                    'Intelligence Notes': f"Enrichment failed: {enriched_data.get('error', 'No data found')}"
                })
                return False
            
            # Use the same update method as enrich_leads.py
            self.lead_enricher.update_lead_record(record_id, enriched_data)
            return True
            
        except Exception as e:
            logger.error(f"    Error enriching lead: {e}")
            return False
    
    # ==================== CAMPAIGN LEADS OPERATIONS ====================
    
    def get_campaign_leads_to_process(self, enrich_only: bool = False) -> List[Dict]:
        """Get campaign leads that need processing"""
        try:
            all_records = self.campaign_leads_table.all()
            
            if enrich_only:
                # Only get leads with Enrich checked and not yet linked
                return [r for r in all_records 
                        if r['fields'].get('Enrich') 
                        and not r['fields'].get('Linked Lead')]
            else:
                # Get leads with Enrich checked
                return [r for r in all_records if r['fields'].get('Enrich')]
                
        except Exception as e:
            logger.error(f"Error getting campaign leads: {e}")
            return []
    
    def get_campaign_leads_for_outreach(self) -> List[Dict]:
        """Get campaign leads that need outreach generation"""
        try:
            all_records = self.campaign_leads_table.all()
            
            # Get leads with Generate Messages checked and linked
            return [r for r in all_records 
                    if r['fields'].get('Generate Messages')
                    and r['fields'].get('Linked Lead')
                    and not r['fields'].get('Email Body')]
                    
        except Exception as e:
            logger.error(f"Error getting campaign leads for outreach: {e}")
            return []
    
    def update_campaign_lead_links(self, record_id: str, 
                                    lead_record_id: Optional[str],
                                    company_record_id: Optional[str],
                                    lead_data: Optional[Dict] = None) -> bool:
        """Update campaign lead with links to main tables"""
        try:
            update = {}
            
            if lead_record_id:
                update['Linked Lead'] = [lead_record_id]
            if company_record_id:
                update['Linked Company'] = [company_record_id]
            
            # Copy basic enrichment data
            if lead_data:
                email = lead_data.get('Email')
                if email and '@' in str(email):
                    update['Email'] = email
                linkedin = lead_data.get('LinkedIn URL')
                if linkedin and 'linkedin.com' in str(linkedin):
                    update['LinkedIn URL'] = linkedin
            
            if update:
                self.campaign_leads_table.update(record_id, update)
            return True
            
        except Exception as e:
            logger.error(f"Error updating campaign lead: {e}")
            return False
    
    # ==================== OUTREACH GENERATION ====================
    
    def generate_outreach_messages(self, lead_fields: Dict, company_fields: Dict, 
                                   campaign_type: str = "general") -> Dict[str, str]:
        """Generate personalized outreach messages"""
        
        name = lead_fields.get('Lead Name', 'there')
        first_name = name.split()[0] if name != 'there' else 'there'
        title = lead_fields.get('Title', '')
        company = company_fields.get('Company Name', '')
        location = company_fields.get('Location/HQ', '')
        pipeline = company_fields.get('Lead Programs', '')
        focus_areas = company_fields.get('Focus Area', [])
        if isinstance(focus_areas, list):
            focus_areas = ', '.join(focus_areas)
        
        prompt = f"""Generate personalized outreach messages for a biologics CDMO (Contract Development and Manufacturing Organization) targeting biotech companies.

LEAD INFORMATION:
- Name: {name}
- Title: {title}
- Company: {company}
- Location: {location}
- Focus Areas: {focus_areas}
- Pipeline: {pipeline}

REZON BIO CONTEXT:
Rezon Bio is a European biologics CDMO specializing in:
- Mammalian cell culture (CHO)
- Monoclonal antibodies, bispecific antibodies, ADCs
- Process development through commercial manufacturing
- European quality standards with competitive pricing

OUTREACH REQUIREMENTS:
Generate 4 different message types. Each should be personalized, professional, and focused on value.

Return ONLY valid JSON:
{{
    "email_subject": "Short, compelling subject line (under 60 chars)",
    "email_body": "Professional email body (150-250 words). Reference their pipeline/focus if known. Clear CTA for a call.",
    "linkedin_connection": "Brief connection request (under 300 chars). Mention something specific about them or their company.",
    "linkedin_inmail_subject": "InMail subject line",
    "linkedin_inmail_body": "Slightly shorter than email (100-150 words). More conversational tone.",
    "linkedin_short": "Very brief follow-up message (under 200 chars)"
}}"""

        try:
            message = self.anthropic_client.messages.create(
                model=self.config['anthropic']['model'],
                max_tokens=2000,
                messages=[{"role": "user", "content": prompt}]
            )
            
            response_text = message.content[0].text if message.content else ""
            
            # Parse JSON
            if "```json" in response_text:
                json_str = response_text.split("```json")[1].split("```")[0].strip()
            elif "{" in response_text:
                start = response_text.find("{")
                end = response_text.rfind("}") + 1
                json_str = response_text[start:end]
            else:
                return {}
            
            return json.loads(json_str)
            
        except Exception as e:
            logger.error(f"Error generating outreach: {e}")
            return {}
    
    def update_campaign_lead_outreach(self, record_id: str, messages: Dict) -> bool:
        """Update campaign lead with generated outreach messages"""
        try:
            update = {}
            
            if messages.get('email_subject'):
                update['Email Subject'] = messages['email_subject']
            if messages.get('email_body'):
                update['Email Body'] = messages['email_body']
            if messages.get('linkedin_connection'):
                update['LinkedIn Connection Request'] = messages['linkedin_connection']
            if messages.get('linkedin_inmail_subject'):
                update['LinkedIn InMail Subject'] = messages['linkedin_inmail_subject']
            if messages.get('linkedin_inmail_body'):
                update['LinkedIn InMail Body'] = messages['linkedin_inmail_body']
            if messages.get('linkedin_short'):
                update['LinkedIn Short Message'] = messages['linkedin_short']
            
            update['Message Generated Date'] = datetime.now().strftime('%Y-%m-%d')
            
            if update:
                self.campaign_leads_table.update(record_id, update)
            return True
            
        except Exception as e:
            logger.error(f"Error updating outreach: {e}")
            return False
    
    # ==================== MAIN WORKFLOWS ====================
    
    def process_enrichment(self, limit: Optional[int] = None):
        """
        Main enrichment workflow:
        1. Get campaign leads with Enrich checked
        2. For each lead, create/enrich company and lead in main tables
        3. Link campaign lead to enriched records
        """
        leads = self.get_campaign_leads_to_process(enrich_only=True)
        
        if limit:
            leads = leads[:limit]
        
        total = len(leads)
        logger.info(f"Processing {total} campaign leads for enrichment")
        
        if total == 0:
            logger.info("No leads to process")
            return
        
        success = 0
        rate_limit_delay = self.config.get('web_search', {}).get('rate_limit_delay', 2)
        
        for idx, record in enumerate(leads, 1):
            fields = record['fields']
            record_id = record['id']
            
            name = fields.get('Lead Name', 'Unknown')
            company = fields.get('Company', 'Unknown')
            title = fields.get('Title', '')
            email = fields.get('Email', '')
            
            logger.info(f"\n[{idx}/{total}] {name} @ {company}")
            
            try:
                # ========== STEP 1: COMPANY ==========
                logger.info("  Checking Companies table...")
                company_data, company_record_id = self.lookup_company(company)
                
                if company_data:
                    logger.info(f"  ✓ Found existing company (ICP: {company_data.get('ICP Fit Score', 'N/A')})")
                else:
                    # Create and enrich company
                    logger.info(f"  ○ Company not found - creating and enriching...")
                    company_record_id = self.create_minimal_company(company)
                    
                    if company_record_id:
                        if self.enrich_company_record(company_record_id, company):
                            # Fetch the enriched data
                            company_data = self.companies_table.get(company_record_id)['fields']
                            logger.info(f"  ✓ Company enriched (ICP: {company_data.get('ICP Fit Score', 'N/A')})")
                        else:
                            logger.warning(f"  ⚠ Company enrichment failed")
                            company_data = {}
                        
                        time.sleep(rate_limit_delay)
                    else:
                        logger.error(f"  ✗ Failed to create company record")
                        company_data = {}
                
                # ========== STEP 2: LEAD ==========
                logger.info("  Checking Leads table...")
                lead_data, lead_record_id = self.lookup_lead(email, name, company)
                
                if lead_data:
                    logger.info(f"  ✓ Found existing lead (ICP: {lead_data.get('Lead ICP Score', 'N/A')})")
                else:
                    # Create and enrich lead
                    logger.info(f"  ○ Lead not found - creating and enriching...")
                    lead_record_id = self.create_minimal_lead(name, title, company_record_id)
                    
                    if lead_record_id:
                        if self.enrich_lead_record(lead_record_id, name, company, title):
                            # Fetch the enriched data
                            lead_data = self.leads_table.get(lead_record_id)['fields']
                            logger.info(f"  ✓ Lead enriched (ICP: {lead_data.get('Lead ICP Score', 'N/A')})")
                            if lead_data.get('Email'):
                                logger.info(f"    Email: {lead_data.get('Email')}")
                        else:
                            logger.warning(f"  ⚠ Lead enrichment failed")
                            lead_data = {}
                        
                        time.sleep(rate_limit_delay)
                    else:
                        logger.error(f"  ✗ Failed to create lead record")
                        lead_data = {}
                
                # ========== STEP 3: LINK CAMPAIGN LEAD ==========
                if self.update_campaign_lead_links(record_id, lead_record_id, company_record_id, lead_data):
                    logger.info(f"  ✓ Campaign lead linked")
                    success += 1
                    
                    # Auto-check Generate Messages
                    try:
                        self.campaign_leads_table.update(record_id, {'Generate Messages': True})
                    except:
                        pass
                
            except Exception as e:
                logger.error(f"  ✗ Error processing: {e}")
                continue
        
        logger.info(f"\n{'='*50}")
        logger.info(f"Enrichment complete: {success}/{total} successful")
    
    def process_outreach(self, limit: Optional[int] = None, campaign_type: str = "general"):
        """
        Generate outreach messages for enriched campaign leads
        """
        leads = self.get_campaign_leads_for_outreach()
        
        if limit:
            leads = leads[:limit]
        
        total = len(leads)
        logger.info(f"Generating outreach for {total} campaign leads")
        
        if total == 0:
            logger.info("No leads need outreach generation")
            return
        
        success = 0
        
        for idx, record in enumerate(leads, 1):
            fields = record['fields']
            record_id = record['id']
            name = fields.get('Lead Name', 'Unknown')
            
            logger.info(f"[{idx}/{total}] Generating outreach for {name}...")
            
            try:
                # Get linked lead and company data
                lead_record_ids = fields.get('Linked Lead', [])
                company_record_ids = fields.get('Linked Company', [])
                
                lead_data = {}
                company_data = {}
                
                if lead_record_ids:
                    lead_data = self.leads_table.get(lead_record_ids[0])['fields']
                if company_record_ids:
                    company_data = self.companies_table.get(company_record_ids[0])['fields']
                
                # Generate messages
                messages = self.generate_outreach_messages(lead_data, company_data, campaign_type)
                
                if messages:
                    if self.update_campaign_lead_outreach(record_id, messages):
                        logger.info(f"  ✓ Outreach generated")
                        success += 1
                else:
                    logger.warning(f"  ⚠ Failed to generate messages")
                    
            except Exception as e:
                logger.error(f"  ✗ Error: {e}")
                continue
        
        logger.info(f"\n{'='*50}")
        logger.info(f"Outreach complete: {success}/{total} successful")
    
    def process_all(self, limit: Optional[int] = None, campaign_type: str = "general"):
        """Run full workflow: enrichment then outreach"""
        logger.info("="*50)
        logger.info("CAMPAIGN LEADS PROCESSOR - FULL WORKFLOW")
        logger.info("="*50)
        
        # Step 1: Enrichment
        logger.info("\n--- PHASE 1: ENRICHMENT ---")
        self.process_enrichment(limit)
        
        # Step 2: Outreach
        logger.info("\n--- PHASE 2: OUTREACH GENERATION ---")
        self.process_outreach(limit, campaign_type)
        
        logger.info("\n" + "="*50)
        logger.info("WORKFLOW COMPLETE")
        logger.info("="*50)


def main():
    parser = argparse.ArgumentParser(description='Campaign Leads Processor')
    parser.add_argument('--config', default='config.yaml', help='Config file path')
    parser.add_argument('--enrich-only', action='store_true', 
                        help='Only run enrichment, skip outreach')
    parser.add_argument('--outreach-only', action='store_true', 
                        help='Only generate outreach for already enriched leads')
    parser.add_argument('--campaign-type', type=str, default='general',
                        help='Campaign type for outreach messaging')
    parser.add_argument('--limit', type=int, help='Max leads to process')
    
    args = parser.parse_args()
    
    processor = CampaignLeadsProcessor(args.config)
    
    if args.enrich_only:
        processor.process_enrichment(args.limit)
    elif args.outreach_only:
        processor.process_outreach(args.limit, args.campaign_type)
    else:
        processor.process_all(args.limit, args.campaign_type)


if __name__ == "__main__":
    main()
