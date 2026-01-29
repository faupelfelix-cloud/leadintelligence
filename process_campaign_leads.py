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
        
        # Trigger History table for tracking outreach triggers
        try:
            self.trigger_history_table = self.base.table('Trigger History')
            logger.info("Trigger History table connected")
        except Exception as e:
            self.trigger_history_table = None
            logger.warning(f"Trigger History table not found: {e}")
        
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
                # Only get leads with Enrich Lead checked and not yet linked
                return [r for r in all_records 
                        if r['fields'].get('Enrich Lead') 
                        and not r['fields'].get('Linked Lead')]
            else:
                # Get leads with Enrich Lead checked
                return [r for r in all_records if r['fields'].get('Enrich Lead')]
                
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
    
    def create_trigger_event(self, lead_record_id: str, company_record_id: Optional[str],
                             campaign_fields: Dict, lead_name: str, company_name: str) -> Optional[str]:
        """Create a trigger event in Trigger History table for campaign lead"""
        
        if not self.trigger_history_table:
            logger.warning("  Trigger History table not available - skipping trigger creation")
            return None
        
        try:
            # Determine trigger type based on campaign type
            campaign_type = campaign_fields.get('Campaign Type', 'Campaign')
            conference_name = campaign_fields.get('Conference Name', '')
            campaign_background = campaign_fields.get('Campaign Background', '')
            campaign_date = campaign_fields.get('Campaign Date', '')
            
            # Map campaign type to existing Airtable trigger types
            # Valid options: CONFERENCE_ATTENDANCE, FUNDING, PROMOTION, JOB_CHANGE, 
            #               PIPELINE, SPEAKING, PAIN_POINT, ROADSHOW, OTHER
            if campaign_type == 'Conference' or conference_name:
                trigger_type = 'CONFERENCE_ATTENDANCE'
            elif campaign_type == 'Roadshow':
                trigger_type = 'ROADSHOW'
            elif campaign_type == 'Funding':
                trigger_type = 'FUNDING'
            elif campaign_type == 'Pipeline':
                trigger_type = 'PIPELINE'
            elif campaign_type == 'Speaking':
                trigger_type = 'SPEAKING'
            else:
                trigger_type = 'OTHER'
            
            # Build description
            description_parts = []
            if campaign_type:
                description_parts.append(f"Campaign: {campaign_type}")
            if conference_name:
                description_parts.append(f"Conference: {conference_name}")
            if campaign_background:
                description_parts.append(campaign_background[:200])
            
            description = " | ".join(description_parts) if description_parts else f"Campaign outreach to {lead_name}"
            
            # Build outreach angle
            if campaign_type == 'Roadshow':
                outreach_angle = f"Roadshow visit - discuss manufacturing partnership opportunities"
            elif conference_name:
                outreach_angle = f"Meeting at {conference_name} - discuss manufacturing needs"
            elif campaign_background:
                outreach_angle = campaign_background[:200]
            else:
                outreach_angle = "Campaign outreach - explore manufacturing partnership"
            
            # Create trigger record
            trigger_fields = {
                'Date Detected': datetime.now().strftime('%Y-%m-%d'),
                'Lead': [lead_record_id],
                'Trigger Type': trigger_type,
                'Urgency': 'MEDIUM',
                'Description': description,
                'Outreach Angle': outreach_angle,
                'Status': 'New',
                'Sources': 'Campaign Leads'
            }
            
            # Add company link if available
            if company_record_id:
                trigger_fields['Company'] = [company_record_id]
            
            # Add conference name if available
            if conference_name:
                trigger_fields['Conference Name'] = conference_name
            
            # Add event date if available
            if campaign_date:
                trigger_fields['Event Date'] = campaign_date
            
            record = self.trigger_history_table.create(trigger_fields)
            trigger_id = record.get('id')
            
            logger.info(f"  ✓ Trigger event created ({trigger_type})")
            return trigger_id
            
        except Exception as e:
            logger.error(f"  Error creating trigger event: {e}")
            return None
    
    # ==================== OUTREACH GENERATION ====================
    
    def generate_outreach_messages(self, lead_fields: Dict, company_fields: Dict, 
                                   campaign_context: Dict = None) -> Dict[str, str]:
        """Generate personalized outreach messages with campaign context"""
        
        # === BASIC INFO ===
        name = lead_fields.get('Lead Name', 'there')
        title = lead_fields.get('Title', '')
        company = company_fields.get('Company Name', '')
        location = company_fields.get('Location/HQ', '')
        
        # === CAMPAIGN CONTEXT ===
        campaign_context = campaign_context or {}
        campaign_type = campaign_context.get('Campaign Type', 'general')
        conference_name = campaign_context.get('Conference Name', '')
        campaign_background = campaign_context.get('Campaign Background', '')
        campaign_date = campaign_context.get('Campaign Date', '')
        
        # === COMPANY CONTEXT (brief, not exhaustive) ===
        # Only include key points that would naturally come up in conversation
        company_context = []
        
        pipeline = company_fields.get('Lead Programs', '')
        if pipeline:
            company_context.append(f"Pipeline: {pipeline[:200]}")  # Truncate
        
        tech_platform = company_fields.get('Technology Platform', [])
        if tech_platform:
            if isinstance(tech_platform, list):
                tech_platform = ', '.join(tech_platform)
            company_context.append(f"Technology: {tech_platform}")
        
        therapeutic = company_fields.get('Therapeutic Areas', [])
        if therapeutic:
            if isinstance(therapeutic, list):
                therapeutic = ', '.join(therapeutic)
            company_context.append(f"Focus: {therapeutic}")
        
        company_context_text = '\n'.join(company_context) if company_context else 'General biotech'
        
        # === BUILD CAMPAIGN-SPECIFIC PROMPT ===
        campaign_section = ""
        if campaign_type == 'Conference' and conference_name:
            campaign_section = f"""
CAMPAIGN: Conference outreach for {conference_name}
Date: {campaign_date}
Background: {campaign_background}

This is conference outreach - the conference is the REASON for reaching out.
- Mention you'll be at {conference_name}
- Suggest meeting at the event
- Keep it natural - like reaching out to someone you'd like to meet
"""
        elif campaign_type == 'Roadshow':
            campaign_section = f"""
CAMPAIGN: Roadshow outreach
Date: {campaign_date}
Background: {campaign_background}

This is roadshow outreach - you're visiting their region/city.
- Mention you'll be in their area for meetings
- Suggest meeting while you're there
- Frame it as convenient timing for both parties
- Keep it natural and low-pressure
"""
        elif campaign_background:
            campaign_section = f"""
CAMPAIGN CONTEXT: {campaign_background}

Use this context as the natural reason for reaching out.
"""
        
        prompt = f"""You are writing business development outreach for a European biologics CDMO.

LEAD: {name}, {title} at {company} ({location})

COMPANY BACKGROUND:
{company_context_text}
{campaign_section}
REZON BIO (your company):
European CDMO specializing in mammalian CHO cell culture for mAbs, bispecifics, and ADCs.
Target: Mid-size biotechs needing manufacturing support.

═══════════════════════════════════════════════════════════
STYLE RULES - CRITICAL:
═══════════════════════════════════════════════════════════
1. **Natural, human language** - slightly imperfect is fine
2. **NO bullet lists** anywhere - weave points into sentences
3. **NO ** for bold** - clean formatting only
4. **Show you know them, don't tell them their situation**
   BAD: "Your company focuses on oncology and has Phase 2 programs"
   GOOD: "Given your work in oncology..."
5. **About THEM, not about us** - lead with their context
6. **Soft CTA** - "would be great to connect" not "let's schedule a call"
7. **Don't overload with intel** - pick 1-2 relevant details max
8. **Sound like a human wrote it** - not an AI that scraped their LinkedIn

═══════════════════════════════════════════════════════════
GENERATE FOUR MESSAGES:
═══════════════════════════════════════════════════════════

MESSAGE 1: EMAIL (100-130 words)
Subject: [Natural, references their context or the conference]
Body: Conversational, references campaign context naturally, soft CTA
Sign: "Best regards, [Your Name]"

MESSAGE 2: LINKEDIN CONNECTION (under 200 chars)
Brief, friendly, reference their role or company or conference
No signature

MESSAGE 3: LINKEDIN INMAIL 
Subject: [Natural, not salesy]
Body: 80-100 words, conversational, references their work
Sign: "Best, [Your Name]"

MESSAGE 4: LINKEDIN SHORT (under 180 chars)
Very brief follow-up style message

Return ONLY valid JSON:
{{
    "email_subject": "Subject",
    "email_body": "Body with signature",
    "linkedin_connection": "Under 200 chars, no signature",
    "linkedin_inmail_subject": "Subject",
    "linkedin_inmail_body": "Body with signature",
    "linkedin_short": "Under 180 chars"
}}"""

        try:
            message = self.anthropic_client.messages.create(
                model=self.config['anthropic']['model'],
                max_tokens=1500,
                messages=[{"role": "user", "content": prompt}]
            )
            
            response_text = message.content[0].text if message.content else ""
            
            # Parse JSON with better error handling
            json_str = ""
            if "```json" in response_text:
                json_str = response_text.split("```json")[1].split("```")[0].strip()
            elif "```" in response_text:
                json_str = response_text.split("```")[1].split("```")[0].strip()
            elif "{" in response_text:
                start = response_text.find("{")
                end = response_text.rfind("}") + 1
                json_str = response_text[start:end]
            
            if not json_str:
                logger.error("No JSON found in response")
                return {}
            
            # Try to fix common JSON issues
            json_str = json_str.replace('\n', ' ').replace('\r', '')
            
            # Handle unterminated strings by trying to close them
            try:
                return json.loads(json_str)
            except json.JSONDecodeError as e:
                # Try to salvage partial JSON
                logger.warning(f"JSON parse error, attempting fix: {str(e)[:50]}")
                
                # Count braces to check if truncated
                open_braces = json_str.count('{')
                close_braces = json_str.count('}')
                
                if open_braces > close_braces:
                    # Truncated - try to close it
                    json_str = json_str.rstrip(',') + '}'
                    try:
                        return json.loads(json_str)
                    except:
                        pass
                
                return {}
            
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
                    
                    # ========== STEP 4: CREATE TRIGGER EVENT ==========
                    if lead_record_id:
                        self.create_trigger_event(
                            lead_record_id=lead_record_id,
                            company_record_id=company_record_id,
                            campaign_fields=fields,
                            lead_name=name,
                            company_name=company
                        )
                    
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
                
                # Extract campaign context from the Campaign Lead record
                campaign_context = {
                    'Campaign Type': fields.get('Campaign Type', campaign_type),
                    'Conference Name': fields.get('Conference Name', ''),
                    'Campaign Background': fields.get('Campaign Background', ''),
                    'Campaign Date': fields.get('Campaign Date', ''),
                }
                
                # Generate messages with campaign context
                messages = self.generate_outreach_messages(lead_data, company_data, campaign_context)
                
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
