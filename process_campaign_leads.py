#!/usr/bin/env python3
"""
Campaign Leads Processor
Two-step workflow for event-based lead outreach:

STEP 1: ENRICH (--enrich)
- Check if lead exists in main Leads table → pull data
- Check if company exists in Companies table → pull data  
- If NOT found → create in main tables, enrich, score ICP
- Update Campaign Leads table with linked data

STEP 2: GENERATE OUTREACH (--generate)
- Generate event-specific messages using enriched data
- Uses Conference Name and Campaign Background for context
"""

import os
import sys
import yaml
import json
import time
import logging
import argparse
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import anthropic
from pyairtable import Api

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
    """Process campaign leads with auto-lookup, enrichment, and outreach generation"""
    
    def __init__(self, config_path: str = "config.yaml"):
        """Initialize with configuration"""
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        # Initialize APIs
        self.airtable = Api(self.config['airtable']['api_key'])
        self.base = self.airtable.base(self.config['airtable']['base_id'])
        
        # Tables
        table_name = self.config['airtable']['tables'].get('campaign_leads', 'Campaign Leads')
        self.campaign_leads_table = self.base.table(table_name)
        self.leads_table = self.base.table(self.config['airtable']['tables']['leads'])
        self.companies_table = self.base.table(self.config['airtable']['tables']['companies'])
        
        # Try to load Company Profile for outreach context
        try:
            self.company_profile_table = self.base.table('Company Profile')
            self.company_profile = self._load_company_profile()
        except:
            self.company_profile = None
        
        self.anthropic_client = anthropic.Anthropic(
            api_key=self.config['anthropic']['api_key']
        )
        
        # Campaign type contexts for outreach
        self.campaign_contexts = {
            'Conference': {
                'angle': 'meeting at the conference',
                'cta': 'schedule a meeting during the event',
                'urgency': 'high',
                'tone': 'peer-to-peer, industry event'
            },
            'Webinar': {
                'angle': 'shared interest in the topic',
                'cta': 'follow up after the session',
                'urgency': 'medium',
                'tone': 'educational, thought leadership'
            },
            'Trade Show': {
                'angle': 'booth visit or demo',
                'cta': 'visit our booth',
                'urgency': 'high',
                'tone': 'product-focused'
            },
            'Partnership Event': {
                'angle': 'potential collaboration',
                'cta': 'discuss partnership',
                'urgency': 'medium',
                'tone': 'collaborative, strategic'
            },
            'Workshop': {
                'angle': 'hands-on learning',
                'cta': 'connect at the workshop',
                'urgency': 'medium',
                'tone': 'technical'
            },
            'Post-Event Follow-up': {
                'angle': 'following up from event',
                'cta': 'continue our conversation',
                'urgency': 'high',
                'tone': 'warm, personal'
            },
            'Custom': {
                'angle': 'general outreach',
                'cta': 'connect and discuss',
                'urgency': 'medium',
                'tone': 'professional'
            }
        }
        
        logger.info("CampaignLeadsProcessor initialized")
    
    def _load_company_profile(self) -> Optional[Dict]:
        """Load company profile for outreach context"""
        try:
            records = self.company_profile_table.all()
            if records:
                return records[0].get('fields', {})
        except:
            pass
        return None
    
    # =========================================================================
    # STEP 1: LOOKUP & ENRICH
    # =========================================================================
    
    def lookup_lead(self, email: str, name: str, company: str) -> Tuple[Optional[Dict], Optional[str]]:
        """
        Look up lead in main Leads table.
        Returns: (lead_data, lead_record_id) or (None, None)
        """
        # Try email first (most reliable)
        if email:
            try:
                safe_email = email.replace("'", "\\'").lower()
                matches = self.leads_table.all(formula=f"LOWER({{Email}}) = '{safe_email}'")
                if matches:
                    lead = matches[0]
                    return lead.get('fields', {}), lead.get('id')
            except Exception as e:
                logger.debug(f"Email lookup error: {e}")
        
        # Try name + company
        if name and company:
            try:
                safe_name = name.replace("'", "\\'")
                safe_company = company.replace("'", "\\'")
                formula = f"AND(LOWER({{Lead Name}}) = LOWER('{safe_name}'), SEARCH(LOWER('{safe_company}'), LOWER({{Company}})))"
                matches = self.leads_table.all(formula=formula)
                if matches:
                    lead = matches[0]
                    return lead.get('fields', {}), lead.get('id')
            except Exception as e:
                logger.debug(f"Name lookup error: {e}")
        
        return None, None
    
    def lookup_company(self, company_name: str) -> Tuple[Optional[Dict], Optional[str]]:
        """
        Look up company in Companies table.
        Returns: (company_data, company_record_id) or (None, None)
        """
        if not company_name:
            return None, None
        
        try:
            safe_name = company_name.replace("'", "\\'")
            matches = self.companies_table.all(formula=f"SEARCH(LOWER('{safe_name}'), LOWER({{Company Name}}))")
            if matches:
                company = matches[0]
                return company.get('fields', {}), company.get('id')
        except Exception as e:
            logger.debug(f"Company lookup error: {e}")
        
        return None, None
    
    def enrich_lead(self, name: str, title: str, company: str) -> Dict:
        """Enrich a new lead - find email, LinkedIn, calculate ICP score"""
        
        prompt = f"""Find professional contact information for this person:

Name: {name}
Title: {title}
Company: {company}

Search for and return:
1. Business email address (company domain preferred, not gmail/yahoo)
2. LinkedIn profile URL
3. X/Twitter profile URL (if available)
4. Current verified title

Also calculate Lead ICP Score (0-100) for a European biologics CDMO:

SCORING:
- Title/Role (0-25): VP/Head Manufacturing/Ops = 25, Director = 18, Manager = 10, Scientist = 5
- Seniority (0-20): C-Level = 20, VP = 18, Director = 15, Manager = 10
- Function Fit (0-20): Manufacturing/CMC/Process Dev = 20, Supply Chain = 15, BD = 8, R&D = 5
- Decision Power (0-15): Budget authority = 15, Evaluates vendors = 12, Consulted = 8
- Geography (0-5): Europe = 5, US = 4, APAC = 2
- Defaults: Career Stage = 8, Engagement = 3

ICP Tier: 85+ = Tier 1 (Perfect), 70-84 = Tier 2 (Strong), 55-69 = Tier 3 (Good), 40-54 = Tier 4, <40 = Tier 5

Return ONLY valid JSON:
{{
    "email": "email@company.com",
    "linkedin_url": "https://linkedin.com/in/...",
    "x_profile": "https://x.com/...",
    "verified_title": "Current Title",
    "lead_icp_score": 75,
    "lead_icp_justification": "Title: VP Manufacturing (25), Seniority: VP (18), Function: Manufacturing (20), Decision: Budget (15), Geography: Europe (5) = 83"
}}"""

        try:
            message = self.anthropic_client.messages.create(
                model=self.config['anthropic']['model'],
                max_tokens=1500,
                tools=[{"type": "web_search_20250305", "name": "web_search"}],
                messages=[{"role": "user", "content": prompt}]
            )
            
            response_text = ""
            for block in message.content:
                if hasattr(block, 'text'):
                    response_text += block.text
            
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
            logger.error(f"  Error enriching lead: {e}")
            return {}
    
    def enrich_company(self, company_name: str) -> Dict:
        """Enrich a new company - find info, calculate ICP score"""
        
        prompt = f"""Research this biotech/pharma company for a European biologics CDMO:

Company: {company_name}

Find:
1. Company website
2. LinkedIn company page  
3. Headquarters location
4. Company size (employees)
5. Focus area (oncology, immunology, rare disease, etc.)
6. Technology platform (mAbs, bispecifics, ADCs, cell therapy, gene therapy)
7. Pipeline stage (preclinical, Phase 1, Phase 2, Phase 3, commercial)
8. Recent funding (latest round, total raised)
9. Current CDMO partnerships (if known)

Calculate Company ICP Score (0-105) for biologics CDMO:

SCORING:
- Location (0-20): Europe = 20, US with EU interest = 15, US = 10, APAC = 5
- Technology (0-25): Mammalian biologics (mAbs, bispecifics, ADCs) = 25, Mixed = 15, Other = 5
- Pipeline Stage (0-20): Phase 2-3 = 20, Phase 1 = 15, Preclinical = 10, Commercial = 5
- Company Size (0-15): 50-500 (mid-size) = 15, 20-50 = 10, >500 or <20 = 5
- Funding (0-15): Series B-D = 15, Series A = 10, Seed/Public = 5
- CDMO Status (0-10): No CDMO = 10, Looking = 10, Has CDMO = 0

EXCLUDE (Score = 0): Big Pharma (>10k employees), CDMO competitors

ICP Tier: 85+ = Perfect, 70-84 = Strong, 55-69 = Good, 40-54 = Acceptable, <40 = Poor

Return ONLY valid JSON:
{{
    "website": "https://...",
    "linkedin_page": "https://linkedin.com/company/...",
    "location": "City, Country",
    "employee_count": "~150 employees",
    "focus_areas": "Oncology, Immunology",
    "technology": "Bispecific antibodies, ADCs",
    "pipeline_info": "2 Phase 2, 3 Phase 1 candidates in oncology using bispecific platform",
    "latest_funding": "Series C ($50M) in 2024",
    "total_funding": 120000000,
    "cdmo_partnerships": "None known" or "Working with Lonza for X",
    "icp_score": 82,
    "icp_justification": "Location: Europe (20), Technology: Mammalian bispecifics (25), Pipeline: Phase 2 (20), Size: ~150 (15), Funding: Series C (15), No CDMO (10) = 105 - minus 23 for X = 82",
    "urgency_score": 75,
    "is_excluded": false,
    "exclusion_reason": ""
}}"""

        try:
            message = self.anthropic_client.messages.create(
                model=self.config['anthropic']['model'],
                max_tokens=2000,
                tools=[{"type": "web_search_20250305", "name": "web_search"}],
                messages=[{"role": "user", "content": prompt}]
            )
            
            response_text = ""
            for block in message.content:
                if hasattr(block, 'text'):
                    response_text += block.text
            
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
            logger.error(f"  Error enriching company: {e}")
            return {}
    
    def create_lead_in_main_table(self, lead_data: Dict, company_record_id: Optional[str] = None) -> Optional[str]:
        """Create a new lead in the main Leads table"""
        try:
            # Only use text/number/URL/date fields to avoid select permission issues
            fields = {
                'Lead Name': lead_data.get('name', ''),
                'Title': lead_data.get('verified_title') or lead_data.get('title', ''),
                'Email': lead_data.get('email', ''),
                'LinkedIn URL': lead_data.get('linkedin_url', ''),
                'X Profile': lead_data.get('x_profile', ''),
                'Lead ICP Score': lead_data.get('lead_icp_score'),
                'Lead ICP Justification': lead_data.get('lead_icp_justification', ''),
                'Last Enrichment Date': datetime.now().strftime('%Y-%m-%d')
            }
            
            # Skip single-select fields (Lead ICP Tier, Enrichment Status) to avoid permission errors
            
            if company_record_id:
                fields['Company'] = [company_record_id]
            
            fields = {k: v for k, v in fields.items() if v}
            record = self.leads_table.create(fields)
            return record.get('id')
            
        except Exception as e:
            logger.error(f"  Error creating lead: {e}")
            return None
    
    def create_company_in_main_table(self, company_data: Dict) -> Optional[str]:
        """Create a new company in the main Companies table"""
        try:
            # Only use text/number/URL fields to avoid select permission issues
            fields = {
                'Company Name': company_data.get('name', ''),
                'Website': company_data.get('website', ''),
                'LinkedIn Company Page': company_data.get('linkedin_page', ''),
                'Location/HQ': company_data.get('location', ''),
                'Lead Programs': company_data.get('pipeline_info', ''),
                'ICP Fit Score': company_data.get('icp_score'),
                'ICP Score Justification': company_data.get('icp_justification', ''),
                'Current CDMO Partnerships': company_data.get('cdmo_partnerships', ''),
                'Latest Funding Round': company_data.get('latest_funding', ''),
                'Urgency Score': company_data.get('urgency_score'),
            }
            
            # Total Funding (currency field - just needs a number)
            if company_data.get('total_funding'):
                fields['Total Funding'] = company_data.get('total_funding')
            
            # Store additional info in Intelligence Notes
            notes_parts = []
            if company_data.get('employee_count'):
                notes_parts.append(f"Size: {company_data.get('employee_count')}")
            if company_data.get('focus_areas'):
                notes_parts.append(f"Focus: {company_data.get('focus_areas')}")
            if company_data.get('technology'):
                notes_parts.append(f"Technology: {company_data.get('technology')}")
            if notes_parts:
                fields['Intelligence Notes'] = '\n'.join(notes_parts)
            
            # Skip multi-select fields (Focus Area, Technology Platform, Pipeline Stage, Therapeutic Areas)
            # and single-select fields (Funding Stage, Company Size, Manufacturing Status, Enrichment Status)
            # to avoid permission errors - these can be set via Airtable automation
            
            # Clean empty values
            fields = {k: v for k, v in fields.items() if v is not None and v != ''}
            record = self.companies_table.create(fields)
            return record.get('id')
            
        except Exception as e:
            logger.error(f"  Error creating company: {e}")
            return None
    
    def update_campaign_lead_with_enrichment(self, record_id: str, 
                                              lead_data: Optional[Dict],
                                              company_data: Optional[Dict],
                                              lead_record_id: Optional[str],
                                              company_record_id: Optional[str],
                                              status: str) -> bool:
        """Update campaign lead record with enriched data and links"""
        try:
            update = {}
            
            # Link to main tables - ICP scores will auto-populate via lookups
            if lead_record_id:
                update['Linked Lead'] = [lead_record_id]
            if company_record_id:
                update['Linked Company'] = [company_record_id]
            
            # Basic fields from enrichment
            if lead_data:
                email = lead_data.get('Email') or lead_data.get('email')
                if email and '@' in str(email) and 'NOT_FOUND' not in str(email).upper():
                    update['Email'] = email
                    
                linkedin = lead_data.get('LinkedIn URL') or lead_data.get('linkedin_url')
                if linkedin and 'linkedin.com' in str(linkedin):
                    update['LinkedIn URL'] = linkedin
            
            # NOTE: ICP Score fields are lookups from linked records - don't update directly
            
            if update:
                self.campaign_leads_table.update(record_id, update)
            return True
            
        except Exception as e:
            logger.error(f"  Error updating campaign lead: {e}")
            # Try update without link fields
            try:
                minimal_update = {}
                if lead_data:
                    email = lead_data.get('Email') or lead_data.get('email')
                    if email and '@' in str(email) and 'NOT_FOUND' not in str(email).upper():
                        minimal_update['Email'] = email
                if lead_record_id:
                    minimal_update['Linked Lead'] = [lead_record_id]
                if company_record_id:
                    minimal_update['Linked Company'] = [company_record_id]
                if minimal_update:
                    self.campaign_leads_table.update(record_id, minimal_update)
                logger.info(f"  ⚠ Updated with minimal fields only")
                return True
            except Exception as e2:
                logger.error(f"  Minimal update also failed: {e2}")
                return False
    
    def process_enrichment(self, limit: Optional[int] = None):
        """
        STEP 1: Process all campaign leads needing enrichment
        - Lookup existing lead/company
        - Enrich if new
        - Link to main tables
        """
        logger.info("=" * 60)
        logger.info("CAMPAIGN LEADS - ENRICHMENT")
        logger.info("=" * 60)
        
        # Get leads needing enrichment (checkbox checked OR status is empty/New)
        formula = "OR({Enrich Lead} = TRUE(), {Enrichment Status} = '', {Enrichment Status} = 'New')"
        leads = self.campaign_leads_table.all(formula=formula)
        
        if limit:
            leads = leads[:limit]
        
        if not leads:
            logger.info("No leads need enrichment")
            return
        
        logger.info(f"Processing {len(leads)} leads for enrichment...")
        
        success = 0
        failed = 0
        
        for idx, record in enumerate(leads, 1):
            record_id = record.get('id')
            fields = record.get('fields', {})
            
            name = fields.get('Lead Name', '').strip()
            title = fields.get('Title', '').strip()
            company = fields.get('Company', '').strip()
            email = fields.get('Email', '').strip()
            
            if not name:
                logger.warning(f"[{idx}/{len(leads)}] Skipping - no lead name")
                continue
            
            logger.info(f"\n[{idx}/{len(leads)}] {name} @ {company}")
            
            try:
                # Step 1A: Look up company
                logger.info("  Checking Companies table...")
                company_data, company_record_id = self.lookup_company(company)
                
                if company_data:
                    logger.info(f"  ✓ Found company: ICP {company_data.get('ICP Score', 'N/A')}")
                else:
                    # Enrich company
                    logger.info(f"  ○ Company not found - enriching...")
                    enriched_company = self.enrich_company(company)
                    
                    if enriched_company and not enriched_company.get('is_excluded'):
                        enriched_company['name'] = company
                        company_record_id = self.create_company_in_main_table(enriched_company)
                        company_data = enriched_company
                        if company_record_id:
                            logger.info(f"  ✓ Created company: ICP {enriched_company.get('icp_score', 'N/A')}")
                    else:
                        logger.info(f"  ⚠ Company excluded or enrichment failed")
                        company_data = enriched_company or {}
                
                # Step 1B: Look up lead
                logger.info("  Checking Leads table...")
                lead_data, lead_record_id = self.lookup_lead(email, name, company)
                
                if lead_data:
                    logger.info(f"  ✓ Found lead: ICP {lead_data.get('Lead ICP Score', 'N/A')}")
                else:
                    # Enrich lead
                    logger.info(f"  ○ Lead not found - enriching...")
                    enriched_lead = self.enrich_lead(name, title, company)
                    
                    if enriched_lead:
                        enriched_lead['name'] = name
                        enriched_lead['title'] = enriched_lead.get('verified_title') or title
                        lead_record_id = self.create_lead_in_main_table(enriched_lead, company_record_id)
                        lead_data = enriched_lead
                        if lead_record_id:
                            logger.info(f"  ✓ Created lead: ICP {enriched_lead.get('lead_icp_score', 'N/A')}")
                            if enriched_lead.get('email'):
                                logger.info(f"    Email: {enriched_lead.get('email')}")
                    else:
                        logger.info(f"  ⚠ Lead enrichment failed")
                        lead_data = {}
                
                # Step 1C: Update campaign lead with all data
                status = 'Enriched' if (lead_data or company_data) else 'Enrichment Failed'
                if self.update_campaign_lead_with_enrichment(
                    record_id, lead_data, company_data, lead_record_id, company_record_id, status
                ):
                    logger.info(f"  ✓ Campaign lead updated")
                    success += 1
                    
                    # Auto-check Generate Messages so outreach runs immediately after
                    try:
                        self.campaign_leads_table.update(record_id, {
                            'Enrich Lead': False,
                            'Generate Messages': True  # Ready for outreach generation
                        })
                    except:
                        pass
                else:
                    failed += 1
                
                time.sleep(2)  # Rate limiting
                
            except Exception as e:
                logger.error(f"  Error: {e}")
                failed += 1
        
        logger.info(f"\n{'=' * 60}")
        logger.info(f"ENRICHMENT COMPLETE: {success} success, {failed} failed")
        logger.info("=" * 60)
    
    # =========================================================================
    # STEP 2: GENERATE OUTREACH
    # =========================================================================
    
    def generate_outreach_messages(self, lead_fields: Dict, 
                                   campaign_type: str,
                                   conference_name: str,
                                   campaign_background: str) -> Dict:
        """Generate event-specific outreach messages"""
        
        name = lead_fields.get('Lead Name', 'there')
        first_name = name.split()[0] if name else 'there'
        title = lead_fields.get('Title', '')
        company = lead_fields.get('Company', '')
        email = lead_fields.get('Email', '')
        
        # Get ICP context
        lead_icp = lead_fields.get('Lead ICP Score', '')
        lead_tier = lead_fields.get('Lead ICP Tier', '')
        company_icp = lead_fields.get('Company ICP Score', '')
        company_focus = lead_fields.get('Company Focus', '')
        company_tech = lead_fields.get('Company Technology', '')
        
        # Get campaign context
        ctx = self.campaign_contexts.get(campaign_type, self.campaign_contexts['Custom'])
        
        # Build company profile context
        our_company = ""
        if self.company_profile:
            our_company = f"""
OUR COMPANY (Rezon Bio):
- Focus: European biologics CDMO, mammalian cell culture
- Services: mAbs, bispecifics, ADCs manufacturing
- Stage: Development through commercial
- Differentiators: European location, flexible capacity, regulatory expertise
"""
        
        prompt = f"""Generate personalized outreach for a {campaign_type} campaign.

TARGET LEAD:
- Name: {name}
- Title: {title}
- Company: {company}
- Lead ICP: {lead_icp}/100 ({lead_tier})
- Company ICP: {company_icp}/105
- Company Focus: {company_focus}
- Company Technology: {company_tech}

CAMPAIGN CONTEXT:
- Type: {campaign_type}
- Conference/Event: {conference_name}
- Background: {campaign_background}
- Angle: {ctx['angle']}
- Urgency: {ctx['urgency']}
- Tone: {ctx['tone']}
{our_company}
GENERATE 4 MESSAGES:

1. EMAIL (120-150 words)
- Subject line referencing the event: {conference_name}
- Natural opening mentioning the event context
- Brief value prop relevant to their technology/focus
- Soft CTA: {ctx['cta']}
- End with [Your Name]

2. LINKEDIN CONNECTION REQUEST (max 200 chars)
- Reference {conference_name}
- Brief, friendly

3. LINKEDIN SHORT MESSAGE (300-400 chars)
- Post-connection follow-up
- Reference event and their work
- Suggest meeting

4. LINKEDIN INMAIL (250-350 words)
- Detailed outreach
- Reference specific event context
- Industry peer conversation
- Value-focused

STYLE GUIDELINES:
- Natural, conversational tone
- Reference the specific event: {conference_name}
- Use campaign background naturally: {campaign_background}
- No salesy language or buzzwords
- Like an industry peer reaching out
- Personalized to their company's focus

Return JSON:
{{
    "email_subject": "Subject line",
    "email_body": "Full email",
    "linkedin_connection": "Connection request",
    "linkedin_short": "Short message",
    "linkedin_inmail_subject": "InMail subject",
    "linkedin_inmail_body": "Full InMail"
}}"""

        try:
            message = self.anthropic_client.messages.create(
                model=self.config['anthropic']['model'],
                max_tokens=3000,
                messages=[{"role": "user", "content": prompt}]
            )
            
            response_text = message.content[0].text
            
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
            logger.error(f"  Error generating outreach: {e}")
            return {}
    
    def process_outreach_generation(self, campaign_type: Optional[str] = None, 
                                     limit: Optional[int] = None):
        """
        STEP 2: Generate outreach for enriched campaign leads
        """
        logger.info("=" * 60)
        logger.info("CAMPAIGN LEADS - OUTREACH GENERATION")
        logger.info("=" * 60)
        
        # Get leads ready for outreach (enriched + Generate Messages checked)
        formula = "AND({Generate Messages} = TRUE(), OR({Enrichment Status} = 'Enriched', {Email} != ''))"
        
        if campaign_type:
            formula = f"AND({formula}, {{Campaign Type}} = '{campaign_type}')"
        
        leads = self.campaign_leads_table.all(formula=formula)
        
        if limit:
            leads = leads[:limit]
        
        if not leads:
            logger.info("No leads ready for outreach generation")
            logger.info("Make sure leads are enriched and 'Generate Messages' is checked")
            return
        
        logger.info(f"Generating outreach for {len(leads)} leads...")
        
        success = 0
        failed = 0
        
        for idx, record in enumerate(leads, 1):
            record_id = record.get('id')
            fields = record.get('fields', {})
            
            name = fields.get('Lead Name', '')
            company = fields.get('Company', '')
            camp_type = fields.get('Campaign Type', 'Custom')
            conference_name = fields.get('Conference Name', fields.get('Campaign Name', ''))
            campaign_background = fields.get('Campaign Background', fields.get('Campaign Details', ''))
            
            logger.info(f"\n[{idx}/{len(leads)}] {name} @ {company}")
            logger.info(f"  Campaign: {camp_type} - {conference_name}")
            
            try:
                messages = self.generate_outreach_messages(
                    fields, camp_type, conference_name, campaign_background
                )
                
                if messages:
                    update = {
                        'Email Subject': messages.get('email_subject', ''),
                        'Email Body': messages.get('email_body', ''),
                        'LinkedIn Connection Request': messages.get('linkedin_connection', ''),
                        'LinkedIn Short Message': messages.get('linkedin_short', ''),
                        'LinkedIn InMail Subject': messages.get('linkedin_inmail_subject', ''),
                        'LinkedIn InMail Body': messages.get('linkedin_inmail_body', ''),
                        'Message Generated Date': datetime.now().strftime('%Y-%m-%d'),
                        'Generate Messages': False,  # Uncheck
                        'Status': 'Ready to Contact'
                    }
                    
                    self.campaign_leads_table.update(record_id, update)
                    logger.info(f"  ✓ Messages generated")
                    success += 1
                else:
                    logger.error(f"  ✗ Generation failed")
                    failed += 1
                
                time.sleep(2)
                
            except Exception as e:
                logger.error(f"  Error: {e}")
                failed += 1
        
        logger.info(f"\n{'=' * 60}")
        logger.info(f"OUTREACH GENERATION COMPLETE: {success} success, {failed} failed")
        logger.info("=" * 60)


def main():
    parser = argparse.ArgumentParser(description='Campaign Leads Processor')
    parser.add_argument('--config', default='config.yaml', help='Config file path')
    parser.add_argument('--enrich-only', action='store_true', 
                        help='Only enrich (skip outreach generation)')
    parser.add_argument('--generate-only', action='store_true', 
                        help='Only generate outreach (skip enrichment)')
    parser.add_argument('--campaign-type', type=str, 
                        help='Filter by campaign type')
    parser.add_argument('--limit', type=int, help='Max leads to process')
    
    args = parser.parse_args()
    
    try:
        processor = CampaignLeadsProcessor(config_path=args.config)
        
        # Default: run both enrich AND generate in one go
        run_enrich = not args.generate_only
        run_generate = not args.enrich_only
        
        if run_enrich:
            processor.process_enrichment(limit=args.limit)
        
        if run_generate:
            processor.process_outreach_generation(
                campaign_type=args.campaign_type,
                limit=args.limit
            )
        
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
