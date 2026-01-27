#!/usr/bin/env python3
"""
Campaign Leads Outreach Generator
Generates event-specific outreach for leads uploaded for conferences, webinars, trade shows, etc.
Separate from main Leads table - use as a staging area for event-specific campaigns.
"""

import os
import sys
import yaml
import json
import time
import logging
import argparse
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
import anthropic
from pyairtable import Api

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('campaign_outreach.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class CampaignOutreachGenerator:
    """Generate event-specific outreach for campaign leads"""
    
    def __init__(self, config_path: str = "config.yaml"):
        """Initialize with configuration"""
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        # Initialize APIs
        self.airtable = Api(self.config['airtable']['api_key'])
        self.base = self.airtable.base(self.config['airtable']['base_id'])
        
        # Campaign Leads table (new table for event-specific leads)
        table_name = self.config['airtable']['tables'].get('campaign_leads', 'Campaign Leads')
        self.campaign_leads_table = self.base.table(table_name)
        
        # Main Leads table (for profile lookup)
        self.leads_table = self.base.table(self.config['airtable']['tables']['leads'])
        
        # Companies table (for ICP context)
        self.companies_table = self.base.table(self.config['airtable']['tables']['companies'])
        
        self.anthropic_client = anthropic.Anthropic(
            api_key=self.config['anthropic']['api_key']
        )
        
        # Campaign type contexts
        self.campaign_contexts = {
            'Conference': {
                'angle': 'meeting at the event',
                'cta': 'schedule a brief meeting during the conference',
                'urgency': 'high - limited time window',
                'tone': 'peer-to-peer, industry event context'
            },
            'Webinar': {
                'angle': 'shared interest in the topic',
                'cta': 'follow up after the webinar',
                'urgency': 'medium - content-driven',
                'tone': 'educational, thought leadership'
            },
            'Trade Show': {
                'angle': 'booth visit or demo',
                'cta': 'visit our booth for a demo',
                'urgency': 'high - event-specific',
                'tone': 'product-focused, demo-ready'
            },
            'Partnership Event': {
                'angle': 'potential collaboration',
                'cta': 'discuss partnership opportunities',
                'urgency': 'medium - relationship building',
                'tone': 'collaborative, strategic'
            },
            'Workshop': {
                'angle': 'hands-on learning',
                'cta': 'connect during the workshop',
                'urgency': 'medium - educational',
                'tone': 'technical, knowledge-sharing'
            },
            'Post-Event Follow-up': {
                'angle': 'following up from the event',
                'cta': 'continue the conversation',
                'urgency': 'high - strike while iron is hot',
                'tone': 'warm, continuing conversation'
            },
            'Custom': {
                'angle': 'general outreach',
                'cta': 'connect and discuss',
                'urgency': 'medium',
                'tone': 'professional, adaptable'
            }
        }
        
        logger.info("CampaignOutreachGenerator initialized")
    
    def lookup_existing_profile(self, email: str, name: str, company: str) -> Optional[Dict]:
        """Look up existing deep profile from main Leads table"""
        
        # Try email match first
        if email:
            try:
                matches = self.leads_table.all(formula=f"{{Email}} = '{email}'")
                if matches:
                    profile = matches[0].get('fields', {})
                    logger.info(f"  ✓ Found existing profile by email: {name}")
                    return {
                        'deep_profile': profile.get('Intelligence Notes', ''),
                        'activity_log': profile.get('Activity Log', ''),
                        'x_profile': profile.get('X Profile', ''),
                        'linkedin_url': profile.get('LinkedIn URL', ''),
                        'lead_icp_score': profile.get('Lead ICP Score'),
                        'lead_icp_tier': profile.get('Lead ICP Tier', ''),
                        'trigger_events': profile.get('Trigger Events', ''),
                        'company_icp_score': None  # Will be fetched separately
                    }
            except Exception as e:
                logger.debug(f"  Email lookup error: {str(e)}")
        
        # Try name + company match
        try:
            formula = f"AND({{Lead Name}} = '{name}', SEARCH('{company}', {{Company}}))"
            matches = self.leads_table.all(formula=formula)
            if matches:
                profile = matches[0].get('fields', {})
                logger.info(f"  ✓ Found existing profile by name+company: {name}")
                return {
                    'deep_profile': profile.get('Intelligence Notes', ''),
                    'activity_log': profile.get('Activity Log', ''),
                    'x_profile': profile.get('X Profile', ''),
                    'linkedin_url': profile.get('LinkedIn URL', ''),
                    'lead_icp_score': profile.get('Lead ICP Score'),
                    'lead_icp_tier': profile.get('Lead ICP Tier', ''),
                    'trigger_events': profile.get('Trigger Events', ''),
                    'company_icp_score': None
                }
        except Exception as e:
            logger.debug(f"  Name+company lookup error: {str(e)}")
        
        logger.info(f"  ○ No existing profile found for: {name}")
        return None
    
    def lookup_company_info(self, company_name: str) -> Dict:
        """Look up company info from Companies table"""
        try:
            # Search for company
            matches = self.companies_table.all(formula=f"SEARCH('{company_name}', {{Company Name}})")
            if matches:
                company = matches[0].get('fields', {})
                return {
                    'icp_score': company.get('ICP Score'),
                    'focus_area': company.get('Focus Area', ''),
                    'technology_platform': company.get('Technology Platform', ''),
                    'pipeline_info': company.get('Pipeline Info', ''),
                    'funding_stage': company.get('Funding Stage', ''),
                    'location': company.get('Location', ''),
                    'company_size': company.get('Company Size', ''),
                    'intelligence_notes': company.get('Intelligence Notes', '')
                }
        except Exception as e:
            logger.debug(f"  Company lookup error: {str(e)}")
        
        return {}
    
    def generate_campaign_outreach(self, lead: Dict, campaign_type: str, 
                                   campaign_name: str, campaign_details: str,
                                   existing_profile: Optional[Dict] = None,
                                   company_info: Optional[Dict] = None) -> Dict:
        """Generate event-specific outreach messages"""
        
        name = lead.get('Lead Name', 'there')
        first_name = name.split()[0] if name else 'there'
        title = lead.get('Title', '')
        company = lead.get('Company', '')
        
        # Get campaign context
        context = self.campaign_contexts.get(campaign_type, self.campaign_contexts['Custom'])
        
        # Build context string
        profile_context = ""
        if existing_profile:
            if existing_profile.get('deep_profile'):
                profile_context += f"\nExisting intelligence: {existing_profile['deep_profile'][:500]}"
            if existing_profile.get('activity_log'):
                profile_context += f"\nRecent activity: {existing_profile['activity_log'][:300]}"
            if existing_profile.get('trigger_events'):
                profile_context += f"\nTrigger events: {existing_profile['trigger_events'][:200]}"
        
        company_context = ""
        if company_info:
            if company_info.get('focus_area'):
                company_context += f"\nCompany focus: {company_info['focus_area']}"
            if company_info.get('technology_platform'):
                company_context += f"\nTechnology: {company_info['technology_platform']}"
            if company_info.get('pipeline_info'):
                company_context += f"\nPipeline: {company_info['pipeline_info'][:200]}"
            if company_info.get('icp_score'):
                company_context += f"\nICP Score: {company_info['icp_score']}"
        
        prompt = f"""Generate event-specific outreach for a campaign lead.

LEAD INFO:
- Name: {name}
- Title: {title}
- Company: {company}

CAMPAIGN INFO:
- Type: {campaign_type}
- Event Name: {campaign_name}
- Details: {campaign_details}

CAMPAIGN APPROACH:
- Angle: {context['angle']}
- Call to Action: {context['cta']}
- Urgency: {context['urgency']}
- Tone: {context['tone']}

{f"EXISTING PROFILE INFO:{profile_context}" if profile_context else "No existing profile available - use basic context only."}

{f"COMPANY INFO:{company_context}" if company_context else "No company intelligence available."}

REZON BIO CONTEXT:
- European CDMO specializing in biologics manufacturing
- Expertise: mAbs, bispecifics, ADCs
- Mammalian cell culture platforms
- Competitive pricing with strong quality track record
- Based in Poland with German business operations

Generate 4 outreach messages tailored to the {campaign_type} context:

1. EMAIL (120-150 words):
- Subject line mentioning the event
- Reference the event naturally
- Connect their role to manufacturing needs
- Soft CTA related to the event
- Sign off with [Your Name], Rezon Bio

2. LINKEDIN CONNECTION REQUEST (Max 200 characters):
- Brief, friendly
- Reference the event
- No salesy language

3. LINKEDIN SHORT MESSAGE (300-400 characters):
- For after they accept connection
- Reference the event
- Suggest meeting/call
- Sign: [Your Name], Rezon Bio

4. LINKEDIN INMAIL (250-350 words):
- Longer, more detailed
- Event context upfront
- Industry peer conversation
- Manufacturing relevance
- Clear but soft CTA
- Sign with [Your Name], Rezon Bio

OUTPUT FORMAT (JSON):
{{
    "email_subject": "...",
    "email_body": "...",
    "linkedin_connection_request": "...",
    "linkedin_short_message": "...",
    "linkedin_inmail_subject": "...",
    "linkedin_inmail_body": "..."
}}

IMPORTANT:
- Keep it natural and conversational
- Reference the event ({campaign_name}) naturally
- Avoid salesy or pushy language
- Make it feel like an industry peer reaching out
- Use [Your Name] placeholder for signature
- Don't mention AI or automation"""

        try:
            message = self.anthropic_client.messages.create(
                model=self.config['anthropic']['model'],
                max_tokens=2000,
                messages=[{"role": "user", "content": prompt}]
            )
            
            response_text = message.content[0].text.strip()
            
            # Parse JSON response
            if "```json" in response_text:
                start = response_text.find("```json") + 7
                end = response_text.find("```", start)
                json_str = response_text[start:end].strip()
            elif "```" in response_text:
                start = response_text.find("```") + 3
                end = response_text.find("```", start)
                json_str = response_text[start:end].strip()
            elif "{" in response_text:
                start = response_text.find("{")
                end = response_text.rfind("}") + 1
                json_str = response_text[start:end]
            else:
                json_str = response_text
            
            return json.loads(json_str)
            
        except Exception as e:
            logger.error(f"  Error generating outreach: {str(e)}")
            return {}
    
    def get_leads_to_process(self, campaign_type: Optional[str] = None, 
                             limit: Optional[int] = None) -> List[Dict]:
        """Get campaign leads that need outreach generation"""
        
        # Build formula
        conditions = [
            "{Generate Messages}",  # Checkbox must be checked
            "OR({Email Body} = '', {Email Body} = BLANK())"  # Not already generated
        ]
        
        if campaign_type:
            conditions.append(f"{{Campaign Type}} = '{campaign_type}'")
        
        formula = f"AND({', '.join(conditions)})"
        
        try:
            leads = self.campaign_leads_table.all(formula=formula)
            logger.info(f"Found {len(leads)} campaign leads to process")
            
            if limit:
                leads = leads[:limit]
                logger.info(f"Limited to {limit} leads")
            
            return leads
        except Exception as e:
            logger.error(f"Error fetching campaign leads: {str(e)}")
            return []
    
    def process_lead(self, record: Dict) -> bool:
        """Process a single campaign lead"""
        
        fields = record.get('fields', {})
        record_id = record.get('id')
        
        name = fields.get('Lead Name', 'Unknown')
        email = fields.get('Email', '')
        title = fields.get('Title', '')
        company = fields.get('Company', '')
        campaign_type = fields.get('Campaign Type', 'Conference')
        campaign_name = fields.get('Campaign Name', '')
        campaign_details = fields.get('Campaign Details', '')
        
        logger.info(f"\nProcessing: {name} ({title} @ {company})")
        logger.info(f"  Campaign: {campaign_type} - {campaign_name}")
        
        # Look up existing profile
        existing_profile = self.lookup_existing_profile(email, name, company)
        
        # Look up company info
        company_info = self.lookup_company_info(company)
        
        # Generate outreach
        outreach = self.generate_campaign_outreach(
            fields,
            campaign_type,
            campaign_name,
            campaign_details,
            existing_profile,
            company_info
        )
        
        if not outreach:
            logger.warning(f"  ✗ Failed to generate outreach for {name}")
            return False
        
        # Update record in Airtable
        update_fields = {
            'Email Subject': outreach.get('email_subject', ''),
            'Email Body': outreach.get('email_body', ''),
            'LinkedIn Connection Request': outreach.get('linkedin_connection_request', ''),
            'LinkedIn Short Message': outreach.get('linkedin_short_message', ''),
            'LinkedIn InMail Subject': outreach.get('linkedin_inmail_subject', ''),
            'LinkedIn InMail Body': outreach.get('linkedin_inmail_body', ''),
            'Message Generated Date': datetime.now().strftime('%Y-%m-%d')
        }
        
        # Add profile info if found
        if existing_profile:
            if existing_profile.get('lead_icp_score'):
                update_fields['Lead ICP Score'] = existing_profile['lead_icp_score']
            if existing_profile.get('lead_icp_tier'):
                update_fields['Lead ICP Tier'] = existing_profile['lead_icp_tier']
        
        if company_info and company_info.get('icp_score'):
            update_fields['Company ICP Score'] = company_info['icp_score']
        
        try:
            self.campaign_leads_table.update(record_id, update_fields)
            logger.info(f"  ✓ Generated outreach for {name}")
            return True
        except Exception as e:
            logger.error(f"  ✗ Error updating Airtable: {str(e)}")
            return False
    
    def run(self, campaign_type: Optional[str] = None, limit: Optional[int] = None):
        """Run campaign outreach generation"""
        
        logger.info("="*60)
        logger.info("CAMPAIGN OUTREACH GENERATOR")
        logger.info("="*60)
        
        if campaign_type:
            logger.info(f"Campaign type filter: {campaign_type}")
        if limit:
            logger.info(f"Limit: {limit} leads")
        
        # Get leads to process
        leads = self.get_leads_to_process(campaign_type, limit)
        
        if not leads:
            logger.info("No campaign leads found to process")
            return
        
        # Process results
        results = {
            'processed': 0,
            'success': 0,
            'failed': 0,
            'with_profile': 0
        }
        
        for lead in leads:
            results['processed'] += 1
            
            if self.process_lead(lead):
                results['success'] += 1
            else:
                results['failed'] += 1
            
            # Rate limiting
            time.sleep(2)
        
        # Summary
        logger.info("\n" + "="*60)
        logger.info("CAMPAIGN OUTREACH SUMMARY")
        logger.info("="*60)
        logger.info(f"Leads processed: {results['processed']}")
        logger.info(f"Success: {results['success']}")
        logger.info(f"Failed: {results['failed']}")
        logger.info("="*60)


def main():
    """CLI entry point"""
    parser = argparse.ArgumentParser(description='Generate Campaign Outreach')
    parser.add_argument('--config', default='config.yaml', help='Path to config file')
    parser.add_argument('--campaign-type', choices=[
        'Conference', 'Webinar', 'Trade Show', 
        'Partnership Event', 'Workshop', 'Post-Event Follow-up', 'Custom'
    ], help='Filter by campaign type')
    parser.add_argument('--limit', type=int, help='Maximum leads to process')
    
    args = parser.parse_args()
    
    try:
        generator = CampaignOutreachGenerator(config_path=args.config)
        generator.run(campaign_type=args.campaign_type, limit=args.limit)
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
