#!/usr/bin/env python3
"""
Outreach Message Validator

Reviews outreach messages across all tables and validates their accuracy against
current market information. Assigns a validity rating to help the team prioritize
manual reviews.

Tables processed:
- Leads (General Outreach Email, LinkedIn Connection Request, LinkedIn Short Message, LinkedIn InMail)
- Trigger History (Email Message, LinkedIn Message, InMail Message)
- Campaign Leads (if exists)

Validation checks:
1. Company information accuracy (funding, pipeline stage, recent news)
2. Lead information accuracy (title, role, company association)
3. Claims and statements verification
4. Outdated information detection
5. Factual consistency

Ratings:
- HIGH (90-100): Safe to send, all claims verified
- MEDIUM (70-89): Minor uncertainties, quick review recommended
- LOW (50-69): Significant uncertainties, manual review required
- CRITICAL (<50): Major issues found, do not send without review

Runs: Twice daily (8 AM and 4 PM UTC) via GitHub Actions
"""

import os
import sys
import yaml
import json
import time
import logging
import argparse
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any

import anthropic
from pyairtable import Api

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('outreach_validation.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class OutreachValidator:
    """Validates outreach messages for accuracy and consistency"""
    
    # Outreach fields to validate in Leads table
    LEAD_OUTREACH_FIELDS = [
        'General Outreach Email',
        'LinkedIn Connection Request', 
        'LinkedIn Short Message',
        'LinkedIn InMail'
    ]
    
    # Outreach fields in Trigger History
    TRIGGER_OUTREACH_FIELDS = [
        'Email Message',
        'LinkedIn Message',
        'InMail Message'
    ]
    
    # Outreach fields in Campaign Leads (if exists)
    CAMPAIGN_OUTREACH_FIELDS = [
        'Campaign Email',
        'Campaign LinkedIn Message',
        'Campaign InMail'
    ]
    
    def __init__(self, config_path: str = "config.yaml"):
        """Initialize with configuration"""
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        # Initialize APIs
        self.airtable = Api(self.config['airtable']['api_key'])
        self.base = self.airtable.base(self.config['airtable']['base_id'])
        
        # Core tables
        self.leads_table = self.base.table(self.config['airtable']['tables']['leads'])
        self.companies_table = self.base.table(self.config['airtable']['tables']['companies'])
        self.trigger_history_table = self.base.table('Trigger History')
        
        # Optional tables
        self.campaign_leads_table = self._init_table('Campaign Leads')
        
        # Company Profile for context
        self.company_profile = self._load_company_profile()
        
        # API client
        self.anthropic_client = anthropic.Anthropic(
            api_key=self.config['anthropic']['api_key']
        )
        
        logger.info("OutreachValidator initialized")
    
    def _init_table(self, table_name: str):
        """Safely initialize a table"""
        try:
            table = self.base.table(table_name)
            # Test access
            table.first()
            return table
        except Exception as e:
            logger.warning(f"Table '{table_name}' not accessible: {e}")
            return None
    
    def _load_company_profile(self) -> Optional[Dict]:
        """Load company profile for context"""
        try:
            table = self.base.table('Company Profile')
            records = table.all()
            if records:
                return records[0]['fields']
        except:
            pass
        return None
    
    # =========================================================================
    # GET RECORDS NEEDING VALIDATION
    # =========================================================================
    
    def get_leads_needing_validation(self, limit: int = None) -> List[Dict]:
        """Get leads with outreach messages that haven't been validated"""
        try:
            # Get leads that have outreach but no validity rating
            # OR validity rating is empty/null
            formula = "AND(OR({General Outreach Email} != '', {LinkedIn InMail} != ''), OR({Outreach Validity Rating} = '', {Outreach Validity Rating} = BLANK()))"
            
            records = self.leads_table.all(formula=formula)
            
            # Filter to only those with actual outreach content
            leads_with_outreach = []
            for record in records:
                has_outreach = any(
                    record['fields'].get(field, '').strip() 
                    for field in self.LEAD_OUTREACH_FIELDS
                )
                if has_outreach:
                    leads_with_outreach.append(record)
            
            if limit:
                leads_with_outreach = leads_with_outreach[:limit]
            
            logger.info(f"Found {len(leads_with_outreach)} leads needing validation")
            return leads_with_outreach
            
        except Exception as e:
            logger.error(f"Error getting leads for validation: {e}")
            return []
    
    def get_triggers_needing_validation(self, limit: int = None) -> List[Dict]:
        """Get trigger history records with outreach that haven't been validated"""
        try:
            # Get triggers with outreach but no validity rating
            formula = "AND(OR({Email Message} != '', {LinkedIn Message} != '', {InMail Message} != ''), OR({Outreach Validity Rating} = '', {Outreach Validity Rating} = BLANK()))"
            
            records = self.trigger_history_table.all(formula=formula)
            
            # Filter to only those with actual outreach content
            triggers_with_outreach = []
            for record in records:
                has_outreach = any(
                    record['fields'].get(field, '').strip() 
                    for field in self.TRIGGER_OUTREACH_FIELDS
                )
                if has_outreach:
                    triggers_with_outreach.append(record)
            
            if limit:
                triggers_with_outreach = triggers_with_outreach[:limit]
            
            logger.info(f"Found {len(triggers_with_outreach)} triggers needing validation")
            return triggers_with_outreach
            
        except Exception as e:
            logger.error(f"Error getting triggers for validation: {e}")
            return []
    
    def get_campaign_leads_needing_validation(self, limit: int = None) -> List[Dict]:
        """Get campaign leads with outreach that haven't been validated"""
        if not self.campaign_leads_table:
            return []
        
        try:
            formula = "AND(OR({Campaign Email} != '', {Campaign LinkedIn Message} != ''), OR({Outreach Validity Rating} = '', {Outreach Validity Rating} = BLANK()))"
            
            records = self.campaign_leads_table.all(formula=formula)
            
            triggers_with_outreach = []
            for record in records:
                has_outreach = any(
                    record['fields'].get(field, '').strip() 
                    for field in self.CAMPAIGN_OUTREACH_FIELDS
                )
                if has_outreach:
                    triggers_with_outreach.append(record)
            
            if limit:
                triggers_with_outreach = triggers_with_outreach[:limit]
            
            logger.info(f"Found {len(triggers_with_outreach)} campaign leads needing validation")
            return triggers_with_outreach
            
        except Exception as e:
            logger.error(f"Error getting campaign leads for validation: {e}")
            return []
    
    # =========================================================================
    # GET CONTEXT FOR VALIDATION
    # =========================================================================
    
    def get_lead_context(self, lead_record: Dict) -> Dict:
        """Get full context for a lead including company info"""
        fields = lead_record['fields']
        
        context = {
            'lead_name': fields.get('Lead Name', ''),
            'lead_title': fields.get('Title', ''),
            'lead_email': fields.get('Email', ''),
            'lead_linkedin': fields.get('LinkedIn URL', ''),
            'lead_location': fields.get('Location', ''),
            'lead_icp_score': fields.get('Lead ICP Score', ''),
            'company_name': '',
            'company_data': {}
        }
        
        # Get company info
        company_ids = fields.get('Company', [])
        if company_ids:
            try:
                company_record = self.companies_table.get(company_ids[0])
                company_fields = company_record['fields']
                context['company_name'] = company_fields.get('Company Name', '')
                context['company_data'] = {
                    'location': company_fields.get('Location/HQ', ''),
                    'website': company_fields.get('Website', ''),
                    'funding': company_fields.get('Latest Funding Round', ''),
                    'pipeline_stage': company_fields.get('Pipeline Stage', ''),
                    'lead_programs': company_fields.get('Lead Programs', ''),
                    'therapeutic_areas': company_fields.get('Therapeutic Areas', ''),
                    'focus_areas': company_fields.get('Focus Area', ''),
                    'manufacturing_status': company_fields.get('Manufacturing Status', ''),
                    'icp_score': company_fields.get('ICP Fit Score', ''),
                    'intelligence_notes': company_fields.get('Intelligence Notes', '')
                }
            except Exception as e:
                logger.debug(f"Could not get company info: {e}")
        
        return context
    
    def get_trigger_context(self, trigger_record: Dict) -> Dict:
        """Get full context for a trigger including lead and company info"""
        fields = trigger_record['fields']
        
        context = {
            'trigger_type': fields.get('Trigger Type', ''),
            'trigger_date': fields.get('Date Detected', ''),
            'trigger_description': fields.get('Description', ''),
            'trigger_urgency': fields.get('Urgency', ''),
            'outreach_angle': fields.get('Outreach Angle', ''),
            'sources': fields.get('Sources', ''),
            'lead_name': '',
            'lead_title': '',
            'company_name': '',
            'company_data': {}
        }
        
        # Get lead info
        lead_ids = fields.get('Lead', [])
        if lead_ids:
            try:
                lead_record = self.leads_table.get(lead_ids[0])
                lead_fields = lead_record['fields']
                context['lead_name'] = lead_fields.get('Lead Name', '')
                context['lead_title'] = lead_fields.get('Title', '')
                
                # Get company from lead
                company_ids = lead_fields.get('Company', [])
                if company_ids:
                    company_record = self.companies_table.get(company_ids[0])
                    company_fields = company_record['fields']
                    context['company_name'] = company_fields.get('Company Name', '')
                    context['company_data'] = {
                        'location': company_fields.get('Location/HQ', ''),
                        'funding': company_fields.get('Latest Funding Round', ''),
                        'pipeline_stage': company_fields.get('Pipeline Stage', ''),
                        'lead_programs': company_fields.get('Lead Programs', ''),
                        'therapeutic_areas': company_fields.get('Therapeutic Areas', ''),
                        'intelligence_notes': company_fields.get('Intelligence Notes', '')
                    }
            except Exception as e:
                logger.debug(f"Could not get lead/company info: {e}")
        
        return context
    
    # =========================================================================
    # VALIDATION LOGIC
    # =========================================================================
    
    def validate_outreach_messages(self, messages: Dict[str, str], context: Dict) -> Dict:
        """
        Validate outreach messages against current market information.
        Uses AI with web search to verify claims.
        
        Returns:
            {
                'validity_rating': 'HIGH|MEDIUM|LOW|CRITICAL',
                'validity_score': 0-100,
                'issues_found': [...],
                'verification_notes': '...',
                'recommendation': '...',
                'validated_at': '...'
            }
        """
        
        # Combine all messages for validation
        all_messages = "\n\n---\n\n".join([
            f"**{msg_type}:**\n{content}" 
            for msg_type, content in messages.items() 
            if content and content.strip()
        ])
        
        if not all_messages.strip():
            return {
                'validity_rating': 'CRITICAL',
                'validity_score': 0,
                'issues_found': ['No outreach messages to validate'],
                'verification_notes': 'No content found',
                'recommendation': 'Generate outreach messages first',
                'validated_at': datetime.now().isoformat()
            }
        
        # Build context string
        context_str = f"""
LEAD INFORMATION:
- Name: {context.get('lead_name', 'Unknown')}
- Title: {context.get('lead_title', 'Unknown')}
- Company: {context.get('company_name', 'Unknown')}

COMPANY INFORMATION:
- Location: {context.get('company_data', {}).get('location', 'Unknown')}
- Latest Funding: {context.get('company_data', {}).get('funding', 'Unknown')}
- Pipeline Stage: {context.get('company_data', {}).get('pipeline_stage', 'Unknown')}
- Lead Programs: {context.get('company_data', {}).get('lead_programs', 'Unknown')}
- Therapeutic Areas: {context.get('company_data', {}).get('therapeutic_areas', 'Unknown')}
- Manufacturing Status: {context.get('company_data', {}).get('manufacturing_status', 'Unknown')}
"""
        
        # Add trigger context if available
        if context.get('trigger_type'):
            context_str += f"""
TRIGGER INFORMATION:
- Type: {context.get('trigger_type', '')}
- Date: {context.get('trigger_date', '')}
- Description: {context.get('trigger_description', '')}
- Sources: {context.get('sources', '')}
"""
        
        validation_prompt = f"""You are a quality assurance specialist reviewing B2B outreach messages for a biologics CDMO (Contract Development and Manufacturing Organization).

CONTEXT (from our database):
{context_str}

OUTREACH MESSAGES TO VALIDATE:
{all_messages}

VALIDATION TASKS:
1. **Verify Lead Information**: Is the person's name, title, and company association accurate?
2. **Verify Company Information**: Check if mentioned company details (funding, pipeline, partnerships, etc.) are current and accurate
3. **Check Claims & Statements**: Are any specific claims made about the company or industry accurate?
4. **Detect Outdated Information**: Has anything changed recently that makes the message outdated?
5. **Check for Hallucinations**: Are there any statements that seem fabricated or unverifiable?
6. **Tone & Appropriateness**: Is the message appropriate for the recipient's seniority and industry?

SEARCH AND VERIFY:
- Search for recent news about the company and lead
- Verify any specific claims (funding amounts, pipeline stages, partnerships)
- Check if the lead is still at the company with the stated title
- Look for any recent developments that should be mentioned or that contradict the message

RATING SCALE:
- HIGH (90-100): All information verified accurate, safe to send as-is
- MEDIUM (70-89): Minor uncertainties but generally accurate, quick review recommended
- LOW (50-69): Significant uncertainties or potential issues, manual review required
- CRITICAL (0-49): Major factual errors or outdated information, do not send without revision

Return ONLY valid JSON:
{{
    "validity_score": 85,
    "validity_rating": "HIGH|MEDIUM|LOW|CRITICAL",
    "issues_found": [
        "Specific issue 1",
        "Specific issue 2"
    ],
    "verified_facts": [
        "Fact 1 confirmed",
        "Fact 2 confirmed"
    ],
    "uncertain_claims": [
        "Claim that could not be verified"
    ],
    "verification_notes": "Summary of verification process and findings",
    "recommendation": "Specific action recommendation for the sales team",
    "suggested_edits": "Any specific edits suggested (or null if none needed)"
}}

Return ONLY JSON, no other text."""

        try:
            message = self.anthropic_client.messages.create(
                model=self.config['anthropic']['model'],
                max_tokens=2000,
                tools=[{"type": "web_search_20250305", "name": "web_search"}],
                messages=[{"role": "user", "content": validation_prompt}]
            )
            
            # Extract text
            response_text = ""
            for block in message.content:
                if hasattr(block, 'text'):
                    response_text += block.text
            
            # Parse JSON
            if "```json" in response_text:
                json_str = response_text.split("```json")[1].split("```")[0]
            elif "{" in response_text:
                start = response_text.find("{")
                end = response_text.rfind("}") + 1
                json_str = response_text[start:end]
            else:
                raise ValueError("No JSON found in response")
            
            result = json.loads(json_str.strip())
            result['validated_at'] = datetime.now().isoformat()
            
            return result
            
        except Exception as e:
            logger.error(f"Error validating outreach: {e}")
            return {
                'validity_rating': 'LOW',
                'validity_score': 50,
                'issues_found': [f'Validation error: {str(e)}'],
                'verification_notes': 'Automated validation failed - manual review recommended',
                'recommendation': 'Manual review required due to validation error',
                'validated_at': datetime.now().isoformat()
            }
    
    # =========================================================================
    # UPDATE RECORDS WITH VALIDATION
    # =========================================================================
    
    def update_lead_validation(self, record_id: str, validation: Dict):
        """Update lead record with validation results"""
        try:
            update_data = {
                'Outreach Validity Rating': validation.get('validity_rating', 'LOW'),
                'Outreach Validity Score': validation.get('validity_score', 50),
                'Outreach Validation Notes': self._format_validation_notes(validation),
                'Outreach Validated At': validation.get('validated_at', datetime.now().isoformat())
            }
            
            self.leads_table.update(record_id, update_data)
            logger.info(f"  ✓ Updated lead validation: {validation.get('validity_rating')}")
            
        except Exception as e:
            logger.error(f"Error updating lead validation: {e}")
            # Try with minimal fields
            try:
                self.leads_table.update(record_id, {
                    'Outreach Validity Rating': validation.get('validity_rating', 'LOW')
                })
            except:
                pass
    
    def update_trigger_validation(self, record_id: str, validation: Dict):
        """Update trigger record with validation results"""
        try:
            update_data = {
                'Outreach Validity Rating': validation.get('validity_rating', 'LOW'),
                'Outreach Validity Score': validation.get('validity_score', 50),
                'Outreach Validation Notes': self._format_validation_notes(validation),
                'Outreach Validated At': validation.get('validated_at', datetime.now().isoformat())
            }
            
            self.trigger_history_table.update(record_id, update_data)
            logger.info(f"  ✓ Updated trigger validation: {validation.get('validity_rating')}")
            
        except Exception as e:
            logger.error(f"Error updating trigger validation: {e}")
            try:
                self.trigger_history_table.update(record_id, {
                    'Outreach Validity Rating': validation.get('validity_rating', 'LOW')
                })
            except:
                pass
    
    def update_campaign_lead_validation(self, record_id: str, validation: Dict):
        """Update campaign lead record with validation results"""
        if not self.campaign_leads_table:
            return
        
        try:
            update_data = {
                'Outreach Validity Rating': validation.get('validity_rating', 'LOW'),
                'Outreach Validity Score': validation.get('validity_score', 50),
                'Outreach Validation Notes': self._format_validation_notes(validation),
                'Outreach Validated At': validation.get('validated_at', datetime.now().isoformat())
            }
            
            self.campaign_leads_table.update(record_id, update_data)
            logger.info(f"  ✓ Updated campaign lead validation: {validation.get('validity_rating')}")
            
        except Exception as e:
            logger.error(f"Error updating campaign lead validation: {e}")
    
    def _format_validation_notes(self, validation: Dict) -> str:
        """Format validation results into readable notes"""
        notes = []
        
        if validation.get('issues_found'):
            notes.append("⚠️ ISSUES FOUND:")
            for issue in validation['issues_found']:
                notes.append(f"  • {issue}")
        
        if validation.get('verified_facts'):
            notes.append("\n✓ VERIFIED:")
            for fact in validation['verified_facts'][:5]:  # Limit to 5
                notes.append(f"  • {fact}")
        
        if validation.get('uncertain_claims'):
            notes.append("\n❓ UNCERTAIN:")
            for claim in validation['uncertain_claims']:
                notes.append(f"  • {claim}")
        
        if validation.get('verification_notes'):
            notes.append(f"\n📋 NOTES: {validation['verification_notes']}")
        
        if validation.get('recommendation'):
            notes.append(f"\n💡 RECOMMENDATION: {validation['recommendation']}")
        
        if validation.get('suggested_edits'):
            notes.append(f"\n✏️ SUGGESTED EDITS: {validation['suggested_edits']}")
        
        return "\n".join(notes)
    
    # =========================================================================
    # MAIN VALIDATION WORKFLOW
    # =========================================================================
    
    def validate_all_pending(self, limit_per_table: int = 20):
        """Validate all pending outreach messages across all tables"""
        
        logger.info("="*70)
        logger.info("OUTREACH VALIDATION - STARTING")
        logger.info(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("="*70)
        
        stats = {
            'leads_processed': 0,
            'leads_high': 0,
            'leads_medium': 0,
            'leads_low': 0,
            'leads_critical': 0,
            'triggers_processed': 0,
            'triggers_high': 0,
            'triggers_medium': 0,
            'triggers_low': 0,
            'triggers_critical': 0,
            'campaign_processed': 0,
            'errors': 0
        }
        
        # 1. Validate Leads
        logger.info("\n--- VALIDATING LEAD OUTREACH ---")
        leads = self.get_leads_needing_validation(limit=limit_per_table)
        
        for idx, lead in enumerate(leads, 1):
            lead_name = lead['fields'].get('Lead Name', 'Unknown')
            company_name = ''
            
            # Get company name
            company_ids = lead['fields'].get('Company', [])
            if company_ids:
                try:
                    company_record = self.companies_table.get(company_ids[0])
                    company_name = company_record['fields'].get('Company Name', '')
                except:
                    pass
            
            logger.info(f"\n[{idx}/{len(leads)}] {lead_name} ({company_name})")
            
            try:
                # Get messages
                messages = {
                    field: lead['fields'].get(field, '') 
                    for field in self.LEAD_OUTREACH_FIELDS
                }
                
                # Get context
                context = self.get_lead_context(lead)
                
                # Validate
                validation = self.validate_outreach_messages(messages, context)
                
                # Update record
                self.update_lead_validation(lead['id'], validation)
                
                # Track stats
                stats['leads_processed'] += 1
                rating = validation.get('validity_rating', 'LOW')
                stats[f'leads_{rating.lower()}'] = stats.get(f'leads_{rating.lower()}', 0) + 1
                
                # Rate limiting
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"  Error: {e}")
                stats['errors'] += 1
        
        # 2. Validate Triggers
        logger.info("\n--- VALIDATING TRIGGER OUTREACH ---")
        triggers = self.get_triggers_needing_validation(limit=limit_per_table)
        
        for idx, trigger in enumerate(triggers, 1):
            trigger_type = trigger['fields'].get('Trigger Type', 'Unknown')
            logger.info(f"\n[{idx}/{len(triggers)}] Trigger: {trigger_type}")
            
            try:
                # Get messages
                messages = {
                    field: trigger['fields'].get(field, '') 
                    for field in self.TRIGGER_OUTREACH_FIELDS
                }
                
                # Get context
                context = self.get_trigger_context(trigger)
                
                # Validate
                validation = self.validate_outreach_messages(messages, context)
                
                # Update record
                self.update_trigger_validation(trigger['id'], validation)
                
                # Track stats
                stats['triggers_processed'] += 1
                rating = validation.get('validity_rating', 'LOW')
                stats[f'triggers_{rating.lower()}'] = stats.get(f'triggers_{rating.lower()}', 0) + 1
                
                # Rate limiting
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"  Error: {e}")
                stats['errors'] += 1
        
        # 3. Validate Campaign Leads (if table exists)
        if self.campaign_leads_table:
            logger.info("\n--- VALIDATING CAMPAIGN OUTREACH ---")
            campaign_leads = self.get_campaign_leads_needing_validation(limit=limit_per_table)
            
            for idx, campaign in enumerate(campaign_leads, 1):
                lead_name = campaign['fields'].get('Name', 'Unknown')
                logger.info(f"\n[{idx}/{len(campaign_leads)}] Campaign: {lead_name}")
                
                try:
                    messages = {
                        field: campaign['fields'].get(field, '') 
                        for field in self.CAMPAIGN_OUTREACH_FIELDS
                    }
                    
                    # Build simple context
                    context = {
                        'lead_name': campaign['fields'].get('Name', ''),
                        'lead_title': campaign['fields'].get('Title', ''),
                        'company_name': campaign['fields'].get('Company', ''),
                        'company_data': {}
                    }
                    
                    validation = self.validate_outreach_messages(messages, context)
                    self.update_campaign_lead_validation(campaign['id'], validation)
                    
                    stats['campaign_processed'] += 1
                    time.sleep(1)
                    
                except Exception as e:
                    logger.error(f"  Error: {e}")
                    stats['errors'] += 1
        
        # Summary
        logger.info("\n" + "="*70)
        logger.info("VALIDATION COMPLETE - SUMMARY")
        logger.info("="*70)
        logger.info(f"Leads validated: {stats['leads_processed']}")
        logger.info(f"  - HIGH: {stats['leads_high']}")
        logger.info(f"  - MEDIUM: {stats['leads_medium']}")
        logger.info(f"  - LOW: {stats['leads_low']}")
        logger.info(f"  - CRITICAL: {stats['leads_critical']}")
        logger.info(f"Triggers validated: {stats['triggers_processed']}")
        logger.info(f"  - HIGH: {stats['triggers_high']}")
        logger.info(f"  - MEDIUM: {stats['triggers_medium']}")
        logger.info(f"  - LOW: {stats['triggers_low']}")
        logger.info(f"  - CRITICAL: {stats['triggers_critical']}")
        if self.campaign_leads_table:
            logger.info(f"Campaign leads validated: {stats['campaign_processed']}")
        logger.info(f"Errors: {stats['errors']}")
        logger.info("="*70)
        
        return stats
    
    def validate_single_lead(self, lead_id: str):
        """Validate a single lead's outreach messages"""
        try:
            lead = self.leads_table.get(lead_id)
            lead_name = lead['fields'].get('Lead Name', 'Unknown')
            
            logger.info(f"Validating lead: {lead_name}")
            
            messages = {
                field: lead['fields'].get(field, '') 
                for field in self.LEAD_OUTREACH_FIELDS
            }
            
            context = self.get_lead_context(lead)
            validation = self.validate_outreach_messages(messages, context)
            self.update_lead_validation(lead_id, validation)
            
            return validation
            
        except Exception as e:
            logger.error(f"Error validating lead {lead_id}: {e}")
            return None
    
    def validate_single_trigger(self, trigger_id: str):
        """Validate a single trigger's outreach messages"""
        try:
            trigger = self.trigger_history_table.get(trigger_id)
            
            logger.info(f"Validating trigger: {trigger['fields'].get('Trigger Type', 'Unknown')}")
            
            messages = {
                field: trigger['fields'].get(field, '') 
                for field in self.TRIGGER_OUTREACH_FIELDS
            }
            
            context = self.get_trigger_context(trigger)
            validation = self.validate_outreach_messages(messages, context)
            self.update_trigger_validation(trigger_id, validation)
            
            return validation
            
        except Exception as e:
            logger.error(f"Error validating trigger {trigger_id}: {e}")
            return None


def main():
    parser = argparse.ArgumentParser(description='Validate outreach messages')
    parser.add_argument('--all', action='store_true', help='Validate all pending messages')
    parser.add_argument('--leads-only', action='store_true', help='Only validate leads')
    parser.add_argument('--triggers-only', action='store_true', help='Only validate triggers')
    parser.add_argument('--lead-id', type=str, help='Validate specific lead by ID')
    parser.add_argument('--trigger-id', type=str, help='Validate specific trigger by ID')
    parser.add_argument('--limit', type=int, default=20, help='Max records per table (default: 20)')
    parser.add_argument('--config', type=str, default='config.yaml', help='Config file path')
    
    args = parser.parse_args()
    
    validator = OutreachValidator(config_path=args.config)
    
    if args.lead_id:
        result = validator.validate_single_lead(args.lead_id)
        if result:
            print(f"\nRating: {result['validity_rating']} ({result['validity_score']}/100)")
            print(f"Recommendation: {result.get('recommendation', 'N/A')}")
    
    elif args.trigger_id:
        result = validator.validate_single_trigger(args.trigger_id)
        if result:
            print(f"\nRating: {result['validity_rating']} ({result['validity_score']}/100)")
            print(f"Recommendation: {result.get('recommendation', 'N/A')}")
    
    else:
        validator.validate_all_pending(limit_per_table=args.limit)


if __name__ == "__main__":
    main()
