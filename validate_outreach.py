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
        'Email Subject',
        'Email Body',
        'LinkedIn Connection Request', 
        'LinkedIn Short Message',
        'LinkedIn InMail Subject',
        'LinkedIn InMail Body'
    ]
    
    # Outreach fields in Trigger History
    TRIGGER_OUTREACH_FIELDS = [
        'Email Subject',
        'Email Body',
        'LinkedIn Connection Request',
        'LinkedIn Short Message'
    ]
    
    # Outreach fields in Campaign Leads (if exists)
    CAMPAIGN_OUTREACH_FIELDS = [
        'Email Subject',
        'Email Body',
        'LinkedIn Connection Request',
        'LinkedIn Short Message',
        'LinkedIn InMail Subject',
        'LinkedIn InMail Body'
    ]
    
    def __init__(self, config_path: str = "config.yaml"):
        """Initialize with configuration"""
        self.config_path = config_path
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
            # Get all leads and filter in Python (more reliable than complex formulas)
            records = self.leads_table.all()
            
            # Filter to those with outreach but no validity rating
            leads_with_outreach = []
            for record in records:
                fields = record['fields']
                
                # Check if has any outreach content
                has_outreach = any(
                    fields.get(field, '').strip() 
                    for field in self.LEAD_OUTREACH_FIELDS
                )
                
                # Check if not yet validated
                validity_rating = fields.get('Outreach Validity Rating', '')
                not_validated = not validity_rating or validity_rating.strip() == ''
                
                if has_outreach and not_validated:
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
            # Get all triggers and filter in Python
            records = self.trigger_history_table.all()
            
            # Filter to those with outreach but no validity rating
            triggers_with_outreach = []
            for record in records:
                fields = record['fields']
                
                # Check if has any outreach content
                has_outreach = any(
                    fields.get(field, '').strip() 
                    for field in self.TRIGGER_OUTREACH_FIELDS
                )
                
                # Check if not yet validated
                validity_rating = fields.get('Outreach Validity Rating', '')
                not_validated = not validity_rating or validity_rating.strip() == ''
                
                if has_outreach and not_validated:
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
            # Get all campaign leads and filter in Python
            records = self.campaign_leads_table.all()
            
            # Filter to those with outreach but no validity rating
            leads_with_outreach = []
            for record in records:
                fields = record['fields']
                
                # Check if has any outreach content
                has_outreach = any(
                    fields.get(field, '').strip() 
                    for field in self.CAMPAIGN_OUTREACH_FIELDS
                )
                
                # Check if not yet validated
                validity_rating = fields.get('Outreach Validity Rating', '')
                not_validated = not validity_rating or validity_rating.strip() == ''
                
                if has_outreach and not_validated:
                    leads_with_outreach.append(record)
            
            if limit:
                leads_with_outreach = leads_with_outreach[:limit]
            
            logger.info(f"Found {len(leads_with_outreach)} campaign leads needing validation")
            return leads_with_outreach
            
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
            'lead_linkedin': '',
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
                context['lead_linkedin'] = lead_fields.get('LinkedIn URL', '')
                
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
    
    def validate_outreach_messages(self, messages: Dict[str, str], context: Dict, source_type: str = "general") -> Dict:
        """
        Validate outreach messages against current market information.
        Uses AI with web search to verify claims.
        
        Args:
            messages: Dict of message type -> content
            context: Lead/company context
            source_type: "general", "trigger", or "campaign" - affects validation rules
        
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
LEAD INFORMATION (already verified - do NOT re-check):
- Name: {context.get('lead_name', 'Unknown')}
- Title: {context.get('lead_title', 'Unknown')}
- Company: {context.get('company_name', 'Unknown')}
- LinkedIn: {context.get('lead_linkedin', 'Not provided')}

COMPANY INFORMATION (from our database):
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
        
        # Add campaign context if available
        campaign_context = context.get('campaign_context', {})
        if source_type == "campaign" or campaign_context:
            campaign_type = campaign_context.get('campaign_type', context.get('campaign_type', ''))
            campaign_name = campaign_context.get('campaign_name', context.get('campaign_name', ''))
            context_str += f"""
CAMPAIGN INFORMATION:
- Campaign Type: {campaign_type or 'Campaign Lead List'}
- Campaign Name: {campaign_name or 'N/A'}
- Source: Campaign Lead Upload
"""
        
        # Build do-not-flag rules based on source type
        do_not_flag_rules = """
DO NOT flag as issues:
- Lead name/title/company (already verified)
- General statements that don't make specific claims
- Standard CDMO value propositions"""
        
        if source_type == "campaign":
            do_not_flag_rules += """
- Conference/event/webinar attendance or registration (this lead was added from an event attendee list - their attendance IS CONFIRMED and should be treated as fact)
- References to meeting at a conference, event, roadshow, or webinar (this is the reason for outreach and is verified by the campaign lead list)
- Mentions of shared event attendance, "looking forward to connecting at [event]", "saw you registered for", "great meeting you at", etc. - ALL of these are verified facts for campaign leads
- Campaign-specific outreach angles related to event context (these are intentional and correct)"""
        
        do_not_flag_rules += """

ONLY flag as issues:
- Specific factual claims in the message that are wrong or outdated
- Recent developments that contradict the message
- Inappropriate tone or content"""
        
        validation_prompt = f"""You are a quality assurance specialist reviewing B2B outreach messages for a biologics CDMO.

CONTEXT (from our database - this info is already verified):
{context_str}

{"IMPORTANT - CAMPAIGN LEAD CONTEXT: This lead comes from a campaign lead list (e.g. conference attendee list, webinar registration, roadshow invite list). Any references to the lead attending, being registered for, or being associated with an event/conference/webinar should be treated as VERIFIED FACTS. Do NOT penalize the score for event attendance claims - they are confirmed by the campaign source data." if source_type == "campaign" else ""}

OUTREACH MESSAGES TO VALIDATE:
{all_messages}

IMPORTANT: The lead's name, title, and company association are ALREADY VERIFIED in our database. Do NOT mark these as issues unless the outreach message contains DIFFERENT information than what's in our database above.

VALIDATION TASKS - Focus on the MESSAGE CONTENT:
1. **Check Specific Claims in Messages**: Are any specific claims made in the messages (funding amounts, pipeline stages, partnerships, recent news) accurate and current?
2. **Detect Outdated Information**: Has anything changed recently that makes claims IN THE MESSAGE outdated?
3. **Check for Hallucinations**: Are there statements in the message that seem fabricated or unverifiable?
4. **Tone & Appropriateness**: Is the message appropriate for the recipient's seniority and industry?
5. **Factual Accuracy**: If the message mentions specific facts (e.g., "$50M Series B", "Phase 2 trial", "recent partnership with X"), verify these are correct.

{do_not_flag_rules}

RATING SCALE:
- HIGH (90-100): Message content is accurate, safe to send as-is
- MEDIUM (70-89): Minor uncertainties in specific claims, quick review recommended
- LOW (50-69): Specific claims appear incorrect or outdated, manual review required
- CRITICAL (0-49): Major factual errors in the message, do not send without revision

Return ONLY valid JSON:
{{
    "validity_score": 85,
    "validity_rating": "HIGH|MEDIUM|LOW|CRITICAL",
    "issues_found": [
        "Specific issue with message content"
    ],
    "verified_facts": [
        "Specific claim in message confirmed accurate"
    ],
    "uncertain_claims": [
        "Specific claim that could not be verified"
    ],
    "verification_notes": "Summary of what was checked in the message",
    "recommendation": "Specific action for the sales team",
    "suggested_edits": "Any specific edits to the message (or null if none needed)"
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
                'Outreach Validated At': datetime.now().strftime('%Y-%m-%d')
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
                'Outreach Validated At': datetime.now().strftime('%Y-%m-%d')
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
                'Outreach Validated At': datetime.now().strftime('%Y-%m-%d')
            }
            
            self.campaign_leads_table.update(record_id, update_data)
            logger.info(f"  ✓ Updated campaign lead validation: {validation.get('validity_rating')}")
            
        except Exception as e:
            logger.error(f"Error updating campaign lead validation: {e}")
    
    # =========================================================================
    # VALIDATION → REGENERATION LOOP
    # =========================================================================
    
    def _get_campaign_processor(self):
        """Lazy-load the CampaignLeadProcessor for regeneration."""
        if not hasattr(self, '_campaign_processor'):
            try:
                from process_campaign_leads import CampaignLeadProcessor
                self._campaign_processor = CampaignLeadProcessor(config_path=self.config_path)
                logger.info("✓ CampaignLeadProcessor loaded for regeneration")
            except Exception as e:
                logger.error(f"Could not load CampaignLeadProcessor: {e}")
                self._campaign_processor = None
        return self._campaign_processor
    
    def _get_outreach_generator(self):
        """Lazy-load the OutreachGenerator for lead regeneration."""
        if not hasattr(self, '_outreach_generator'):
            try:
                from generate_outreach import OutreachGenerator
                self._outreach_generator = OutreachGenerator(config_path=self.config_path)
                logger.info("✓ OutreachGenerator loaded for regeneration")
            except Exception as e:
                logger.error(f"Could not load OutreachGenerator: {e}")
                self._outreach_generator = None
        return self._outreach_generator
    
    def _get_trigger_outreach_generator(self):
        """Lazy-load the TriggerOutreachGenerator for trigger regeneration."""
        if not hasattr(self, '_trigger_generator'):
            try:
                from generate_trigger_outreach import TriggerOutreachGenerator
                self._trigger_generator = TriggerOutreachGenerator(config_path=self.config_path)
                logger.info("✓ TriggerOutreachGenerator loaded for regeneration")
            except Exception as e:
                logger.error(f"Could not load TriggerOutreachGenerator: {e}")
                self._trigger_generator = None
        return self._trigger_generator
    
    def regenerate_campaign_lead(self, record: Dict, validation: Dict) -> bool:
        """Regenerate a campaign lead's outreach using validation feedback.
        
        Calls the same outreach generator but injects the validation issues
        as additional guidance so the AI avoids the same mistakes.
        
        Args:
            record: Full Airtable record 
            validation: Validation results with issues_found, suggested_edits, etc.
            
        Returns:
            True if regeneration succeeded and new messages were saved
        """
        processor = self._get_campaign_processor()
        if not processor:
            logger.error("Cannot regenerate — CampaignLeadProcessor not available")
            return False
        
        fields = record['fields']
        record_id = record['id']
        name = fields.get('Lead Name', fields.get('Name', 'Unknown'))
        
        try:
            # Get linked data
            lead_record_ids = fields.get('Linked Lead', [])
            company_record_ids = fields.get('Linked Company', [])
            
            lead_data = {}
            company_data = {}
            
            if lead_record_ids:
                lead_data = self.leads_table.get(lead_record_ids[0])['fields']
            if company_record_ids:
                company_data = self.companies_table.get(company_record_ids[0])['fields']
            
            campaign_context = {
                'Campaign Type': fields.get('Campaign Type', 'general'),
                'Conference Name': fields.get('Conference Name', ''),
                'Campaign Background': fields.get('Campaign Background', ''),
                'Campaign Date': fields.get('Campaign Date', ''),
            }
            
            # === INJECT VALIDATION FEEDBACK ===
            # Build a feedback block from the validation results
            issues = validation.get('issues_found', [])
            suggested_edits = validation.get('suggested_edits', '')
            recommendation = validation.get('recommendation', '')
            
            feedback_parts = []
            if issues:
                feedback_parts.append("ISSUES TO FIX:\n" + "\n".join(f"- {i}" for i in issues))
            if suggested_edits:
                feedback_parts.append(f"SUGGESTED EDITS: {suggested_edits}")
            if recommendation:
                feedback_parts.append(f"RECOMMENDATION: {recommendation}")
            
            validation_feedback = "\n".join(feedback_parts)
            
            # Temporarily patch the campaign background to include feedback
            original_background = campaign_context.get('Campaign Background', '')
            campaign_context['Campaign Background'] = (
                f"{original_background}\n\n"
                f"═══ IMPORTANT — PREVIOUS VERSION FAILED VALIDATION (score: {validation.get('validity_score', 0)}/100) ═══\n"
                f"{validation_feedback}\n"
                f"═══ FIX THESE ISSUES IN THE NEW VERSION ═══"
            )
            
            # Generate new messages
            messages = processor.generate_outreach_messages(lead_data, company_data, campaign_context)
            
            if messages:
                # Save new messages
                processor.update_campaign_lead_outreach(record_id, messages)
                
                # Clear the old validation so the new messages get re-validated
                try:
                    self.campaign_leads_table.update(record_id, {
                        'Outreach Validity Rating': '',
                        'Outreach Validity Score': None,
                        'Outreach Validation Notes': f"⟳ Regenerated after score {validation.get('validity_score', 0)}/100. Previous issues: {'; '.join(issues[:3])}",
                        'Outreach Validated At': None,
                    })
                except Exception:
                    # Some fields may not exist or be clearable
                    try:
                        self.campaign_leads_table.update(record_id, {
                            'Outreach Validation Notes': f"⟳ Regenerated after score {validation.get('validity_score', 0)}/100"
                        })
                    except:
                        pass
                
                logger.info(f"  ⟳ Regenerated outreach for {name}")
                return True
            else:
                logger.warning(f"  ⚠ Regeneration produced no messages for {name}")
                return False
                
        except Exception as e:
            logger.error(f"  ✗ Regeneration error for {name}: {e}")
            return False
    
    def validate_and_regenerate_campaign(self, limit: int = None, 
                                          regen_threshold: int = 85,
                                          max_regen_attempts: int = 1):
        """Validate campaign leads and auto-regenerate those below threshold.
        
        Flow:
        1. Validate all unvalidated campaign leads
        2. For any with score < regen_threshold, regenerate using validation feedback
        3. Re-validate the regenerated messages (one pass only to avoid loops)
        
        Args:
            limit: Max campaign leads to process
            regen_threshold: Score below which to auto-regenerate (default 85)
            max_regen_attempts: Max regeneration attempts per lead (default 1)
        """
        if not self.campaign_leads_table:
            logger.error("Campaign Leads table not available")
            return
        
        logger.info("="*70)
        logger.info("CAMPAIGN OUTREACH: VALIDATE → REGENERATE LOOP")
        logger.info(f"Regen threshold: <{regen_threshold}/100")
        logger.info(f"Max regen attempts: {max_regen_attempts}")
        logger.info("="*70)
        
        stats = {
            'validated': 0, 'high': 0, 'medium': 0, 'low': 0, 'critical': 0,
            'regenerated': 0, 'regen_success': 0, 'regen_improved': 0,
            'errors': 0
        }
        
        # === PHASE 1: VALIDATE ===
        logger.info("\n--- PHASE 1: VALIDATION ---")
        campaign_leads = self.get_campaign_leads_needing_validation(limit=limit)
        total = len(campaign_leads)
        logger.info(f"Found {total} campaign leads needing validation")
        
        needs_regen = []  # (record, validation) pairs
        
        for idx, campaign in enumerate(campaign_leads, 1):
            lead_name = campaign['fields'].get('Lead Name', campaign['fields'].get('Name', 'Unknown'))
            company_name = campaign['fields'].get('Company', 'Unknown')
            logger.info(f"\n[{idx}/{total}] {lead_name} @ {company_name}")
            
            try:
                messages = {
                    field: campaign['fields'].get(field, '') 
                    for field in self.CAMPAIGN_OUTREACH_FIELDS
                }
                
                context = {
                    'lead_name': lead_name,
                    'lead_title': campaign['fields'].get('Title', ''),
                    'lead_email': campaign['fields'].get('Email', ''),
                    'lead_linkedin': campaign['fields'].get('LinkedIn URL', ''),
                    'company_name': company_name,
                    'company_data': {
                        'location': campaign['fields'].get('Location', ''),
                        'funding': campaign['fields'].get('Funding', ''),
                        'pipeline_stage': campaign['fields'].get('Pipeline Stage', ''),
                        'therapeutic_areas': campaign['fields'].get('Therapeutic Areas', ''),
                        'intelligence_notes': campaign['fields'].get('Notes', campaign['fields'].get('Processing Notes', ''))
                    },
                    'campaign_context': {
                        'campaign_type': campaign['fields'].get('Campaign Type', ''),
                        'campaign_name': campaign['fields'].get('Campaign Name', campaign['fields'].get('Conference', ''))
                    }
                }
                
                validation = self.validate_outreach_messages(messages, context, source_type="campaign")
                self.update_campaign_lead_validation(campaign['id'], validation)
                
                score = validation.get('validity_score', 0)
                rating = validation.get('validity_rating', 'LOW')
                stats['validated'] += 1
                stats[rating.lower()] = stats.get(rating.lower(), 0) + 1
                
                logger.info(f"  ✓ Score: {score}/100 ({rating})")
                
                if score < regen_threshold:
                    needs_regen.append((campaign, validation))
                    logger.info(f"  → Flagged for regeneration (below {regen_threshold})")
                
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"  ✗ Error: {e}")
                stats['errors'] += 1
        
        # === PHASE 2: REGENERATE ===
        if needs_regen:
            logger.info(f"\n--- PHASE 2: REGENERATION ({len(needs_regen)} leads) ---")
            
            for idx, (campaign, validation) in enumerate(needs_regen, 1):
                lead_name = campaign['fields'].get('Lead Name', campaign['fields'].get('Name', 'Unknown'))
                old_score = validation.get('validity_score', 0)
                logger.info(f"\n[{idx}/{len(needs_regen)}] Regenerating: {lead_name} (was {old_score}/100)")
                
                stats['regenerated'] += 1
                
                if self.regenerate_campaign_lead(campaign, validation):
                    stats['regen_success'] += 1
                    
                    # === PHASE 3: RE-VALIDATE regenerated message ===
                    time.sleep(2)  # Let Airtable settle
                    try:
                        # Fetch fresh record
                        fresh_record = self.campaign_leads_table.get(campaign['id'])
                        fresh_messages = {
                            field: fresh_record['fields'].get(field, '')
                            for field in self.CAMPAIGN_OUTREACH_FIELDS
                        }
                        
                        # Re-validate with same context
                        re_validation = self.validate_outreach_messages(
                            fresh_messages, 
                            {
                                'lead_name': lead_name,
                                'lead_title': campaign['fields'].get('Title', ''),
                                'company_name': campaign['fields'].get('Company', ''),
                                'company_data': {},
                                'campaign_context': {
                                    'campaign_type': campaign['fields'].get('Campaign Type', ''),
                                    'campaign_name': campaign['fields'].get('Campaign Name', campaign['fields'].get('Conference', ''))
                                }
                            },
                            source_type="campaign"
                        )
                        
                        new_score = re_validation.get('validity_score', 0)
                        self.update_campaign_lead_validation(campaign['id'], re_validation)
                        
                        if new_score > old_score:
                            stats['regen_improved'] += 1
                            logger.info(f"  ✓ Improved: {old_score} → {new_score}/100")
                        else:
                            logger.info(f"  → Score: {old_score} → {new_score}/100 (no improvement)")
                        
                    except Exception as e:
                        logger.error(f"  Re-validation error: {e}")
                
                time.sleep(1)
        else:
            logger.info("\n--- No leads need regeneration ---")
        
        # === SUMMARY ===
        logger.info("\n" + "="*70)
        logger.info("VALIDATE → REGENERATE COMPLETE")
        logger.info("="*70)
        logger.info(f"Validated: {stats['validated']}")
        logger.info(f"  HIGH: {stats['high']} | MEDIUM: {stats['medium']} | LOW: {stats['low']} | CRITICAL: {stats['critical']}")
        logger.info(f"Regenerated: {stats['regenerated']} (success: {stats['regen_success']}, improved: {stats['regen_improved']})")
        logger.info(f"Errors: {stats['errors']}")
        logger.info("="*70)
        
        return stats
    
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
                company_name = campaign['fields'].get('Company', 'Unknown')
                logger.info(f"\n[{idx}/{len(campaign_leads)}] Campaign: {lead_name} ({company_name})")
                
                try:
                    messages = {
                        field: campaign['fields'].get(field, '') 
                        for field in self.CAMPAIGN_OUTREACH_FIELDS
                    }
                    
                    # Build context from campaign lead fields
                    context = {
                        'lead_name': campaign['fields'].get('Lead Name', campaign['fields'].get('Name', '')),
                        'lead_title': campaign['fields'].get('Title', ''),
                        'lead_email': campaign['fields'].get('Email', ''),
                        'lead_linkedin': campaign['fields'].get('LinkedIn URL', ''),
                        'company_name': campaign['fields'].get('Company', ''),
                        'company_data': {
                            'location': campaign['fields'].get('Location', ''),
                            'funding': campaign['fields'].get('Funding', ''),
                            'pipeline_stage': campaign['fields'].get('Pipeline Stage', ''),
                            'therapeutic_areas': campaign['fields'].get('Therapeutic Areas', ''),
                            'intelligence_notes': campaign['fields'].get('Notes', '')
                        },
                        'campaign_context': {
                            'campaign_type': campaign['fields'].get('Campaign Type', ''),
                            'campaign_name': campaign['fields'].get('Campaign Name', campaign['fields'].get('Conference', ''))
                        }
                    }
                    
                    validation = self.validate_outreach_messages(messages, context, source_type="campaign")
                    self.update_campaign_lead_validation(campaign['id'], validation)
                    
                    # Track stats
                    stats['campaign_processed'] += 1
                    rating = validation.get('validity_rating', 'LOW')
                    stats[f'campaign_{rating.lower()}'] = stats.get(f'campaign_{rating.lower()}', 0) + 1
                    
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
            logger.info(f"  - HIGH: {stats.get('campaign_high', 0)}")
            logger.info(f"  - MEDIUM: {stats.get('campaign_medium', 0)}")
            logger.info(f"  - LOW: {stats.get('campaign_low', 0)}")
            logger.info(f"  - CRITICAL: {stats.get('campaign_critical', 0)}")
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
    
    def validate_campaign_leads_only(self, limit: int = None):
        """Validate only campaign leads outreach messages"""
        if not self.campaign_leads_table:
            logger.error("Campaign Leads table not available")
            return
        
        logger.info("="*70)
        logger.info("CAMPAIGN LEADS OUTREACH VALIDATION")
        logger.info("="*70)
        
        stats = {
            'processed': 0,
            'high': 0,
            'medium': 0,
            'low': 0,
            'critical': 0,
            'errors': 0
        }
        
        campaign_leads = self.get_campaign_leads_needing_validation(limit=limit)
        total = len(campaign_leads)
        
        logger.info(f"Found {total} campaign leads needing validation")
        
        for idx, campaign in enumerate(campaign_leads, 1):
            lead_name = campaign['fields'].get('Lead Name', campaign['fields'].get('Name', 'Unknown'))
            company_name = campaign['fields'].get('Company', 'Unknown')
            logger.info(f"\n[{idx}/{total}] {lead_name} @ {company_name}")
            
            try:
                messages = {
                    field: campaign['fields'].get(field, '') 
                    for field in self.CAMPAIGN_OUTREACH_FIELDS
                }
                
                # Build context from campaign lead fields
                context = {
                    'lead_name': lead_name,
                    'lead_title': campaign['fields'].get('Title', ''),
                    'lead_email': campaign['fields'].get('Email', ''),
                    'lead_linkedin': campaign['fields'].get('LinkedIn URL', ''),
                    'company_name': company_name,
                    'company_data': {
                        'location': campaign['fields'].get('Location', ''),
                        'funding': campaign['fields'].get('Funding', ''),
                        'pipeline_stage': campaign['fields'].get('Pipeline Stage', ''),
                        'therapeutic_areas': campaign['fields'].get('Therapeutic Areas', ''),
                        'intelligence_notes': campaign['fields'].get('Notes', campaign['fields'].get('Processing Notes', ''))
                    },
                    'campaign_context': {
                        'campaign_type': campaign['fields'].get('Campaign Type', ''),
                        'campaign_name': campaign['fields'].get('Campaign Name', campaign['fields'].get('Conference', ''))
                    }
                }
                
                validation = self.validate_outreach_messages(messages, context, source_type="campaign")
                self.update_campaign_lead_validation(campaign['id'], validation)
                
                stats['processed'] += 1
                rating = validation.get('validity_rating', 'LOW').lower()
                stats[rating] = stats.get(rating, 0) + 1
                
                logger.info(f"  ✓ Rating: {validation.get('validity_rating')} ({validation.get('validity_score')}/100)")
                
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"  ✗ Error: {e}")
                stats['errors'] += 1
        
        # Summary
        logger.info("\n" + "="*70)
        logger.info("CAMPAIGN VALIDATION COMPLETE")
        logger.info("="*70)
        logger.info(f"Total processed: {stats['processed']}")
        logger.info(f"  HIGH: {stats['high']}")
        logger.info(f"  MEDIUM: {stats['medium']}")
        logger.info(f"  LOW: {stats['low']}")
        logger.info(f"  CRITICAL: {stats['critical']}")
        logger.info(f"Errors: {stats['errors']}")
        
        return stats


def main():
    parser = argparse.ArgumentParser(description='Validate outreach messages')
    parser.add_argument('--all', action='store_true', help='Validate all pending messages')
    parser.add_argument('--leads-only', action='store_true', help='Only validate leads')
    parser.add_argument('--triggers-only', action='store_true', help='Only validate triggers')
    parser.add_argument('--campaign-only', action='store_true', help='Only validate campaign leads')
    parser.add_argument('--campaign-regen', action='store_true', 
                        help='Validate campaign leads and auto-regenerate those below threshold')
    parser.add_argument('--regen-threshold', type=int, default=85,
                        help='Score below which to auto-regenerate (default: 85)')
    parser.add_argument('--lead-id', type=str, help='Validate specific lead by ID')
    parser.add_argument('--trigger-id', type=str, help='Validate specific trigger by ID')
    parser.add_argument('--limit', type=int, default=None, help='Max records per table (default: no limit)')
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
    
    elif args.campaign_regen:
        # NEW: Validate + auto-regenerate below threshold
        validator.validate_and_regenerate_campaign(
            limit=args.limit,
            regen_threshold=args.regen_threshold
        )
    
    elif args.campaign_only:
        # Only validate campaign leads (no regeneration)
        validator.validate_campaign_leads_only(limit=args.limit)
    
    else:
        validator.validate_all_pending(limit_per_table=args.limit)


if __name__ == "__main__":
    main()
