#!/usr/bin/env python3
"""
Trigger-Based Outreach Generator
Generates personalized outreach messages based on trigger events (conference attendance, 
funding, promotions, etc.) using lead profiles and company context.
"""

import os
import sys
import yaml
import json
import re
import logging
from datetime import datetime
from typing import Dict, List, Optional
import anthropic
from pyairtable import Api
from company_profile_utils import (build_outreach_philosophy, build_value_proposition,
                                   load_company_profile, load_persona_messaging,
                                   inline_quality_check, validate_and_retry,
                                   full_validate_outreach, generate_validate_loop,
                                   validation_fields_for_airtable, classify_persona)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('trigger_outreach.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class TriggerOutreachGenerator:
    """Generate personalized outreach messages for trigger events"""
    
    def __init__(self, config_path: str = "config.yaml"):
        """Initialize with configuration"""
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        # Initialize APIs
        self.airtable = Api(self.config['airtable']['api_key'])
        self.base = self.airtable.base(self.config['airtable']['base_id'])
        self.trigger_history_table = self.base.table('Trigger History')
        self.leads_table = self.base.table(self.config['airtable']['tables']['leads'])
        self.companies_table = self.base.table(self.config['airtable']['tables']['companies'])
        
        # Try to access Company Profile table
        try:
            self.company_profile_table = self.base.table('Company Profile')
            self.company_profile = self.load_company_profile()
            logger.info("Company Profile loaded - outreach will be strategically aligned")
        except:
            self.company_profile_table = None
            self.company_profile = None
            logger.info("Note: Company Profile table not found - using generic messaging")
        
        # Load persona messaging for persona-driven proof point selection
        self.persona_messaging = load_persona_messaging(self.base)
        
        self.anthropic_client = anthropic.Anthropic(
            api_key=self.config['anthropic']['api_key']
        )
        
        logger.info("TriggerOutreachGenerator initialized successfully")
    
    def load_company_profile(self) -> Optional[Dict]:
        """Load company profile data from Airtable"""
        try:
            records = self.company_profile_table.all()
            if records:
                return records[0]['fields']
            return None
        except Exception as e:
            logger.warning(f"Could not load company profile: {str(e)}")
            return None
    
    def get_triggers_for_outreach(self, limit: Optional[int] = None) -> List[Dict]:
        """Get triggers that need outreach messages generated"""
        
        # Find triggers with Status = "New"
        # We'll check for missing outreach in Python since field names may vary
        formula = "{Status} = 'New'"
        all_triggers = self.trigger_history_table.all(formula=formula)
        
        # Filter to only those without outreach generated
        triggers = []
        for trigger in all_triggers:
            fields = trigger['fields']
            # Check if outreach already generated (try multiple possible field names)
            has_outreach = (
                fields.get('Email Body') or 
                fields.get('Email body') or
                fields.get('Outreach Messages') or
                fields.get('Outreach Generated Date')
            )
            if not has_outreach:
                triggers.append(trigger)
        
        if limit:
            triggers = triggers[:limit]
        
        logger.info(f"Found {len(triggers)} triggers needing outreach (out of {len(all_triggers)} with Status=New)")
        return triggers
    
    def get_lead_context(self, lead_ids: list) -> Dict:
        """Get comprehensive lead information"""
        if not lead_ids:
            return {}
        
        try:
            lead = self.leads_table.get(lead_ids[0])
            fields = lead['fields']
            
            context = {
                'name': fields.get('Lead Name', ''),
                'title': fields.get('Title', ''),
                'email': fields.get('Email', ''),
                'linkedin': fields.get('LinkedIn URL', ''),
                'company_name': '',
                'company_id': None,
                'deep_profile': fields.get('Intelligence Notes', ''),
                'activity_log': fields.get('Activity Log', ''),
                'lead_icp': fields.get('Lead ICP Fit Score', 0)
            }
            
            # Get company info
            if 'Company' in fields:
                context['company_id'] = fields['Company'][0]
                try:
                    company = self.companies_table.get(fields['Company'][0])
                    company_fields = company['fields']
                    context['company_name'] = company_fields.get('Company Name', '')
                    context['company_focus'] = company_fields.get('Focus Area', [])
                    context['company_stage'] = company_fields.get('Pipeline Stage', [])
                    context['company_funding'] = company_fields.get('Funding Stage', '')
                    context['company_technology'] = company_fields.get('Technology Platform', [])
                    context['company_icp'] = company_fields.get('ICP Fit Score', 0)
                except:
                    pass
            
            return context
        except Exception as e:
            logger.warning(f"Could not get lead context: {str(e)}")
            return {}
    
    def generate_trigger_outreach(self, trigger_data: Dict, lead_context: Dict) -> Dict:
        """Generate personalized outreach messages for a trigger event"""
        
        trigger_type = trigger_data.get('Trigger Type', 'OTHER')
        description = trigger_data.get('Description', '')
        outreach_angle = trigger_data.get('Outreach Angle', '')
        timing = trigger_data.get('Timing Recommendation', '')
        urgency = trigger_data.get('Urgency', 'MEDIUM')
        conference_name = trigger_data.get('Conference Name', '')
        event_date = trigger_data.get('Event Date', '')
        sources = trigger_data.get('Sources', '')
        
        # Build trigger context
        trigger_context = f"""
TRIGGER EVENT DETAILS:
Type: {trigger_type}
Urgency: {urgency}
Description: {description}
"""
        
        if conference_name:
            trigger_context += f"Conference: {conference_name}\n"
        if event_date:
            trigger_context += f"Event Date: {event_date}\n"
        if outreach_angle:
            trigger_context += f"Suggested Angle: {outreach_angle}\n"
        if timing:
            trigger_context += f"Timing: {timing}\n"
        
        # Build lead context
        lead_info = f"""
LEAD INFORMATION:
Name: {lead_context.get('name', 'Unknown')}
Title: {lead_context.get('title', 'Unknown')}
Company: {lead_context.get('company_name', 'Unknown')}
"""
        
        # Add company details if available
        if lead_context.get('company_focus'):
            lead_info += f"Company Focus: {', '.join(lead_context.get('company_focus', []))}\n"
        if lead_context.get('company_stage'):
            lead_info += f"Pipeline Stage: {', '.join(lead_context.get('company_stage', []))}\n"
        if lead_context.get('company_technology'):
            lead_info += f"Technology: {', '.join(lead_context.get('company_technology', []))}\n"
        
        # Add deep profile if available
        profile_section = ""
        if lead_context.get('deep_profile'):
            profile_section = f"""
LEAD BACKGROUND (from research):
{lead_context['deep_profile'][:1500]}
"""
        
        # Add recent activity if available
        activity_section = ""
        if lead_context.get('activity_log'):
            activity_parts = lead_context['activity_log'].split("ACTIVITY SURVEILLANCE REPORT")
            if len(activity_parts) > 1:
                activity_section = f"""
RECENT ACTIVITY:
{activity_parts[-1][:1000]}
"""
        
        # Add OUR company profile — uses shared value proposition builder
        # that pulls from Company Profile table and matches to persona
        lead_title = lead_context.get('title', '')
        company_data = lead_context.get('company_context', {})
        our_profile_section = build_value_proposition(
            self.company_profile or {},
            company_data,
            lead_title,
            persona_messaging=self.persona_messaging
        )
        
        # Build the main prompt
        prompt = f"""You are an expert business development writer creating personalized outreach for a SPECIFIC TRIGGER EVENT.

{trigger_context}
{lead_info}
{our_profile_section}
{profile_section}
{activity_section}

{build_outreach_philosophy()}

YOUR MISSION: Create outreach that uses the TRIGGER EVENT as the reason for reaching out, then answers "Why should I take this meeting?" from THEIR perspective.

CRITICAL RULES FOR TRIGGER-BASED OUTREACH:

1. Lead with the trigger. The event/news is WHY you're reaching out
2. THEN connect ONE Rezon strength to THEIR specific situation arising from the trigger
3. Show you know them. Reference their background naturally, don't explain their situation to them
4. Include ONE proof point woven naturally (multinational pharma track record, regulatory credentials, technical capability)
5. Be human, not AI-polished. Natural language, no excessive formatting
6. No bullet point lists. Weave key points into natural sentences
7. Soft CTA. Suggest a conversation, don't push a meeting
8. ABSOLUTELY NO em-dashes (—). Use commas, periods, or "and" instead. Zero tolerance.
9. No ** for emphasis. Clean, simple text only
10. Write full sentences with "I" as subject. Don't drop pronouns to sound punchy.
    BAD: "Saw you're speaking at..."  GOOD: "I noticed you're speaking at..."
    BAD: "Heading to BioEurope and..."  GOOD: "I'll be at BioEurope and..."

TRIGGER-SPECIFIC GUIDANCE:
"""
        
        # Add trigger-specific guidance
        if trigger_type == 'CONFERENCE_ATTENDANCE':
            prompt += f"""
This person is attending/speaking at {conference_name or 'a conference'}.
- Reference the conference naturally
- If they're speaking, mention their topic/expertise
- Suggest meeting at the event or connecting beforehand
- Don't be generic about "the conference" - be specific
"""
        elif trigger_type == 'FUNDING':
            prompt += """
This company just raised funding.
- Congratulate genuinely (not over-the-top)
- Reference what the funding enables (scale-up, expansion)
- Connect to manufacturing needs naturally
- Don't immediately pitch - acknowledge their milestone
"""
        elif trigger_type in ['PROMOTION', 'JOB_CHANGE']:
            prompt += """
This person has a new role/promotion.
- Congratulate on the new position
- Reference the expanded responsibilities
- Connect to how you might be relevant in their new role
- Give them time to settle - soft approach
"""
        elif trigger_type == 'PIPELINE':
            prompt += """
This company has a pipeline milestone (phase advancement, approval, etc.).
- Acknowledge the achievement
- Reference what comes next (scale-up, commercial manufacturing)
- Connect to their specific modality/technology
"""
        elif trigger_type == 'SPEAKING':
            prompt += """
This person is speaking at an event.
- Reference their talk topic specifically
- Show genuine interest in their expertise
- Suggest connecting at the event or after
"""
        elif trigger_type == 'PAIN_POINT':
            prompt += """
This person discussed a challenge/pain point.
- Reference what they said (don't quote directly)
- Show empathy for the challenge
- Offer perspective without being salesy
"""
        elif trigger_type == 'ROADSHOW':
            prompt += """
You're visiting their region/city on a roadshow.
- Mention you'll be in their area for meetings
- Suggest meeting while you're there - convenient timing
- Frame it as an opportunity to connect in person
- Keep it low-pressure and natural
- Don't make it sound like a mass outreach
"""
        
        prompt += f"""

Generate THREE versions:

MESSAGE 1: EMAIL (Trigger-Focused, Conversational)
Subject line: [Reference the trigger naturally - their news, their conference, their achievement]

[100-130 word email body]

Requirements:
- OPEN with the trigger event (their conference, their funding, their promotion)
- Show genuine interest/congratulations
- One natural transition to why connecting makes sense
- Soft CTA - "would love to hear more" or "happy to share thoughts"
- Sign as: "Best regards, [Your Name], Rezon Bio Business Development"

MESSAGE 2: LINKEDIN CONNECTION REQUEST (Under 200 characters)
[Very brief, reference the trigger, reason for connecting]
- No signature needed
- Must be under 200 characters
- Natural and friendly

MESSAGE 3: LINKEDIN MESSAGE (After Connection - 250-350 chars)
[For after they accept the connection]
- Thank them for connecting
- Brief reference to trigger
- Suggest value of conversation
- End with: "Best regards, [Your Name], Rezon Bio BD"

EXAMPLES OF GOOD VS BAD:

BAD (Generic CDMO pitch):
"I noticed you're attending BioEurope. As a CDMO, we'd love to meet to discuss your manufacturing needs."

GOOD (Their situation + Rezon's specific relevance):
"I noticed you're speaking on ADC development at BioEurope. Your work on site-specific conjugation at commercial scale is exactly where our pharma-validated facilities add value. Would love to grab a coffee at the event."

BAD (Features list after trigger):
"Congratulations on the Series C! Rezon Bio offers cost-effective manufacturing solutions that could help you scale."

GOOD (Trigger + why meeting helps THEM):
"Great news on the Series C, exciting to see Tubulis advancing the ADC platform. As you plan the next manufacturing stage, our EU-based setup can cut costs significantly vs US CDMOs while staying EMA/FDA-aligned. Happy to share specifics."

Return as JSON:
{{
  "email_subject": "Subject referencing trigger",
  "email_body": "Full email with signature using [Your Name] placeholder",
  "linkedin_connection_request": "Under 200 chars, no signature",
  "linkedin_short": "250-350 chars ending with 'Best regards, [Your Name], Rezon Bio BD'",
  "personalization_hooks": [
    "Trigger event referenced",
    "Background detail used",
    "Specific expertise mentioned"
  ],
  "best_time_to_send": "Timing recommendation based on trigger",
  "follow_up_angle": "If no response, try this angle"
}}

Only return valid JSON, no other text."""

        try:
            logger.info(f"  Generating outreach for {trigger_type} trigger...")
            
            message = self.anthropic_client.messages.create(
                model=self.config['anthropic']['model'],
                max_tokens=2500,
                messages=[{
                    "role": "user",
                    "content": prompt
                }]
            )
            
            # Extract text content
            result_text = ""
            for block in message.content:
                if block.type == "text":
                    result_text += block.text
            
            # Parse JSON with robust cleaning
            result_text = result_text.strip()
            
            # Remove markdown code blocks
            if result_text.startswith("```json"):
                result_text = result_text[7:]
            elif result_text.startswith("```"):
                result_text = result_text[3:]
            
            if result_text.endswith("```"):
                result_text = result_text[:-3]
            
            result_text = result_text.strip()
            
            # Find JSON if there's preamble
            if not result_text.startswith("{"):
                start_idx = result_text.find("{")
                end_idx = result_text.rfind("}")
                if start_idx != -1 and end_idx != -1:
                    result_text = result_text[start_idx:end_idx+1]
            
            # Remove trailing commas
            result_text = re.sub(r',(\s*[}\]])', r'\1', result_text)
            
            messages_data = json.loads(result_text)
            
            # Full validation loop: structural + web search + auto-regen
            lead_title = lead_context.get('title', '')
            lead_name = lead_context.get('name', '')
            company_name = lead_context.get('company_name', '')
            persona = classify_persona(lead_title)
            
            def gen_fn(feedback=None):
                regen_prompt = prompt + (f"\n\n{feedback}" if feedback else "")
                msg = self.anthropic_client.messages.create(
                    model=self.config['anthropic']['model'], max_tokens=2000,
                    messages=[{"role": "user", "content": regen_prompt}]
                )
                rt = "".join(b.text for b in msg.content if hasattr(b, 'text')).strip()
                if not rt.startswith("{"):
                    s = rt.find("{"); e = rt.rfind("}")
                    if s != -1: rt = rt[s:e+1]
                rt = re.sub(r',(\s*[}\]])', r'\1', rt)
                return json.loads(rt)
            
            val_context = {
                'lead_name': lead_name, 'lead_title': lead_title,
                'company_name': company_name,
                'company_data': lead_context.get('company_data', {}),
                'trigger_type': lead_context.get('trigger_type', ''),
                'trigger_description': lead_context.get('trigger_description', ''),
            }
            
            messages_data, quality = generate_validate_loop(
                self.anthropic_client, self.config['anthropic']['model'],
                gen_fn, val_context, persona=persona, source_type='trigger',
                pre_generated=messages_data
            )
            
            vs = quality.get('validation_score', 0)
            vr = quality.get('validation_rating', '?')
            logger.info(f"  Messages generated (validation: {vs}/100 {vr})")
            messages_data['_validation'] = quality
            return messages_data
            
        except json.JSONDecodeError as e:
            logger.error(f"  JSON parsing failed: {str(e)}")
            logger.error(f"  First 300 chars: {result_text[:300]}")
            return None
        except Exception as e:
            logger.error(f"  Error generating messages: {str(e)}")
            return None
    
    def update_trigger_with_outreach(self, trigger_record_id: str, messages_data: Dict):
        """Update trigger record with generated outreach in separate columns"""
        
        if not messages_data:
            return False
        
        try:
            update_fields = {
                'Email Subject': messages_data.get('email_subject', ''),
                'Email Body': messages_data.get('email_body', ''),
                'LinkedIn Connection Request': messages_data.get('linkedin_connection_request', ''),
                'LinkedIn Short Message': messages_data.get('linkedin_short', ''),
                'Outreach Generated Date': datetime.now().strftime('%Y-%m-%d'),
                'Best Time to Send': messages_data.get('best_time_to_send', ''),
                'Follow Up Angle': messages_data.get('follow_up_angle', '')
            }
            
            # Add validation fields if available
            quality = messages_data.get('_validation')
            if quality:
                update_fields.update(validation_fields_for_airtable(quality))
            
            self.trigger_history_table.update(trigger_record_id, update_fields)
            logger.info(f"  Saved to Trigger History")
            return True
        except Exception as e:
            logger.error(f"  Failed to save: {str(e)}")
            return False
    
    def generate_all_trigger_outreach(self, limit: Optional[int] = None):
        """Generate outreach for all triggers needing messages"""
        
        logger.info("=" * 60)
        logger.info("TRIGGER-BASED OUTREACH GENERATION")
        logger.info("=" * 60)
        
        triggers = self.get_triggers_for_outreach(limit)
        
        if not triggers:
            logger.info("No triggers need outreach generation")
            return True
        
        total = len(triggers)
        success_count = 0
        failed_count = 0
        skipped_no_lead = 0  # Track orphan triggers separately
        
        for idx, trigger in enumerate(triggers, 1):
            record_id = trigger['id']
            fields = trigger['fields']
            
            # Get lead info
            lead_ids = fields.get('Lead', [])
            if not lead_ids:
                logger.warning(f"[{idx}/{total}] Trigger has no lead - skipping (run cleanup_orphan_triggers.py to fix)")
                skipped_no_lead += 1
                continue
            
            lead_context = self.get_lead_context(lead_ids)
            lead_name = lead_context.get('name', 'Unknown')
            company_name = lead_context.get('company_name', 'Unknown')
            trigger_type = fields.get('Trigger Type', 'OTHER')
            conference_name = fields.get('Conference Name', '')
            
            logger.info("")
            logger.info(f"[{idx}/{total}] {lead_name} at {company_name}")
            logger.info(f"  Trigger: {trigger_type}" + (f" - {conference_name}" if conference_name else ""))
            
            try:
                # Generate outreach
                messages_data = self.generate_trigger_outreach(fields, lead_context)
                
                if not messages_data:
                    logger.error(f"  Generation failed")
                    failed_count += 1
                    continue
                
                # Validation already done inside generate_trigger_outreach via generate_validate_loop
                
                # Save to trigger record
                if self.update_trigger_with_outreach(record_id, messages_data):
                    success_count += 1
                    logger.info(f"  Complete!")
                else:
                    failed_count += 1
                
                # Rate limiting
                import time
                time.sleep(2)
                
            except Exception as e:
                logger.error(f"  Error: {str(e)}")
                failed_count += 1
        
        logger.info("")
        logger.info("=" * 60)
        logger.info("TRIGGER OUTREACH GENERATION COMPLETE")
        logger.info(f"Total: {total} | Success: {success_count} | Failed: {failed_count} | Skipped (no lead): {skipped_no_lead}")
        if skipped_no_lead > 0:
            logger.info(f"TIP: Run 'python cleanup_orphan_triggers.py --link' to fix orphan triggers")
        logger.info("=" * 60)
        
        # Only count actual failures, not skipped orphans
        return failed_count == 0


def main():
    """CLI entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Generate outreach for trigger events')
    parser.add_argument('--limit', type=int, help='Max number of triggers to process')
    parser.add_argument('--config', default='config.yaml', help='Path to config file')
    
    args = parser.parse_args()
    
    try:
        generator = TriggerOutreachGenerator(config_path=args.config)
        success = generator.generate_all_trigger_outreach(limit=args.limit)
        sys.exit(0 if success else 1)
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
