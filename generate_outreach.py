#!/usr/bin/env python3
"""
Outreach Message Generator - Creates personalized outreach messages
Uses deep profile data, surveillance intelligence, and trigger events
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
                                   full_validate_outreach, generate_validate_loop)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('outreach.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class OutreachGenerator:
    """Generate personalized outreach messages for leads"""
    
    def __init__(self, config_path: str = "config.yaml"):
        """Initialize with configuration"""
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        # Initialize APIs
        self.airtable = Api(self.config['airtable']['api_key'])
        self.base = self.airtable.base(self.config['airtable']['base_id'])
        self.leads_table = self.base.table(self.config['airtable']['tables']['leads'])
        self.companies_table = self.base.table(self.config['airtable']['tables']['companies'])
        
        # Try to access Company Profile table (optional but highly recommended)
        try:
            self.company_profile_table = self.base.table('Company Profile')
            self.company_profile = self.load_company_profile()
            logger.info("✓ Company Profile table found - outreach will be strategically aligned")
        except:
            self.company_profile_table = None
            self.company_profile = None
            logger.info("Note: Company Profile table not found - using generic messaging")
        
        # Load persona messaging for persona-driven proof point selection
        self.persona_messaging = load_persona_messaging(self.base)
        
        self.anthropic_client = anthropic.Anthropic(
            api_key=self.config['anthropic']['api_key']
        )
        
        logger.info("OutreachGenerator initialized successfully")
    
    def load_company_profile(self) -> Optional[Dict]:
        """Load company profile data from Airtable"""
        try:
            records = self.company_profile_table.all()
            if records:
                return records[0]['fields']  # Single record table
            return None
        except Exception as e:
            logger.warning(f"Could not load company profile: {str(e)}")
            return None
    
    def get_leads_for_outreach(self, limit: Optional[int] = None) -> List[Dict]:
        """Get all leads marked for outreach message generation"""
        
        # Get leads marked for message generation
        formula = "{Generate Messages} = TRUE()"
        leads = self.leads_table.all(formula=formula)
        
        if limit:
            leads = leads[:limit]
        
        logger.info(f"Found {len(leads)} leads marked for outreach")
        return leads
    
    def get_company_context(self, company_ids: list) -> Optional[Dict]:
        """Get company information for context"""
        if not company_ids:
            return None
        
        try:
            company = self.companies_table.get(company_ids[0])
            return company['fields']
        except:
            return None
    
    def generate_outreach_messages(self, lead_name: str, title: str = "", 
                                   company_name: str = "", deep_profile: str = "",
                                   activity_log: str = "", trigger_events: str = "",
                                   outreach_reason: str = "General Introduction",
                                   company_context: Dict = None) -> Dict:
        """Generate personalized outreach messages using AI"""
        
        context_info = f"""
Lead: {lead_name}
Title: {title or 'Unknown'}
Company: {company_name or 'Unknown'}
Outreach Reason: {outreach_reason}
"""
        
        if company_context:
            context_info += f"""
Company Details:
- Focus: {', '.join(company_context.get('Focus Area', []))}
- Stage: {', '.join(company_context.get('Pipeline Stage', []))}
- Funding: {company_context.get('Funding Stage', 'Unknown')}
- Technology: {', '.join(company_context.get('Technology Platform', []))}
"""
        
        # Add deep profile intelligence
        profile_section = ""
        if deep_profile:
            profile_section = f"""
DEEP PROFILE INTELLIGENCE:
{deep_profile[:2000]}  # Truncate if too long
"""
        
        # Add recent activity
        activity_section = ""
        if activity_log:
            # Get most recent activity report
            activity_parts = activity_log.split("ACTIVITY SURVEILLANCE REPORT")
            if len(activity_parts) > 1:
                activity_section = f"""
RECENT ACTIVITY (Last 2 Weeks):
{activity_parts[-1][:1500]}  # Most recent report, truncated
"""
        
        # Add trigger events
        trigger_section = ""
        if trigger_events:
            trigger_section = f"""
ACTIVE TRIGGER EVENTS:
{trigger_events}
"""
        
        # Add OUR company profile — uses shared value proposition builder
        # that pulls from Company Profile table and matches to persona
        our_profile_section = build_value_proposition(
            self.company_profile or {}, 
            company_context or {},
            title,
            persona_messaging=self.persona_messaging
        )
        
        outreach_prompt = f"""You are an expert business development writer specializing in personalized, authentic outreach to pharma/biotech executives.

{context_info}
{our_profile_section}
{profile_section}
{activity_section}
{trigger_section}

{build_outreach_philosophy()}

OUTREACH REASON CONTEXT:
{outreach_reason}

YOUR MISSION: Create THREE outreach messages that answer "Why should I take this meeting?" from THEIR perspective.

CRITICAL RULES FOR AUTHENTIC OUTREACH:
1. Lead with THEIR world — start with an observation about their work, stage, or challenge
2. Connect ONE Rezon strength to THEIR specific situation — don't list capabilities
3. Include ONE proof point woven naturally (Sandoz qualification, biosimilar track record, regulatory credentials)
4. Be human, not AI-polished — natural language, occasional imperfection is fine
5. Keep it about THEM, not about us — never say "you're our primary focus"
6. Avoid bullet point lists — weave key points into natural sentences
7. No aggressive "here's what you need" — suggest, don't prescribe
8. Conversational length — Email 120-150 words MAX, LinkedIn Long 250-350 words MAX
9. No ** or excessive dashes — clean, simple formatting only
10. Reference their background naturally — mention past companies, education, expertise without being creepy

Generate THREE versions:

═══════════════════════════════════════════════════════════
MESSAGE 1: EMAIL (Conversational & Direct)
═══════════════════════════════════════════════════════════
Subject line: [Specific, natural - reference their work or trigger]

[120-150 word email body - conversational, not polished]

Requirements:
- Start with what THEY'RE doing (their post, their talk, their news)
- One or two key points woven naturally into sentences (no bullet lists)
- Why it might be relevant to connect (not "here's what you need")
- Soft CTA - natural conversation starter
- Sound like you're writing to a colleague, not a prospect
- Reference to being in their geography/at their event
- Sign as: "Best regards, [Your Name], Rezon Bio Business Development"

AVOID:
✗ "As a [their role] at a [company type], you're likely..."
✗ Bullet point lists of benefits
✗ "You're our primary focus"
✗ Explaining their situation back to them
✗ ** for emphasis or excessive formatting

═══════════════════════════════════════════════════════════
MESSAGE 2A: LINKEDIN CONNECTION REQUEST (Very Brief)
═══════════════════════════════════════════════════════════
[200 characters max - for initial connection request]

Requirements:
- Reference shared interest or their recent activity
- Reason for connecting
- Natural and brief
- No signature needed

═══════════════════════════════════════════════════════════
MESSAGE 2B: LINKEDIN MESSAGE (After Connected)
═══════════════════════════════════════════════════════════
[300-400 characters - for follow-up after connection accepted]

Requirements:
- Conversational opener
- Reference their background or recent post
- Suggest why connecting makes sense
- Soft CTA (question or suggestion to chat)
- Friendly tone
- End with: "Best regards, [Your Name], Rezon Bio BD"

═══════════════════════════════════════════════════════════
MESSAGE 3: LINKEDIN INMAIL (Longer but Still Human)
═══════════════════════════════════════════════════════════
Subject: [Natural, not salesy]

[250-350 words - conversational, not essay-like]

Requirements:
- Open with observation about their work/industry
- Reference 2-3 specific things about them naturally (background, posts, expertise)
- Share relevant perspective or insight (not sales pitch)
- Sound like industry peer having a conversation
- Weave in why Rezon might be relevant without listing benefits
- Natural next step suggestion
- NO bullet lists - write in paragraphs
- Sign as: "Best regards, [Your Name], Rezon Bio Business Development"

AVOID:
✗ Essay structure with sections
✗ Lists of benefits or features  
✗ "Here's what we offer:" followed by bullets
✗ Overly polished, marketing copy tone
✗ More than 350 words (too long)

═══════════════════════════════════════════════════════════

TONE EXAMPLES:

GOOD - Answers "why should I take this meeting?":
"I saw your post about CMC partner challenges. The 'too small for big CDMOs, too risky for unproven ones' dilemma is real — it's exactly the gap we built Rezon Bio to fill. Worth comparing notes? I'm in Boston next month."

BAD - Generic CDMO pitch:
"We are a leading European CDMO offering state-of-the-art mammalian cell culture manufacturing. We'd love to discuss our capabilities."

GOOD - Connects Rezon strength to their situation:
"Scaling a bispecific from Phase 2 into commercial manufacturing is where timelines get tight. Our Sandoz-qualified facilities were built for exactly this transition — happy to share how we've handled similar programs."

BAD - Lists features:
"As Head of CMC at a mid-size biotech, you're likely facing several challenges:
• Capacity constraints at major CDMOs
• Cost pressure from Series C deployment
• Regulatory compliance requirements"

GOOD - Shows knowledge without telling:
"Your work scaling bispecific-ADCs from Sanofi to BIOMUNEX is an interesting progression - those conjugation challenges at commercial scale are no joke."

BAD - Tells them their situation:
"As a French biotech with 150 employees in Phase 2/3, you're at the inflection point where manufacturing strategy becomes critical."

Return messages in this exact JSON format:
{{
  "email_subject": "Subject line here",
  "email_body": "Email body with signature using [Your Name] placeholder",
  "linkedin_connection_request": "Connection request (200 chars max, no signature)",
  "linkedin_short": "Short message ending with 'Best regards, [Your Name], Rezon Bio BD'",
  "linkedin_inmail_subject": "InMail subject line",
  "linkedin_inmail_body": "InMail body ending with 'Best regards, [Your Name], Rezon Bio Business Development'",
  "personalization_notes": [
    "Background detail used",
    "Recent activity referenced",
    "Expertise area mentioned"
  ],
  "best_time_to_send": "Timing recommendation",
  "alternative_angles": [
    "If no response, try this angle",
    "Or this alternative approach"
  ]
}}

Only return valid JSON, no other text."""

        try:
            logger.info(f"  Generating outreach messages...")
            
            # Use Claude to generate messages
            message = self.anthropic_client.messages.create(
                model=self.config['anthropic']['model'],
                max_tokens=self.config['anthropic']['max_tokens'],
                messages=[{
                    "role": "user",
                    "content": outreach_prompt
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
            
            # Try to find JSON if there's preamble text
            if not result_text.startswith("{"):
                start_idx = result_text.find("{")
                end_idx = result_text.rfind("}")
                if start_idx != -1 and end_idx != -1:
                    result_text = result_text[start_idx:end_idx+1]
            
            # Remove trailing commas
            result_text = re.sub(r',(\s*[}\]])', r'\1', result_text)
            
            try:
                messages_data = json.loads(result_text)
                
                # Full validation loop: structural + web search + auto-regen
                persona = classify_persona(title)
                
                def gen_fn(feedback=None):
                    regen_prompt = outreach_prompt + (f"\n\n{feedback}" if feedback else "")
                    msg = self.anthropic_client.messages.create(
                        model=self.config['anthropic']['model'], max_tokens=2000,
                        messages=[{"role": "user", "content": regen_prompt}]
                    )
                    rt = "".join(b.text for b in msg.content if hasattr(b, 'text')).strip()
                    if rt.startswith("```json"): rt = rt[7:]
                    if rt.startswith("```"): rt = rt[3:]
                    if rt.endswith("```"): rt = rt[:-3]
                    rt = rt.strip()
                    if not rt.startswith("{"):
                        s = rt.find("{"); e = rt.rfind("}")
                        if s != -1: rt = rt[s:e+1]
                    rt = re.sub(r',(\s*[}\]])', r'\1', rt)
                    return json.loads(rt)
                
                val_context = {
                    'lead_name': lead_name, 'lead_title': title,
                    'company_name': company_name,
                    'company_data': company_context or {},
                }
                
                messages_data, quality = generate_validate_loop(
                    self.anthropic_client, self.config['anthropic']['model'],
                    gen_fn, val_context, persona=persona, pre_generated=messages_data
                )
                
                vs = quality.get('validation_score', 0)
                vr = quality.get('validation_rating', '?')
                logger.info(f"  ✓ Messages generated (validation: {vs}/100 {vr})")
                return messages_data
            except json.JSONDecodeError as json_err:
                logger.error(f"  ✗ JSON parsing failed: {str(json_err)}")
                logger.error(f"  First 200 chars: {result_text[:200]}")
                raise
            
        except Exception as e:
            logger.error(f"  ✗ Error generating messages: {str(e)}")
            return {
                "error": str(e),
                "email": {"subject": "Error", "body": "Generation failed"},
                "linkedin_short": {"message": "Error generating message"},
                "linkedin_long": {"subject": "Error", "body": "Generation failed"}
            }
    
    def update_lead_with_messages(self, lead_record_id: str, messages_data: Dict):
        """Update Airtable lead record with generated messages in separate columns"""
        
        update_fields = {
            'Email Subject': messages_data.get('email_subject', ''),
            'Email Body': messages_data.get('email_body', ''),
            'LinkedIn Connection Request': messages_data.get('linkedin_connection_request', ''),
            'LinkedIn Short Message': messages_data.get('linkedin_short', ''),
            'LinkedIn InMail Subject': messages_data.get('linkedin_inmail_subject', ''),
            'LinkedIn InMail Body': messages_data.get('linkedin_inmail_body', ''),
            'Message Generated Date': datetime.now().strftime('%Y-%m-%d'),
            'Generate Messages': False  # Uncheck the checkbox
        }
        
        # Update the record
        try:
            self.leads_table.update(lead_record_id, update_fields)
            logger.info(f"  ✓ Messages saved to Airtable (separate columns)")
        except Exception as e:
            logger.error(f"  ✗ Failed to update Airtable: {str(e)}")
            raise
    
    def generate_messages_batch(self, limit: Optional[int] = None, 
                                outreach_reason: str = "General Introduction"):
        """Generate messages for all leads marked for outreach"""
        
        logger.info("=" * 60)
        logger.info("OUTREACH MESSAGE GENERATION")
        logger.info("=" * 60)
        
        leads = self.get_leads_for_outreach(limit)
        
        if not leads:
            logger.info("✓ No leads marked for message generation")
            return True
        
        total = len(leads)
        success_count = 0
        failed_count = 0
        
        for idx, lead in enumerate(leads, 1):
            fields = lead['fields']
            lead_name = fields.get('Lead Name', 'Unknown')
            record_id = lead['id']
            
            logger.info("")
            logger.info(f"[{idx}/{total}] Generating for: {lead_name}")
            
            title = fields.get('Title', '')
            deep_profile = fields.get('Intelligence Notes', '')
            activity_log = fields.get('Activity Log', '')
            trigger_events = fields.get('Trigger Events', '')
            
            # Get company context
            company_name = "Unknown"
            company_context = None
            if 'Company' in fields:
                company_context = self.get_company_context(fields['Company'])
                if company_context:
                    company_name = company_context.get('Company Name', 'Unknown')
            
            logger.info(f"  Company: {company_name}")
            logger.info(f"  Reason: {outreach_reason}")
            
            try:
                # Generate messages
                messages_data = self.generate_outreach_messages(
                    lead_name=lead_name,
                    title=title,
                    company_name=company_name,
                    deep_profile=deep_profile,
                    activity_log=activity_log,
                    trigger_events=trigger_events,
                    outreach_reason=outreach_reason,
                    company_context=company_context
                )
                
                if messages_data.get('error'):
                    logger.error(f"  ✗ Generation failed: {messages_data['error']}")
                    failed_count += 1
                    continue
                
                # Validation already done inside generate_outreach_messages via generate_validate_loop
                
                # Save to Airtable
                logger.info("  Saving messages...")
                self.update_lead_with_messages(record_id, messages_data)
                
                success_count += 1
                logger.info(f"  ✓ Complete!")
                
                # Rate limiting
                import time
                time.sleep(2)
                
            except Exception as e:
                logger.error(f"  ✗ Error: {str(e)}")
                failed_count += 1
        
        logger.info("")
        logger.info("=" * 60)
        logger.info("MESSAGE GENERATION COMPLETE")
        logger.info(f"Total: {total} | Success: {success_count} | Failed: {failed_count}")
        logger.info("=" * 60)
        
        return failed_count == 0


def main():
    """CLI entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Generate outreach messages for leads')
    parser.add_argument('--limit', type=int, help='Max number of leads to generate for')
    parser.add_argument('--reason', default='General Introduction', 
                       help='Outreach reason (e.g., "Conference Meeting", "Funding Announcement", "General Introduction")')
    parser.add_argument('--config', default='config.yaml', help='Path to config file')
    
    args = parser.parse_args()
    
    try:
        generator = OutreachGenerator(config_path=args.config)
        success = generator.generate_messages_batch(
            limit=args.limit,
            outreach_reason=args.reason
        )
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
