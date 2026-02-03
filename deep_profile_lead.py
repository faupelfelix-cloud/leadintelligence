#!/usr/bin/env python3
"""
Deep Lead Profiling - Comprehensive personality and background analysis
Finds the "needle in the haystack" details that make leads memorable
"""

import os
import sys
import yaml
import json
import re
import logging
from datetime import datetime
from typing import Dict, Optional
import anthropic
from pyairtable import Api
from company_profile_utils import (load_company_profile, build_value_proposition, 
                                   build_outreach_philosophy, filter_by_confidence,
                                   suppressed_to_do_not_mention)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('deep_profile.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class DeepLeadProfiler:
    """Deep intelligence profiling for individual leads"""
    
    def __init__(self, config_path: str = "config.yaml"):
        """Initialize with configuration"""
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        # Initialize APIs
        self.airtable = Api(self.config['airtable']['api_key'])
        self.base = self.airtable.base(self.config['airtable']['base_id'])
        self.leads_table = self.base.table(self.config['airtable']['tables']['leads'])
        self.companies_table = self.base.table(self.config['airtable']['tables']['companies'])
        
        self.anthropic_client = anthropic.Anthropic(
            api_key=self.config['anthropic']['api_key']
        )
        
        # Load company profile for outreach context
        self.company_profile = load_company_profile(self.base)
        
        logger.info("DeepLeadProfiler initialized successfully")
    
    def _generate_outreach_with_profile(self, lead_id: str, lead_name: str, lead_title: str,
                                        company_name: str, profile_data: Dict) -> Optional[Dict]:
        """Generate personalized outreach messages using deep profile intelligence"""
        
        # Extract key insights from profile for personalization
        personality = profile_data.get('personality_assessment', {})
        communication = profile_data.get('communication_style', {})
        strategy = profile_data.get('outreach_strategy', {})
        icebreakers = profile_data.get('icebreakers', [])
        
        # Get company profile for our context
        our_company = "European biologics CDMO specializing in mammalian cell culture (mAbs, bispecifics, ADCs)"
        our_strengths = "Mammalian cell culture, mAbs, bispecifics, ADCs"
        our_target = "Mid-size biotech companies"
        
        if self.company_profile:
            our_company = self.company_profile.get('Capabilities', our_company)
            our_strengths = self.company_profile.get('Strengths', our_strengths)
            our_target = self.company_profile.get('Market Positioning', our_target)
        
        # Fetch and filter company data for value prop matching
        company_fields = {}
        do_not_mention_text = ""
        try:
            lead_record = self.leads_table.get(lead_id)
            company_ids = lead_record['fields'].get('Company', [])
            if company_ids:
                raw_fields = self.companies_table.get(company_ids[0])['fields']
                company_fields, suppressed = filter_by_confidence(raw_fields)
                do_not_mention_text = suppressed_to_do_not_mention(suppressed)
        except:
            pass
        
        # Build value proposition matched to prospect
        value_prop = build_value_proposition(self.company_profile, company_fields, lead_title)
        outreach_rules = build_outreach_philosophy()
        
        # Build personalization context from deep profile
        personalization = f"""
DEEP PROFILE INSIGHTS:
- Communication style: {communication.get('preferred_style', 'Professional')}
- Tone preference: {communication.get('tone_preference', 'Formal but friendly')}
- Detail level: {communication.get('detail_orientation', 'Medium')}
- Decision style: {personality.get('decision_making_style', 'Unknown')}

ICEBREAKERS TO USE:
{chr(10).join(['- ' + ib for ib in icebreakers[:3]]) if icebreakers else '- Reference their role and company'}

APPROACH RECOMMENDATIONS:
- Best channels: {', '.join(strategy.get('optimal_channels', ['Email', 'LinkedIn']))}
- Best timing: {', '.join(strategy.get('timing_signals', ['Avoid Mondays'])[:2])}
- Avoid: {', '.join(strategy.get('red_flags_avoid', ['Hard sales pitch'])[:2])}
"""
        
        prompt = f"""Generate highly personalized outreach messages using deep profile intelligence.

LEAD:
Name: {lead_name}
Title: {lead_title}
Company: {company_name}
{do_not_mention_text}

{personalization}

{value_prop}

{outreach_rules}

TONE & STYLE - ADAPT TO THEIR PROFILE:
- Match their communication style preference
- Use appropriate detail level for their profile
- Incorporate one of the icebreakers naturally
- Conversational and warm, NOT salesy
- Focus on THEIR perspective, not your pitch
- Soft CTAs
- NO bullet points in emails

Generate 4 messages:

1. EMAIL (120-150 words):
Subject: [Natural, personalized subject using an icebreaker insight]
- Open with personalized hook from their profile
- Why you might be relevant to THEM
- Soft CTA matching their communication style
- Sign: "Best regards, [Your Name], Business Development"

2. LINKEDIN CONNECTION REQUEST (180-200 chars):
- Brief, personalized to their interests/role
- Why you'd like to connect

3. LINKEDIN SHORT MESSAGE (300-400 chars):
- For after connection accepted
- Reference something specific from their profile
- End: "Best regards, [Your Name]"

4. LINKEDIN INMAIL (250-350 words):
Subject: [Personalized subject]
- Open with personalized observation
- Share relevant perspective
- NO bullet lists
- Sign: "Best regards, [Your Name], Business Development"

Return ONLY valid JSON:
{{
    "email_subject": "Subject line",
    "email_body": "Full email body",
    "linkedin_connection": "Connection request text",
    "linkedin_short": "Short message text",
    "linkedin_inmail_subject": "InMail subject",
    "linkedin_inmail": "Full InMail body"
}}

Return ONLY JSON, no other text."""

        try:
            message = self.anthropic_client.messages.create(
                model=self.config['anthropic']['model'],
                max_tokens=3000,
                messages=[{"role": "user", "content": prompt}]
            )
            
            response_text = ""
            for block in message.content:
                if hasattr(block, 'text'):
                    response_text += block.text
            
            if not response_text.strip():
                logger.warning(f"  Empty response from API for outreach")
                return None
            
            # Extract JSON
            json_str = None
            if "```json" in response_text:
                json_str = response_text.split("```json")[1].split("```")[0]
            elif "```" in response_text and "{" in response_text:
                json_str = response_text.split("```")[1].split("```")[0]
            elif "{" in response_text:
                start = response_text.find("{")
                end = response_text.rfind("}") + 1
                json_str = response_text[start:end]
            
            if not json_str:
                logger.warning(f"  No JSON found in outreach response")
                return None
            
            # Clean and parse
            json_str = json_str.strip()
            json_str = re.sub(r',(\s*[}\]])', r'\1', json_str)
            
            return json.loads(json_str)
            
        except Exception as e:
            logger.warning(f"  Error generating outreach: {e}")
            return None
    
    def _update_lead_outreach(self, lead_id: str, outreach: Dict):
        """Update lead record with new outreach messages"""
        
        update_fields = {
            'Message Generated Date': datetime.now().strftime('%Y-%m-%d')
        }
        
        if outreach.get('email_subject'):
            update_fields['Email Subject'] = outreach['email_subject']
        if outreach.get('email_body'):
            update_fields['Email Body'] = outreach['email_body']
        if outreach.get('linkedin_connection'):
            update_fields['LinkedIn Connection Request'] = outreach['linkedin_connection']
        if outreach.get('linkedin_short'):
            update_fields['LinkedIn Short Message'] = outreach['linkedin_short']
        if outreach.get('linkedin_inmail_subject'):
            update_fields['LinkedIn InMail Subject'] = outreach['linkedin_inmail_subject']
        if outreach.get('linkedin_inmail'):
            update_fields['LinkedIn InMail Body'] = outreach['linkedin_inmail']
        
        try:
            self.leads_table.update(lead_id, update_fields)
            logger.info(f"  ✓ Outreach messages updated")
        except Exception as e:
            logger.warning(f"  Could not update outreach: {e}")
    
    def get_leads_for_deep_profile(self, limit: Optional[int] = None, 
                                     min_icp: int = 0, max_icp: int = None,
                                     refresh_days: int = 180) -> list:
        """Get leads that need deep profiling based on ICP tier and refresh period.
        
        Args:
            limit: Max number of leads to return
            min_icp: Minimum Lead ICP score
            max_icp: Maximum Lead ICP score (exclusive)
            refresh_days: Days since last profile before re-profiling
                         - 30 for monthly (ICP 75+)
                         - 180 for bi-annual (ICP 50-74)
        
        Selection criteria:
        1. "Deep Profile" checkbox = TRUE
        2. Lead ICP Score in range [min_icp, max_icp)
        3. Last Deep Profile Date is older than refresh_days OR never profiled
        """
        from datetime import datetime, timedelta
        
        # Get leads marked for deep profiling
        formula = "{Deep Profile} = TRUE()"
        all_marked_leads = self.leads_table.all(formula=formula)
        
        # Filter by ICP and refresh period
        cutoff_date = datetime.now() - timedelta(days=refresh_days)
        leads_to_profile = []
        skipped_low_icp = 0
        skipped_high_icp = 0
        skipped_recent = 0
        
        for lead in all_marked_leads:
            fields = lead['fields']
            lead_name = fields.get('Lead Name', 'Unknown')
            lead_icp = fields.get('Lead ICP Score', 0) or fields.get('Lead ICP Fit Score', 0) or 0
            
            # Check ICP range
            if lead_icp < min_icp:
                skipped_low_icp += 1
                continue
            if max_icp is not None and lead_icp >= max_icp:
                skipped_high_icp += 1
                continue
            
            # Check last profile date
            last_profiled = fields.get('Last Deep Profile Date')
            
            if not last_profiled:
                # Never profiled - include it
                leads_to_profile.append(lead)
                logger.debug(f"  Including {lead_name} (ICP {lead_icp}) - never profiled")
            else:
                # Parse the date and check if it's old enough
                try:
                    last_profiled_date = datetime.strptime(last_profiled, '%Y-%m-%d')
                    if last_profiled_date < cutoff_date:
                        # Old enough - include it
                        leads_to_profile.append(lead)
                        logger.debug(f"  Including {lead_name} (ICP {lead_icp}) - last profiled {last_profiled}")
                    else:
                        # Too recent - skip it
                        skipped_recent += 1
                        logger.debug(f"  Skipping {lead_name} - recently profiled on {last_profiled}")
                except:
                    # Can't parse date - include it to be safe
                    leads_to_profile.append(lead)
        
        if limit:
            leads_to_profile = leads_to_profile[:limit]
        
        icp_range = f"ICP {min_icp}-{max_icp if max_icp else '100'}"
        logger.info(f"Found {len(all_marked_leads)} leads marked for profiling")
        logger.info(f"Filter: {icp_range}, refresh > {refresh_days} days")
        logger.info(f"Result: {len(leads_to_profile)} leads need profiling")
        if skipped_low_icp > 0:
            logger.info(f"  Skipped {skipped_low_icp} below ICP threshold")
        if skipped_high_icp > 0:
            logger.info(f"  Skipped {skipped_high_icp} above ICP max (handled by other tier)")
        if skipped_recent > 0:
            logger.info(f"  Skipped {skipped_recent} recently profiled")
        
        return leads_to_profile
    
    def find_lead(self, lead_name: str) -> Optional[Dict]:
        """Find a lead by name in Airtable"""
        formula = f"{{Lead Name}} = '{lead_name}'"
        results = self.leads_table.all(formula=formula)
        
        if not results:
            logger.warning(f"Lead '{lead_name}' not found")
            return None
        
        if len(results) > 1:
            logger.warning(f"Multiple leads found with name '{lead_name}', using first match")
        
        return results[0]
    
    def get_company_context(self, company_ids: list) -> Optional[Dict]:
        """Get company information for context"""
        if not company_ids:
            return None
        
        try:
            company = self.companies_table.get(company_ids[0])
            return company['fields']
        except:
            return None
    
    def deep_profile_lead(self, lead_name: str, title: str = "", company_name: str = "", 
                         linkedin_url: str = "", company_context: Dict = None) -> Dict:
        """Generate comprehensive deep profile using AI with web search"""
        
        context_info = f"""
Lead: {lead_name}
Title: {title or 'Unknown'}
Company: {company_name or 'Unknown'}
LinkedIn: {linkedin_url or 'Not provided'}
X (Twitter): Search for their X/Twitter profile
"""
        
        if company_context:
            context_info += f"""
Company Context:
- Focus: {', '.join(company_context.get('Focus Area', []))}
- Stage: {', '.join(company_context.get('Pipeline Stage', []))}
- Funding: {company_context.get('Funding Stage', 'Unknown')}
"""
        
        profile_prompt = f"""You are an elite business intelligence researcher specializing in finding unique, memorable details about business professionals in the biologics/pharma industry.

{context_info}

Your mission: Create a COMPREHENSIVE deep profile that finds the "needle in the haystack" - those unique, standout details that make this person memorable and help craft the perfect outreach.

SEARCH EXTENSIVELY for:

🎯 STANDOUT DETAILS (The Gold Nuggets):
- Unique hobbies and interests (not generic "golf" - find the unusual)
- Unexpected backgrounds (former careers, international experience)
- Personal causes and volunteer work (specific organizations)
- Awards, recognition, speaking engagements
- Publications, patents, thought leadership
- Quirky facts that stand out
- Geographic connections and cultural ties
- Educational uniqueness (fellowships, dual degrees, prestigious labs)

👤 PROFESSIONAL BACKGROUND:
- Current role details (responsibilities, budget, team size, reporting structure)
- Complete career trajectory with dates
- Key accomplishments and milestones
- Technical expertise areas
- Publications and patents
- Education (undergrad, grad, postdoc, thesis topics)

🧠 PERSONALITY & COMMUNICATION STYLE:
- Analyze their LinkedIn posts, articles, quotes
- Analyze their X (Twitter) posts and engagement style (if they have X profile)
- Compare communication style between LinkedIn and X
- Communication tone (formal/casual, technical/business, humor)
- Values and priorities (what they champion, criticize)
- Language patterns and phrases they use
- Decision-making style indicators
- Risk tolerance signals

🌐 NETWORK & INFLUENCE:
- LinkedIn connection count and followers
- X (Twitter) followers and engagement (if applicable)
- Key connections in industry
- Speaking engagements and conferences
- Industry presence and thought leadership
- Advisory roles or board seats
- Influence indicators

📊 RECENT ACTIVITY (Last 6 months):
- LinkedIn posts and themes
- X (Twitter) activity and topics (if they have X profile)
- Company milestones they've been involved in
- Career changes or promotions
- Speaking events attended
- Articles published or quoted in
- Engagement patterns (when they post, what they respond to)
- Cross-platform behavior (different on LinkedIn vs X?)

💡 OPTIMAL OUTREACH STRATEGY:
- Best approach based on personality
- Talking points that will resonate
- Timing considerations
- Channels (email, LinkedIn, conference)
- Personal connection angles
- Red flags to avoid

Return your findings in this EXACT JSON format:
{{
  "standout_details": [
    "Specific unique detail with context",
    "Another memorable detail",
    ...
  ],
  "professional_background": {{
    "current_role": {{
      "title": "",
      "responsibilities": "",
      "budget_authority": "",
      "team_size": "",
      "reports_to": ""
    }},
    "career_trajectory": [
      {{"period": "2022-Present", "role": "VP Technical Operations", "company": "Acme", "key_achievements": []}},
      ...
    ],
    "education": [
      {{"degree": "PhD", "field": "Chemical Engineering", "institution": "MIT", "year": "2009", "thesis": "..."}}
    ],
    "expertise_areas": ["area1", "area2"],
    "publications_count": 15,
    "patents_count": 3,
    "key_accomplishments": []
  }},
  "personality_communication": {{
    "communication_style": {{
      "tone": "Professional but warm",
      "content_type": "Data-driven with storytelling",
      "technical_depth": "High",
      "posting_frequency": "2-3x per week",
      "x_activity": "Active on X with 5-10 tweets/week (if applicable)",
      "cross_platform_differences": "More technical on LinkedIn, more casual on X (if applicable)"
    }},
    "language_patterns": ["phrase1", "phrase2"],
    "values_priorities": ["Patient access", "Scientific rigor"],
    "decision_making_style": "Evidence-based, collaborative",
    "risk_profile": "Moderate - innovative but proven",
    "red_flags": ["None detected"] or ["Specific concern"]
  }},
  "network_influence": {{
    "linkedin_connections": 2847,
    "linkedin_followers": 4200,
    "x_followers": 1500,
    "x_engagement_rate": "3-5% (if applicable)",
    "engagement_rate": "50-100 likes per post",
    "key_connections": ["Connection type/group"],
    "speaking_engagements": ["WCBP 2024"],
    "publications": 15,
    "thought_leadership": "Moderate",
    "influence_indicators": ["indicator1"],
    "x_presence": "Active/Inactive/Not found"
  }},
  "recent_activity": {{
    "linkedin_highlights": [
      {{"date": "Nov 2024", "content": "Posted about ADC challenges", "engagement": "200+ likes"}},
      ...
    ],
    "company_milestones": [],
    "speaking_events": [],
    "content_themes": ["theme1", "theme2"],
    "engagement_patterns": "Most active Tuesday-Thursday mornings"
  }},
  "outreach_strategy": {{
    "best_approach": ["Lead with data", "Reference their work"],
    "talking_points": ["Point 1", "Point 2"],
    "timing_signals": ["Just raised funding", "Hiring for team"],
    "optimal_channels": ["LinkedIn", "Email"],
    "personal_angles": ["Music connection", "Rare disease work"],
    "red_flags_avoid": ["Don't oversell", "Don't rush"]
  }},
  "metadata": {{
    "confidence_level": "High/Medium/Low",
    "sources": ["source1", "source2"],
    "profile_completeness": "95%",
    "last_updated": "2026-01-22",
    "recommended_action": "HIGH PRIORITY OUTREACH"
  }}
}}

CRITICAL: Find the UNIQUE details. Don't give me generic "enjoys networking" - find "Amateur astronomer with telescope named after their PhD advisor". Dig deep!

Only return valid JSON, no other text."""

        try:
            logger.info(f"  Generating deep profile with AI + web search...")
            
            # Use Claude with web search
            message = self.anthropic_client.messages.create(
                model=self.config['anthropic']['model'],
                max_tokens=self.config['anthropic']['max_tokens'],
                tools=[{
                    "type": "web_search_20250305",
                    "name": "web_search"
                }],
                messages=[{
                    "role": "user",
                    "content": profile_prompt
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
            
            # Handle empty response
            if not result_text:
                logger.error(f"  ✗ Empty response from API")
                return {
                    "error": "Empty response from API - insufficient data found",
                    "metadata": {
                        "confidence_level": "Failed",
                        "last_updated": datetime.now().strftime('%Y-%m-%d')
                    }
                }
            
            # Try to find JSON if there's preamble text
            if not result_text.startswith("{"):
                # Look for first { and last }
                start_idx = result_text.find("{")
                end_idx = result_text.rfind("}")
                if start_idx != -1 and end_idx != -1:
                    result_text = result_text[start_idx:end_idx+1]
            
            # Remove any trailing commas before closing braces (common JSON error)
            import re
            result_text = re.sub(r',(\s*[}\]])', r'\1', result_text)
            
            # Fix common JSON issues with quotes inside strings
            # This handles cases where the AI includes unescaped quotes in values
            def fix_json_quotes(json_str):
                """Attempt to fix unescaped quotes in JSON string values"""
                # Try parsing first - if it works, return as-is
                try:
                    json.loads(json_str)
                    return json_str
                except:
                    pass
                
                # Try to fix by escaping problematic quotes
                # This is a simple heuristic - replace quotes that appear mid-value
                fixed = json_str
                # Replace smart quotes with regular quotes first
                fixed = fixed.replace('"', '"').replace('"', '"')
                fixed = fixed.replace(''', "'").replace(''', "'")
                
                return fixed
            
            result_text = fix_json_quotes(result_text)
            
            try:
                profile_data = json.loads(result_text)
                logger.info(f"  ✓ Deep profile generated successfully")
                return profile_data
            except json.JSONDecodeError as json_err:
                # Log the problematic JSON for debugging
                logger.error(f"  ✗ JSON parsing failed: {str(json_err)}")
                logger.error(f"  First 200 chars of response: {result_text[:200]}")
                logger.error(f"  Last 200 chars of response: {result_text[-200:]}")
                
                # Return a partial result so the lead isn't completely skipped
                return {
                    "error": f"JSON parsing failed: {str(json_err)}",
                    "standout_details": ["Profile generation encountered formatting issues - retry recommended"],
                    "metadata": {
                        "confidence_level": "Failed - Parse Error",
                        "last_updated": datetime.now().strftime('%Y-%m-%d'),
                        "recommended_action": "Retry profiling"
                    }
                }
            
        except json.JSONDecodeError as e:
            logger.error(f"  ✗ Error generating profile (JSON parse error): {str(e)}")
            return {
                "error": f"JSON parsing failed: {str(e)}",
                "metadata": {
                    "confidence_level": "Failed",
                    "last_updated": datetime.now().strftime('%Y-%m-%d')
                }
            }
    
    def format_profile_for_airtable(self, profile_data: Dict) -> str:
        """Format the profile data into readable text for Airtable"""
        
        output = []
        output.append("=" * 60)
        output.append("DEEP LEAD PROFILE")
        output.append(f"Generated: {profile_data.get('metadata', {}).get('last_updated', 'Unknown')}")
        output.append("=" * 60)
        output.append("")
        
        # Standout Details
        output.append("🎯 STANDOUT DETAILS (The Needles!)")
        output.append("-" * 60)
        for detail in profile_data.get('standout_details', []):
            output.append(f"• {detail}")
        output.append("")
        
        # Professional Background
        prof = profile_data.get('professional_background', {})
        output.append("👤 PROFESSIONAL BACKGROUND")
        output.append("-" * 60)
        
        current = prof.get('current_role', {})
        if current:
            output.append(f"CURRENT: {current.get('title', 'Unknown')}")
            if current.get('responsibilities'):
                output.append(f"  Responsibilities: {current['responsibilities']}")
            if current.get('team_size'):
                output.append(f"  Team: {current['team_size']}")
        
        career = prof.get('career_trajectory', [])
        if career:
            output.append("\nCAREER:")
            for role in career[:5]:  # Top 5 roles
                output.append(f"  {role.get('period', '')}: {role.get('role', '')} @ {role.get('company', '')}")
        
        output.append(f"\nEXPERTISE: {', '.join(prof.get('expertise_areas', []))}")
        output.append(f"PUBLICATIONS: {prof.get('publications_count', 0)} | PATENTS: {prof.get('patents_count', 0)}")
        output.append("")
        
        # Personality & Communication
        personality = profile_data.get('personality_communication', {})
        output.append("🧠 PERSONALITY & COMMUNICATION")
        output.append("-" * 60)
        
        style = personality.get('communication_style', {})
        output.append(f"Tone: {style.get('tone', 'Unknown')}")
        output.append(f"Style: {style.get('content_type', 'Unknown')}")
        output.append(f"Decision-making: {personality.get('decision_making_style', 'Unknown')}")
        output.append(f"Risk profile: {personality.get('risk_profile', 'Unknown')}")
        
        # X profile activity if present
        if style.get('x_activity'):
            output.append(f"X Activity: {style['x_activity']}")
        if style.get('cross_platform_differences'):
            output.append(f"Platform differences: {style['cross_platform_differences']}")
        
        values = personality.get('values_priorities', [])
        if values:
            output.append(f"\nVALUES: {', '.join(values)}")
        
        red_flags = personality.get('red_flags', [])
        if red_flags and red_flags[0] != "None detected":
            output.append(f"\n⚠️ RED FLAGS: {', '.join(red_flags)}")
        output.append("")
        
        # Network & Influence
        network = profile_data.get('network_influence', {})
        output.append("🌐 NETWORK & INFLUENCE")
        output.append("-" * 60)
        output.append(f"LinkedIn: {network.get('linkedin_connections', 0)} connections, {network.get('linkedin_followers', 0)} followers")
        
        # X presence
        x_presence = network.get('x_presence', 'Not found')
        if x_presence != 'Not found':
            x_followers = network.get('x_followers', 0)
            x_engagement = network.get('x_engagement_rate', 'Unknown')
            output.append(f"X (Twitter): {x_followers} followers, {x_engagement} engagement")
        
        output.append(f"Engagement: {network.get('engagement_rate', 'Unknown')}")
        output.append(f"Thought leadership: {network.get('thought_leadership', 'Unknown')}")
        
        speaking = network.get('speaking_engagements', [])
        if speaking:
            output.append(f"Speaking: {', '.join(speaking[:3])}")
        output.append("")
        
        # Recent Activity
        recent = profile_data.get('recent_activity', {})
        output.append("📊 RECENT ACTIVITY (Last 6 Months)")
        output.append("-" * 60)
        
        highlights = recent.get('linkedin_highlights', [])
        for h in highlights[:5]:  # Top 5
            output.append(f"• {h.get('date', '')}: {h.get('content', '')}")
        
        themes = recent.get('content_themes', [])
        if themes:
            output.append(f"\nTHEMES: {', '.join(themes)}")
        output.append("")
        
        # Outreach Strategy
        strategy = profile_data.get('outreach_strategy', {})
        output.append("💡 OPTIMAL OUTREACH STRATEGY")
        output.append("-" * 60)
        
        output.append("BEST APPROACH:")
        for approach in strategy.get('best_approach', []):
            output.append(f"  ✓ {approach}")
        
        output.append("\nTALKING POINTS:")
        for point in strategy.get('talking_points', []):
            output.append(f"  • {point}")
        
        output.append("\nTIMING:")
        for signal in strategy.get('timing_signals', []):
            output.append(f"  ✅ {signal}")
        
        output.append("\nAVOID:")
        for flag in strategy.get('red_flags_avoid', []):
            output.append(f"  ✗ {flag}")
        
        angles = strategy.get('personal_angles', [])
        if angles:
            output.append("\nPERSONAL ANGLES:")
            for angle in angles:
                output.append(f"  • {angle}")
        
        output.append("")
        output.append("=" * 60)
        
        metadata = profile_data.get('metadata', {})
        output.append(f"Confidence: {metadata.get('confidence_level', 'Unknown')}")
        output.append(f"Completeness: {metadata.get('profile_completeness', 'Unknown')}")
        output.append(f"Action: {metadata.get('recommended_action', 'Unknown')}")
        output.append("=" * 60)
        
        return '\n'.join(output)
    
    def update_lead_profile(self, lead_record_id: str, profile_data: Dict):
        """Update Airtable lead record with deep profile"""
        
        formatted_profile = self.format_profile_for_airtable(profile_data)
        
        update_fields = {
            'Intelligence Notes': formatted_profile
        }
        
        try:
            self.leads_table.update(lead_record_id, update_fields)
            logger.info(f"  ✓ Profile saved to Airtable")
        except Exception as e:
            logger.error(f"  ✗ Failed to update Airtable: {str(e)}")
            raise
    
    def profile_lead_batch(self, limit: Optional[int] = None,
                           min_icp: int = 0, max_icp: int = None,
                           refresh_days: int = 180, tier_name: str = None):
        """Process leads marked for deep profiling based on ICP tier.
        
        Args:
            limit: Max leads to process
            min_icp: Minimum Lead ICP score
            max_icp: Maximum Lead ICP score (exclusive)
            refresh_days: Days since last profile before re-profiling
            tier_name: Name for logging (e.g., "MONTHLY HIGH-PRIORITY")
        """
        
        tier_display = tier_name or f"ICP {min_icp}-{max_icp if max_icp else '100'}"
        
        logger.info("=" * 60)
        logger.info(f"DEEP PROFILING - {tier_display}")
        logger.info(f"ICP Range: {min_icp} - {max_icp if max_icp else '100'}")
        logger.info(f"Refresh Period: {refresh_days} days")
        logger.info("=" * 60)
        
        leads = self.get_leads_for_deep_profile(
            limit=limit,
            min_icp=min_icp,
            max_icp=max_icp,
            refresh_days=refresh_days
        )
        
        if not leads:
            logger.info("✓ No leads marked for deep profiling")
            return True
        
        total = len(leads)
        success_count = 0
        failed_count = 0
        
        for idx, lead in enumerate(leads, 1):
            fields = lead['fields']
            lead_name = fields.get('Lead Name', 'Unknown')
            record_id = lead['id']
            
            logger.info("")
            logger.info(f"[{idx}/{total}] Processing: {lead_name}")
            
            title = fields.get('Title', '')
            linkedin_url = fields.get('LinkedIn URL', '')
            
            # Get company context
            company_name = "Unknown"
            company_context = None
            if 'Company' in fields:
                company_context = self.get_company_context(fields['Company'])
                if company_context:
                    company_name = company_context.get('Company Name', 'Unknown')
            
            logger.info(f"  Company: {company_name}")
            logger.info(f"  Title: {title or 'Unknown'}")
            
            try:
                # Generate deep profile
                logger.info("  Generating deep profile (30-60 seconds)...")
                profile_data = self.deep_profile_lead(
                    lead_name=lead_name,
                    title=title,
                    company_name=company_name,
                    linkedin_url=linkedin_url,
                    company_context=company_context
                )
                
                if profile_data.get('error'):
                    logger.error(f"  ✗ Profile generation failed: {profile_data['error']}")
                    failed_count += 1
                    continue
                
                # Save to Airtable
                logger.info("  Saving profile...")
                self.update_lead_profile(record_id, profile_data)
                
                # Generate personalized outreach using the deep profile
                logger.info("  Generating personalized outreach messages...")
                outreach = self._generate_outreach_with_profile(
                    lead_id=record_id,
                    lead_name=lead_name,
                    lead_title=title,
                    company_name=company_name,
                    profile_data=profile_data
                )
                
                if outreach:
                    self._update_lead_outreach(record_id, outreach)
                else:
                    logger.warning("  ⚠ Could not generate outreach messages")
                
                # Update tracking fields
                self.leads_table.update(record_id, {
                    'Deep Profile': False,  # Uncheck the checkbox
                    'Deep Profile Status': 'Completed',  # Mark as completed
                    'Last Deep Profile Date': datetime.now().strftime('%Y-%m-%d')  # Save date
                })
                
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
        logger.info("BATCH PROFILING COMPLETE")
        logger.info(f"Total: {total} | Success: {success_count} | Failed: {failed_count}")
        logger.info("=" * 60)
        
        # Return True if at least some succeeded (partial success is OK)
        return success_count > 0
    
    def profile_lead(self, lead_name: str):
        """Main workflow: find lead, generate profile, save to Airtable"""
        
        logger.info("=" * 60)
        logger.info(f"DEEP PROFILING: {lead_name}")
        logger.info("=" * 60)
        
        # Find lead
        logger.info("Step 1: Finding lead in Airtable...")
        lead = self.find_lead(lead_name)
        
        if not lead:
            logger.error(f"✗ Lead '{lead_name}' not found in Airtable")
            return False
        
        fields = lead['fields']
        record_id = lead['id']
        
        title = fields.get('Title', '')
        linkedin_url = fields.get('LinkedIn URL', '')
        
        # Get company context
        company_name = "Unknown"
        company_context = None
        if 'Company' in fields:
            company_context = self.get_company_context(fields['Company'])
            if company_context:
                company_name = company_context.get('Company Name', 'Unknown')
        
        logger.info(f"  ✓ Found: {lead_name}")
        logger.info(f"  Title: {title or 'Unknown'}")
        logger.info(f"  Company: {company_name}")
        logger.info("")
        
        # Generate deep profile
        logger.info("Step 2: Generating deep profile (this may take 30-60 seconds)...")
        profile_data = self.deep_profile_lead(
            lead_name=lead_name,
            title=title,
            company_name=company_name,
            linkedin_url=linkedin_url,
            company_context=company_context
        )
        
        if profile_data.get('error'):
            logger.error(f"✗ Profile generation failed: {profile_data['error']}")
            return False
        
        logger.info("")
        
        # Save to Airtable
        logger.info("Step 3: Saving profile to Airtable...")
        self.update_lead_profile(record_id, profile_data)
        
        # Update tracking fields
        try:
            self.leads_table.update(record_id, {
                'Deep Profile Status': 'Completed',
                'Last Deep Profile Date': datetime.now().strftime('%Y-%m-%d')
            })
        except Exception as e:
            logger.warning(f"Could not update tracking fields: {str(e)}")
        
        logger.info("")
        logger.info("=" * 60)
        logger.info("✓ DEEP PROFILE COMPLETE!")
        logger.info(f"View in Airtable: Leads table → {lead_name} → Intelligence Notes field")
        logger.info("=" * 60)
        
        return True


def main():
    """CLI entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Generate deep intelligence profile for leads')
    parser.add_argument('--auto', action='store_true', help='Auto mode: process all leads with "Deep Profile" checkbox')
    parser.add_argument('--lead', help='Manual mode: specific lead name (must match Airtable exactly)')
    parser.add_argument('--limit', type=int, help='Max number of leads to profile in auto mode')
    parser.add_argument('--tier', choices=['monthly', 'biannual', 'all'],
                       help='Tier preset: monthly (ICP 75+, 30d), biannual (ICP 50-74, 180d), all (ICP 50+)')
    parser.add_argument('--min-icp', type=int, default=0, help='Minimum Lead ICP score')
    parser.add_argument('--max-icp', type=int, default=None, help='Maximum Lead ICP score (exclusive)')
    parser.add_argument('--refresh-days', type=int, default=180, help='Days since last profile before re-profiling')
    parser.add_argument('--config', default='config.yaml', help='Path to config file')
    
    args = parser.parse_args()
    
    if not args.auto and not args.lead and not args.tier:
        parser.error("Must specify either --auto, --lead <n>, or --tier <tier>")
    
    # Apply tier presets
    tier_name = None
    if args.tier == 'monthly':
        # Monthly: ICP 75+, refresh after 30 days
        args.min_icp = 75
        args.max_icp = None
        args.refresh_days = 30
        tier_name = "MONTHLY HIGH-PRIORITY (ICP 75+)"
        args.auto = True
    elif args.tier == 'biannual':
        # Bi-annual: ICP 50-74, refresh after 180 days
        args.min_icp = 50
        args.max_icp = 75
        args.refresh_days = 180
        tier_name = "BI-ANNUAL MEDIUM-PRIORITY (ICP 50-74)"
        args.auto = True
    elif args.tier == 'all':
        # All ICP 50+, use default 180 day refresh
        args.min_icp = 50
        args.max_icp = None
        args.refresh_days = 180
        tier_name = "ALL PRIORITY LEADS (ICP 50+)"
        args.auto = True
    
    try:
        profiler = DeepLeadProfiler(config_path=args.config)
        
        if args.auto or args.tier:
            logger.info("Running in AUTO mode")
            success = profiler.profile_lead_batch(
                limit=args.limit,
                min_icp=args.min_icp,
                max_icp=args.max_icp,
                refresh_days=args.refresh_days,
                tier_name=tier_name
            )
        else:
            logger.info("Running in MANUAL mode")
            success = profiler.profile_lead(args.lead)
        
        sys.exit(0 if success else 1)
    except FileNotFoundError:
        logger.error(f"Config file not found: {args.config}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
