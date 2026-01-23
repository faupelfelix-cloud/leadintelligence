#!/usr/bin/env python3
"""
Deep Lead Profiling - Comprehensive personality and background analysis
Finds the "needle in the haystack" details that make leads memorable
"""

import os
import sys
import yaml
import json
import logging
from datetime import datetime
from typing import Dict, Optional
import anthropic
from pyairtable import Api

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
        
        logger.info("DeepLeadProfiler initialized successfully")
    
    def get_leads_for_deep_profile(self, limit: Optional[int] = None) -> list:
        """Get all leads marked for deep profiling (checkbox checked) that need profiling"""
        from datetime import datetime, timedelta
        
        # Get leads marked for deep profiling
        formula = "{Deep Profile} = TRUE()"
        all_marked_leads = self.leads_table.all(formula=formula)
        
        # Filter out leads that were profiled less than 6 months ago
        six_months_ago = datetime.now() - timedelta(days=180)
        leads_to_profile = []
        
        for lead in all_marked_leads:
            fields = lead['fields']
            last_profiled = fields.get('Last Deep Profile Date')
            
            if not last_profiled:
                # Never profiled - include it
                leads_to_profile.append(lead)
            else:
                # Parse the date and check if it's old enough
                try:
                    last_profiled_date = datetime.strptime(last_profiled, '%Y-%m-%d')
                    if last_profiled_date < six_months_ago:
                        # More than 6 months old - include it
                        logger.info(f"  Including {fields.get('Lead Name', 'Unknown')} - last profiled {last_profiled}")
                        leads_to_profile.append(lead)
                    else:
                        # Too recent - skip it
                        logger.info(f"  Skipping {fields.get('Lead Name', 'Unknown')} - recently profiled on {last_profiled}")
                except:
                    # Can't parse date - include it to be safe
                    leads_to_profile.append(lead)
        
        if limit:
            leads_to_profile = leads_to_profile[:limit]
        
        logger.info(f"Found {len(all_marked_leads)} leads marked for profiling")
        logger.info(f"After 6-month filter: {len(leads_to_profile)} leads need profiling")
        
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
            
            # Parse JSON
            result_text = result_text.strip()
            if result_text.startswith("```json"):
                result_text = result_text[7:]
            if result_text.startswith("```"):
                result_text = result_text[3:]
            if result_text.endswith("```"):
                result_text = result_text[:-3]
            
            profile_data = json.loads(result_text.strip())
            logger.info(f"  ✓ Deep profile generated successfully")
            return profile_data
            
        except Exception as e:
            logger.error(f"  ✗ Error generating profile: {str(e)}")
            return {
                "error": str(e),
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
    
    def profile_lead_batch(self, limit: Optional[int] = None):
        """Process all leads marked for deep profiling"""
        
        logger.info("=" * 60)
        logger.info("AUTO MODE: DEEP PROFILING MARKED LEADS")
        logger.info("=" * 60)
        
        leads = self.get_leads_for_deep_profile(limit)
        
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
        
        return failed_count == 0
    
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
    parser.add_argument('--config', default='config.yaml', help='Path to config file')
    
    args = parser.parse_args()
    
    if not args.auto and not args.lead:
        parser.error("Must specify either --auto or --lead <name>")
    
    try:
        profiler = DeepLeadProfiler(config_path=args.config)
        
        if args.auto:
            logger.info("Running in AUTO mode")
            success = profiler.profile_lead_batch(limit=args.limit)
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
