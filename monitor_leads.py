#!/usr/bin/env python3
"""
Lead Surveillance System - Bi-weekly monitoring of lead activity
Tracks LinkedIn, X (Twitter), conferences, company news, and trigger events
"""

import os
import sys
import yaml
import json
import re
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import anthropic
from pyairtable import Api
from company_profile_utils import (load_company_profile, load_persona_messaging, build_value_proposition, 
                                   build_outreach_philosophy, filter_by_confidence,
                                   suppressed_to_do_not_mention, classify_persona)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('surveillance.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class LeadSurveillance:
    """Monitor lead activity across multiple platforms"""
    
    def __init__(self, config_path: str = "config.yaml"):
        """Initialize with configuration"""
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        # Initialize APIs
        self.airtable = Api(self.config['airtable']['api_key'])
        self.base = self.airtable.base(self.config['airtable']['base_id'])
        self.leads_table = self.base.table(self.config['airtable']['tables']['leads'])
        self.companies_table = self.base.table(self.config['airtable']['tables']['companies'])
        self.trigger_history_table = self.base.table('Trigger History')
        
        # Try to access Conferences table (optional)
        try:
            self.conferences_table = self.base.table('Conferences We Attend')
            self.has_conference_table = True
        except:
            self.conferences_table = None
            self.has_conference_table = False
            logger.info("Note: 'Conferences We Attend' table not found - conference matching disabled")
        
        self.anthropic_client = anthropic.Anthropic(
            api_key=self.config['anthropic']['api_key']
        )
        
        # Load company profile for outreach context
        self.company_profile = load_company_profile(self.base)
        self.persona_messaging = load_persona_messaging(self.base)
        
        logger.info("LeadSurveillance initialized successfully")
    
    def get_our_conferences(self) -> str:
        """Get list of conferences we're attending for matching"""
        if not self.has_conference_table:
            return ""
        
        try:
            # Get all upcoming conferences
            today = datetime.now().strftime('%Y-%m-%d')
            formula = f"IS_AFTER({{Conference Date}}, '{today}')"
            conferences = self.conferences_table.all(formula=formula)
            
            if not conferences:
                return ""
            
            conf_list = []
            for conf in conferences:
                fields = conf['fields']
                name = fields.get('Conference Name', '')
                date = fields.get('Conference Date', '')
                location = fields.get('Location', '')
                conf_list.append(f"- {name} ({date}, {location})")
            
            return "CONFERENCES WE ARE ATTENDING:\n" + "\n".join(conf_list[:10])  # Max 10
        except Exception as e:
            logger.warning(f"Could not fetch conference list: {str(e)}")
            return ""
    
    def get_leads_for_monitoring(self, limit: Optional[int] = None, 
                                   min_icp: int = 50, max_icp: int = None,
                                   include_company_icp: bool = False) -> List[Dict]:
        """Get leads for surveillance monitoring
        
        Selection criteria:
        1. Monitor Activity = TRUE (checkbox)
        2. Lead ICP Score in range [min_icp, max_icp)
        3. OR Company ICP >= min_icp (if include_company_icp=True and Lead ICP < min_icp)
        
        Tiered monitoring:
        - Weekly (min_icp=75): High-priority leads
        - Bi-weekly (min_icp=50, max_icp=75): Medium-priority leads  
        - Monthly (min_icp=30, max_icp=50): Lower-priority leads
        - Monthly company ICP (include_company_icp=True): Leads at high-value companies
        """
        
        # Get leads marked for monitoring
        formula = "{Monitor Activity} = TRUE()"
        all_marked_leads = self.leads_table.all(formula=formula)
        
        # Filter by ICP score
        filtered_leads = []
        skipped_low_icp = 0
        skipped_high_icp = 0
        included_by_company_icp = 0
        
        for lead in all_marked_leads:
            fields = lead['fields']
            lead_icp = fields.get('Lead ICP Score', 0) or fields.get('Lead ICP Fit Score', 0) or 0
            company_icp = 0
            
            # Get company ICP
            if 'Company' in fields:
                try:
                    company = self.companies_table.get(fields['Company'][0])
                    company_icp = company['fields'].get('ICP Fit Score', 0) or 0
                except:
                    pass
            
            # Check if lead qualifies based on Lead ICP
            if lead_icp >= min_icp:
                if max_icp is None or lead_icp < max_icp:
                    filtered_leads.append(lead)
                else:
                    skipped_high_icp += 1  # Will be handled by higher-tier run
            # Check if lead qualifies based on Company ICP (for monthly company ICP run)
            elif include_company_icp and company_icp >= 50 and lead_icp < 30:
                # Only include if lead ICP is below 30 (not covered by other tiers)
                filtered_leads.append(lead)
                included_by_company_icp += 1
            else:
                skipped_low_icp += 1
        
        if limit:
            filtered_leads = filtered_leads[:limit]
        
        tier_desc = f"Lead ICP {min_icp}-{max_icp if max_icp else '100'}"
        if include_company_icp:
            tier_desc += " OR Company ICP >= 50"
        
        logger.info(f"Found {len(filtered_leads)} leads for monitoring ({tier_desc})")
        if skipped_low_icp > 0:
            logger.info(f"  Skipped {skipped_low_icp} leads below threshold")
        if skipped_high_icp > 0:
            logger.info(f"  Skipped {skipped_high_icp} leads above max (handled by higher tier)")
        if included_by_company_icp > 0:
            logger.info(f"  Included {included_by_company_icp} leads by Company ICP")
        
        return filtered_leads
    
    def get_company_context(self, company_ids: list) -> Optional[Dict]:
        """Get company information for context"""
        if not company_ids:
            return None
        
        try:
            company = self.companies_table.get(company_ids[0])
            return company['fields']
        except:
            return None
    
    def monitor_lead_activity(self, lead_name: str, title: str = "", company_name: str = "",
                             linkedin_url: str = "", x_profile: str = "", 
                             last_monitored: str = None, company_context: Dict = None,
                             lookback_days: int = 14) -> Dict:
        """Monitor lead's recent activity using AI with web search
        
        Args:
            lookback_days: Maximum days to look back for news/activity.
                          - 7 for weekly (high-priority leads)
                          - 14 for bi-weekly (medium-priority)
                          - 30 for monthly (lower-priority)
        """
        
        # Calculate monitoring period based on lookback_days
        # If last_monitored is set and more recent than lookback, use that instead
        if last_monitored:
            try:
                last_date = datetime.strptime(last_monitored, '%Y-%m-%d')
                days_since = (datetime.now() - last_date).days
                # Use the smaller of days_since or lookback_days
                actual_lookback = min(days_since, lookback_days)
                if actual_lookback <= 0:
                    actual_lookback = lookback_days
            except:
                actual_lookback = lookback_days
        else:
            actual_lookback = lookback_days
        
        period = f"last {actual_lookback} days"
        cutoff_date = (datetime.now() - timedelta(days=actual_lookback)).strftime('%Y-%m-%d')
        
        context_info = f"""
Lead: {lead_name}
Title: {title or 'Unknown'}
Company: {company_name or 'Unknown'}
LinkedIn: {linkedin_url or 'Not provided'}
X Profile: {x_profile or 'Not provided'}
Monitoring Period: {period} (since {cutoff_date})
"""
        
        if company_context:
            context_info += f"""
Company Context:
- Focus: {', '.join(company_context.get('Focus Area', []))}
- Stage: {', '.join(company_context.get('Pipeline Stage', []))}
- Recent Funding: {company_context.get('Latest Funding Round', 'Unknown')}
"""
        
        # Add our conference list for matching
        our_conferences = self.get_our_conferences()
        if our_conferences:
            context_info += f"\n{our_conferences}\n"
            context_info += "PRIORITY: If this lead is attending any of OUR conferences, flag as HIGH priority trigger!\n"
        
        surveillance_prompt = f"""You are a business intelligence analyst monitoring professional activity in the biologics/pharma industry.

{context_info}

Your mission: Monitor this lead's recent activity and identify relevant updates and trigger events for business development outreach.

═══════════════════════════════════════════════════════════════════
CRITICAL DATE FILTERING - READ CAREFULLY
═══════════════════════════════════════════════════════════════════
Today's date: {datetime.now().strftime('%Y-%m-%d')}
Cutoff date: {cutoff_date}
Lookback period: {actual_lookback} days

STRICT RULES:
1. ONLY report information dated {cutoff_date} or later
2. COMPLETELY IGNORE anything before {cutoff_date}
3. If you find news from December 2025 or earlier January 2026, CHECK THE DATE
4. If the date is before {cutoff_date}, DO NOT INCLUDE IT
5. For EVERY piece of information, verify its date is >= {cutoff_date}

Examples of what to EXCLUDE (if cutoff is {cutoff_date}):
- News article from 2025-12-15 → EXCLUDE (too old)
- LinkedIn post from 2025-12-28 → EXCLUDE (too old) 
- Funding announcement from early January → CHECK DATE, exclude if before {cutoff_date}

Examples of what to INCLUDE:
- News from {cutoff_date} or later → INCLUDE
- Future conferences (any date in the future) → INCLUDE
═══════════════════════════════════════════════════════════════════

TECHNOLOGY FILTERING:
- ONLY include mammalian cell culture biologics (mAbs, bispecific antibodies, ADCs, Fc-fusion proteins)
- EXCLUDE: Cell therapy, gene therapy, viral vectors, plasmids, mRNA, oligonucleotides, vaccines
- If in doubt about technology platform, CHECK the company's pipeline/technology

SEARCH for activity ONLY from {cutoff_date} onwards:

📱 LINKEDIN ACTIVITY (posts dated {cutoff_date} or later):
- Recent posts and their themes
- Job changes, promotions, role updates
- Company announcements they've shared
- Who they're engaging with

🐦 X (TWITTER) ACTIVITY (tweets dated {cutoff_date} or later):
- Recent tweets and topics
- Tone and sentiment
- Industry discussions
- Conference mentions

📅 CONFERENCES & EVENTS:
- ONLY upcoming/future conferences (starting {datetime.now().strftime('%Y-%m-%d')} or later)
- Upcoming speaking engagements
- Include specific dates and verify they are IN THE FUTURE

🏢 COMPANY NEWS (dated {cutoff_date} or later):
- Funding announcements
- Pipeline updates (IND filings, trial results, approvals)
- Partnership announcements
- Hiring activity in CMC/manufacturing roles

🎯 TRIGGER EVENTS (High Priority):
Identify actionable trigger events using ONLY these types:
- FUNDING: Company raised money (need manufacturing scale-up)
- PROMOTION: Got promoted to new role with decision authority
- JOB_CHANGE: Started new job at different company
- SPEAKING: Confirmed for upcoming conference presentation
- CONFERENCE_ATTENDANCE: Attending (not speaking) at upcoming conference
- HIRING: Posted CMC/manufacturing jobs (capacity issues)
- PIPELINE: IND filed, Phase transition, positive trial results
- PARTNERSHIP: Announced CDMO partnership (competitive intel)
- AWARD: Won award or recognition
- PAIN_POINT: Discussed challenges in manufacturing/CMC
- OTHER: Any other trigger that doesn't fit above categories

IMPORTANT: 
- Use ONLY the exact type names listed above. Do not create new categories like "PIPELINE_MILESTONE" or "COMPANY_SUCCESS" - map them to the closest match from the list.
- For EACH trigger event, include the source URLs where you found this information (LinkedIn post URL, news article, press release, conference website, etc.)

💡 BEHAVIORAL INSIGHTS:
- Activity level changes (more/less active than before)
- Topic shifts (new interests or concerns)
- Engagement patterns (who they interact with)
- Sentiment changes (positive/negative tone)

BEFORE RETURNING RESULTS - DATE VALIDATION:
1. For EVERY item with a date, verify the date is {cutoff_date} or later
2. If NO activity found since {cutoff_date}, return empty arrays
3. Do NOT include old news just to have something to report
4. It's better to report "no recent activity" than to include outdated information

Return your findings in this exact JSON format:
{{
  "linkedin_activity": {{
    "posts_count": 0,
    "recent_posts": [
      {{"date": "YYYY-MM-DD", "topic": "...", "engagement": "...", "key_quote": "..."}}
    ],
    "themes": [],
    "engagement_level": "None/Low/Medium/High",
    "notable_interactions": []
  }},
  "x_activity": {{
    "active": false,
    "tweets_count": 0,
    "recent_tweets": [],
    "themes": [],
    "tone": "N/A"
  }},
  "conferences_events": [
    {{
      "event": "Conference Name", 
      "date": "YYYY-MM-DD (must be future)", 
      "location": "City, Country",
      "role": "Speaker/Attendee/Panelist", 
      "topic": "Topic if speaking"
    }}
  ],
  "company_news": [
    {{"date": "YYYY-MM-DD (must be >= {cutoff_date})", "headline": "...", "relevance": "..."}}
  ],
  "trigger_events": [
    {{
      "type": "FUNDING/PROMOTION/SPEAKING/CONFERENCE_ATTENDANCE/HIRING/PIPELINE/PARTNERSHIP/AWARD/PAIN_POINT/JOB_CHANGE",
      "date": "YYYY-MM-DD (must be >= {cutoff_date})",
      "description": "Detailed description",
      "urgency": "HIGH/MEDIUM/LOW",
      "outreach_angle": "How to leverage this for outreach",
      "timing_recommendation": "...",
      "sources": ["https://url1.com"]
    }}
  ],
  "behavioral_insights": {{
    "activity_change": "Description or 'No recent activity detected'",
    "topic_shifts": [],
    "sentiment": "Unknown - insufficient recent data",
    "engagement_patterns": "..."
  }},
  "summary": "2-3 sentence summary. If no activity since {cutoff_date}, say 'No significant activity detected in the past {actual_lookback} days.'",
  "recommendation": "HIGH PRIORITY / MEDIUM / LOW / NO ACTION - No recent activity",
  "cutoff_date_used": "{cutoff_date}",
  "data_quality": "High/Medium/Low/None - None if no recent activity found"
}}

CRITICAL REMINDERS:
- If you find NO activity since {cutoff_date}, return empty arrays and state "No recent activity"
- Do NOT pad results with old information
- Every date in your response MUST be >= {cutoff_date} (except future conference dates)
- Focus on ACTIONABLE, RECENT intelligence only

Only return valid JSON, no other text."""

        try:
            logger.info(f"  Monitoring activity with AI + web search...")
            
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
                    "content": surveillance_prompt
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
                activity_data = json.loads(result_text)
                
                # POST-PROCESSING: Validate and filter dates
                activity_data = self._filter_by_cutoff_date(activity_data, cutoff_date)
                
                logger.info(f"  ✓ Activity monitoring complete")
                return activity_data
            except json.JSONDecodeError as json_err:
                logger.error(f"  ✗ JSON parsing failed: {str(json_err)}")
                logger.error(f"  First 200 chars: {result_text[:200]}")
                raise
            
        except Exception as e:
            logger.error(f"  ✗ Error monitoring activity: {str(e)}")
            return {
                "error": str(e),
                "summary": "Monitoring failed due to technical error",
                "recommendation": "RETRY",
                "last_updated": datetime.now().strftime('%Y-%m-%d')
            }
    
    def _filter_by_cutoff_date(self, activity_data: Dict, cutoff_date: str) -> Dict:
        """Filter out any items with dates before the cutoff date.
        
        This is a safety net in case the AI includes old information despite instructions.
        """
        try:
            cutoff = datetime.strptime(cutoff_date, '%Y-%m-%d')
            today = datetime.now()
            filtered_count = 0
            
            def is_valid_date(date_str: str, allow_future: bool = False) -> bool:
                """Check if date is >= cutoff (and optionally in future for conferences)"""
                if not date_str:
                    return False
                try:
                    # Handle various date formats
                    date_str_clean = date_str.strip()
                    
                    # Try YYYY-MM-DD format
                    if re.match(r'^\d{4}-\d{2}-\d{2}$', date_str_clean):
                        parsed = datetime.strptime(date_str_clean, '%Y-%m-%d')
                    # Try DD.MM.YYYY format
                    elif re.match(r'^\d{1,2}\.\d{1,2}\.\d{4}$', date_str_clean):
                        parsed = datetime.strptime(date_str_clean, '%d.%m.%Y')
                    # Try "Month YYYY" format (e.g., "March 2026")
                    elif re.match(r'^[A-Za-z]+ \d{4}$', date_str_clean):
                        parsed = datetime.strptime(date_str_clean, '%B %Y')
                    # Try "DD Month YYYY" format
                    elif re.match(r'^\d{1,2} [A-Za-z]+ \d{4}$', date_str_clean):
                        parsed = datetime.strptime(date_str_clean, '%d %B %Y')
                    else:
                        # Can't parse - be lenient and include it
                        return True
                    
                    if allow_future:
                        # For conferences, only check it's in the future
                        return parsed >= today
                    else:
                        # For news/posts, must be >= cutoff
                        return parsed >= cutoff
                        
                except (ValueError, TypeError):
                    # If we can't parse the date, include it (benefit of doubt)
                    return True
            
            # Filter LinkedIn posts
            if 'linkedin_activity' in activity_data and activity_data['linkedin_activity']:
                linkedin = activity_data['linkedin_activity']
                if 'recent_posts' in linkedin and linkedin['recent_posts']:
                    original_count = len(linkedin['recent_posts'])
                    linkedin['recent_posts'] = [
                        p for p in linkedin['recent_posts'] 
                        if is_valid_date(p.get('date', ''))
                    ]
                    filtered_count += original_count - len(linkedin['recent_posts'])
                    linkedin['posts_count'] = len(linkedin['recent_posts'])
            
            # Filter X/Twitter activity
            if 'x_activity' in activity_data and activity_data['x_activity']:
                x_activity = activity_data['x_activity']
                if 'recent_tweets' in x_activity and x_activity['recent_tweets']:
                    original_count = len(x_activity['recent_tweets'])
                    x_activity['recent_tweets'] = [
                        t for t in x_activity['recent_tweets']
                        if is_valid_date(t.get('date', ''))
                    ]
                    filtered_count += original_count - len(x_activity['recent_tweets'])
                    x_activity['tweets_count'] = len(x_activity['recent_tweets'])
            
            # Filter company news
            if 'company_news' in activity_data and activity_data['company_news']:
                original_count = len(activity_data['company_news'])
                activity_data['company_news'] = [
                    n for n in activity_data['company_news']
                    if is_valid_date(n.get('date', ''))
                ]
                filtered_count += original_count - len(activity_data['company_news'])
            
            # Filter conferences (must be in future, not just >= cutoff)
            if 'conferences_events' in activity_data and activity_data['conferences_events']:
                original_count = len(activity_data['conferences_events'])
                activity_data['conferences_events'] = [
                    c for c in activity_data['conferences_events']
                    if is_valid_date(c.get('date', ''), allow_future=True)
                ]
                filtered_count += original_count - len(activity_data['conferences_events'])
            
            # Filter trigger events - CRITICAL
            if 'trigger_events' in activity_data and activity_data['trigger_events']:
                original_count = len(activity_data['trigger_events'])
                valid_triggers = []
                for trigger in activity_data['trigger_events']:
                    trigger_date = trigger.get('date', '')
                    trigger_type = trigger.get('type', '').upper()
                    
                    # For conferences/speaking, check if the event is in the future
                    if trigger_type in ['SPEAKING', 'CONFERENCE_ATTENDANCE']:
                        if is_valid_date(trigger_date, allow_future=True):
                            valid_triggers.append(trigger)
                        else:
                            logger.warning(f"    Filtered out old {trigger_type} trigger: {trigger_date}")
                    else:
                        # For other triggers, must be >= cutoff
                        if is_valid_date(trigger_date, allow_future=False):
                            valid_triggers.append(trigger)
                        else:
                            logger.warning(f"    Filtered out old {trigger_type} trigger: {trigger_date}")
                
                activity_data['trigger_events'] = valid_triggers
                filtered_count += original_count - len(valid_triggers)
            
            if filtered_count > 0:
                logger.info(f"  ⚠ Filtered {filtered_count} items with dates before {cutoff_date}")
            
            return activity_data
            
        except Exception as e:
            logger.warning(f"  Date filtering failed: {e} - returning unfiltered data")
            return activity_data
    
    def format_activity_report(self, activity_data: Dict) -> str:
        """Format activity data into readable report for Airtable"""
        
        output = []
        output.append("=" * 60)
        output.append("ACTIVITY SURVEILLANCE REPORT")
        output.append(f"Generated: {activity_data.get('last_updated', 'Unknown')}")
        output.append(f"Recommendation: {activity_data.get('recommendation', 'Unknown')}")
        output.append("=" * 60)
        output.append("")
        
        # Executive Summary
        output.append("📋 EXECUTIVE SUMMARY")
        output.append("-" * 60)
        output.append(activity_data.get('summary', 'No summary available'))
        output.append("")
        
        # Trigger Events (Most Important!)
        trigger_events = activity_data.get('trigger_events', [])
        if trigger_events:
            output.append("🎯 TRIGGER EVENTS (Action Required!)")
            output.append("-" * 60)
            for event in trigger_events:
                urgency = event.get('urgency', 'UNKNOWN')
                event_type = event.get('type', 'Unknown')
                output.append(f"[{urgency}] {event_type}")
                output.append(f"  Date: {event.get('date', 'Unknown')}")
                output.append(f"  Details: {event.get('description', 'No details')}")
                output.append(f"  Outreach Angle: {event.get('outreach_angle', 'TBD')}")
                output.append(f"  Timing: {event.get('timing_recommendation', 'TBD')}")
                
                # Add sources if available
                sources = event.get('sources', [])
                if sources:
                    output.append(f"  Sources:")
                    for source in sources[:3]:  # Show up to 3 sources
                        output.append(f"    • {source}")
                
                output.append("")
        else:
            output.append("🎯 TRIGGER EVENTS")
            output.append("-" * 60)
            output.append("No significant trigger events detected")
            output.append("")
        
        # LinkedIn Activity
        linkedin = activity_data.get('linkedin_activity', {})
        if linkedin and linkedin.get('posts_count', 0) > 0:
            output.append("📱 LINKEDIN ACTIVITY")
            output.append("-" * 60)
            output.append(f"Posts: {linkedin.get('posts_count', 0)}")
            output.append(f"Engagement Level: {linkedin.get('engagement_level', 'Unknown')}")
            output.append(f"Themes: {', '.join(linkedin.get('themes', []))}")
            
            recent_posts = linkedin.get('recent_posts', [])
            if recent_posts:
                output.append("\nRecent Posts:")
                for post in recent_posts[:3]:
                    output.append(f"  • {post.get('date', '')}: {post.get('topic', 'Unknown topic')}")
            output.append("")
        
        # X Activity
        x_activity = activity_data.get('x_activity', {})
        if x_activity and x_activity.get('active'):
            output.append("🐦 X (TWITTER) ACTIVITY")
            output.append("-" * 60)
            output.append(f"Tweets: {x_activity.get('tweets_count', 0)}")
            output.append(f"Tone: {x_activity.get('tone', 'Unknown')}")
            output.append(f"Themes: {', '.join(x_activity.get('themes', []))}")
            output.append("")
        
        # Conferences & Events
        conferences = activity_data.get('conferences_events', [])
        if conferences:
            output.append("📅 CONFERENCES & EVENTS")
            output.append("-" * 60)
            for conf in conferences:
                event_name = conf.get('event', 'Unknown')
                role = conf.get('role', 'Attendee')
                
                # Highlight speaking vs attending
                if 'speaker' in role.lower() or 'panelist' in role.lower():
                    output.append(f"  🎤 {event_name} [SPEAKING]")
                else:
                    output.append(f"  📍 {event_name} [ATTENDING]")
                
                output.append(f"    Date: {conf.get('date', 'TBD')}")
                if conf.get('location'):
                    output.append(f"    Location: {conf['location']}")
                output.append(f"    Role: {role}")
                
                if conf.get('topic'):
                    output.append(f"    Topic: {conf['topic']}")
                
                if conf.get('booth_visiting'):
                    output.append(f"    Visiting: {conf['booth_visiting']}")
                
                if conf.get('networking_intent'):
                    output.append(f"    Intent: {conf['networking_intent']}")
            output.append("")
        
        # Company News
        company_news = activity_data.get('company_news', [])
        if company_news:
            output.append("🏢 COMPANY NEWS")
            output.append("-" * 60)
            for news in company_news:
                output.append(f"  • {news.get('date', '')}: {news.get('headline', 'Unknown')}")
                if news.get('relevance'):
                    output.append(f"    Relevance: {news['relevance']}")
            output.append("")
        
        # Behavioral Insights
        insights = activity_data.get('behavioral_insights', {})
        if insights:
            output.append("💡 BEHAVIORAL INSIGHTS")
            output.append("-" * 60)
            if insights.get('activity_change'):
                output.append(f"Activity: {insights['activity_change']}")
            if insights.get('sentiment'):
                output.append(f"Sentiment: {insights['sentiment']}")
            if insights.get('topic_shifts'):
                output.append(f"Topic Shifts: {', '.join(insights['topic_shifts'])}")
            output.append("")
        
        output.append("=" * 60)
        output.append(f"Data Quality: {activity_data.get('data_quality', 'Unknown')}")
        output.append("=" * 60)
        
        return '\n'.join(output)
    
    def _generate_trigger_outreach(self, lead_record_id: str, lead_name: str,
                                   trigger_event: Dict, company_name: str = "") -> Optional[Dict]:
        """Generate trigger-specific outreach messages matching enrich flow tone/style"""
        
        trigger_type = trigger_event.get('type', 'OTHER')
        description = trigger_event.get('description', '')
        outreach_angle = trigger_event.get('outreach_angle', '')
        
        # Get lead info and filtered company data
        lead_title = ""
        company_fields = {}
        do_not_mention_text = ""
        try:
            lead_record = self.leads_table.get(lead_record_id)
            lead_title = lead_record['fields'].get('Title', '')
            if not company_name:
                company_ids = lead_record['fields'].get('Company', [])
                if company_ids:
                    raw_fields = self.companies_table.get(company_ids[0])['fields']
                    company_fields, suppressed = filter_by_confidence(raw_fields)
                    company_name = company_fields.get('Company Name', '')
                    do_not_mention_text = suppressed_to_do_not_mention(suppressed)
            else:
                # Company name was passed in — still try to get fields for value prop
                company_ids = lead_record['fields'].get('Company', [])
                if company_ids:
                    raw_fields = self.companies_table.get(company_ids[0])['fields']
                    company_fields, suppressed = filter_by_confidence(raw_fields)
                    do_not_mention_text = suppressed_to_do_not_mention(suppressed)
        except:
            pass
        
        # Build value proposition and philosophy
        value_prop = build_value_proposition(self.company_profile, company_fields, lead_title, persona_messaging=self.persona_messaging)
        outreach_rules = build_outreach_philosophy()
        
        prompt = f"""Generate trigger-specific outreach messages based on this intelligence.

TRIGGER EVENT:
Type: {trigger_type}
Description: {description}
Suggested Angle: {outreach_angle}

LEAD:
Name: {lead_name}
Title: {lead_title}
Company: {company_name}
{do_not_mention_text}

{value_prop}

{outreach_rules}

Lead with the trigger — this event is WHY you're reaching out NOW.

Generate 3 messages:

1. EMAIL (100-120 words):
Subject: [Natural subject referencing the trigger]
- Reference the trigger naturally (NOT "Congratulations on...")
- Connect ONE Rezon strength to their situation
- Soft CTA
- Sign: "Best regards, [Your Name], Rezon Bio Business Development"

2. LINKEDIN CONNECTION REQUEST (under 200 chars):
- Brief, friendly, reference the trigger or their role
- Why you'd like to connect

3. LINKEDIN SHORT MESSAGE (under 300 chars):
- Reference the trigger conversationally
- Brief mention of relevance
- End: "Best regards, [Your Name]"

Return ONLY valid JSON:
{{
    "email_subject": "Subject line",
    "email_body": "Full email body",
    "linkedin_connection": "Connection request text",
    "linkedin_short": "Short message text"
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
                logger.warning(f"    Empty response from API")
                return None
            
            # Try to extract JSON
            json_str = None
            if "```json" in response_text:
                json_str = response_text.split("```json")[1].split("```")[0]
            elif "```" in response_text and "{" in response_text:
                # Handle ```\n{...}\n```
                json_str = response_text.split("```")[1].split("```")[0]
            elif "{" in response_text:
                start = response_text.find("{")
                end = response_text.rfind("}") + 1
                json_str = response_text[start:end]
            
            if not json_str:
                logger.warning(f"    No JSON found in response")
                logger.debug(f"    Response: {response_text[:300]}")
                return None
            
            # Clean up common issues
            json_str = json_str.strip()
            # Remove trailing commas before } or ]
            json_str = re.sub(r',(\s*[}\]])', r'\1', json_str)
            
            return json.loads(json_str)
            
        except json.JSONDecodeError as je:
            logger.warning(f"    JSON parse error in trigger outreach: {je}")
            logger.debug(f"    Response was: {response_text[:500] if response_text else 'empty'}")
            return None
        except Exception as e:
            logger.warning(f"    Error generating trigger outreach: {e}")
            return None
    
    def log_trigger_to_history(self, lead_record_id: str, lead_name: str, 
                               company_record_id: str, trigger_event: Dict, 
                               sources: list = None, company_name: str = ""):
        """Log a trigger event to the Trigger History table for permanent record"""
        
        try:
            history_record = {
                'Date Detected': datetime.now().strftime('%Y-%m-%d'),
                'Lead': [lead_record_id],  # Link to lead
                'Trigger Type': trigger_event.get('type', 'OTHER'),
                'Trigger Source': 'Lead Monitoring',  # New standardized field
                'Urgency': trigger_event.get('urgency', 'MEDIUM'),
                'Description': trigger_event.get('description', ''),
                'Outreach Angle': trigger_event.get('outreach_angle', ''),
                'Timing Recommendation': trigger_event.get('timing_recommendation', ''),
                'Event Date': trigger_event.get('date', ''),
                'Status': 'New'  # Options: New, In Progress, Contacted, Completed
            }
            
            # Add company link if available
            if company_record_id:
                history_record['Company'] = [company_record_id]
            
            # Add sources if available
            if sources:
                sources_text = '\n'.join([f"• {source}" for source in sources[:5]])  # Limit to 5 sources
                history_record['Sources'] = sources_text
            elif trigger_event.get('source'):
                history_record['Sources'] = trigger_event.get('source')
            elif trigger_event.get('sources'):
                sources_list = trigger_event.get('sources', [])
                if sources_list:
                    sources_text = '\n'.join([f"• {s}" for s in sources_list[:5]])
                    history_record['Sources'] = sources_text
            
            # Generate outreach messages for this trigger
            logger.info(f"    Generating trigger outreach messages...")
            outreach = self._generate_trigger_outreach(
                lead_record_id=lead_record_id,
                lead_name=lead_name,
                trigger_event=trigger_event,
                company_name=company_name
            )
            
            if outreach:
                if outreach.get('email_subject'):
                    history_record['Email Subject'] = outreach['email_subject']
                if outreach.get('email_body'):
                    history_record['Email Body'] = outreach['email_body']
                if outreach.get('linkedin_connection'):
                    history_record['LinkedIn Connection Request'] = outreach['linkedin_connection']
                if outreach.get('linkedin_short'):
                    history_record['LinkedIn Short Message'] = outreach['linkedin_short']
                history_record['Outreach Generated Date'] = datetime.now().strftime('%Y-%m-%d')
                logger.info(f"    ✓ Outreach messages generated")
            else:
                logger.warning(f"    ⚠ Could not generate outreach messages")
            
            self.trigger_history_table.create(history_record)
            logger.info(f"    ✓ Logged to Trigger History")
            
        except Exception as e:
            logger.warning(f"    Could not log to Trigger History: {str(e)}")
    
    def update_lead_activity(self, lead_record_id: str, activity_data: Dict, 
                            company_record_id: str = None):
        """Update Airtable lead record with activity report"""
        
        formatted_report = self.format_activity_report(activity_data)
        
        # Prepare update fields
        update_fields = {
            'Last Monitored': datetime.now().strftime('%Y-%m-%d')
        }
        
        # Add activity log (append mode)
        try:
            existing_log = self.leads_table.get(lead_record_id)['fields'].get('Activity Log', '')
            separator = f"\n\n{'='*60}\n"
            update_fields['Activity Log'] = existing_log + separator + formatted_report
        except:
            update_fields['Activity Log'] = formatted_report
        
        # Flag trigger events if any
        trigger_events = activity_data.get('trigger_events', [])
        if trigger_events:
            high_priority = [e for e in trigger_events if e.get('urgency') == 'HIGH']
            if high_priority:
                update_fields['Has Trigger Event'] = True
                
                # Store trigger summary
                trigger_summary = '\n'.join([
                    f"[{e.get('urgency')}] {e.get('type')}: {e.get('description', '')[:100]}"
                    for e in trigger_events
                ])
                update_fields['Trigger Events'] = trigger_summary
                
                # Categorize trigger types with strict validation
                # Only use exact matches from the predefined list
                valid_categories = {
                    'FUNDING', 'PROMOTION', 'SPEAKING', 'CONFERENCE_ATTENDANCE',
                    'HIRING', 'PIPELINE', 'PARTNERSHIP', 'AWARD', 
                    'PAIN_POINT', 'JOB_CHANGE', 'OTHER'
                }
                
                trigger_types_raw = [e.get('type', 'OTHER') for e in trigger_events]
                # Map variations to valid categories
                trigger_types = []
                for t in trigger_types_raw:
                    t_upper = t.upper()
                    # Direct match
                    if t_upper in valid_categories:
                        trigger_types.append(t_upper)
                    # Map common variations
                    elif 'PIPELINE' in t_upper or 'MILESTONE' in t_upper:
                        trigger_types.append('PIPELINE')
                    elif 'CONFERENCE' in t_upper and 'ATTEND' in t_upper:
                        trigger_types.append('CONFERENCE_ATTENDANCE')
                    elif 'CONFERENCE' in t_upper or 'SPEAKING' in t_upper:
                        trigger_types.append('SPEAKING')
                    elif 'FUND' in t_upper or 'CAPITAL' in t_upper or 'INVESTMENT' in t_upper:
                        trigger_types.append('FUNDING')
                    elif 'HIRE' in t_upper or 'HIRING' in t_upper or 'RECRUIT' in t_upper:
                        trigger_types.append('HIRING')
                    elif 'JOB' in t_upper or 'PROMOTION' in t_upper or 'ROLE' in t_upper:
                        trigger_types.append('JOB_CHANGE')
                    elif 'PARTNER' in t_upper:
                        trigger_types.append('PARTNERSHIP')
                    elif 'AWARD' in t_upper or 'RECOGNITION' in t_upper:
                        trigger_types.append('AWARD')
                    elif 'PAIN' in t_upper or 'CHALLENGE' in t_upper or 'PROBLEM' in t_upper:
                        trigger_types.append('PAIN_POINT')
                    else:
                        trigger_types.append('OTHER')
                
                # Remove duplicates and store
                trigger_types = list(set(trigger_types))
                update_fields['Trigger Categories'] = trigger_types
            
            # Log each trigger to history table for permanent record
            lead_name = ""
            company_name = ""
            try:
                lead_record = self.leads_table.get(lead_record_id)
                lead_name = lead_record['fields'].get('Lead Name', 'Unknown')
                # Get company name
                if company_record_id:
                    try:
                        company_record = self.companies_table.get(company_record_id)
                        company_name = company_record['fields'].get('Company Name', '')
                    except:
                        pass
            except:
                pass
            
            # Get overall sources from the activity data
            overall_sources = activity_data.get('sources', [])
            
            for trigger_event in trigger_events:
                # Use trigger-specific sources if available, otherwise use overall sources
                trigger_sources = trigger_event.get('sources', overall_sources)
                
                self.log_trigger_to_history(
                    lead_record_id=lead_record_id,
                    lead_name=lead_name,
                    company_record_id=company_record_id,
                    trigger_event=trigger_event,
                    sources=trigger_sources,
                    company_name=company_name
                )
        else:
            # Clear trigger fields if no events
            update_fields['Has Trigger Event'] = False
            update_fields['Trigger Events'] = None
            update_fields['Trigger Categories'] = []
        
        # Update the record
        try:
            self.leads_table.update(lead_record_id, update_fields)
            logger.info(f"  ✓ Activity report saved to Airtable")
        except Exception as e:
            logger.error(f"  ✗ Failed to update Airtable: {str(e)}")
            raise
    
    def monitor_leads_batch(self, limit: Optional[int] = None, min_icp: int = 50, 
                           max_icp: int = None, lookback_days: int = 14,
                           include_company_icp: bool = False, tier_name: str = None):
        """Monitor all leads marked for surveillance
        
        Args:
            limit: Max number of leads to process
            min_icp: Minimum Lead ICP score to monitor
            max_icp: Maximum Lead ICP score (exclusive) - for tiered monitoring
            lookback_days: How many days back to search for news/activity
            include_company_icp: Also include leads with Company ICP >= 50
            tier_name: Name for logging (e.g., "WEEKLY HIGH-PRIORITY")
        
        Tiered monitoring schedule:
            Weekly (ICP 75+): lookback_days=7
            Bi-weekly (ICP 50-74): lookback_days=14
            Monthly (ICP 30-49): lookback_days=30
            Monthly Company ICP (Lead ICP <30, Company ICP 50+): lookback_days=30
        """
        
        tier_display = tier_name or f"ICP {min_icp}-{max_icp if max_icp else '100'}"
        
        logger.info("=" * 60)
        logger.info(f"LEAD SURVEILLANCE - {tier_display}")
        logger.info(f"ICP Range: {min_icp} - {max_icp if max_icp else '100'}")
        logger.info(f"Lookback Period: {lookback_days} days")
        if include_company_icp:
            logger.info(f"Including leads with Company ICP >= 50")
        logger.info("=" * 60)
        
        leads = self.get_leads_for_monitoring(
            limit=limit, 
            min_icp=min_icp, 
            max_icp=max_icp,
            include_company_icp=include_company_icp
        )
        
        if not leads:
            logger.info("✓ No leads to monitor in this tier")
            return True
        
        total = len(leads)
        success_count = 0
        failed_count = 0
        trigger_count = 0
        
        for idx, lead in enumerate(leads, 1):
            fields = lead['fields']
            lead_name = fields.get('Lead Name', 'Unknown')
            record_id = lead['id']
            
            logger.info("")
            logger.info(f"[{idx}/{total}] Monitoring: {lead_name}")
            
            title = fields.get('Title', '')
            linkedin_url = fields.get('LinkedIn URL', '')
            x_profile = fields.get('X Profile', '')
            last_monitored = fields.get('Last Monitored')
            
            # Get company context
            company_name = "Unknown"
            company_context = None
            if 'Company' in fields:
                company_context = self.get_company_context(fields['Company'])
                if company_context:
                    company_name = company_context.get('Company Name', 'Unknown')
            
            logger.info(f"  Company: {company_name}")
            logger.info(f"  Last monitored: {last_monitored or 'Never'}")
            logger.info(f"  Lookback: {lookback_days} days")
            
            try:
                # Monitor activity with tier-specific lookback
                activity_data = self.monitor_lead_activity(
                    lead_name=lead_name,
                    title=title,
                    company_name=company_name,
                    linkedin_url=linkedin_url,
                    x_profile=x_profile,
                    last_monitored=last_monitored,
                    company_context=company_context,
                    lookback_days=lookback_days
                )
                
                if activity_data.get('error'):
                    logger.error(f"  ✗ Monitoring failed: {activity_data['error']}")
                    failed_count += 1
                    continue
                
                # Save to Airtable
                logger.info("  Saving activity report...")
                
                # Get company record ID for linking
                company_record_id = None
                if 'Company' in fields and fields['Company']:
                    company_record_id = fields['Company'][0]
                
                self.update_lead_activity(record_id, activity_data, company_record_id)
                
                # Check for triggers
                trigger_events = activity_data.get('trigger_events', [])
                if trigger_events:
                    high_priority = [e for e in trigger_events if e.get('urgency') == 'HIGH']
                    logger.info(f"  🎯 Found {len(trigger_events)} trigger events ({len(high_priority)} high priority)")
                    trigger_count += len(trigger_events)
                
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
        logger.info("SURVEILLANCE COMPLETE")
        logger.info(f"Total: {total} | Success: {success_count} | Failed: {failed_count}")
        logger.info(f"Trigger Events Found: {trigger_count}")
        logger.info("=" * 60)
        
        return failed_count == 0


def main():
    """CLI entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Monitor lead activity across platforms')
    parser.add_argument('--limit', type=int, help='Max number of leads to monitor')
    parser.add_argument('--min-icp', type=int, default=50, 
                       help='Minimum Lead ICP score to monitor (default: 50)')
    parser.add_argument('--max-icp', type=int, default=None,
                       help='Maximum Lead ICP score (exclusive) for tiered monitoring')
    parser.add_argument('--lookback-days', type=int, default=14,
                       help='Days to look back for news/activity (default: 14)')
    parser.add_argument('--include-company-icp', action='store_true',
                       help='Also include leads with Company ICP >= 50 (for monthly run)')
    parser.add_argument('--tier', choices=['weekly', 'biweekly', 'monthly', 'monthly-company'],
                       help='Preset tier configuration')
    parser.add_argument('--config', default='config.yaml', help='Path to config file')
    
    args = parser.parse_args()
    
    # Apply tier presets
    if args.tier == 'weekly':
        # Weekly: ICP 75+, lookback 7 days
        args.min_icp = 75
        args.max_icp = None
        args.lookback_days = 7
        tier_name = "WEEKLY HIGH-PRIORITY (ICP 75+)"
    elif args.tier == 'biweekly':
        # Bi-weekly: ICP 50-74, lookback 14 days
        args.min_icp = 50
        args.max_icp = 75
        args.lookback_days = 14
        tier_name = "BI-WEEKLY MEDIUM-PRIORITY (ICP 50-74)"
    elif args.tier == 'monthly':
        # Monthly: ICP 30-49, lookback 30 days
        args.min_icp = 30
        args.max_icp = 50
        args.lookback_days = 30
        tier_name = "MONTHLY LOWER-PRIORITY (ICP 30-49)"
    elif args.tier == 'monthly-company':
        # Monthly Company ICP: Lead ICP <30 but Company ICP 50+, lookback 30 days
        args.min_icp = 0
        args.max_icp = 30
        args.lookback_days = 30
        args.include_company_icp = True
        tier_name = "MONTHLY COMPANY-ICP (Lead ICP <30, Company ICP 50+)"
    else:
        tier_name = None
    
    try:
        surveillance = LeadSurveillance(config_path=args.config)
        success = surveillance.monitor_leads_batch(
            limit=args.limit, 
            min_icp=args.min_icp,
            max_icp=args.max_icp,
            lookback_days=args.lookback_days,
            include_company_icp=args.include_company_icp,
            tier_name=tier_name
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
