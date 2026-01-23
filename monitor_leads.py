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
        
        self.anthropic_client = anthropic.Anthropic(
            api_key=self.config['anthropic']['api_key']
        )
        
        logger.info("LeadSurveillance initialized successfully")
    
    def get_leads_for_monitoring(self, limit: Optional[int] = None) -> List[Dict]:
        """Get all leads marked for surveillance monitoring"""
        
        # Get leads marked for monitoring
        formula = "{Monitor Activity} = TRUE()"
        all_marked_leads = self.leads_table.all(formula=formula)
        
        if limit:
            all_marked_leads = all_marked_leads[:limit]
        
        logger.info(f"Found {len(all_marked_leads)} leads marked for monitoring")
        return all_marked_leads
    
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
                             last_monitored: str = None, company_context: Dict = None) -> Dict:
        """Monitor lead's recent activity using AI with web search"""
        
        # Calculate monitoring period
        if last_monitored:
            try:
                last_date = datetime.strptime(last_monitored, '%Y-%m-%d')
                days_since = (datetime.now() - last_date).days
                period = f"last {days_since} days"
            except:
                period = "last 2 weeks"
        else:
            period = "last 2 weeks"
        
        context_info = f"""
Lead: {lead_name}
Title: {title or 'Unknown'}
Company: {company_name or 'Unknown'}
LinkedIn: {linkedin_url or 'Not provided'}
X Profile: {x_profile or 'Not provided'}
Monitoring Period: {period}
"""
        
        if company_context:
            context_info += f"""
Company Context:
- Focus: {', '.join(company_context.get('Focus Area', []))}
- Stage: {', '.join(company_context.get('Pipeline Stage', []))}
- Recent Funding: {company_context.get('Latest Funding Round', 'Unknown')}
"""
        
        surveillance_prompt = f"""You are a business intelligence analyst monitoring professional activity in the biologics/pharma industry.

{context_info}

Your mission: Monitor this lead's recent activity and identify relevant updates and trigger events for business development outreach.

IMPORTANT DATE RULES:
- Today's date is: {datetime.now().strftime('%Y-%m-%d')}
- Only report FUTURE conferences (starting today or later)
- Only report triggers from the last 3 months maximum
- Do NOT report past events or old news

SEARCH EXTENSIVELY for activity in the {period}:

📱 LINKEDIN ACTIVITY:
- Recent posts and their themes
- What topics are they discussing?
- Job changes, promotions, role updates
- Company announcements they've shared
- Who they're engaging with (comments, likes)
- New connections or endorsements

🐦 X (TWITTER) ACTIVITY (if they have X profile):
- Recent tweets and topics
- Tone and sentiment
- Industry discussions they're participating in
- Conference mentions or event attendance
- Retweets and engagement patterns

📅 CONFERENCES & EVENTS:
- ONLY upcoming/future conferences (not past events)
- Upcoming speaking engagements
- Conferences they'll attend (search conference websites)
- Panel discussions or presentations
- Networking events mentioned
- Include specific dates and verify they are in the future

🏢 COMPANY NEWS:
- Funding announcements
- Pipeline updates (IND filings, trial results, approvals)
- Partnership announcements
- Hiring activity in CMC/manufacturing roles
- Press releases mentioning the lead or company

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

IMPORTANT: Use ONLY the exact type names listed above. Do not create new categories like "PIPELINE_MILESTONE" or "COMPANY_SUCCESS" - map them to the closest match from the list.

💡 BEHAVIORAL INSIGHTS:
- Activity level changes (more/less active than before)
- Topic shifts (new interests or concerns)
- Engagement patterns (who they interact with)
- Sentiment changes (positive/negative tone)

Return your findings in this exact JSON format:
{{
  "linkedin_activity": {{
    "posts_count": 3,
    "recent_posts": [
      {{"date": "2026-01-20", "topic": "ADC manufacturing challenges", "engagement": "50+ likes", "key_quote": "Brief quote if relevant"}},
      ...
    ],
    "themes": ["CMC challenges", "Hiring", "Conference prep"],
    "engagement_level": "High/Medium/Low",
    "notable_interactions": ["Engaged with Lonza post about scale-up"]
  }},
  "x_activity": {{
    "active": true/false,
    "tweets_count": 5,
    "recent_tweets": [
      {{"date": "2026-01-21", "topic": "BIO conference", "content": "Brief summary"}},
      ...
    ],
    "themes": ["Conference attendance", "Industry trends"],
    "tone": "Professional/Casual/Technical"
  }},
  "conferences_events": [
    {{
      "event": "BIO-Europe Spring", 
      "date": "March 2025", 
      "location": "Barcelona, Spain",
      "role": "Speaker/Attendee/Panelist", 
      "topic": "Bispecific manufacturing (if speaking)",
      "booth_visiting": "List of CDMO booths they mentioned visiting (if any)",
      "networking_intent": "Any signals about networking goals"
    }},
    ...
  ],
  "company_news": [
    {{"date": "2026-01-15", "headline": "Series C funding $50M", "relevance": "Will need scale-up manufacturing"}},
    ...
  ],
  "trigger_events": [
    {{
      "type": "FUNDING/PROMOTION/SPEAKING/CONFERENCE_ATTENDANCE/HIRING/PIPELINE/PARTNERSHIP/AWARD/PAIN_POINT/JOB_CHANGE",
      "date": "2026-01-15",
      "description": "Detailed description",
      "urgency": "HIGH/MEDIUM/LOW",
      "outreach_angle": "How to leverage this for outreach",
      "timing_recommendation": "Reach out in next 2 weeks / Wait until after conference / etc"
    }},
    ...
  ],
  "behavioral_insights": {{
    "activity_change": "Increased by 50% vs previous period",
    "topic_shifts": ["Now focused more on regulatory vs technical"],
    "sentiment": "Positive - excited about company progress",
    "engagement_patterns": "Actively engaging with CDMO content"
  }},
  "summary": "2-3 sentence executive summary of key findings",
  "recommendation": "HIGH PRIORITY: Reach out now / MEDIUM: Monitor / LOW: No urgent action",
  "last_updated": "2026-01-23",
  "data_quality": "High/Medium/Low - how confident are you in these findings"
}}

CRITICAL: Focus on ACTIONABLE intelligence. Prioritize trigger events that create outreach opportunities. If there's no significant activity, say so clearly.

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
    
    def log_trigger_to_history(self, lead_record_id: str, lead_name: str, 
                               company_record_id: str, trigger_event: Dict):
        """Log a trigger event to the Trigger History table for permanent record"""
        
        try:
            history_record = {
                'Date Detected': datetime.now().strftime('%Y-%m-%d'),
                'Lead': [lead_record_id],  # Link to lead
                'Trigger Type': trigger_event.get('type', 'OTHER'),
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
            try:
                lead_record = self.leads_table.get(lead_record_id)
                lead_name = lead_record['fields'].get('Lead Name', 'Unknown')
            except:
                pass
            
            for trigger_event in trigger_events:
                self.log_trigger_to_history(
                    lead_record_id=lead_record_id,
                    lead_name=lead_name,
                    company_record_id=company_record_id,
                    trigger_event=trigger_event
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
    
    def monitor_leads_batch(self, limit: Optional[int] = None):
        """Monitor all leads marked for surveillance"""
        
        logger.info("=" * 60)
        logger.info("LEAD SURVEILLANCE - BATCH MONITORING")
        logger.info("=" * 60)
        
        leads = self.get_leads_for_monitoring(limit)
        
        if not leads:
            logger.info("✓ No leads marked for monitoring")
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
            
            try:
                # Monitor activity
                activity_data = self.monitor_lead_activity(
                    lead_name=lead_name,
                    title=title,
                    company_name=company_name,
                    linkedin_url=linkedin_url,
                    x_profile=x_profile,
                    last_monitored=last_monitored,
                    company_context=company_context
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
    parser.add_argument('--config', default='config.yaml', help='Path to config file')
    
    args = parser.parse_args()
    
    try:
        surveillance = LeadSurveillance(config_path=args.config)
        success = surveillance.monitor_leads_batch(limit=args.limit)
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
