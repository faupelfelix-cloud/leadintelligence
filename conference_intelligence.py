#!/usr/bin/env python3
"""
Conference Intelligence System
Monitors upcoming conferences and finds relevant attendees from ICP-fit companies
"""

import os
import sys
import yaml
import json
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import anthropic
from pyairtable import Api

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('conference_intelligence.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class ConferenceIntelligence:
    """Monitors conferences and finds relevant attendees"""
    
    def __init__(self, config_path: str = "config.yaml"):
        """Initialize with configuration"""
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        # Initialize APIs
        self.airtable = Api(self.config['airtable']['api_key'])
        self.base = self.airtable.base(self.config['airtable']['base_id'])
        self.conferences_table = self.base.table(self.config['airtable']['tables']['conferences'])
        self.leads_table = self.base.table(self.config['airtable']['tables']['leads'])
        self.companies_table = self.base.table(self.config['airtable']['tables']['companies'])
        self.trigger_history_table = self.base.table(self.config['airtable']['tables']['trigger_history'])
        
        self.anthropic_client = anthropic.Anthropic(
            api_key=self.config['anthropic']['api_key']
        )
        
        logger.info("ConferenceIntelligence initialized successfully")
    
    def get_conferences_to_monitor(self) -> List[Dict]:
        """Get conferences that should be monitored today"""
        today = datetime.now()
        
        # Get all conferences
        all_conferences = self.conferences_table.all()
        
        to_monitor = []
        for conf in all_conferences:
            fields = conf['fields']
            
            # Get conference date
            conf_date_str = fields.get('Conference Date')
            if not conf_date_str:
                continue
            
            try:
                conf_date = datetime.strptime(conf_date_str, '%Y-%m-%d')
            except:
                logger.warning(f"Invalid date format for {fields.get('Conference Name')}: {conf_date_str}")
                continue
            
            # Calculate monitoring start date (4 months before)
            monitoring_start = conf_date - timedelta(days=120)
            
            # Check if we should monitor this conference
            if today < monitoring_start:
                # Too early
                continue
            
            if today > conf_date:
                # Conference already happened
                continue
            
            # Check last monitored date
            last_monitored_str = fields.get('Last Monitored')
            if last_monitored_str:
                try:
                    last_monitored = datetime.strptime(last_monitored_str, '%Y-%m-%d')
                    days_since = (today - last_monitored).days
                    
                    if days_since < 14:
                        # Monitored less than 2 weeks ago
                        logger.info(f"Skipping {fields.get('Conference Name')} - monitored {days_since} days ago")
                        continue
                except:
                    pass
            
            # Check ICP filter
            icp_filter = fields.get('ICP Filter', False)
            if icp_filter:
                focus_areas = fields.get('Focus Areas', [])
                if 'Biologics' not in focus_areas:
                    logger.info(f"Skipping {fields.get('Conference Name')} - ICP filter enabled but not Biologics-focused")
                    continue
            
            to_monitor.append(conf)
            logger.info(f"✓ Will monitor: {fields.get('Conference Name')} (Date: {conf_date_str})")
        
        return to_monitor
    
    def search_conference_attendees(self, conference_name: str, conference_date: str, 
                                   conference_website: str = None) -> List[Dict]:
        """Search for conference attendees using Claude with web search"""
        
        search_prompt = f"""Find people attending or speaking at this conference:

Conference: {conference_name}
Date: {conference_date}
{f'Website: {conference_website}' if conference_website else ''}

Search for:
1. Speaker lists (keynotes, panel discussions, presentations)
2. Exhibitor lists (booth representatives, company attendees)
3. Company announcements about attendance or participation
4. LinkedIn posts mentioning attendance at this conference
5. Press releases about participation

Focus on:
- People from BIOTECH/PHARMA companies
- Companies focused on BIOLOGICS (monoclonal antibodies, bispecifics, ADCs, fusion proteins, biosimilars)
- Exclude companies focused only on: cell & gene therapy, small molecules, diagnostics, medical devices
- Focus on decision-maker titles:
  * Manufacturing, Technical Operations, Operations
  * CMC, Supply Chain, Procurement
  * C-level: CEO, COO, CSO, CTO
  * VP, SVP, Head of, Director level
- Skip: researchers, scientists, junior roles, recruiters, service providers

For each person found, determine:
- Full name
- Current job title
- Company name
- Role at conference (Speaker/Panelist/Exhibitor/Attendee)
- Source URL where you found this information
- Confidence level (High/Medium/Low)

Return results in this JSON format:
{{
  "attendees": [
    {{
      "name": "Full Name",
      "title": "Job Title",
      "company": "Company Name",
      "role_at_conference": "Speaker/Panelist/Exhibitor/Attendee",
      "session_topic": "Topic if speaker/panelist, otherwise null",
      "source_url": "URL where found",
      "confidence": "High/Medium/Low"
    }}
  ],
  "total_found": 15,
  "sources_checked": ["List of sources you searched"]
}}

Search thoroughly and return all relevant people you find."""

        try:
            logger.info(f"  Searching for attendees at {conference_name}...")
            
            message = self.anthropic_client.messages.create(
                model=self.config['anthropic']['model'],
                max_tokens=4000,
                tools=[{
                    "type": "web_search_20250305",
                    "name": "web_search"
                }],
                messages=[{
                    "role": "user",
                    "content": search_prompt
                }]
            )
            
            # Extract text and tool results
            result_text = ""
            for block in message.content:
                if block.type == "text":
                    result_text += block.text
            
            logger.info(f"  Raw response length: {len(result_text)} chars")
            
            # Parse JSON - handle multiple formats
            result_text = result_text.strip()
            
            # Try to find JSON in the response
            json_str = None
            
            # Method 1: Check for markdown code blocks
            if "```json" in result_text:
                start = result_text.find("```json") + 7
                end = result_text.find("```", start)
                if end > start:
                    json_str = result_text[start:end].strip()
            elif "```" in result_text:
                start = result_text.find("```") + 3
                end = result_text.find("```", start)
                if end > start:
                    json_str = result_text[start:end].strip()
            
            # Method 2: Try to find JSON object with curly braces
            if not json_str and "{" in result_text:
                start = result_text.find("{")
                # Find the matching closing brace
                depth = 0
                end = start
                for i in range(start, len(result_text)):
                    if result_text[i] == "{":
                        depth += 1
                    elif result_text[i] == "}":
                        depth -= 1
                        if depth == 0:
                            end = i + 1
                            break
                if end > start:
                    json_str = result_text[start:end].strip()
            
            # Method 3: Use entire text if it looks like JSON
            if not json_str:
                json_str = result_text
            
            # Parse the JSON
            if not json_str:
                logger.warning(f"  No JSON found in response")
                return []
            
            try:
                data = json.loads(json_str)
                attendees = data.get('attendees', [])
                
                if attendees:
                    logger.info(f"  ✓ Found {len(attendees)} potential attendees")
                else:
                    logger.warning(f"  No attendees in parsed JSON")
                    # Log a sample of the response for debugging
                    logger.info(f"  Response sample: {result_text[:200]}...")
                
                return attendees
            except json.JSONDecodeError as e:
                logger.error(f"  JSON parse error: {str(e)}")
                logger.info(f"  Attempted to parse: {json_str[:200]}...")
                return []
            
        except Exception as e:
            logger.error(f"  ✗ Error searching for attendees: {str(e)}")
            return []
    
    def quick_company_icp(self, company_name: str) -> int:
        """Quick ICP assessment for unknown company"""
        
        prompt = f"""Quick ICP assessment for: {company_name}

Assess if this company is a good fit for a biologics CDMO (Contract Development and Manufacturing Organization).

Search for information about:
1. Company size (employees/revenue)
2. Focus area (biologics, cell & gene, small molecules, etc.)
3. Technology platform (mammalian cell culture preferred)
4. Development stage (clinical/commercial)
5. Geographic location

Scoring criteria:
- Company size: 50-500 employees = 15 pts, 500-1000 = 10 pts, <50 or >1000 = 5 pts
- Focus: Biologics (mAbs, bispecifics, ADCs) = 25 pts, Mixed = 15 pts, Non-biologics = 0 pts
- Stage: Phase 2/3 or Commercial = 20 pts, Phase 1 = 15 pts, Preclinical = 10 pts
- Location: EU/US = 10 pts, Other = 5 pts
- Technology: Mammalian cell culture = 15 pts, Other biologics = 10 pts, Non-biologics = 0 pts

Return JSON:
{{
  "company_size": "50-200 employees",
  "focus_area": "Biologics (mAbs, bispecifics)",
  "technology": "Mammalian cell culture",
  "stage": "Phase 2/3",
  "location": "Germany",
  "icp_score": 85,
  "reasoning": "Brief explanation"
}}

Search and assess now."""

        try:
            message = self.anthropic_client.messages.create(
                model=self.config['anthropic']['model'],
                max_tokens=1000,
                tools=[{
                    "type": "web_search_20250305",
                    "name": "web_search"
                }],
                messages=[{
                    "role": "user",
                    "content": prompt
                }]
            )
            
            # Extract text
            result_text = ""
            for block in message.content:
                if block.type == "text":
                    result_text += block.text
            
            # Parse JSON
            result_text = result_text.strip()
            if result_text.startswith("```json"):
                result_text = result_text[7:]
            elif result_text.startswith("```"):
                result_text = result_text[3:]
            if result_text.endswith("```"):
                result_text = result_text[:-3]
            result_text = result_text.strip()
            
            data = json.loads(result_text)
            icp_score = data.get('icp_score', 0)
            
            logger.info(f"    Quick ICP for {company_name}: {icp_score}")
            return icp_score
            
        except Exception as e:
            logger.error(f"    Error assessing ICP for {company_name}: {str(e)}")
            return 0
    
    def find_company(self, company_name: str) -> Optional[Dict]:
        """Find company in Companies table"""
        try:
            # Search by company name
            formula = f"{{Company Name}} = '{company_name}'"
            records = self.companies_table.all(formula=formula)
            
            if records:
                return records[0]
            return None
        except:
            return None
    
    def create_company(self, company_name: str, icp_score: int) -> Dict:
        """Create a new company record"""
        try:
            company_data = {
                'Company Name': company_name,
                'ICP Fit Score': icp_score,
                'Source': 'Conference Intelligence',
                'Status': 'Not Enriched'
            }
            
            record = self.companies_table.create(company_data)
            logger.info(f"    ✓ Created company: {company_name} (ICP: {icp_score})")
            return record
        except Exception as e:
            logger.error(f"    ✗ Failed to create company {company_name}: {str(e)}")
            return None
    
    def find_lead(self, name: str, company_name: str) -> Optional[Dict]:
        """Find lead in Leads table"""
        try:
            # Search by name
            formula = f"{{Lead Name}} = '{name}'"
            records = self.leads_table.all(formula=formula)
            
            # Check if company matches
            for record in records:
                company_field = record['fields'].get('Company Name')
                if company_field and company_name.lower() in company_field.lower():
                    return record
            
            return None
        except:
            return None
    
    def create_lead(self, name: str, title: str, company_id: str, source: str) -> Dict:
        """Create a new lead record"""
        try:
            lead_data = {
                'Lead Name': name,
                'Title': title,
                'Company': [company_id],
                'Source': source,
                'Enrichment Status': 'Not Enriched',
                'Intelligence Notes': f"Discovered via Conference Intelligence\nSource: {source}"
            }
            
            record = self.leads_table.create(lead_data)
            logger.info(f"    ✓ Created lead: {name}")
            return record
        except Exception as e:
            logger.error(f"    ✗ Failed to create lead {name}: {str(e)}")
            return None
    
    def create_conference_trigger(self, lead_id: str, conference_name: str, 
                                  conference_date: str, role_at_conference: str,
                                  session_topic: str = None, source_url: str = None) -> bool:
        """Create CONFERENCE_ATTENDANCE trigger"""
        try:
            details = f"""Conference: {conference_name}
Date: {conference_date}
Role: {role_at_conference}"""
            
            if session_topic:
                details += f"\nTopic: {session_topic}"
            
            if source_url:
                details += f"\nSource: {source_url}"
            
            details += "\n\nDetected via Conference Intelligence System"
            
            trigger_data = {
                'Lead': [lead_id],
                'Trigger Type': 'CONFERENCE_ATTENDANCE',
                'Trigger Date': datetime.now().strftime('%Y-%m-%d'),
                'Status': 'New',
                'Trigger Details': details,
                'Confidence Score': 95,
                'Priority': 'High'
            }
            
            self.trigger_history_table.create(trigger_data)
            return True
            
        except Exception as e:
            logger.error(f"    ✗ Failed to create trigger: {str(e)}")
            return False
    
    def check_duplicate_trigger(self, lead_id: str, conference_name: str) -> bool:
        """Check if trigger already exists for this lead and conference"""
        try:
            # Get all triggers for this lead
            formula = f"AND({{Lead}} = '{lead_id}', {{Trigger Type}} = 'CONFERENCE_ATTENDANCE')"
            triggers = self.trigger_history_table.all(formula=formula)
            
            # Check if any trigger is for this conference
            for trigger in triggers:
                details = trigger['fields'].get('Trigger Details', '')
                if conference_name in details:
                    return True
            
            return False
        except:
            return False
    
    def process_attendee(self, attendee: Dict, conference_info: Dict) -> Dict:
        """Process a conference attendee - create lead and/or trigger"""
        
        name = attendee.get('name')
        title = attendee.get('title')
        company_name = attendee.get('company')
        role = attendee.get('role_at_conference')
        session_topic = attendee.get('session_topic')
        source_url = attendee.get('source_url')
        confidence = attendee.get('confidence', 'Medium')
        
        if not all([name, title, company_name]):
            logger.warning(f"  Incomplete data for attendee: {name}")
            return {'status': 'skipped', 'reason': 'incomplete_data'}
        
        logger.info(f"  Processing: {name} ({title}) at {company_name}")
        
        # Step 1: Check company and get ICP
        company_record = self.find_company(company_name)
        company_icp = None
        
        if company_record:
            company_icp = company_record['fields'].get('ICP Fit Score', 0)
            logger.info(f"    Company exists (ICP: {company_icp})")
        else:
            # Quick ICP assessment
            logger.info(f"    Company not found - assessing ICP...")
            company_icp = self.quick_company_icp(company_name)
            
            if company_icp >= 60:
                company_record = self.create_company(company_name, company_icp)
            else:
                logger.info(f"    Skipping - Company ICP too low ({company_icp})")
                return {'status': 'skipped', 'reason': 'low_icp', 'icp': company_icp}
        
        if not company_record:
            return {'status': 'error', 'reason': 'no_company'}
        
        # Step 2: Check if lead exists
        lead_record = self.find_lead(name, company_name)
        
        conference_name = conference_info['fields'].get('Conference Name')
        conference_date = conference_info['fields'].get('Conference Date')
        
        if lead_record:
            # Existing lead - check for duplicate trigger
            lead_id = lead_record['id']
            
            if self.check_duplicate_trigger(lead_id, conference_name):
                logger.info(f"    Trigger already exists for this conference")
                return {'status': 'skipped', 'reason': 'duplicate_trigger'}
            
            # Create trigger
            success = self.create_conference_trigger(
                lead_id=lead_id,
                conference_name=conference_name,
                conference_date=conference_date,
                role_at_conference=role,
                session_topic=session_topic,
                source_url=source_url
            )
            
            if success:
                logger.info(f"    ✓ Created trigger for existing lead")
                return {'status': 'trigger_created', 'lead_id': lead_id}
            else:
                return {'status': 'error', 'reason': 'trigger_failed'}
        else:
            # New lead - create lead + trigger
            source = f"Conference Intelligence: {conference_name}"
            lead_record = self.create_lead(
                name=name,
                title=title,
                company_id=company_record['id'],
                source=source
            )
            
            if not lead_record:
                return {'status': 'error', 'reason': 'lead_creation_failed'}
            
            # Create trigger
            success = self.create_conference_trigger(
                lead_id=lead_record['id'],
                conference_name=conference_name,
                conference_date=conference_date,
                role_at_conference=role,
                session_topic=session_topic,
                source_url=source_url
            )
            
            if success:
                logger.info(f"    ✓ Created new lead + trigger")
                return {'status': 'lead_and_trigger_created', 'lead_id': lead_record['id']}
            else:
                logger.warning(f"    Lead created but trigger failed")
                return {'status': 'partial', 'lead_id': lead_record['id']}
    
    def monitor_conference(self, conference: Dict) -> Dict:
        """Monitor a single conference for attendees"""
        
        fields = conference['fields']
        conference_name = fields.get('Conference Name', 'Unknown')
        conference_date = fields.get('Conference Date')
        conference_website = fields.get('Website')
        
        logger.info(f"\n{'='*60}")
        logger.info(f"Monitoring: {conference_name}")
        logger.info(f"Date: {conference_date}")
        logger.info(f"{'='*60}")
        
        # Search for attendees
        attendees = self.search_conference_attendees(
            conference_name=conference_name,
            conference_date=conference_date,
            conference_website=conference_website
        )
        
        if not attendees:
            logger.warning(f"No attendees found for {conference_name}")
            logger.info(f"  This could mean:")
            logger.info(f"  - Speaker/exhibitor lists not yet published")
            logger.info(f"  - Conference website doesn't have public attendee info")
            logger.info(f"  - Search didn't find relevant results")
            logger.info(f"  Will try again in next monitoring run (2 weeks)")
            return {
                'conference': conference_name,
                'attendees_found': 0,
                'leads_created': 0,
                'triggers_created': 0
            }
        
        # Process each attendee
        results = {
            'leads_created': 0,
            'triggers_created': 0,
            'skipped_low_icp': 0,
            'skipped_duplicate': 0,
            'errors': 0
        }
        
        for attendee in attendees:
            result = self.process_attendee(attendee, conference)
            
            if result['status'] == 'lead_and_trigger_created':
                results['leads_created'] += 1
                results['triggers_created'] += 1
            elif result['status'] == 'trigger_created':
                results['triggers_created'] += 1
            elif result['status'] == 'skipped' and result.get('reason') == 'low_icp':
                results['skipped_low_icp'] += 1
            elif result['status'] == 'skipped' and result.get('reason') == 'duplicate_trigger':
                results['skipped_duplicate'] += 1
            elif result['status'] == 'error':
                results['errors'] += 1
            
            # Rate limiting
            time.sleep(1)
        
        # Update conference record
        try:
            attendees_found = fields.get('Attendees Found', 0)
            self.conferences_table.update(conference['id'], {
                'Last Monitored': datetime.now().strftime('%Y-%m-%d'),
                'Monitoring Status': 'Monitoring',
                'Attendees Found': attendees_found + results['leads_created'] + results['triggers_created']
            })
        except Exception as e:
            logger.error(f"Failed to update conference record: {str(e)}")
        
        # Summary
        logger.info(f"\n{'='*60}")
        logger.info(f"SUMMARY: {conference_name}")
        logger.info(f"{'='*60}")
        logger.info(f"Attendees found: {len(attendees)}")
        logger.info(f"New leads created: {results['leads_created']}")
        logger.info(f"Triggers created: {results['triggers_created']}")
        logger.info(f"Skipped (low ICP): {results['skipped_low_icp']}")
        logger.info(f"Skipped (duplicate): {results['skipped_duplicate']}")
        logger.info(f"Errors: {results['errors']}")
        logger.info(f"{'='*60}\n")
        
        return {
            'conference': conference_name,
            'attendees_found': len(attendees),
            **results
        }
    
    def run(self):
        """Main monitoring workflow"""
        logger.info("Starting Conference Intelligence monitoring...")
        
        # Get conferences to monitor
        conferences = self.get_conferences_to_monitor()
        
        if not conferences:
            logger.info("No conferences to monitor at this time")
            return
        
        logger.info(f"Found {len(conferences)} conference(s) to monitor\n")
        
        # Monitor each conference
        all_results = []
        for conference in conferences:
            try:
                result = self.monitor_conference(conference)
                all_results.append(result)
                
                # Rate limiting between conferences
                time.sleep(5)
            except Exception as e:
                logger.error(f"Error monitoring conference: {str(e)}")
                continue
        
        # Final summary
        logger.info("\n" + "="*60)
        logger.info("FINAL SUMMARY")
        logger.info("="*60)
        logger.info(f"Conferences monitored: {len(all_results)}")
        
        total_attendees = sum(r['attendees_found'] for r in all_results)
        total_leads = sum(r['leads_created'] for r in all_results)
        total_triggers = sum(r['triggers_created'] for r in all_results)
        
        logger.info(f"Total attendees found: {total_attendees}")
        logger.info(f"Total new leads: {total_leads}")
        logger.info(f"Total triggers: {total_triggers}")
        logger.info("="*60)
        
        logger.info("\nConference Intelligence monitoring complete!")


def main():
    """CLI entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Monitor conferences for relevant attendees')
    parser.add_argument('--config', default='config.yaml',
                       help='Path to config file')
    
    args = parser.parse_args()
    
    try:
        monitor = ConferenceIntelligence(config_path=args.config)
        monitor.run()
    except FileNotFoundError:
        logger.error(f"Config file not found: {args.config}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
