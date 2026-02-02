#!/usr/bin/env python3
"""
Daily Trigger Digest - Sends beautiful HTML email with new trigger events

Structure:
1. Categorized by SOURCE (News Intelligence, Conference, Campaign, etc.)
2. Within each source: grouped by COMPANY
3. Company header shows: ICP, Location, Clinical Phase, Focus Area, Trigger Types
4. Source URL below company header
5. Table of LEADS with their ICP and outreach message links
6. Trigger summary and outreach angle
7. Action buttons
"""

import os
import sys
import re
import yaml
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from collections import defaultdict
from pyairtable import Api
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email, To, Content, HtmlContent

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('trigger_digest.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class TriggerDigest:
    """Generate and send daily trigger digest emails"""
    
    def __init__(self, config_path: str = "config.yaml"):
        """Initialize with configuration"""
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        # Initialize Airtable
        self.airtable = Api(self.config['airtable']['api_key'])
        self.base = self.airtable.base(self.config['airtable']['base_id'])
        self.trigger_history_table = self.base.table('Trigger History')
        self.leads_table = self.base.table(self.config['airtable']['tables']['leads'])
        self.companies_table = self.base.table(self.config['airtable']['tables']['companies'])
        
        # SendGrid
        self.sendgrid_api_key = os.environ.get('SENDGRID_API_KEY', '')
        
        # Email settings
        self.from_email = os.environ.get('DIGEST_FROM_EMAIL', 'triggers@leadintelligence.io')
        self.to_email = os.environ.get('DIGEST_TO_EMAIL', '')
        self.company_name = os.environ.get('DIGEST_COMPANY_NAME', 'Lead Intelligence')
        
        # Airtable base URL for direct links
        self.airtable_base_id = self.config['airtable']['base_id']
        
        # Cache for company and lead data
        self._company_cache = {}
        self._lead_cache = {}
        
        logger.info("TriggerDigest initialized")
    
    def _get_trigger_url(self, record_id: str) -> str:
        """Generate URL to Trigger History record"""
        # Airtable URL format: https://airtable.com/{baseId}/{tableId}/{recordId}
        # We use the base ID and actual record ID
        return f"https://airtable.com/{self.airtable_base_id}/Trigger%20History/{record_id}"
    
    def _get_lead_url(self, record_id: str) -> str:
        """Generate URL to Lead record"""
        return f"https://airtable.com/{self.airtable_base_id}/Leads/{record_id}"
    
    def _get_company_url(self, record_id: str) -> str:
        """Generate URL to Company record"""
        return f"https://airtable.com/{self.airtable_base_id}/Companies/{record_id}"
    
    def _get_company_data(self, company_id: str) -> Dict:
        """Get company data with caching"""
        if company_id in self._company_cache:
            return self._company_cache[company_id]
        
        try:
            company = self.companies_table.get(company_id)
            data = {
                'id': company_id,
                'name': company['fields'].get('Company Name', 'Unknown'),
                'icp': company['fields'].get('ICP Fit Score'),
                'hq_country': company['fields'].get('HQ Country') or company['fields'].get('Headquarters Country') or company['fields'].get('Country', ''),
                'clinical_phase': company['fields'].get('Clinical Phase') or company['fields'].get('Development Stage', ''),
                'focus_area': company['fields'].get('Focus Area') or company['fields'].get('Therapeutic Focus') or company['fields'].get('Technology', ''),
                'website': company['fields'].get('Website', '')
            }
            self._company_cache[company_id] = data
            return data
        except Exception as e:
            logger.warning(f"Could not fetch company {company_id}: {e}")
            return {'id': company_id, 'name': 'Unknown', 'icp': None, 'hq_country': '', 'clinical_phase': '', 'focus_area': ''}
    
    def _get_lead_data(self, lead_id: str) -> Dict:
        """Get lead data with caching"""
        if lead_id in self._lead_cache:
            return self._lead_cache[lead_id]
        
        try:
            lead = self.leads_table.get(lead_id)
            data = {
                'id': lead_id,
                'name': lead['fields'].get('Lead Name', 'Unknown'),
                'title': lead['fields'].get('Title') or lead['fields'].get('Job Title', ''),
                'lead_icp': lead['fields'].get('Lead ICP'),
                'combined_icp': lead['fields'].get('Combined ICP'),
                'email_subject': lead['fields'].get('Email Subject', ''),
                'email_body': lead['fields'].get('Email Body', ''),
                'linkedin_message': lead['fields'].get('LinkedIn Message', '')
            }
            self._lead_cache[lead_id] = data
            return data
        except Exception as e:
            logger.warning(f"Could not fetch lead {lead_id}: {e}")
            return {'id': lead_id, 'name': 'Unknown', 'title': '', 'lead_icp': None, 'combined_icp': None}
    
    def _categorize_source(self, trigger: Dict) -> str:
        """
        Determine the source category for a trigger.
        
        Priority:
        1. Use 'Trigger Source' field if set (new standardized field)
        2. Fall back to inference from 'Source' field and 'Trigger Type'
        """
        fields = trigger['fields']
        
        # First check the new standardized Trigger Source field
        trigger_source = fields.get('Trigger Source', '')
        if trigger_source:
            # Map the field value to our display categories
            source_map = {
                'News Intelligence': 'News Intelligence',
                'Conference Monitor': 'Conference Monitor',
                'Conference Intelligence': 'Conference Monitor',
                'Lead Monitoring': 'Lead Monitoring',
                'Campaign Leads': 'Campaign Leads',
                'Campaign': 'Campaign Leads'
            }
            return source_map.get(trigger_source, trigger_source)
        
        # Fallback: infer from Source field and Trigger Type
        source = fields.get('Source', '').lower()
        sources_field = fields.get('Sources', '').lower()
        trigger_type = fields.get('Trigger Type', '')
        
        # Check Sources field first (more reliable)
        if 'news:' in sources_field or 'market' in sources_field:
            return 'News Intelligence'
        if 'conference intelligence' in sources_field:
            return 'Conference Monitor'
        if 'lead monitoring' in sources_field or 'surveillance' in sources_field:
            return 'Lead Monitoring'
        if 'campaign' in sources_field:
            return 'Campaign Leads'
        
        # Check Source field
        if 'news' in source or 'market' in source:
            return 'News Intelligence'
        if 'conference' in source:
            return 'Conference Monitor'
        if 'campaign' in source:
            return 'Campaign Leads'
        if 'monitor' in source or 'surveillance' in source:
            return 'Lead Monitoring'
        
        # Infer from trigger type
        if trigger_type in ['FUNDING', 'PARTNERSHIP', 'ACQUISITION', 'EXPANSION', 'REGULATORY', 'NEWS', 'PIPELINE']:
            return 'News Intelligence'
        if trigger_type in ['SPEAKING', 'CONFERENCE_ATTENDANCE', 'CONFERENCE']:
            return 'Conference Monitor'
        if trigger_type in ['JOB_CHANGE', 'LINKEDIN_POST', 'CONTENT', 'HIRING', 'PROMOTION']:
            return 'Lead Monitoring'
        
        return 'Other'
    
    def get_new_triggers(self, days_back: int = 1) -> Dict[str, Dict[str, List[Dict]]]:
        """
        Get triggers organized by SOURCE CATEGORY -> COMPANY -> triggers
        
        Key changes:
        - Groups triggers by Source URL (same news = one entry, multiple trigger types)
        - Excludes Campaign Leads from digest
        - Uses 'Trigger Source' field when available
        
        Returns:
            {
                'News Intelligence': {
                    'company_id_1': [
                        {
                            'triggers': [trigger1, trigger2],  # Multiple triggers from same source
                            'source_url': 'https://...',
                            'trigger_types': ['FUNDING', 'PIPELINE'],  # Combined types
                            ...
                        }
                    ],
                },
                ...
            }
        """
        cutoff_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
        
        # Get all triggers
        all_triggers = self.trigger_history_table.all()
        
        # First pass: group triggers by source URL and company
        # Key: (source_category, company_id, source_url)
        grouped = defaultdict(list)
        
        for trigger in all_triggers:
            fields = trigger['fields']
            
            # Only include triggers that haven't been notified
            status = fields.get('Status', '')
            if status == 'Notified':
                continue
            
            # Check if recent enough
            date_detected = fields.get('Date Detected', '')
            is_recent = date_detected >= cutoff_date if date_detected else True
            
            if not is_recent:
                continue
            
            # Get source category
            source_category = self._categorize_source(trigger)
            
            # EXCLUDE Campaign Leads from digest
            if source_category == 'Campaign Leads':
                continue
            
            # Get company ID (use first if multiple)
            company_ids = fields.get('Company', [])
            company_id = company_ids[0] if company_ids else 'unknown'
            
            # Get source URL for deduplication
            source_url = self._extract_source_url(trigger)
            
            # Enrich trigger with additional data
            trigger['_id'] = trigger['id']
            trigger['_source_category'] = source_category
            trigger['_source_url'] = source_url
            
            # Get lead data for this trigger
            lead_ids = fields.get('Lead', [])
            trigger['_leads'] = []
            for lid in lead_ids:
                lead_data = self._get_lead_data(lid)
                trigger['_leads'].append(lead_data)
            
            # Group key: (source_category, company_id, source_url)
            # If no source URL, use trigger ID to keep them separate
            group_key = (source_category, company_id, source_url or trigger['id'])
            grouped[group_key].append(trigger)
        
        # Second pass: organize into final structure
        # Source -> Company -> list of grouped trigger events
        triggers_by_source_company = defaultdict(lambda: defaultdict(list))
        
        for (source_category, company_id, source_url), triggers in grouped.items():
            # Combine trigger types from all triggers with same source
            trigger_types = list(set(t['fields'].get('Trigger Type', 'OTHER') for t in triggers))
            
            # Combine all leads from all triggers
            all_leads = {}
            for trigger in triggers:
                for lead in trigger.get('_leads', []):
                    if lead['id'] not in all_leads:
                        all_leads[lead['id']] = {
                            'data': lead,
                            'triggers': []
                        }
                    all_leads[lead['id']]['triggers'].append(trigger)
            
            # Create combined event entry
            combined_event = {
                'triggers': triggers,  # All triggers from this source
                'source_url': source_url,
                'trigger_types': trigger_types,
                '_leads_combined': all_leads,
                '_company_id': company_id,
                '_source_category': source_category,
                # Get urgency (highest among all triggers)
                '_max_urgency': self._get_max_urgency(triggers),
                # Use first trigger for description/outreach
                '_primary_trigger': triggers[0]
            }
            
            triggers_by_source_company[source_category][company_id].append(combined_event)
        
        # Count totals for logging
        total_events = sum(
            len(events) 
            for companies in triggers_by_source_company.values() 
            for events in companies.values()
        )
        total_triggers = sum(
            len(event['triggers'])
            for companies in triggers_by_source_company.values() 
            for events in companies.values()
            for event in events
        )
        
        logger.info(f"Found {total_triggers} triggers grouped into {total_events} events")
        for source, companies in triggers_by_source_company.items():
            event_count = sum(len(e) for e in companies.values())
            logger.info(f"  - {source}: {event_count} events across {len(companies)} companies")
        
        return dict(triggers_by_source_company)
    
    def _extract_source_url(self, trigger: Dict) -> str:
        """Extract source URL from trigger for deduplication"""
        fields = trigger['fields']
        
        # Check various URL fields
        url = fields.get('Source URL', '') or fields.get('Article URL', '')
        
        # Also check Sources field for URL
        if not url:
            sources = fields.get('Sources', '')
            if 'URL:' in sources:
                # Extract URL from "URL: https://..." format
                for line in sources.split('\n'):
                    if line.strip().startswith('URL:'):
                        url = line.replace('URL:', '').strip()
                        break
            elif 'http' in sources:
                # Find any URL in sources
                urls = re.findall(r'https?://[^\s,]+', sources)
                if urls:
                    url = urls[0]
        
        return url
    
    def _get_max_urgency(self, triggers: List[Dict]) -> str:
        """Get the highest urgency from a list of triggers"""
        urgencies = [t['fields'].get('Urgency', 'LOW') for t in triggers]
        if 'HIGH' in urgencies:
            return 'HIGH'
        elif 'MEDIUM' in urgencies:
            return 'MEDIUM'
        return 'LOW'
    
    def generate_html_email(self, triggers_by_source_company: Dict) -> str:
        """Generate beautiful HTML email organized by source -> company -> combined events"""
        
        # Count totals
        total_events = 0
        total_triggers = 0
        high_count = 0
        medium_count = 0
        low_count = 0
        
        for companies in triggers_by_source_company.values():
            for events in companies.values():
                for event in events:
                    total_events += 1
                    total_triggers += len(event['triggers'])
                    urgency = event['_max_urgency']
                    if urgency == 'HIGH':
                        high_count += 1
                    elif urgency == 'MEDIUM':
                        medium_count += 1
                    else:
                        low_count += 1
        
        # Source styling
        source_config = {
            'News Intelligence': {'icon': '📰', 'color': '#0066cc', 'bg': '#e7f3ff'},
            'Conference Monitor': {'icon': '🎤', 'color': '#6f42c1', 'bg': '#f3e8ff'},
            'Campaign Leads': {'icon': '🎯', 'color': '#20c997', 'bg': '#e6fff9'},
            'Lead Monitoring': {'icon': '👁️', 'color': '#fd7e14', 'bg': '#fff3e0'},
            'Other': {'icon': '📋', 'color': '#6c757d', 'bg': '#f8f9fa'}
        }
        
        # Generate source sections
        source_sections = ""
        
        for source_name in ['News Intelligence', 'Conference Monitor', 'Lead Monitoring', 'Other']:
            if source_name not in triggers_by_source_company:
                continue
            
            companies = triggers_by_source_company[source_name]
            if not companies:
                continue
            
            config = source_config.get(source_name, source_config['Other'])
            
            # Source header - count events (not individual triggers)
            source_event_count = sum(len(events) for events in companies.values())
            source_sections += f"""
            <tr>
                <td style="background-color: {config['color']}; padding: 15px 20px; color: white;">
                    <span style="font-size: 20px; margin-right: 10px;">{config['icon']}</span>
                    <span style="font-size: 18px; font-weight: 600;">{source_name}</span>
                    <span style="float: right; background: rgba(255,255,255,0.2); padding: 4px 12px; border-radius: 20px; font-size: 14px;">
                        {source_event_count} event{'s' if source_event_count != 1 else ''}
                    </span>
                </td>
            </tr>
            """
            
            # Company cards within this source
            for company_id, events in companies.items():
                company_data = self._get_company_data(company_id) if company_id != 'unknown' else {
                    'name': 'Unknown Company', 'icp': None, 'hq_country': '', 'clinical_phase': '', 'focus_area': ''
                }
                
                # Process each event (grouped by source URL)
                for event in events:
                    triggers = event['triggers']
                    trigger_types = event['trigger_types']
                    source_url = event['source_url']
                    max_urgency = event['_max_urgency']
                    all_leads = event['_leads_combined']
                    primary_trigger = event['_primary_trigger']
                    
                    # Urgency colors
                    if max_urgency == 'HIGH':
                        urgency_color = '#dc3545'
                    elif max_urgency == 'MEDIUM':
                        urgency_color = '#fd7e14'
                    else:
                        urgency_color = '#28a745'
                    
                    # Company card
                    source_sections += f"""
            <tr>
                <td style="padding: 15px 20px; border-bottom: 1px solid #eee;">
                    <!-- Event Card -->
                    <table width="100%" cellpadding="0" cellspacing="0" style="background: linear-gradient(135deg, #f8f9fa 0%, #ffffff 100%); border-radius: 12px; border: 1px solid #e0e0e0; overflow: hidden;">
                        <tr>
                            <td style="padding: 20px;">
                                <!-- Company Name & Urgency -->
                                <table width="100%" cellpadding="0" cellspacing="0">
                                    <tr>
                                        <td>
                                            <span style="font-size: 20px; font-weight: 700; color: #333;">
                                                🏢 {company_data['name']}
                                            </span>
                                        </td>
                                        <td style="text-align: right;">
                                            <span style="display: inline-block; padding: 4px 12px; background-color: {urgency_color}; color: white; border-radius: 20px; font-size: 12px; font-weight: bold;">
                                                {max_urgency}
                                            </span>
                                        </td>
                                    </tr>
                                </table>
                                
                                <!-- Company Details Row -->
                                <div style="margin-top: 12px; font-size: 13px; color: #666;">
                                    {f'<span style="margin-right: 15px;">📊 <strong>ICP:</strong> {int(company_data["icp"])}</span>' if company_data.get('icp') else ''}
                                    {f'<span style="margin-right: 15px;">📍 {company_data["hq_country"]}</span>' if company_data.get('hq_country') else ''}
                                    {f'<span style="margin-right: 15px;">🧪 {company_data["clinical_phase"]}</span>' if company_data.get('clinical_phase') else ''}
                                    {f'<span style="margin-right: 15px;">🎯 {company_data["focus_area"]}</span>' if company_data.get('focus_area') else ''}
                                </div>
                                
                                <!-- Trigger Types (combined) -->
                                <div style="margin-top: 10px;">
                                    {' '.join([f'<span style="display: inline-block; padding: 3px 10px; background-color: #e9ecef; color: #495057; border-radius: 15px; font-size: 11px; margin-right: 5px; margin-bottom: 5px;">{tt}</span>' for tt in trigger_types])}
                                </div>
                                
                                <!-- Source URL -->
                                {f'<div style="margin-top: 12px; font-size: 12px;"><a href="{source_url}" style="color: #0066cc; text-decoration: none;">🔗 {source_url[:70]}{"..." if len(source_url) > 70 else ""}</a></div>' if source_url else ''}
                            </td>
                        </tr>
                        
                        <!-- Leads Table -->
                        <tr>
                            <td style="padding: 0 20px 15px 20px;">
                                <table width="100%" cellpadding="0" cellspacing="0" style="border: 1px solid #dee2e6; border-radius: 8px; overflow: hidden; font-size: 13px;">
                                    <tr style="background-color: #f8f9fa;">
                                        <th style="padding: 10px; text-align: left; border-bottom: 1px solid #dee2e6; font-weight: 600; color: #495057;">Lead</th>
                                        <th style="padding: 10px; text-align: left; border-bottom: 1px solid #dee2e6; font-weight: 600; color: #495057;">Title</th>
                                        <th style="padding: 10px; text-align: center; border-bottom: 1px solid #dee2e6; font-weight: 600; color: #495057;">ICP</th>
                                        <th style="padding: 10px; text-align: center; border-bottom: 1px solid #dee2e6; font-weight: 600; color: #495057;">Outreach</th>
                                    </tr>
                                    {self._generate_leads_rows(all_leads)}
                                </table>
                            </td>
                        </tr>
                        
                        <!-- Trigger Summary & Outreach Angle -->
                        <tr>
                            <td style="padding: 0 20px 15px 20px;">
                                {self._generate_trigger_summaries(triggers)}
                            </td>
                        </tr>
                        
                        <!-- Action Buttons -->
                        <tr>
                            <td style="padding: 0 20px 20px 20px;">
                                <table cellpadding="0" cellspacing="0">
                                    <tr>
                                        <td style="padding-right: 8px;">
                                            <a href="{self._get_company_url(company_id)}" style="display: inline-block; padding: 10px 16px; background-color: #667eea; color: white; text-decoration: none; border-radius: 6px; font-size: 12px; font-weight: 500;">
                                                🏢 View Company
                                            </a>
                                        </td>
                                        <td style="padding-right: 8px;">
                                            <a href="{self._get_trigger_url(primary_trigger['_id'])}" style="display: inline-block; padding: 10px 16px; background-color: #28a745; color: white; text-decoration: none; border-radius: 6px; font-size: 12px; font-weight: 500;">
                                                📋 View Trigger
                                            </a>
                                        </td>
                                        {f'<td><a href="{source_url}" style="display: inline-block; padding: 10px 16px; background-color: #17a2b8; color: white; text-decoration: none; border-radius: 6px; font-size: 12px; font-weight: 500;">🔗 Read Article</a></td>' if source_url else ''}
                                    </tr>
                                </table>
                            </td>
                        </tr>
                    </table>
                </td>
            </tr>
                    """
        
        # Full HTML template
        html = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
</head>
<body style="margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif; background-color: #f5f5f5;">
    <table width="100%" cellpadding="0" cellspacing="0" style="background-color: #f5f5f5; padding: 20px 0;">
        <tr>
            <td align="center">
                <table width="700" cellpadding="0" cellspacing="0" style="background-color: #ffffff; border-radius: 12px; overflow: hidden; box-shadow: 0 4px 6px rgba(0,0,0,0.1);">
                    
                    <!-- Header -->
                    <tr>
                        <td style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 30px; text-align: center;">
                            <h1 style="margin: 0; color: white; font-size: 28px; font-weight: 600;">
                                🎯 Trigger Digest
                            </h1>
                            <p style="margin: 10px 0 0 0; color: rgba(255,255,255,0.9); font-size: 16px;">
                                {datetime.now().strftime('%A, %B %d, %Y')}
                            </p>
                        </td>
                    </tr>
                    
                    <!-- Summary Stats -->
                    <tr>
                        <td style="padding: 20px;">
                            <table width="100%" cellpadding="0" cellspacing="0">
                                <tr>
                                    <td width="25%" style="text-align: center; padding: 15px;">
                                        <div style="font-size: 32px; font-weight: bold; color: #333;">{total_events}</div>
                                        <div style="font-size: 12px; color: #666; margin-top: 4px;">Events</div>
                                    </td>
                                    <td width="25%" style="text-align: center; padding: 15px; border-left: 1px solid #eee;">
                                        <div style="font-size: 32px; font-weight: bold; color: #dc3545;">{high_count}</div>
                                        <div style="font-size: 12px; color: #666; margin-top: 4px;">🔴 High</div>
                                    </td>
                                    <td width="25%" style="text-align: center; padding: 15px; border-left: 1px solid #eee;">
                                        <div style="font-size: 32px; font-weight: bold; color: #fd7e14;">{medium_count}</div>
                                        <div style="font-size: 12px; color: #666; margin-top: 4px;">🟡 Medium</div>
                                    </td>
                                    <td width="25%" style="text-align: center; padding: 15px; border-left: 1px solid #eee;">
                                        <div style="font-size: 32px; font-weight: bold; color: #28a745;">{low_count}</div>
                                        <div style="font-size: 12px; color: #666; margin-top: 4px;">🟢 Low</div>
                                    </td>
                                </tr>
                            </table>
                        </td>
                    </tr>
                    
                    <!-- Source Sections with Event Cards -->
                    {source_sections}
                    
                    <!-- Footer -->
                    <tr>
                        <td style="background-color: #f8f9fa; padding: 20px; text-align: center; border-top: 1px solid #eee;">
                            <p style="margin: 0; color: #666; font-size: 13px;">
                                Generated by Lead Intelligence System
                            </p>
                            <p style="margin: 8px 0 0 0; color: #999; font-size: 12px;">
                                {self.company_name}
                            </p>
                        </td>
                    </tr>
                    
                </table>
            </td>
        </tr>
    </table>
</body>
</html>
"""
        return html
    
    def _generate_leads_rows(self, all_leads: Dict) -> str:
        """Generate table rows for leads"""
        rows = ""
        
        for lead_id, lead_info in all_leads.items():
            lead = lead_info['data']
            triggers = lead_info['triggers']
            
            # Check if lead has outreach messages
            has_email = bool(lead.get('email_subject') or lead.get('email_body'))
            has_linkedin = bool(lead.get('linkedin_message'))
            
            # Also check trigger-specific outreach
            for t in triggers:
                if t['fields'].get('Email Subject') or t['fields'].get('Email Body'):
                    has_email = True
                if t['fields'].get('LinkedIn Message'):
                    has_linkedin = True
            
            # ICP display
            icp_display = ''
            if lead.get('combined_icp'):
                icp_display = f"{int(lead['combined_icp'])}"
            elif lead.get('lead_icp'):
                icp_display = f"{int(lead['lead_icp'])}"
            
            # Outreach buttons
            outreach_buttons = ""
            if has_email:
                outreach_buttons += f'<a href="{self._get_lead_url(lead_id)}" style="display: inline-block; padding: 4px 8px; background-color: #0066cc; color: white; text-decoration: none; border-radius: 4px; font-size: 10px; margin-right: 4px;">✉️</a>'
            if has_linkedin:
                outreach_buttons += f'<a href="{self._get_lead_url(lead_id)}" style="display: inline-block; padding: 4px 8px; background-color: #0077b5; color: white; text-decoration: none; border-radius: 4px; font-size: 10px;">in</a>'
            if not outreach_buttons:
                outreach_buttons = '<span style="color: #999;">-</span>'
            
            rows += f"""
                <tr>
                    <td style="padding: 10px; border-bottom: 1px solid #f0f0f0;">
                        <a href="{self._get_lead_url(lead_id)}" style="color: #333; text-decoration: none; font-weight: 500;">{lead['name']}</a>
                    </td>
                    <td style="padding: 10px; border-bottom: 1px solid #f0f0f0; color: #666;">{lead.get('title', '')[:30]}</td>
                    <td style="padding: 10px; border-bottom: 1px solid #f0f0f0; text-align: center;">
                        {f'<span style="background-color: #e7f3ff; padding: 2px 8px; border-radius: 10px; font-weight: 600;">{icp_display}</span>' if icp_display else '-'}
                    </td>
                    <td style="padding: 10px; border-bottom: 1px solid #f0f0f0; text-align: center;">
                        {outreach_buttons}
                    </td>
                </tr>
            """
        
        if not rows:
            rows = '<tr><td colspan="4" style="padding: 15px; text-align: center; color: #999; font-style: italic;">No specific leads linked</td></tr>'
        
        return rows
    
    def _generate_trigger_summaries(self, triggers: List[Dict]) -> str:
        """Generate trigger summary and outreach angle sections"""
        
        # Combine descriptions and outreach angles from all triggers
        descriptions = []
        outreach_angles = []
        
        for t in triggers:
            desc = t['fields'].get('Description', '')
            if desc and desc not in descriptions:
                descriptions.append(desc)
            
            angle = t['fields'].get('Outreach Angle', '')
            if angle and angle not in outreach_angles:
                outreach_angles.append(angle)
        
        html = ""
        
        # Description
        if descriptions:
            combined_desc = descriptions[0][:500] + ('...' if len(descriptions[0]) > 500 else '')
            if len(descriptions) > 1:
                combined_desc += f" (+{len(descriptions)-1} more)"
            
            html += f"""
                <div style="background-color: #f8f9fa; padding: 12px; border-radius: 8px; margin-bottom: 10px;">
                    <div style="font-size: 11px; font-weight: 600; color: #666; margin-bottom: 6px;">📝 TRIGGER SUMMARY</div>
                    <div style="font-size: 13px; color: #333; line-height: 1.5;">{combined_desc}</div>
                </div>
            """
        
        # Outreach angle
        if outreach_angles:
            combined_angle = outreach_angles[0][:300] + ('...' if len(outreach_angles[0]) > 300 else '')
            
            html += f"""
                <div style="background-color: #e7f3ff; padding: 12px; border-radius: 8px; border-left: 4px solid #0066cc;">
                    <div style="font-size: 11px; font-weight: 600; color: #0066cc; margin-bottom: 6px;">💡 OUTREACH ANGLE</div>
                    <div style="font-size: 13px; color: #333; line-height: 1.5;">{combined_angle}</div>
                </div>
            """
        
        return html
    
    def generate_no_triggers_email(self) -> str:
        """Generate email when there are no new triggers"""
        
        html = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
</head>
<body style="margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif; background-color: #f5f5f5;">
    <table width="100%" cellpadding="0" cellspacing="0" style="background-color: #f5f5f5; padding: 20px 0;">
        <tr>
            <td align="center">
                <table width="600" cellpadding="0" cellspacing="0" style="background-color: #ffffff; border-radius: 12px; overflow: hidden; box-shadow: 0 4px 6px rgba(0,0,0,0.1);">
                    
                    <!-- Header -->
                    <tr>
                        <td style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 30px; text-align: center;">
                            <h1 style="margin: 0; color: white; font-size: 28px; font-weight: 600;">
                                🎯 Trigger Digest
                            </h1>
                            <p style="margin: 10px 0 0 0; color: rgba(255,255,255,0.9); font-size: 16px;">
                                {datetime.now().strftime('%A, %B %d, %Y')}
                            </p>
                        </td>
                    </tr>
                    
                    <!-- No Triggers Message -->
                    <tr>
                        <td style="padding: 60px 40px; text-align: center;">
                            <div style="font-size: 64px; margin-bottom: 20px;">✨</div>
                            <h2 style="margin: 0; color: #333; font-size: 24px;">All Caught Up!</h2>
                            <p style="margin: 15px 0 0 0; color: #666; font-size: 16px; line-height: 1.6;">
                                No new trigger events detected today.<br>
                                Your lead intelligence system is monitoring and will alert you when opportunities arise.
                            </p>
                        </td>
                    </tr>
                    
                    <!-- Footer -->
                    <tr>
                        <td style="background-color: #f8f9fa; padding: 20px; text-align: center; border-top: 1px solid #eee;">
                            <p style="margin: 0; color: #666; font-size: 13px;">
                                Generated by Lead Intelligence System
                            </p>
                            <p style="margin: 8px 0 0 0; color: #999; font-size: 12px;">
                                {self.company_name}
                            </p>
                        </td>
                    </tr>
                    
                </table>
            </td>
        </tr>
    </table>
</body>
</html>
"""
        return html
    
    def send_email(self, html_content: str, trigger_count: int) -> bool:
        """Send email via SendGrid to one or multiple recipients"""
        
        if not self.sendgrid_api_key:
            logger.error("SENDGRID_API_KEY not set")
            return False
        
        if not self.to_email:
            logger.error("DIGEST_TO_EMAIL not set")
            return False
        
        try:
            sg = SendGridAPIClient(self.sendgrid_api_key)
            
            # Create subject based on trigger count
            if trigger_count > 0:
                subject = f"🎯 Trigger Digest: {trigger_count} New Trigger{'s' if trigger_count != 1 else ''}"
            else:
                subject = "🎯 Trigger Digest: All Caught Up!"
            
            # Handle multiple recipients (comma-separated)
            recipients = [email.strip() for email in self.to_email.split(',') if email.strip()]
            
            for recipient in recipients:
                message = Mail(
                    from_email=Email(self.from_email, "Lead Intelligence"),
                    to_emails=To(recipient),
                    subject=subject,
                    html_content=HtmlContent(html_content)
                )
                
                response = sg.send(message)
                
                if response.status_code in [200, 201, 202]:
                    logger.info(f"✓ Email sent successfully to {recipient}")
                else:
                    logger.error(f"Email send failed to {recipient}: {response.status_code}")
            
            return True
                
        except Exception as e:
            logger.error(f"Error sending email: {str(e)}")
            return False
    
    def mark_triggers_as_notified(self, triggers_by_source_company: Dict):
        """Update trigger status to 'Notified' after sending email"""
        
        for companies in triggers_by_source_company.values():
            for events in companies.values():
                for event in events:
                    for trigger in event['triggers']:
                        try:
                            self.trigger_history_table.update(trigger['_id'], {
                                'Status': 'Notified'
                            })
                        except Exception as e:
                            logger.warning(f"Could not update trigger status: {e}")
    
    def run(self, days_back: int = 1, skip_if_empty: bool = False, mark_notified: bool = True):
        """Main workflow"""
        
        logger.info("=" * 60)
        logger.info("DAILY TRIGGER DIGEST")
        logger.info(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        logger.info("=" * 60)
        
        # Get new triggers organized by source -> company -> events
        triggers_by_source_company = self.get_new_triggers(days_back=days_back)
        
        # Count total events
        total_events = sum(
            len(events) 
            for companies in triggers_by_source_company.values() 
            for events in companies.values()
        )
        
        # Skip sending if no triggers and skip_if_empty is True
        if total_events == 0 and skip_if_empty:
            logger.info("No new triggers - skipping email (skip_if_empty=True)")
            return True
        
        # Generate email content
        if total_events > 0:
            html_content = self.generate_html_email(triggers_by_source_company)
        else:
            html_content = self.generate_no_triggers_email()
        
        # Send email
        success = self.send_email(html_content, total_events)
        
        # Mark triggers as notified
        if success and total_events > 0 and mark_notified:
            logger.info("Marking triggers as notified...")
            self.mark_triggers_as_notified(triggers_by_source_company)
        
        logger.info("=" * 60)
        logger.info("DIGEST COMPLETE")
        logger.info(f"Events: {total_events} | Email sent: {'Yes' if success else 'No'}")
        logger.info("=" * 60)
        
        return success


def main():
    """CLI entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Send daily trigger digest email')
    parser.add_argument('--days-back', type=int, default=1,
                       help='How many days back to look for triggers (default: 1)')
    parser.add_argument('--skip-if-empty', action='store_true',
                       help='Skip sending email if no new triggers')
    parser.add_argument('--no-mark-notified', action='store_true',
                       help='Do not mark triggers as notified after sending')
    parser.add_argument('--config', default='config.yaml',
                       help='Path to config file')
    
    args = parser.parse_args()
    
    try:
        digest = TriggerDigest(config_path=args.config)
        success = digest.run(
            days_back=args.days_back,
            skip_if_empty=args.skip_if_empty,
            mark_notified=not args.no_mark_notified
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
