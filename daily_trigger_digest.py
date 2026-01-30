#!/usr/bin/env python3
"""
Daily Trigger Digest - Sends beautiful HTML email with new trigger events
"""

import os
import sys
import yaml
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional
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
        
        # Email settings (can be overridden via environment variables)
        self.from_email = os.environ.get('DIGEST_FROM_EMAIL', 'triggers@leadintelligence.io')
        self.to_email = os.environ.get('DIGEST_TO_EMAIL', '')
        self.company_name = os.environ.get('DIGEST_COMPANY_NAME', 'Your Company')
        
        # Airtable base URL for direct links
        self.airtable_base_id = self.config['airtable']['base_id']
        
        logger.info("TriggerDigest initialized")
    
    def _get_airtable_record_url(self, table_name: str, record_id: str) -> str:
        """Generate direct URL to Airtable record"""
        # Airtable URL format: https://airtable.com/{baseId}/{tableId}/{recordId}
        # We use a simplified format that redirects properly
        return f"https://airtable.com/{self.airtable_base_id}/{table_name}/{record_id}"
    
    def _get_trigger_history_url(self, record_id: str) -> str:
        """Generate URL to Trigger History record"""
        return f"https://airtable.com/{self.airtable_base_id}/tblTriggerHistory/{record_id}"
    
    def _get_lead_url(self, record_id: str) -> str:
        """Generate URL to Lead record"""
        return f"https://airtable.com/{self.airtable_base_id}/tblLeads/{record_id}"
    
    def get_new_triggers(self, days_back: int = 1) -> Dict[str, List[Dict]]:
        """Get triggers from the last N days, organized by source"""
        
        cutoff_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
        
        # Get all triggers
        all_triggers = self.trigger_history_table.all()
        
        # Organize by source
        triggers_by_source = {
            'News Intelligence': [],
            'Conference Monitor': [],
            'Lead Monitoring': [],
            'Other': []
        }
        
        for trigger in all_triggers:
            fields = trigger['fields']
            
            # Check if Status is "New" OR detected recently
            status = fields.get('Status', '')
            date_detected = fields.get('Date Detected', '')
            
            is_new_status = status == 'New'
            is_recent = date_detected >= cutoff_date if date_detected else False
            
            if is_new_status or is_recent:
                # Enrich with lead and company names
                enriched = self._enrich_trigger(trigger)
                
                # Determine source based on trigger type or source field
                source = fields.get('Source', '')
                trigger_type = fields.get('Trigger Type', '')
                
                # Categorize by source
                if 'news' in source.lower() or 'market' in source.lower():
                    triggers_by_source['News Intelligence'].append(enriched)
                elif 'conference' in source.lower() or trigger_type in ['SPEAKING', 'CONFERENCE_ATTENDANCE']:
                    triggers_by_source['Conference Monitor'].append(enriched)
                elif 'monitor' in source.lower() or 'surveillance' in source.lower():
                    triggers_by_source['Lead Monitoring'].append(enriched)
                else:
                    # Try to infer from trigger type
                    if trigger_type in ['FUNDING', 'PARTNERSHIP', 'ACQUISITION', 'EXPANSION', 'REGULATORY']:
                        triggers_by_source['News Intelligence'].append(enriched)
                    elif trigger_type in ['SPEAKING', 'CONFERENCE_ATTENDANCE', 'CONFERENCE']:
                        triggers_by_source['Conference Monitor'].append(enriched)
                    elif trigger_type in ['JOB_CHANGE', 'LINKEDIN_POST', 'CONTENT', 'HIRING']:
                        triggers_by_source['Lead Monitoring'].append(enriched)
                    else:
                        triggers_by_source['Other'].append(enriched)
        
        # Sort each category by urgency (HIGH first) then by date
        urgency_order = {'HIGH': 0, 'MEDIUM': 1, 'LOW': 2}
        for source in triggers_by_source:
            triggers_by_source[source].sort(key=lambda x: (
                urgency_order.get(x['fields'].get('Urgency', 'LOW'), 3),
                x['fields'].get('Date Detected', '9999')
            ))
        
        # Count total
        total = sum(len(t) for t in triggers_by_source.values())
        logger.info(f"Found {total} new/recent triggers")
        for source, triggers in triggers_by_source.items():
            if triggers:
                logger.info(f"  - {source}: {len(triggers)}")
        
        return triggers_by_source
    
    def _enrich_trigger(self, trigger: Dict) -> Dict:
        """Add lead and company names to trigger record"""
        fields = trigger['fields']
        
        # Get lead name and ID
        lead_name = "Unknown"
        lead_record_id = None
        lead_ids = fields.get('Lead', [])
        if lead_ids:
            lead_record_id = lead_ids[0]
            try:
                lead = self.leads_table.get(lead_record_id)
                lead_name = lead['fields'].get('Lead Name', 'Unknown')
            except:
                pass
        
        # Get company name
        company_name = "Unknown"
        company_ids = fields.get('Company', [])
        if company_ids:
            try:
                company = self.companies_table.get(company_ids[0])
                company_name = company['fields'].get('Company Name', 'Unknown')
            except:
                pass
        
        # Add to fields
        trigger['fields']['_lead_name'] = lead_name
        trigger['fields']['_company_name'] = company_name
        trigger['fields']['_lead_record_id'] = lead_record_id
        trigger['_id'] = trigger['id']
        
        return trigger
    
    def generate_html_email(self, triggers_by_source: Dict[str, List[Dict]]) -> str:
        """Generate beautiful HTML email organized by source"""
        
        # Count totals
        total_triggers = sum(len(t) for t in triggers_by_source.values())
        
        # Count by urgency across all sources
        all_triggers = [t for triggers in triggers_by_source.values() for t in triggers]
        high_count = len([t for t in all_triggers if t['fields'].get('Urgency') == 'HIGH'])
        medium_count = len([t for t in all_triggers if t['fields'].get('Urgency') == 'MEDIUM'])
        low_count = len([t for t in all_triggers if t['fields'].get('Urgency') == 'LOW'])
        
        # Source icons and colors
        source_config = {
            'News Intelligence': {'icon': '📰', 'color': '#0066cc', 'bg': '#e7f3ff'},
            'Conference Monitor': {'icon': '🎤', 'color': '#6f42c1', 'bg': '#f3e8ff'},
            'Lead Monitoring': {'icon': '👁️', 'color': '#fd7e14', 'bg': '#fff3e0'},
            'Other': {'icon': '📋', 'color': '#6c757d', 'bg': '#f8f9fa'}
        }
        
        # Generate sections for each source
        source_sections = ""
        for source_name, triggers in triggers_by_source.items():
            if not triggers:
                continue
            
            config = source_config.get(source_name, source_config['Other'])
            
            # Generate trigger cards for this source
            trigger_cards = self._generate_trigger_cards(triggers)
            
            source_sections += f"""
            <!-- {source_name} Section -->
            <tr>
                <td style="padding: 20px 20px 10px 20px;">
                    <table width="100%" cellpadding="0" cellspacing="0" style="background-color: {config['bg']}; border-radius: 8px; border-left: 4px solid {config['color']};">
                        <tr>
                            <td style="padding: 15px 20px;">
                                <span style="font-size: 24px; margin-right: 10px;">{config['icon']}</span>
                                <span style="font-size: 18px; font-weight: bold; color: {config['color']};">{source_name}</span>
                                <span style="display: inline-block; padding: 4px 10px; background-color: {config['color']}; color: white; border-radius: 12px; font-size: 12px; font-weight: bold; margin-left: 10px;">
                                    {len(triggers)} trigger{'s' if len(triggers) != 1 else ''}
                                </span>
                            </td>
                        </tr>
                    </table>
                </td>
            </tr>
            
            <!-- Trigger Cards for {source_name} -->
            <tr>
                <td style="padding: 0 20px 20px 20px;">
                    <table width="100%" cellpadding="0" cellspacing="0" style="background-color: #ffffff; border-radius: 8px; border: 1px solid #eee;">
                        {trigger_cards}
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
                <table width="650" cellpadding="0" cellspacing="0" style="background-color: #ffffff; border-radius: 12px; overflow: hidden; box-shadow: 0 4px 6px rgba(0,0,0,0.1);">
                    
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
                        <td style="padding: 20px; background-color: #f8f9fa;">
                            <table width="100%" cellpadding="0" cellspacing="0">
                                <tr>
                                    <td width="25%" style="text-align: center; padding: 15px;">
                                        <div style="font-size: 36px; font-weight: bold; color: #333;">{total_triggers}</div>
                                        <div style="font-size: 12px; color: #666; margin-top: 4px; text-transform: uppercase;">Total</div>
                                    </td>
                                    <td width="25%" style="text-align: center; padding: 15px; border-left: 1px solid #ddd;">
                                        <div style="font-size: 28px; font-weight: bold; color: #dc3545;">{high_count}</div>
                                        <div style="font-size: 12px; color: #666; margin-top: 4px;">🔴 High</div>
                                    </td>
                                    <td width="25%" style="text-align: center; padding: 15px; border-left: 1px solid #ddd;">
                                        <div style="font-size: 28px; font-weight: bold; color: #fd7e14;">{medium_count}</div>
                                        <div style="font-size: 12px; color: #666; margin-top: 4px;">🟡 Medium</div>
                                    </td>
                                    <td width="25%" style="text-align: center; padding: 15px; border-left: 1px solid #ddd;">
                                        <div style="font-size: 28px; font-weight: bold; color: #28a745;">{low_count}</div>
                                        <div style="font-size: 12px; color: #666; margin-top: 4px;">🟢 Low</div>
                                    </td>
                                </tr>
                            </table>
                        </td>
                    </tr>
                    
                    <!-- Source Sections -->
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
    
    def _generate_trigger_cards(self, triggers: List[Dict]) -> str:
        """Generate HTML for trigger cards"""
        
        trigger_rows = ""
        for trigger in triggers:
            fields = trigger['fields']
            urgency = fields.get('Urgency', 'LOW')
            
            # Urgency colors and icons
            if urgency == 'HIGH':
                urgency_color = '#dc3545'
                urgency_bg = '#fff5f5'
                urgency_icon = '🔴'
            elif urgency == 'MEDIUM':
                urgency_color = '#fd7e14'
                urgency_bg = '#fff8f0'
                urgency_icon = '🟡'
            else:
                urgency_color = '#28a745'
                urgency_bg = '#f0fff4'
                urgency_icon = '🟢'
            
            trigger_type = fields.get('Trigger Type', 'OTHER')
            lead_name = fields.get('_lead_name', 'Unknown')
            company_name = fields.get('_company_name', 'Unknown')
            description = fields.get('Description', 'No description')
            outreach_angle = fields.get('Outreach Angle', '')
            timing = fields.get('Timing Recommendation', '')
            event_date = fields.get('Event Date', '')
            
            # Get record IDs for links
            trigger_record_id = trigger.get('_id', '')
            lead_record_id = fields.get('_lead_record_id', '')
            
            # Generate Airtable URLs
            trigger_url = self._get_trigger_history_url(trigger_record_id) if trigger_record_id else '#'
            lead_url = self._get_lead_url(lead_record_id) if lead_record_id else '#'
            
            # Check if outreach messages exist
            has_email = bool(fields.get('Email Subject') or fields.get('Email Body'))
            
            # Build action buttons
            action_buttons = f"""
                <tr>
                    <td colspan="2" style="padding-top: 16px;">
                        <table cellpadding="0" cellspacing="0">
                            <tr>
                                <td style="padding-right: 8px;">
                                    <a href="{trigger_url}" style="display: inline-block; padding: 10px 16px; background-color: #667eea; color: white; text-decoration: none; border-radius: 6px; font-size: 13px; font-weight: 500;">
                                        📋 View Trigger
                                    </a>
                                </td>
                                <td style="padding-right: 8px;">
                                    <a href="{lead_url}" style="display: inline-block; padding: 10px 16px; background-color: #28a745; color: white; text-decoration: none; border-radius: 6px; font-size: 13px; font-weight: 500;">
                                        👤 View Lead
                                    </a>
                                </td>
                                {"<td><a href='" + trigger_url + "' style='display: inline-block; padding: 10px 16px; background-color: #0066cc; color: white; text-decoration: none; border-radius: 6px; font-size: 13px; font-weight: 500;'>✉️ Email Draft</a></td>" if has_email else ""}
                            </tr>
                        </table>
                    </td>
                </tr>
            """

            trigger_rows += f"""
            <tr>
                <td style="padding: 20px; border-bottom: 1px solid #eee;">
                    <table width="100%" cellpadding="0" cellspacing="0">
                        <tr>
                            <td>
                                <span style="display: inline-block; padding: 4px 12px; background-color: {urgency_color}; color: white; border-radius: 20px; font-size: 12px; font-weight: bold;">
                                    {urgency_icon} {urgency}
                                </span>
                                <span style="display: inline-block; padding: 4px 12px; background-color: #6c757d; color: white; border-radius: 20px; font-size: 12px; margin-left: 8px;">
                                    {trigger_type}
                                </span>
                            </td>
                            <td style="text-align: right; color: #666; font-size: 13px;">
                                {event_date}
                            </td>
                        </tr>
                        <tr>
                            <td colspan="2" style="padding-top: 12px;">
                                <div style="font-size: 18px; font-weight: bold; color: #333;">
                                    {lead_name}
                                </div>
                                <div style="font-size: 14px; color: #666; margin-top: 4px;">
                                    {company_name}
                                </div>
                            </td>
                        </tr>
                        <tr>
                            <td colspan="2" style="padding-top: 12px;">
                                <div style="font-size: 14px; color: #333; line-height: 1.5;">
                                    {description}
                                </div>
                            </td>
                        </tr>
                        {"<tr><td colspan='2' style='padding-top: 12px;'><div style='background-color: #e7f3ff; padding: 12px; border-radius: 8px; border-left: 4px solid #0066cc;'><strong style='color: #0066cc;'>💡 Outreach Angle:</strong><br><span style='color: #333;'>" + outreach_angle + "</span></div></td></tr>" if outreach_angle else ""}
                        {"<tr><td colspan='2' style='padding-top: 8px;'><div style='font-size: 13px; color: #666;'>⏰ <strong>Timing:</strong> " + timing + "</div></td></tr>" if timing else ""}
                        {action_buttons}
                    </table>
                </td>
            </tr>
            """
        
        return trigger_rows
    
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
        """Send email via SendGrid"""
        
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
            
            message = Mail(
                from_email=Email(self.from_email, "Lead Intelligence"),
                to_emails=To(self.to_email),
                subject=subject,
                html_content=HtmlContent(html_content)
            )
            
            response = sg.send(message)
            
            if response.status_code in [200, 201, 202]:
                logger.info(f"✓ Email sent successfully to {self.to_email}")
                return True
            else:
                logger.error(f"Email send failed: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"Error sending email: {str(e)}")
            return False
    
    def mark_triggers_as_notified(self, triggers: List[Dict]):
        """Update trigger status to 'Notified' after sending email"""
        
        for trigger in triggers:
            try:
                self.trigger_history_table.update(trigger['_id'], {
                    'Status': 'Notified',
                    'Notified Date': datetime.now().strftime('%Y-%m-%d')
                })
            except Exception as e:
                logger.warning(f"Could not update trigger status: {e}")
    
    def run(self, days_back: int = 1, skip_if_empty: bool = False, mark_notified: bool = True):
        """Main workflow"""
        
        logger.info("=" * 60)
        logger.info("DAILY TRIGGER DIGEST")
        logger.info(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        logger.info("=" * 60)
        
        # Get new triggers organized by source
        triggers_by_source = self.get_new_triggers(days_back=days_back)
        
        # Calculate total
        total_triggers = sum(len(t) for t in triggers_by_source.values())
        all_triggers = [t for triggers in triggers_by_source.values() for t in triggers]
        
        # Skip sending if no triggers and skip_if_empty is True
        if total_triggers == 0 and skip_if_empty:
            logger.info("No new triggers - skipping email (skip_if_empty=True)")
            return True
        
        # Generate email content
        if total_triggers > 0:
            html_content = self.generate_html_email(triggers_by_source)
        else:
            html_content = self.generate_no_triggers_email()
        
        # Send email
        success = self.send_email(html_content, total_triggers)
        
        # Mark triggers as notified
        if success and all_triggers and mark_notified:
            logger.info("Marking triggers as notified...")
            self.mark_triggers_as_notified(all_triggers)
        
        logger.info("=" * 60)
        logger.info("DIGEST COMPLETE")
        logger.info(f"Triggers: {total_triggers} | Email sent: {'Yes' if success else 'No'}")
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
