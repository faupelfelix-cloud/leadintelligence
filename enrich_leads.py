#!/usr/bin/env python3
"""
Lead Enrichment Script
Finds missing contact information for leads using web search and AI
"""

import os
import sys
import yaml
import json
import time
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
import anthropic
from pyairtable import Api

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('enrichment.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class LeadEnricher:
    """Handles lead data enrichment using web search and AI"""
    
    def __init__(self, config_path: str = "config.yaml"):
        """Initialize with configuration"""
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        # Initialize APIs
        self.airtable = Api(self.config['airtable']['api_key'])
        self.base = self.airtable.base(self.config['airtable']['base_id'])
        self.leads_table = self.base.table(self.config['airtable']['tables']['leads'])
        self.companies_table = self.base.table(self.config['airtable']['tables']['companies'])
        self.intelligence_table = self.base.table(self.config['airtable']['tables']['intelligence_log'])
        
        self.anthropic_client = anthropic.Anthropic(
            api_key=self.config['anthropic']['api_key']
        )
        
        logger.info("LeadEnricher initialized successfully")
    
    def get_leads_to_enrich(self, status: str = "Not Enriched") -> List[Dict]:
        """Fetch leads that need enrichment"""
        formula = f"{{Enrichment Status}} = '{status}'"
        records = self.leads_table.all(formula=formula)
        logger.info(f"Found {len(records)} leads with status '{status}'")
        return records
    
    def get_company_info(self, company_record_ids: List[str]) -> Optional[Dict]:
        """Fetch company information for context"""
        if not company_record_ids:
            return None
        
        try:
            company = self.companies_table.get(company_record_ids[0])
            return company['fields']
        except:
            return None
    
    def search_lead_info(self, lead_name: str, company_name: str, 
                        current_title: Optional[str] = None,
                        company_website: Optional[str] = None) -> Dict[str, Any]:
        """Use Claude with web search to find missing lead information"""
        
        context = f"Lead: {lead_name} at {company_name}"
        if current_title:
            context += f" (Current title: {current_title})"
        if company_website:
            context += f"\nCompany website: {company_website}"
        
        search_prompt = f"""You are a business intelligence researcher specializing in finding professional contact information.

{context}

Find and verify the following information:

CONTACT INFORMATION:
- Professional email address - CRITICAL: Put maximum effort into finding this
  * Search company website thoroughly: team page, about us, contact page, leadership bios
  * Check press releases and news articles (often quote emails)
  * Look for conference speaker lists (usually include contact info)
  * Search for published papers, patents, posters (author contact emails)
  * Check LinkedIn "Contact Info" section (sometimes public)
  * Search "[name] [company] email" directly
  * Look for university/previous company emails if recently moved
  * If not found: Research company email pattern from OTHER employees (firstname.lastname@company.com, flastname@company.com, etc.) and suggest pattern
  * Try tools like RocketReach, Hunter.io results if they appear in search
- Current job title (verify it's current, not outdated)
- LinkedIn profile URL (ensure it's the correct person)
- X (Twitter) profile URL (if they have one - look for verified account or bio mentioning their company/role)

EMAIL FINDING PRIORITY:
This is the MOST IMPORTANT field. Spend extra search effort finding the email.
- Search at least 5-10 different sources for email
- Try multiple search queries with variations
- If you find company email pattern, suggest it with "Pattern Suggested" confidence
- Example patterns: firstname.lastname@company.com, f.lastname@company.com, firstnamel@company.com

IMPORTANT GUIDELINES:
- For email: Only provide if found on official sources. If not found, suggest likely pattern based on company email format if you can identify it, but mark as "needs verification"
- For title: Make sure it's their CURRENT title, not a previous role
- For LinkedIn: Verify it's the right person by cross-referencing company and location
- For X profile: Format as full URL (https://x.com/username or https://twitter.com/username). Only include if confident it's the right person
- Prioritize recent, official sources (last 12 months)

Return your findings in this exact JSON format:
{{
  "email": "email@company.com or null",
  "email_confidence": "High/Medium/Low/Pattern Suggested",
  "email_source": "source description or null",
  "title": "Current Job Title or null",
  "title_confidence": "High/Medium/Low",
  "title_source": "source description or null",
  "linkedin_url": "LinkedIn URL or null",
  "linkedin_confidence": "High/Medium/Low",
  "linkedin_source": "source description or null",
  "x_profile": "https://x.com/username or null",
  "x_confidence": "High/Medium/Low",
  "x_source": "source description or null",
  "recent_activity": "Any recent news, posts, or mentions (optional)",
  "last_updated": "Date of most recent information found",
  "sources": ["url1", "url2"],
  "overall_confidence": "High/Medium/Low"
}}

Only return the JSON, no other text."""

        try:
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
                    "content": search_prompt
                }]
            )
            
            # Extract text content from response
            result_text = ""
            for block in message.content:
                if block.type == "text":
                    result_text += block.text
            
            # Parse JSON from response
            result_text = result_text.strip()
            if result_text.startswith("```json"):
                result_text = result_text[7:]
            if result_text.startswith("```"):
                result_text = result_text[3:]
            if result_text.endswith("```"):
                result_text = result_text[:-3]
            
            result = json.loads(result_text.strip())
            logger.info(f"Successfully enriched {lead_name}")
            return result
            
        except Exception as e:
            logger.error(f"Error enriching {lead_name}: {str(e)}")
            return {
                "overall_confidence": "Failed",
                "error": str(e)
            }
    
    def update_lead_record(self, record_id: str, enriched_data: Dict):
        """Update Airtable lead record with enriched data"""
        
        # Determine overall confidence
        overall_conf = enriched_data.get('overall_confidence', 'Low')
        
        # Map confidence to valid Airtable options
        confidence_map = {
            'high': 'High',
            'medium': 'Medium', 
            'low': 'Low',
            'failed': 'Low'
        }
        confidence = confidence_map.get(overall_conf.lower(), 'Low')
        
        # Prepare update payload
        update_fields = {
            'Enrichment Status': 'Enriched' if overall_conf != 'Failed' else 'Failed',
            'Enrichment Confidence': confidence
        }
        
        # Build intelligence notes
        notes_parts = []
        
        # Add email info with validation
        if enriched_data.get('email'):
            email = enriched_data['email'].strip()
            # Basic email validation
            if '@' in email and '.' in email:
                update_fields['Email'] = email
                email_conf = enriched_data.get('email_confidence', 'Unknown')
                email_source = enriched_data.get('email_source', 'Not specified')
                notes_parts.append(f"Email: {email} (Confidence: {email_conf}, Source: {email_source})")
                
                # If pattern suggested, add warning
                if 'pattern' in email_conf.lower() or 'suggested' in email_conf.lower():
                    notes_parts.append("⚠️ Email is a suggested pattern - needs verification before use")
            else:
                logger.warning(f"Invalid email format: {email}")
        
        # Add title info
        if enriched_data.get('title'):
            title = enriched_data['title'].strip()
            update_fields['Title'] = title
            title_conf = enriched_data.get('title_confidence', 'Unknown')
            title_source = enriched_data.get('title_source', 'Not specified')
            notes_parts.append(f"Title: {title} (Confidence: {title_conf}, Source: {title_source})")
        
        # Add LinkedIn info with URL validation
        if enriched_data.get('linkedin_url'):
            linkedin_url = enriched_data['linkedin_url'].strip()
            # Basic LinkedIn URL validation
            if 'linkedin.com' in linkedin_url.lower():
                update_fields['LinkedIn URL'] = linkedin_url
                linkedin_conf = enriched_data.get('linkedin_confidence', 'Unknown')
                notes_parts.append(f"LinkedIn: Verified (Confidence: {linkedin_conf})")
            else:
                logger.warning(f"Invalid LinkedIn URL: {linkedin_url}")
        
        # Add X (Twitter) profile with URL validation
        if enriched_data.get('x_profile'):
            x_profile = enriched_data['x_profile'].strip()
            # Basic X/Twitter URL validation
            if 'x.com' in x_profile.lower() or 'twitter.com' in x_profile.lower():
                update_fields['X Profile'] = x_profile
                x_conf = enriched_data.get('x_confidence', 'Unknown')
                notes_parts.append(f"X Profile: Found (Confidence: {x_conf})")
            else:
                logger.warning(f"Invalid X profile URL: {x_profile}")
        
        # Add recent activity if available
        if enriched_data.get('recent_activity'):
            notes_parts.append(f"\nRecent Activity: {enriched_data['recent_activity']}")
        
        # Add last updated
        if enriched_data.get('last_updated'):
            notes_parts.append(f"\nLast Updated: {enriched_data['last_updated']}")
        
        # Compile notes (append to existing, don't overwrite)
        if notes_parts:
            new_notes = '\n'.join(notes_parts)
            enrichment_header = f"\n\n---\nEnrichment on {datetime.now().strftime('%Y-%m-%d')}:\n"
            update_fields['Intelligence Notes'] = enrichment_header + new_notes
        
        # Update the record with error handling
        try:
            # For Intelligence Notes, we want to append, not replace
            # First get existing notes if any
            if 'Intelligence Notes' in update_fields:
                try:
                    existing_record = self.leads_table.get(record_id)
                    existing_notes = existing_record['fields'].get('Intelligence Notes', '')
                    if existing_notes:
                        update_fields['Intelligence Notes'] = existing_notes + update_fields['Intelligence Notes']
                except:
                    pass  # If we can't get existing, just use new
            
            self.leads_table.update(record_id, update_fields)
            logger.info(f"✓ Updated lead record {record_id} (Confidence: {confidence})")
        except Exception as e:
            logger.error(f"✗ Failed to update record {record_id}: {str(e)}")
            logger.error(f"Attempted to update with fields: {list(update_fields.keys())}")
            raise
        
        # Log intelligence if sources available
        if enriched_data.get('sources'):
            try:
                self.log_intelligence(
                    record_type='Lead',
                    lead_id=record_id,
                    summary=f"Enriched contact data (Confidence: {confidence})",
                    sources=enriched_data['sources']
                )
            except Exception as e:
                logger.warning(f"Could not log intelligence: {str(e)}")

    
    def log_intelligence(self, record_type: str, lead_id: str, 
                        summary: str, sources: List[str]):
        """Log intelligence gathering to Intelligence Log table"""
        
        intelligence_record = {
            'Date': datetime.now().strftime('%Y-%m-%d'),  # Airtable date format
            'Record Type': record_type,
            'Summary': summary,
            'Intelligence Type': 'Enrichment',
            'Confidence Level': 'High',
            'Source URL': sources[0] if sources else None,
            'Lead': [lead_id]
        }
        
        self.intelligence_table.create(intelligence_record)
    
    def enrich_leads(self, status: str = "Not Enriched", limit: Optional[int] = None):
        """Main enrichment workflow"""
        leads = self.get_leads_to_enrich(status)
        
        if limit:
            leads = leads[:limit]
        
        total = len(leads)
        logger.info(f"Starting enrichment of {total} leads")
        
        success_count = 0
        failed_count = 0
        
        for idx, lead in enumerate(leads, 1):
            fields = lead['fields']
            lead_name = fields.get('Lead Name', 'Unknown')
            record_id = lead['id']
            current_title = fields.get('Title')
            
            # Get company info for context
            company_name = "Unknown Company"
            company_website = None
            if 'Company' in fields:
                company_info = self.get_company_info(fields['Company'])
                if company_info:
                    company_name = company_info.get('Company Name', 'Unknown Company')
                    company_website = company_info.get('Website')
            
            logger.info(f"[{idx}/{total}] Processing: {lead_name} at {company_name}")
            
            max_retries = self.config['processing'].get('max_retries', 3)
            retry_delay = self.config['processing'].get('retry_delay', 5)
            
            for attempt in range(max_retries):
                try:
                    # Search and enrich
                    logger.info(f"  Searching for contact info... (attempt {attempt + 1}/{max_retries})")
                    enriched_data = self.search_lead_info(
                        lead_name=lead_name,
                        company_name=company_name,
                        current_title=current_title,
                        company_website=company_website
                    )
                    
                    # Check if enrichment actually returned data
                    if enriched_data.get('overall_confidence') == 'Failed' or enriched_data.get('error'):
                        error_msg = enriched_data.get('error', 'AI could not find sufficient information')
                        logger.warning(f"  Enrichment returned failure: {error_msg}")
                        if attempt < max_retries - 1:
                            logger.info(f"  Retrying in {retry_delay} seconds...")
                            time.sleep(retry_delay)
                            continue
                        else:
                            # Final attempt failed
                            self.leads_table.update(record_id, {
                                'Enrichment Status': 'Failed',
                                'Enrichment Confidence': 'Low',
                                'Intelligence Notes': f"Failed after {max_retries} attempts: {error_msg}"
                            })
                            failed_count += 1
                            break
                    
                    # Update Airtable
                    logger.info(f"  Updating Airtable record...")
                    self.update_lead_record(record_id, enriched_data)
                    success_count += 1
                    logger.info(f"  ✓ Successfully enriched {lead_name}")
                    
                    # Rate limiting
                    time.sleep(self.config['web_search']['rate_limit_delay'])
                    break  # Success, exit retry loop
                    
                except json.JSONDecodeError as e:
                    logger.error(f"  ✗ JSON parsing error: {str(e)}")
                    if attempt < max_retries - 1:
                        logger.info(f"  Retrying in {retry_delay} seconds...")
                        time.sleep(retry_delay)
                    else:
                        self.leads_table.update(record_id, {
                            'Enrichment Status': 'Failed',
                            'Enrichment Confidence': 'Low',
                            'Intelligence Notes': f"JSON parsing error after {max_retries} attempts"
                        })
                        failed_count += 1
                
                except Exception as e:
                    logger.error(f"  ✗ Error enriching {lead_name}: {str(e)}")
                    if attempt < max_retries - 1:
                        logger.info(f"  Retrying in {retry_delay} seconds...")
                        time.sleep(retry_delay)
                    else:
                        # Mark as failed after all retries
                        try:
                            self.leads_table.update(record_id, {
                                'Enrichment Status': 'Failed',
                                'Enrichment Confidence': 'Low',
                                'Intelligence Notes': f"Error after {max_retries} attempts: {str(e)}"
                            })
                        except Exception as update_error:
                            logger.error(f"  ✗ Could not even mark as failed: {str(update_error)}")
                        failed_count += 1
        
        logger.info(f"\n{'='*60}")
        logger.info(f"Enrichment complete!")
        logger.info(f"Total processed: {total}")
        logger.info(f"Successful: {success_count}")
        logger.info(f"Failed: {failed_count}")
        logger.info(f"{'='*60}")



def main():
    """CLI entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Enrich lead records with contact information')
    parser.add_argument('--status', default='Not Enriched', 
                       help='Enrichment status to filter by (default: Not Enriched)')
    parser.add_argument('--limit', type=int, default=None,
                       help='Limit number of leads to process')
    parser.add_argument('--config', default='config.yaml',
                       help='Path to config file')
    
    args = parser.parse_args()
    
    try:
        enricher = LeadEnricher(config_path=args.config)
        enricher.enrich_leads(status=args.status, limit=args.limit)
    except FileNotFoundError:
        logger.error(f"Config file not found: {args.config}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
