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
    
    def calculate_lead_icp_score(self, lead_data: Dict, company_icp: int = None) -> tuple:
        """
        Calculate Lead ICP score based on title, seniority, function, etc.
        Returns: (score, tier, justification_text, combined_priority)
        """
        score = 0
        justification = []
        
        title = lead_data.get('title', '').lower() if lead_data.get('title') else ''
        
        # 1. Title/Role Relevance (0-25 points)
        title_score = self.score_title_relevance(title)
        score += title_score
        title_display = lead_data.get('title', 'Unknown')
        if title_score >= 20:
            justification.append(f"✓ Title: {title_display} (+{title_score} pts - PRIMARY INFLUENCER)")
        elif title_score >= 15:
            justification.append(f"✓ Title: {title_display} (+{title_score} pts - SECONDARY INFLUENCER)")
        elif title_score >= 8:
            justification.append(f"○ Title: {title_display} (+{title_score} pts)")
        else:
            justification.append(f"✗ Title: {title_display} (+{title_score} pts - LOW RELEVANCE)")
        
        # 2. Seniority Level (0-20 points)
        seniority_score = self.score_seniority(title)
        score += seniority_score
        if seniority_score >= 18:
            justification.append(f"✓ Seniority: C-Level/VP (+{seniority_score} pts)")
        elif seniority_score >= 15:
            justification.append(f"✓ Seniority: Director (+{seniority_score} pts)")
        elif seniority_score >= 10:
            justification.append(f"○ Seniority: Senior Manager (+{seniority_score} pts)")
        else:
            justification.append(f"○ Seniority: Manager/IC (+{seniority_score} pts)")
        
        # 3. Function Fit (0-20 points)
        function_score = self.score_function_fit(title)
        score += function_score
        if function_score >= 18:
            justification.append(f"✓ Function: Manufacturing/Ops (+{function_score} pts - PERFECT)")
        elif function_score >= 15:
            justification.append(f"✓ Function: Operations (+{function_score} pts)")
        elif function_score >= 10:
            justification.append(f"○ Function: R&D/Tech (+{function_score} pts)")
        else:
            justification.append(f"✗ Function: Other (+{function_score} pts)")
        
        # 4. Decision Power (0-15 points)
        decision_score = self.score_decision_power(title)
        score += decision_score
        if decision_score >= 12:
            justification.append(f"✓ Decision Power: Budget authority (+{decision_score} pts)")
        elif decision_score >= 8:
            justification.append(f"○ Decision Power: Strong influence (+{decision_score} pts)")
        else:
            justification.append(f"○ Decision Power: Limited (+{decision_score} pts)")
        
        # 5. Career Stage (0-10 points) - default
        career_score = 8
        justification.append(f"○ Career Stage: Established (+{career_score} pts)")
        score += career_score
        
        # 6. Geography (0-5 points)
        location = lead_data.get('location', '') or ''
        geo_score = self.score_geography(location.lower() if location else '')
        score += geo_score
        if geo_score >= 5:
            justification.append(f"✓ Geography: Europe (+{geo_score} pts)")
        elif geo_score >= 4:
            justification.append(f"✓ Geography: US (+{geo_score} pts)")
        else:
            loc_display = location if location else 'Unknown'
            justification.append(f"○ Geography: {loc_display} (+{geo_score} pts)")
        
        # 7. Engagement (0-5 points) - default
        engagement_score = 3
        justification.append(f"○ Engagement: Not yet analyzed (+{engagement_score} pts)")
        score += engagement_score
        
        # Determine tier
        if score >= 85:
            tier = "Perfect Fit (Tier 1)"
        elif score >= 70:
            tier = "Strong Fit (Tier 2)"
        elif score >= 55:
            tier = "Good Fit (Tier 3)"
        elif score >= 40:
            tier = "Acceptable Fit (Tier 4)"
        else:
            tier = "Poor Fit (Tier 5)"
        
        # Build justification
        justification_text = "\n".join(justification)
        justification_text += f"\n\nTOTAL: {score}/100 points\n→ {tier}"
        
        # Combined priority
        combined_priority = None
        if company_icp is not None:
            combined_priority = self.calculate_combined_priority(company_icp, score)
            justification_text += f"\n\nCOMPANY ICP: {company_icp}/105\n→ COMBINED: {combined_priority}"
        
        return (score, tier, justification_text, combined_priority)
    
    def score_title_relevance(self, title: str) -> int:
        """Score title relevance (0-25 points)"""
        if not title:
            return 0
        
        decision_makers = ['ceo', 'coo', 'president', 'cso', 'cto', 'founder']
        if any(dm in title for dm in decision_makers):
            return 25
        
        primary = ['vp manufacturing', 'vp technical operations', 'vp operations',
                  'vp supply chain', 'vp cmc', 'svp manufacturing', 'svp operations',
                  'head of manufacturing', 'head of operations', 'head of supply chain']
        if any(p in title for p in primary):
            return 20
        
        secondary = ['director manufacturing', 'director operations', 'director supply chain',
                    'director cmc', 'senior director']
        if any(s in title for s in secondary):
            return 15
        
        if any(c in title for c in ['associate director', 'senior manager', 'manager']):
            return 8
        
        if any(l in title for l in ['scientist', 'research', 'clinical']):
            return 3
        
        return 0
    
    def score_seniority(self, title: str) -> int:
        """Score seniority (0-20 points)"""
        if not title:
            return 0
        
        if any(c in title for c in ['ceo', 'coo', 'cso', 'cto', 'cfo', 'chief']):
            return 20
        if any(v in title for v in ['vp', 'vice president', 'svp']):
            return 18
        if 'head of' in title or ('director' in title and 'associate' not in title):
            return 15
        if 'senior manager' in title or 'associate director' in title:
            return 10
        if 'manager' in title:
            return 5
        return 2
    
    def score_function_fit(self, title: str) -> int:
        """Score function fit (0-20 points)"""
        if not title:
            return 0
        
        if any(p in title for p in ['manufacturing', 'technical operations', 'cmc', 'supply chain']):
            return 20
        if any(g in title for g in ['operations', 'production', 'quality']):
            return 15
        if any(a in title for a in ['r&d', 'research', 'development', 'process']):
            return 10
        if any(l in title for l in ['clinical', 'regulatory', 'business']):
            return 5
        return 0
    
    def score_decision_power(self, title: str) -> int:
        """Score decision power (0-15 points)"""
        if not title:
            return 0
        
        if any(b in title for b in ['ceo', 'coo', 'cfo', 'vp', 'svp', 'head of']):
            return 15
        if 'director' in title and 'associate' not in title:
            return 12
        if 'senior manager' in title or 'associate director' in title:
            return 8
        if 'manager' in title:
            return 4
        return 0
    
    def score_geography(self, location: str) -> int:
        """Score geography (0-5 points)"""
        if not location:
            return 3
        
        europe = ['germany', 'poland', 'uk', 'france', 'netherlands', 'switzerland',
                 'belgium', 'sweden', 'denmark', 'austria', 'italy', 'spain']
        if any(e in location for e in europe):
            return 5
        
        us = ['usa', 'united states', 'california', 'massachusetts', 'new york']
        if any(u in location for u in us):
            return 4
        
        return 2
    
    def calculate_combined_priority(self, company_icp: int, lead_icp: int) -> str:
        """Calculate combined priority"""
        if company_icp >= 90 and lead_icp >= 70:
            return "🔥 HOT - Priority 1"
        elif company_icp >= 90 and lead_icp >= 55:
            return "📈 WARM - Priority 2"
        elif company_icp >= 75 and lead_icp >= 70:
            return "📈 WARM - Priority 2"
        elif company_icp >= 75 and lead_icp >= 55:
            return "➡️ MEDIUM - Priority 3"
        elif company_icp >= 60 and lead_icp >= 40:
            return "⬇️ LOW - Priority 4"
        else:
            return "❌ SKIP - Priority 5"
    
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
        
        # Calculate Lead ICP Score
        # Get company ICP if lead is linked to company
        try:
            existing_record = self.leads_table.get(record_id)
            company_ids = existing_record['fields'].get('Company', [])
            company_icp = None
            
            if company_ids:
                try:
                    company = self.companies_table.get(company_ids[0])
                    company_icp = company['fields'].get('ICP Fit Score')
                except:
                    pass
            
            # Calculate Lead ICP
            lead_icp_score, lead_icp_tier, lead_icp_justification, combined_priority = self.calculate_lead_icp_score(
                enriched_data, 
                company_icp
            )
            
            update_fields['Lead ICP Score'] = lead_icp_score
            update_fields['Lead ICP Tier'] = lead_icp_tier
            update_fields['Lead ICP Justification'] = lead_icp_justification
            
            if combined_priority:
                update_fields['Combined Priority'] = combined_priority
                logger.info(f"  Lead ICP: {lead_icp_score}/100 ({lead_icp_tier}) | Combined: {combined_priority}")
            else:
                logger.info(f"  Lead ICP: {lead_icp_score}/100 ({lead_icp_tier})")
        except Exception as e:
            logger.warning(f"  Could not calculate Lead ICP: {str(e)}")
        
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
