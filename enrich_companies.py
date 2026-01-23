#!/usr/bin/env python3
"""
Company Enrichment Script
Automatically enriches company records with business intelligence using web search and Claude AI
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


class CompanyEnricher:
    """Handles company data enrichment using web search and AI analysis"""
    
    # Valid Airtable field options (must match exactly)
    VALID_COMPANY_SIZE = ['1-10', '11-50', '51-200', '201-500', '501-1000', '1000+']
    VALID_FOCUS_AREAS = ['mAbs', 'Bispecifics', 'ADCs', 'Recombinant Proteins', 
                         'Cell Therapy', 'Gene Therapy', 'Vaccines', 'Other']
    VALID_TECH_PLATFORMS = ['Mammalian CHO', 'Mammalian Non-CHO', 'Microbial', 'Cell-Free', 'Other']
    VALID_FUNDING_STAGES = ['Seed', 'Series A', 'Series B', 'Series C', 'Series D+', 'Public', 'Acquired', 'Unknown']
    VALID_PIPELINE_STAGES = ['Preclinical', 'Phase 1', 'Phase 2', 'Phase 3', 'Commercial', 'Unknown']
    VALID_THERAPEUTIC_AREAS = ['Oncology', 'Autoimmune', 'Rare Disease', 'Infectious Disease', 
                               'CNS', 'Metabolic', 'Other']
    VALID_MANUFACTURING_STATUS = ['No Public Partner', 'Has Partner', 'Building In-House', 'Unknown']
    
    def __init__(self, config_path: str = "config.yaml"):
        """Initialize with configuration"""
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        # Initialize APIs
        self.airtable = Api(self.config['airtable']['api_key'])
        self.base = self.airtable.base(self.config['airtable']['base_id'])
        self.companies_table = self.base.table(self.config['airtable']['tables']['companies'])
        self.intelligence_table = self.base.table(self.config['airtable']['tables']['intelligence_log'])
        
        # Try to access Company Profile and ICP Scoring tables (optional but recommended)
        try:
            self.company_profile_table = self.base.table('Company Profile')
            self.icp_scoring_table = self.base.table('ICP Scoring Criteria')
            self.has_strategic_tables = True
            logger.info("✓ Company Profile and ICP Scoring tables found")
        except:
            self.company_profile_table = None
            self.icp_scoring_table = None
            self.has_strategic_tables = False
            logger.info("Note: Company Profile/ICP Scoring tables not found - using basic scoring")
        
        self.anthropic_client = anthropic.Anthropic(
            api_key=self.config['anthropic']['api_key']
        )
        
        logger.info("CompanyEnricher initialized successfully")
    
    def validate_single_select(self, value: str, valid_options: List[str], default: str = 'Unknown') -> str:
        """Validate and map a single select field value"""
        if not value:
            return default
        
        value = str(value).strip()
        
        # Exact match
        if value in valid_options:
            return value
        
        # Case-insensitive match
        value_lower = value.lower() if value else None
        if value_lower:
            for option in valid_options:
                if option and option.lower() == value_lower:
                    return option
        
        # Fuzzy match (contains)
        for option in valid_options:
            if option and value_lower and (value_lower in option.lower() or option.lower() in value_lower):
                logger.warning(f"Fuzzy matched '{value}' to '{option}'")
                return option
        
        # No match found
        logger.warning(f"Could not match '{value}' to valid options {valid_options}. Using default: {default}")
        return default
    
    def validate_multiple_select(self, values: List[str], valid_options: List[str]) -> List[str]:
        """Validate and map multiple select field values"""
        if not values or not isinstance(values, list):
            return []
        
        validated = []
        for value in values:
            matched = self.validate_single_select(value, valid_options, default=None)
            if matched and matched != 'Unknown':
                validated.append(matched)
        
        # If nothing matched, add 'Other' if available
        if not validated and 'Other' in valid_options:
            validated.append('Other')
        
        return validated
    
    def parse_company_size(self, size_str: str) -> str:
        """Parse company size from various formats to valid Airtable option"""
        if not size_str:
            return '11-50'  # Default assumption for biotech
        
        size_str = str(size_str).lower().replace(',', '')
        
        # Extract numbers
        import re
        numbers = re.findall(r'\d+', size_str)
        
        if not numbers:
            return '11-50'
        
        # Take the first or average number found
        num = int(numbers[0])
        
        if num <= 10:
            return '1-10'
        elif num <= 50:
            return '11-50'
        elif num <= 200:
            return '51-200'
        elif num <= 500:
            return '201-500'
        elif num <= 1000:
            return '501-1000'
        else:
            return '1000+'
    
    def get_companies_to_enrich(self, status: str = "Not Enriched") -> List[Dict]:
        """Fetch companies that need enrichment"""
        formula = f"{{Enrichment Status}} = '{status}'"
        records = self.companies_table.all(formula=formula)
        logger.info(f"Found {len(records)} companies with status '{status}'")
        return records
    
    def search_company_info(self, company_name: str) -> Dict[str, Any]:
        """Use Claude with web search to gather company intelligence"""
        
        search_prompt = f"""You are a business intelligence researcher specializing in biologics and pharmaceutical companies.

Research the company: {company_name}

Find and extract the following information:

BASIC INFO:
- Official website URL
- LinkedIn company page URL
- Headquarters location (City, Country)
- Company size (number of employees)

BUSINESS INTELLIGENCE:
- Focus areas - MUST be one or more from this EXACT list: {', '.join(self.VALID_FOCUS_AREAS)}
- Technology platform - MUST be one or more from this EXACT list: {', '.join(self.VALID_TECH_PLATFORMS)}
- Therapeutic areas - MUST be one or more from this EXACT list: {', '.join(self.VALID_THERAPEUTIC_AREAS)}

FUNDING & PIPELINE:
- Current funding stage - MUST be EXACTLY one of: {', '.join(self.VALID_FUNDING_STAGES)}
- Total funding amount in USD (just the number, e.g., "75000000" for $75M)
- Latest funding round details (e.g., "Series B - $75M - Oct 2024")
- Pipeline stage - MUST be one or more from this EXACT list: {', '.join(self.VALID_PIPELINE_STAGES)}
- Lead programs and their indications (free text description)

CDMO RELEVANCE:
- Any publicly announced CDMO partnerships (Lonza, Samsung Biologics, Fujifilm, etc.)
- Manufacturing status - MUST be EXACTLY one of: {', '.join(self.VALID_MANUFACTURING_STATUS)}
- Recent news about manufacturing, CMC, or technical operations

IMPORTANT INSTRUCTIONS:
- For select fields, you MUST use EXACTLY the options provided above (copy them exactly, including capitalization)
- If you cannot determine a value, use "Unknown" for single select fields or "Other" for multiple select fields
- If a company does multiple things, you can select multiple options for Focus Area, Technology Platform, Therapeutic Areas, and Pipeline Stage
- For dates, always include the year (e.g., "Oct 2024" not just "October")

Return your findings in this exact JSON format:
{{
  "website": "URL or null",
  "linkedin_company_page": "URL or null",
  "location": "City, Country or null",
  "company_size_employees": 50,
  "focus_areas": ["exact option from list", "another exact option"] or [],
  "technology_platforms": ["exact option from list"] or [],
  "therapeutic_areas": ["exact option from list"] or [],
  "funding_stage": "exact option from list",
  "total_funding_usd": 75000000,
  "latest_funding_round": "Series B - $75M - Oct 2024",
  "pipeline_stages": ["exact option from list"] or [],
  "lead_programs": "description or null",
  "cdmo_partnerships": "details or null",
  "manufacturing_status": "exact option from list",
  "confidence": "High/Medium/Low",
  "sources": ["url1", "url2"],
  "intelligence_notes": "Key findings and recent news"
}}

Only return valid JSON, no other text."""

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
            # Remove markdown code blocks if present
            result_text = result_text.strip()
            if result_text.startswith("```json"):
                result_text = result_text[7:]
            if result_text.startswith("```"):
                result_text = result_text[3:]
            if result_text.endswith("```"):
                result_text = result_text[:-3]
            
            result = json.loads(result_text.strip())
            logger.info(f"Successfully enriched {company_name}")
            return result
            
        except Exception as e:
            logger.error(f"Error enriching {company_name}: {str(e)}")
            return {
                "confidence": "Failed",
                "error": str(e)
            }
    
    def calculate_icp_score(self, company_data: Dict) -> int:
        """Calculate ICP Fit Score based on criteria from ICP Scoring table or fallback"""
        
        if self.has_strategic_tables:
            return self.calculate_icp_score_strategic(company_data)
        else:
            return self.calculate_icp_score_basic(company_data)
    
    def calculate_icp_score_strategic(self, company_data: Dict) -> int:
        """Calculate ICP score using Company Profile and ICP Scoring Criteria tables"""
        score = 0
        max_score = 105  # Total possible from all criteria
        
        # Company Size Score (0-15 points)
        company_size = company_data.get('company_size', '')
        if '51-200' in company_size or '201-500' in company_size:
            score += 15  # Lower mid-size - PERFECT
        elif '501-1000' in company_size:
            score += 15  # Upper mid-size - PERFECT
        elif '1000+' in company_size:
            score += 3  # Large - low priority
        elif '11-50' in company_size:
            score += 3  # Startup - low priority
        elif '1-10' in company_size:
            score += 1  # Too small
        
        # Revenue Score (0-15 points) - estimated from company size + funding
        funding = company_data.get('funding_stage', '').lower()
        if 'series b' in funding:
            score += 7
        elif 'series c' in funding or 'public' in funding:
            score += 15  # Has capital
        elif 'series a' in funding:
            score += 3
        
        # Pipeline Stage Score (0-20 points)
        pipeline_stages = [s.lower() for s in company_data.get('pipeline_stages', [])]
        if ('phase 2' in pipeline_stages or 'phase 3' in pipeline_stages):
            score += 20  # Clinical stage - PERFECT
        elif 'commercial' in pipeline_stages:
            score += 15  # Commercial - good
        elif 'phase 1' in pipeline_stages:
            score += 10
        elif 'preclinical' in pipeline_stages:
            score += 5
        
        # Technology Platform Score (0-20 points) - CRITICAL
        platforms = [p.lower() for p in company_data.get('technology_platforms', [])]
        focus_areas = [f.lower() for f in company_data.get('focus_areas', [])]
        
        # Check if purely mammalian
        mammalian_keywords = ['mammalian', 'cho', 'mab', 'monoclonal', 'bispecific', 'adc', 'antibody']
        non_mammalian_keywords = ['cell therapy', 'gene therapy', 'viral', 'mrna', 'oligo', 'vaccine']
        
        has_mammalian = any(kw in str(platforms + focus_areas).lower() for kw in mammalian_keywords)
        has_non_mammalian = any(kw in str(platforms + focus_areas).lower() for kw in non_mammalian_keywords)
        
        if has_mammalian and not has_non_mammalian:
            score += 20  # Purely mammalian - PERFECT FIT
        elif has_mammalian and has_non_mammalian:
            score += 12  # Mixed - acceptable
        elif has_non_mammalian:
            score += 0  # Not our fit
        
        # Geographic Location Score (0-10 points)
        location = company_data.get('location', '') or ''
        location = location.lower() if location else ''
        us_states = ['california', 'massachusetts', 'new york', 'new jersey', 'pennsylvania',
                    'maryland', 'north carolina', 'texas', 'washington', 'usa', 'united states']
        eu_priority = ['germany', 'uk', 'united kingdom', 'france', 'netherlands', 'switzerland', 
                      'belgium', 'sweden', 'denmark']
        
        if location and any(loc in location for loc in us_states):
            score += 10  # US - priority
        elif location and any(loc in location for loc in eu_priority):
            score += 10  # EU priority - priority
        elif location and any(country in location for country in ['italy', 'spain', 'austria', 'ireland', 'norway', 'finland']):
            score += 8  # Other Western Europe
        elif location and ('poland' in location or 'czech' in location):
            score += 6  # Eastern Europe
        
        # Funding Stage Score (already counted above in revenue)
        # Manufacturing Need Score (0-10 points)
        manufacturing_status = company_data.get('manufacturing_status', '') or ''
        manufacturing_status = manufacturing_status.lower() if manufacturing_status else ''
        if 'no public partner' in manufacturing_status:
            score += 10  # OPPORTUNITY
        elif 'has partner' in manufacturing_status:
            score += 8  # Looking for alternatives
        elif 'building in-house' in manufacturing_status:
            score += 3  # Less immediate need
        else:
            score += 5  # Unknown
        
        # Product Type Score (0-5 points)
        # Already partially covered in technology platform
        focus = str(focus_areas).lower()
        if 'bispecific' in focus or 'adc' in focus:
            score += 5  # Our specialties
        elif 'mab' in focus or 'antibod' in focus:
            score += 5
        
        return min(score, max_score)
    
    def calculate_icp_score_basic(self, company_data: Dict) -> int:
        """Fallback basic ICP calculation if strategic tables not available"""
        score = 0
        criteria = self.config['icp_scoring']['criteria']
        bonuses = self.config['icp_scoring']['bonuses']
        
        # Location check (Europe)
        location = company_data.get('location', '') or ''
        location = location.lower() if location else ''
        european_countries = ['uk', 'germany', 'france', 'switzerland', 'netherlands', 
                            'belgium', 'italy', 'spain', 'sweden', 'denmark', 'ireland',
                            'austria', 'norway', 'finland', 'poland']
        if location and any(country in location for country in european_countries):
            score += criteria['location_europe']
        
        # Funding stage
        funding_stage = company_data.get('funding_stage', '') or ''
        funding_stage = funding_stage.lower() if funding_stage else ''
        if funding_stage and any(stage in funding_stage for stage in ['series b', 'series c', 'series d', 'public']):
            score += criteria['funding_series_b_plus']
        
        # Pipeline stage
        pipeline_stages = [s.lower() for s in company_data.get('pipeline_stages', [])]
        if 'phase 2' in pipeline_stages or 'phase 3' in pipeline_stages:
            score += criteria['phase_2_3_programs']
        
        # Technology platform
        platforms = [p.lower() for p in company_data.get('technology_platforms', [])]
        if any('mammalian' in p for p in platforms):
            score += criteria['mammalian_platform']
        
        # CDMO partnership status
        manufacturing_status = company_data.get('manufacturing_status', '') or ''
        manufacturing_status = manufacturing_status.lower() if manufacturing_status else ''
        if manufacturing_status and 'no public partner' in manufacturing_status:
            score += criteria['no_cdmo_partner']
        
        # Bonuses
        focus_areas = [f.lower() for f in company_data.get('focus_areas', [])]
        if 'bispecifics' in focus_areas:
            score += bonuses['bispecifics_focus']
        
        # Check for recent funding (would need date parsing)
        latest_funding = company_data.get('latest_funding_round', '')
        if '2024' in latest_funding or '2025' in latest_funding:
            score += bonuses['recent_funding']
        
        # Multiple programs
        lead_programs = company_data.get('lead_programs', '')
        if lead_programs and len(lead_programs.split(',')) > 1:
            score += bonuses['multiple_programs']
        
        return min(score, 100)  # Cap at 100
    
    def calculate_urgency_score(self, company_data: Dict) -> int:
        """Calculate Urgency Score based on timing indicators"""
        score = 0
        criteria = self.config['urgency_scoring']['criteria']
        
        # Recent funding (simplified - would need proper date parsing)
        latest_funding = company_data.get('latest_funding_round', '') or ''
        if latest_funding and ('2024' in latest_funding or '2025' in latest_funding):
            score += criteria['recent_funding_6mo']
        
        # Check intelligence notes for key indicators
        notes = company_data.get('intelligence_notes', '') or ''
        notes = notes.lower() if notes else ''
        
        if notes and any(word in notes for word in ['phase 2', 'phase 3', 'advancing', 'clinical trial']):
            score += criteria['advancing_phase']
        
        if notes and any(word in notes for word in ['cmo', 'coo', 'chief operating', 'head of operations']):
            score += criteria['new_cmo_coo_hire']
        
        if notes and any(word in notes for word in ['manufacturing', 'cdmo', 'cmo', 'production', 'scale-up']):
            score += criteria['manufacturing_mentioned']
        
        return min(score, 100)  # Cap at 100
    
    def update_company_record(self, record_id: str, enriched_data: Dict):
        """Update Airtable company record with enriched data"""
        
        # Calculate scores
        icp_score = self.calculate_icp_score(enriched_data)
        urgency_score = self.calculate_urgency_score(enriched_data)
        
        # Prepare update payload
        update_fields = {
            'Enrichment Status': 'Enriched' if enriched_data.get('confidence') != 'Failed' else 'Failed',
            'Last Intelligence Check': datetime.now().strftime('%Y-%m-%d'),  # Airtable date format
            'ICP Fit Score': icp_score,
            'Urgency Score': urgency_score
        }
        
        # Add found data with validation (only if not null)
        if enriched_data.get('website'):
            update_fields['Website'] = enriched_data['website']
        
        if enriched_data.get('linkedin_company_page'):
            update_fields['LinkedIn Company Page'] = enriched_data['linkedin_company_page']
        
        if enriched_data.get('location'):
            update_fields['Location/HQ'] = enriched_data['location']
        
        # Company Size - parse from employee count
        if enriched_data.get('company_size_employees'):
            update_fields['Company Size'] = self.parse_company_size(str(enriched_data['company_size_employees']))
        
        # Focus Area - validate multiple select
        if enriched_data.get('focus_areas'):
            validated_focus = self.validate_multiple_select(enriched_data['focus_areas'], self.VALID_FOCUS_AREAS)
            if validated_focus:
                update_fields['Focus Area'] = validated_focus
        
        # Technology Platform - validate multiple select
        if enriched_data.get('technology_platforms'):
            validated_tech = self.validate_multiple_select(enriched_data['technology_platforms'], self.VALID_TECH_PLATFORMS)
            if validated_tech:
                update_fields['Technology Platform'] = validated_tech
        
        # Funding Stage - validate single select
        if enriched_data.get('funding_stage'):
            update_fields['Funding Stage'] = self.validate_single_select(
                enriched_data['funding_stage'], 
                self.VALID_FUNDING_STAGES, 
                default='Unknown'
            )
        
        # Total Funding - format as currency
        if enriched_data.get('total_funding_usd'):
            try:
                # Convert to float, Airtable currency field expects number
                funding = float(enriched_data['total_funding_usd'])
                update_fields['Total Funding'] = funding
            except (ValueError, TypeError):
                logger.warning(f"Could not parse funding amount: {enriched_data.get('total_funding_usd')}")
        
        # Latest Funding Round - free text
        if enriched_data.get('latest_funding_round'):
            update_fields['Latest Funding Round'] = enriched_data['latest_funding_round']
        
        # Pipeline Stage - validate multiple select
        if enriched_data.get('pipeline_stages'):
            validated_pipeline = self.validate_multiple_select(enriched_data['pipeline_stages'], self.VALID_PIPELINE_STAGES)
            if validated_pipeline:
                update_fields['Pipeline Stage'] = validated_pipeline
        
        # Lead Programs - free text
        if enriched_data.get('lead_programs'):
            update_fields['Lead Programs'] = enriched_data['lead_programs']
        
        # Therapeutic Areas - validate multiple select
        if enriched_data.get('therapeutic_areas'):
            validated_therapeutic = self.validate_multiple_select(enriched_data['therapeutic_areas'], self.VALID_THERAPEUTIC_AREAS)
            if validated_therapeutic:
                update_fields['Therapeutic Areas'] = validated_therapeutic
        
        # CDMO Partnerships - free text
        if enriched_data.get('cdmo_partnerships'):
            update_fields['Current CDMO Partnerships'] = enriched_data['cdmo_partnerships']
        
        # Manufacturing Status - validate single select
        if enriched_data.get('manufacturing_status'):
            update_fields['Manufacturing Status'] = self.validate_single_select(
                enriched_data['manufacturing_status'],
                self.VALID_MANUFACTURING_STATUS,
                default='Unknown'
            )
        
        # Intelligence Notes - free text
        if enriched_data.get('intelligence_notes'):
            update_fields['Intelligence Notes'] = enriched_data['intelligence_notes']
        
        # Update the record with error handling
        try:
            self.companies_table.update(record_id, update_fields)
            logger.info(f"✓ Updated company record {record_id} (ICP: {icp_score}, Urgency: {urgency_score})")
        except Exception as e:
            logger.error(f"✗ Failed to update record {record_id}: {str(e)}")
            logger.error(f"Attempted to update with fields: {list(update_fields.keys())}")
            raise
        
        # Log intelligence if sources available
        if enriched_data.get('sources'):
            try:
                self.log_intelligence(
                    record_type='Company',
                    company_id=record_id,
                    summary=f"Enriched company data (Confidence: {enriched_data.get('confidence', 'Unknown')})",
                    sources=enriched_data['sources']
                )
            except Exception as e:
                logger.warning(f"Could not log intelligence: {str(e)}")

    
    def log_intelligence(self, record_type: str, company_id: str, 
                        summary: str, sources: List[str], lead_id: str = None):
        """Log intelligence gathering to Intelligence Log table"""
        
        intelligence_record = {
            'Date': datetime.now().strftime('%Y-%m-%d'),  # Airtable date format
            'Record Type': record_type,
            'Summary': summary,
            'Intelligence Type': 'Enrichment',
            'Confidence Level': 'High',
            'Source URL': sources[0] if sources else None
        }
        
        if company_id:
            intelligence_record['Company'] = [company_id]
        if lead_id:
            intelligence_record['Lead'] = [lead_id]
        
        self.intelligence_table.create(intelligence_record)
    
    def enrich_companies(self, status: str = "Not Enriched", limit: Optional[int] = None):
        """Main enrichment workflow"""
        companies = self.get_companies_to_enrich(status)
        
        if limit:
            companies = companies[:limit]
        
        total = len(companies)
        logger.info(f"Starting enrichment of {total} companies")
        
        success_count = 0
        failed_count = 0
        
        for idx, company in enumerate(companies, 1):
            company_name = company['fields'].get('Company Name', 'Unknown')
            record_id = company['id']
            
            logger.info(f"[{idx}/{total}] Processing: {company_name}")
            
            max_retries = self.config['processing'].get('max_retries', 3)
            retry_delay = self.config['processing'].get('retry_delay', 5)
            
            for attempt in range(max_retries):
                try:
                    # Search and enrich
                    logger.info(f"  Searching for intelligence... (attempt {attempt + 1}/{max_retries})")
                    enriched_data = self.search_company_info(company_name)
                    
                    # Check if enrichment actually returned data
                    if enriched_data.get('confidence') == 'Failed' or enriched_data.get('error'):
                        error_msg = enriched_data.get('error', 'AI could not find sufficient information')
                        logger.warning(f"  Enrichment returned failure: {error_msg}")
                        if attempt < max_retries - 1:
                            logger.info(f"  Retrying in {retry_delay} seconds...")
                            time.sleep(retry_delay)
                            continue
                        else:
                            # Final attempt failed
                            self.companies_table.update(record_id, {
                                'Enrichment Status': 'Failed',
                                'Intelligence Notes': f"Failed after {max_retries} attempts: {error_msg}"
                            })
                            failed_count += 1
                            break
                    
                    # Update Airtable
                    logger.info(f"  Updating Airtable record...")
                    self.update_company_record(record_id, enriched_data)
                    success_count += 1
                    logger.info(f"  ✓ Successfully enriched {company_name}")
                    
                    # Rate limiting
                    time.sleep(self.config['web_search']['rate_limit_delay'])
                    break  # Success, exit retry loop
                    
                except json.JSONDecodeError as e:
                    logger.error(f"  ✗ JSON parsing error: {str(e)}")
                    if attempt < max_retries - 1:
                        logger.info(f"  Retrying in {retry_delay} seconds...")
                        time.sleep(retry_delay)
                    else:
                        self.companies_table.update(record_id, {
                            'Enrichment Status': 'Failed',
                            'Intelligence Notes': f"JSON parsing error after {max_retries} attempts"
                        })
                        failed_count += 1
                
                except Exception as e:
                    logger.error(f"  ✗ Error enriching {company_name}: {str(e)}")
                    if attempt < max_retries - 1:
                        logger.info(f"  Retrying in {retry_delay} seconds...")
                        time.sleep(retry_delay)
                    else:
                        # Mark as failed after all retries
                        try:
                            self.companies_table.update(record_id, {
                                'Enrichment Status': 'Failed',
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
    
    parser = argparse.ArgumentParser(description='Enrich company records with business intelligence')
    parser.add_argument('--status', default='Not Enriched', 
                       help='Enrichment status to filter by (default: Not Enriched)')
    parser.add_argument('--limit', type=int, default=None,
                       help='Limit number of companies to process')
    parser.add_argument('--config', default='config.yaml',
                       help='Path to config file')
    
    args = parser.parse_args()
    
    try:
        enricher = CompanyEnricher(config_path=args.config)
        enricher.enrich_companies(status=args.status, limit=args.limit)
    except FileNotFoundError:
        logger.error(f"Config file not found: {args.config}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
