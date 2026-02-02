#!/usr/bin/env python3
"""
Campaign Leads Processor - Unified Enrichment and Outreach

Processes leads from Campaign Leads table:
1. Creates company in Companies table if not exists
2. Runs INLINE company enrichment (web search)
3. Creates lead in Leads table if not exists
4. Runs INLINE lead enrichment (web search)
5. Links Campaign Lead to both records
6. Generates personalized outreach messages
"""

import os
import sys
import yaml
import json
import time
import logging
import argparse
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any

import anthropic
from pyairtable import Api
from pyairtable.formulas import match

# Configure logging FIRST
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('campaign_leads.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Import fuzzy matching utilities
try:
    from fuzzy_match import FuzzyMatcher, normalize_company_name, normalize_lead_name, similarity_score
    HAS_FUZZY_MATCH = True
    logger.info("✓ Fuzzy matching module loaded")
except ImportError as e:
    HAS_FUZZY_MATCH = False
    FuzzyMatcher = None
    normalize_company_name = lambda x: x.lower().strip() if x else ""
    normalize_lead_name = lambda x: x.lower().strip() if x else ""
    similarity_score = lambda x, y, f: 1.0 if f(x) == f(y) else 0.0
    logger.warning(f"⚠ Fuzzy matching not available: {e}")


class CampaignLeadsProcessor:
    """Process campaign leads with full inline enrichment and outreach generation"""
    
    def __init__(self, config_path: str = "config.yaml"):
        """Initialize with configuration"""
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        # Initialize Airtable
        self.airtable = Api(self.config['airtable']['api_key'])
        self.base = self.airtable.base(self.config['airtable']['base_id'])
        
        # Tables
        table_name = self.config['airtable']['tables'].get('campaign_leads', 'Campaign Leads')
        self.campaign_leads_table = self.base.table(table_name)
        self.companies_table = self.base.table(self.config['airtable']['tables']['companies'])
        self.leads_table = self.base.table(self.config['airtable']['tables']['leads'])
        
        # Trigger History table for tracking outreach triggers
        try:
            self.trigger_history_table = self.base.table('Trigger History')
            logger.info("✓ Trigger History table connected")
        except Exception as e:
            self.trigger_history_table = None
            logger.warning(f"⚠ Trigger History table not found: {e}")
        
        # Initialize Claude for enrichment and outreach generation
        self.anthropic_client = anthropic.Anthropic(
            api_key=self.config['anthropic']['api_key']
        )
        
        logger.info("✓ CampaignLeadsProcessor initialized (inline enrichment mode)")
    
    # ==================== COMPANY OPERATIONS ====================
    
    def lookup_company(self, company_name: str) -> Tuple[Optional[Dict], Optional[str]]:
        """Look up company in Companies table with fuzzy matching"""
        try:
            # First try exact match
            formula = match({"Company Name": company_name})
            records = self.companies_table.all(formula=formula)
            
            if records:
                record = records[0]
                return record['fields'], record['id']
            
            # Try fuzzy match
            if HAS_FUZZY_MATCH:
                # Normalize the query
                norm_query = normalize_company_name(company_name)
                
                # Get all companies and find best match
                all_companies = self.companies_table.all()
                best_match = None
                best_score = 0.0
                
                for record in all_companies:
                    existing_name = record['fields'].get('Company Name', '')
                    score = similarity_score(company_name, existing_name, normalize_company_name)
                    
                    if score > best_score:
                        best_score = score
                        best_match = record
                
                # If good fuzzy match found (>= 85% similarity)
                if best_score >= 0.85 and best_match:
                    matched_name = best_match['fields'].get('Company Name', '')
                    logger.info(f"    Fuzzy matched company '{company_name}' -> '{matched_name}' (score: {best_score:.2f})")
                    return best_match['fields'], best_match['id']
            
            return None, None
        except Exception as e:
            logger.error(f"Error looking up company: {e}")
            return None, None
    
    def _is_excluded_company(self, company_data: Dict, company_name: str) -> Optional[str]:
        """
        Check if company should be excluded (CDMO, competitor, service provider).
        
        Returns:
            None if company is OK to process
            String with exclusion reason if should be excluded
        """
        if not company_data:
            return None
        
        company_name_lower = company_name.lower() if company_name else ""
        
        # Known CDMO/CMO competitors to always exclude
        cdmo_keywords = [
            'lonza', 'samsung biologics', 'wuxi', 'catalent', 'fujifilm', 'fuji diosynth',
            'agc biologics', 'boehringer ingelheim biopharmaceuticals', 'patheon', 'thermo fisher',
            'cytiva', 'sartorius', 'merck millipore', 'pall', 'rentschler', 'celltrion',
            'samsung bioepis', 'binex', 'cellgen', 'abzena', 'evotec', 'charles river'
        ]
        
        for cdmo in cdmo_keywords:
            if cdmo in company_name_lower:
                return f"CDMO/CMO competitor: {cdmo}"
        
        # Check company type indicators in notes or other fields
        notes = (company_data.get('Intelligence Notes', '') or '').lower()
        justification = (company_data.get('ICP Score Justification', '') or '').lower()
        
        # Check for service provider indicators
        service_indicators = [
            'contract manufacturing', 'contract development', 'cdmo', 'cmo', 
            'contract research', 'cro', 'service provider', 'consulting',
            'equipment manufacturer', 'reagent supplier', 'lab equipment'
        ]
        
        combined_text = f"{notes} {justification}"
        
        for indicator in service_indicators:
            if indicator in combined_text and 'competitor' in combined_text:
                return f"Service provider/competitor: {indicator}"
        
        # Check Manufacturing Status field if it indicates they ARE a CDMO
        manufacturing_status = company_data.get('Manufacturing Status', '')
        if manufacturing_status and 'cdmo' in manufacturing_status.lower():
            # This is fine - they NEED a CDMO, not that they ARE one
            pass
        
        # Check focus areas for non-target areas
        focus_areas = company_data.get('Focus Area', [])
        if isinstance(focus_areas, list):
            excluded_focus = ['Cell Therapy', 'Gene Therapy']
            for area in focus_areas:
                if area in excluded_focus:
                    return f"Non-target focus area: {area}"
        
        return None
    
    def _is_known_excluded_company(self, company_name: str) -> Optional[str]:
        """
        Pre-check if company name matches known excluded companies.
        This runs BEFORE creating any records to save API calls.
        
        Returns:
            None if company should be processed
            String with exclusion reason if should be skipped
        """
        if not company_name:
            return None
        
        company_name_lower = company_name.lower().strip()
        
        # Known CDMO/CMO competitors - skip immediately
        cdmo_competitors = [
            'lonza', 'samsung biologics', 'wuxi biologics', 'wuxi apptec', 'wuxi',
            'catalent', 'fujifilm', 'fujifilm diosynth', 'fuji diosynth',
            'agc biologics', 'agc bio', 'patheon', 'thermo fisher',
            'cytiva', 'sartorius', 'merck millipore', 'pall', 'rentschler',
            'celltrion', 'samsung bioepis', 'binex', 'cellgen', 'abzena',
            'evotec', 'charles river', 'eurofins', 'icon plc', 'iqvia',
            'pra health', 'ppd', 'syneos', 'parexel', 'covance', 'labcorp',
            'cmic', 'medidata', 'veeva', 'oracle health', 'medpace'
        ]
        
        # Known service providers/equipment companies
        service_providers = [
            'bio-techne', 'biotechne', 'bio techne', 'r&d systems',
            'abcam', 'cell signaling', 'beckman coulter', 'bd biosciences',
            'agilent', 'illumina', 'thermo scientific', 'life technologies',
            'ge healthcare', 'danaher', 'waters corporation', 'bruker',
            'perkinelmer', 'biorad', 'bio-rad', 'qiagen', 'roche diagnostics',
            'siemens healthineers', 'abbott diagnostics', 'hologic',
            'stryker', 'medtronic', 'boston scientific', 'zimmer biomet',
            'mckinsey', 'bcg', 'bain', 'deloitte', 'accenture', 'kpmg', 'pwc', 'ey',
            'lek consulting', 'simon-kucher', 'zs associates', 'putnam'
        ]
        
        # Check for exact or partial matches
        for cdmo in cdmo_competitors:
            if cdmo in company_name_lower or company_name_lower in cdmo:
                return f"Known CDMO/CMO competitor: {cdmo}"
        
        for provider in service_providers:
            if provider in company_name_lower or company_name_lower in provider:
                return f"Known service provider: {provider}"
        
        # Check for generic indicators in company name
        exclusion_indicators = [
            'consulting', 'advisors', 'advisory', 'partners llp',
            'cro ', ' cro', 'cdmo', ' cmo', 'contract research',
            'contract manufacturing', 'clinical trials', 'diagnostics inc',
            'equipment', 'instruments', 'laboratory services', 'lab services'
        ]
        
        for indicator in exclusion_indicators:
            if indicator in company_name_lower:
                return f"Company name contains exclusion indicator: {indicator.strip()}"
        
        return None
    
    def _quick_prescreen_company(self, company_name: str) -> Optional[Dict]:
        """
        Quick pre-screen to check if company is a potential customer or should be excluded.
        Uses a lightweight web search to classify the company BEFORE creating any records.
        
        This saves expensive full enrichment API calls for non-relevant companies.
        
        Returns:
            Dict with 'is_excluded' (bool) and 'reason' (str) if excluded
            Dict with 'is_excluded': False and 'company_type' if OK
            None if pre-screen failed (will proceed with caution)
        """
        prompt = f"""Quickly classify this company for a biologics CDMO's sales targeting:

COMPANY: {company_name}

Determine if this is:
1. POTENTIAL CUSTOMER: Biotech/pharma developing biologics (mAbs, bispecifics, ADCs, therapeutic proteins)
2. COMPETITOR: CDMO, CMO, or contract manufacturer (Lonza, WuXi, Catalent, Samsung Biologics, etc.)
3. SERVICE PROVIDER: CRO, consulting firm, equipment/reagent supplier, diagnostics company
4. NON-TARGET: Cell/gene therapy company, small molecule pharma, medical device company

Return ONLY valid JSON:
{{
    "company_type": "biotech|pharma|cdmo|cmo|cro|consulting|equipment|diagnostics|cell_therapy|gene_therapy|other",
    "is_potential_customer": true/false,
    "confidence": "high|medium|low",
    "brief_reason": "One sentence explanation"
}}

Return ONLY JSON, no other text."""

        try:
            message = self.anthropic_client.messages.create(
                model=self.config['anthropic']['model'],
                max_tokens=500,
                tools=[{"type": "web_search_20250305", "name": "web_search"}],
                messages=[{"role": "user", "content": prompt}]
            )
            
            # Extract text
            response_text = ""
            for block in message.content:
                if hasattr(block, 'text'):
                    response_text += block.text
            
            # Parse JSON
            if "```json" in response_text:
                json_str = response_text.split("```json")[1].split("```")[0]
            elif "{" in response_text:
                start = response_text.find("{")
                end = response_text.rfind("}") + 1
                json_str = response_text[start:end]
            else:
                logger.warning(f"    Pre-screen: no JSON in response, proceeding with caution")
                return None
            
            data = json.loads(json_str.strip())
            
            is_potential_customer = data.get('is_potential_customer', False)
            company_type = data.get('company_type', 'unknown')
            confidence = data.get('confidence', 'low')
            reason = data.get('brief_reason', '')
            
            # Excluded types
            excluded_types = ['cdmo', 'cmo', 'cro', 'consulting', 'equipment', 
                            'diagnostics', 'cell_therapy', 'gene_therapy']
            
            if not is_potential_customer or company_type in excluded_types:
                return {
                    'is_excluded': True,
                    'reason': f"{company_type.upper()}: {reason}",
                    'company_type': company_type,
                    'confidence': confidence
                }
            
            logger.info(f"    Pre-screen: {company_type} ({confidence} confidence)")
            return {
                'is_excluded': False,
                'company_type': company_type,
                'confidence': confidence,
                'reason': reason
            }
            
        except Exception as e:
            logger.warning(f"    Pre-screen failed: {e} - proceeding with caution")
            return None
    
    def create_minimal_company(self, company_name: str) -> Optional[str]:
        """Create a minimal company record for enrichment"""
        try:
            fields = {
                'Company Name': company_name,
                'Enrichment Status': 'Not Enriched'
            }
            record = self.companies_table.create(fields)
            return record.get('id')
        except Exception as e:
            logger.error(f"Error creating company: {e}")
            return None
    
    def enrich_company_record(self, record_id: str, company_name: str) -> Optional[Dict]:
        """Run inline company enrichment with web search"""
        
        # Valid options for select fields
        VALID_COMPANY_SIZE = ['1-10', '11-50', '51-200', '201-500', '501-1000', '1000+']
        VALID_FOCUS_AREAS = ['mAbs', 'Bispecifics', 'ADCs', 'Recombinant Proteins', 
                            'Cell Therapy', 'Gene Therapy', 'Vaccines', 'Other']
        VALID_TECH_PLATFORMS = ['Mammalian CHO', 'Mammalian Non-CHO', 'Microbial', 'Cell-Free', 'Other']
        VALID_FUNDING_STAGES = ['Seed', 'Series A', 'Series B', 'Series C', 'Series D+', 'Public', 'Acquired', 'Unknown']
        VALID_PIPELINE_STAGES = ['Preclinical', 'Phase 1', 'Phase 2', 'Phase 3', 'Commercial', 'Unknown']
        VALID_THERAPEUTIC_AREAS = ['Oncology', 'Autoimmune', 'Rare Disease', 'Infectious Disease', 
                                   'CNS', 'Metabolic', 'Cardiovascular', 'Other']
        VALID_MANUFACTURING_STATUS = ['No Public Partner', 'Has Partner', 'Building In-House', 'Unknown']
        
        prompt = f"""Research this biotech/pharma company for business intelligence:

COMPANY: {company_name}

Find and return ALL of the following:
1. Website URL
2. LinkedIn company page URL
3. Headquarters location (city, country)
4. Company size - MUST be exactly one of: {', '.join(VALID_COMPANY_SIZE)}
5. Focus areas - MUST be from: {', '.join(VALID_FOCUS_AREAS)}
6. Technology platform - MUST be from: {', '.join(VALID_TECH_PLATFORMS)}
7. Funding stage - MUST be exactly one of: {', '.join(VALID_FUNDING_STAGES)}
8. Total funding raised (USD number only, e.g. 75000000)
9. Latest funding round description (e.g. "Series B - $50M - Jan 2024")
10. Pipeline stages - MUST be from: {', '.join(VALID_PIPELINE_STAGES)}
11. Lead programs/products (text description)
12. Therapeutic areas - MUST be from: {', '.join(VALID_THERAPEUTIC_AREAS)}
13. Current CDMO partnerships (text - names of CDMOs if any)
14. Manufacturing status - MUST be exactly one of: {', '.join(VALID_MANUFACTURING_STATUS)}
15. Recent news or developments
16. ICP Score (0-90) with justification - score 0 if this is a CDMO/CMO/service provider
17. Urgency Score (0-100) - how urgent is outreach?

Return ONLY valid JSON:
{{
    "website": "https://...",
    "linkedin_company_page": "https://linkedin.com/company/...",
    "location": "City, Country",
    "company_size": "51-200",
    "focus_areas": ["mAbs", "Bispecifics"],
    "technology_platforms": ["Mammalian CHO"],
    "funding_stage": "Series B",
    "total_funding_usd": 75000000,
    "latest_funding_round": "Series B - $50M - Jan 2024",
    "pipeline_stages": ["Phase 2"],
    "lead_programs": "ABC-123 (anti-CD20 mAb) for autoimmune diseases",
    "therapeutic_areas": ["Oncology", "Autoimmune"],
    "cdmo_partnerships": "None publicly announced",
    "manufacturing_status": "No Public Partner",
    "recent_news": "Recently announced positive Phase 2 data...",
    "icp_score": 65,
    "icp_justification": "Mid-stage biotech with biologics focus, no public CDMO partner.",
    "urgency_score": 75
}}

Return ONLY JSON."""

        try:
            logger.info(f"    Running inline company enrichment...")
            
            message = self.anthropic_client.messages.create(
                model=self.config['anthropic']['model'],
                max_tokens=2000,
                tools=[{"type": "web_search_20250305", "name": "web_search"}],
                messages=[{"role": "user", "content": prompt}]
            )
            
            # Extract text
            response_text = ""
            for block in message.content:
                if hasattr(block, 'text'):
                    response_text += block.text
            
            # Parse JSON
            if "```json" in response_text:
                json_str = response_text.split("```json")[1].split("```")[0]
            elif "{" in response_text:
                start = response_text.find("{")
                end = response_text.rfind("}") + 1
                json_str = response_text[start:end]
            else:
                logger.warning(f"    Company enrichment: no JSON in response")
                return None
            
            data = json.loads(json_str.strip())
            
            # Helper function to validate single select
            def validate_single(value, valid_options, default='Unknown'):
                if not value:
                    return default
                if value in valid_options:
                    return value
                for opt in valid_options:
                    if opt.lower() == str(value).lower():
                        return opt
                return default
            
            # Helper function to validate multi-select
            def validate_multi(values, valid_options):
                if not values:
                    return []
                if isinstance(values, str):
                    values = [values]
                validated = []
                for val in values:
                    matched = validate_single(val, valid_options, default=None)
                    if matched and matched != 'Unknown':
                        validated.append(matched)
                return validated if validated else ['Other'] if 'Other' in valid_options else []
            
            # Update company record with all fields
            update_fields = {
                'Enrichment Status': 'Enriched',
                'Last Intelligence Check': datetime.now().strftime('%Y-%m-%d')
            }
            
            # Basic fields
            if data.get('website'):
                update_fields['Website'] = data['website']
            if data.get('linkedin_company_page'):
                update_fields['LinkedIn Company Page'] = data['linkedin_company_page']
            if data.get('location'):
                update_fields['Location/HQ'] = data['location']
            
            # Company Size
            if data.get('company_size'):
                update_fields['Company Size'] = validate_single(data['company_size'], VALID_COMPANY_SIZE, '51-200')
            
            # Focus Area - multi-select
            if data.get('focus_areas'):
                validated = validate_multi(data['focus_areas'], VALID_FOCUS_AREAS)
                if validated:
                    update_fields['Focus Area'] = validated
            
            # Technology Platform - multi-select
            if data.get('technology_platforms'):
                validated = validate_multi(data['technology_platforms'], VALID_TECH_PLATFORMS)
                if validated:
                    update_fields['Technology Platform'] = validated
            
            # Funding Stage
            if data.get('funding_stage'):
                update_fields['Funding Stage'] = validate_single(data['funding_stage'], VALID_FUNDING_STAGES, 'Unknown')
            
            # Total Funding
            if data.get('total_funding_usd'):
                try:
                    update_fields['Total Funding'] = float(data['total_funding_usd'])
                except:
                    pass
            
            # Latest Funding Round
            if data.get('latest_funding_round'):
                update_fields['Latest Funding Round'] = data['latest_funding_round']
            
            # Pipeline Stage - multi-select
            if data.get('pipeline_stages'):
                validated = validate_multi(data['pipeline_stages'], VALID_PIPELINE_STAGES)
                if validated:
                    update_fields['Pipeline Stage'] = validated
            
            # Lead Programs
            if data.get('lead_programs'):
                update_fields['Lead Programs'] = data['lead_programs']
            
            # Therapeutic Areas - multi-select
            if data.get('therapeutic_areas'):
                validated = validate_multi(data['therapeutic_areas'], VALID_THERAPEUTIC_AREAS)
                if validated:
                    update_fields['Therapeutic Areas'] = validated
            
            # Current CDMO Partnerships
            if data.get('cdmo_partnerships'):
                update_fields['Current CDMO Partnerships'] = data['cdmo_partnerships']
            
            # Manufacturing Status
            if data.get('manufacturing_status'):
                update_fields['Manufacturing Status'] = validate_single(data['manufacturing_status'], VALID_MANUFACTURING_STATUS, 'Unknown')
            
            # Intelligence Notes
            if data.get('recent_news'):
                update_fields['Intelligence Notes'] = f"Source: Campaign Leads\n\n{data['recent_news'][:900]}"
            
            # Scores and justifications
            if data.get('icp_score') is not None:
                update_fields['ICP Fit Score'] = min(max(int(data['icp_score']), 0), 90)
            if data.get('icp_justification'):
                update_fields['ICP Score Justification'] = data['icp_justification']
            if data.get('urgency_score') is not None:
                update_fields['Urgency Score'] = min(max(int(data['urgency_score']), 0), 100)
            
            # Update record
            try:
                self.companies_table.update(record_id, update_fields)
                logger.info(f"    ✓ Company enriched - ICP: {data.get('icp_score', 'N/A')}, Urgency: {data.get('urgency_score', 'N/A')}")
            except Exception as e:
                logger.warning(f"    Full company update failed: {e}")
                # Try with minimal fields
                minimal = {
                    'Enrichment Status': 'Enriched',
                    'Last Intelligence Check': datetime.now().strftime('%Y-%m-%d')
                }
                if data.get('website'):
                    minimal['Website'] = data['website']
                if data.get('location'):
                    minimal['Location/HQ'] = data['location']
                if data.get('icp_score') is not None:
                    minimal['ICP Fit Score'] = min(max(int(data['icp_score']), 0), 90)
                try:
                    self.companies_table.update(record_id, minimal)
                except:
                    self.companies_table.update(record_id, {'Enrichment Status': 'Enriched'})
            
            return data
            
        except Exception as e:
            logger.error(f"    Error in inline company enrichment: {e}")
            return None
    
    # ==================== LEAD OPERATIONS ====================
    
    def lookup_lead(self, email: str, name: str, company: str) -> Tuple[Optional[Dict], Optional[str]]:
        """Look up lead in Leads table by email or name+company with fuzzy matching"""
        try:
            # Try by email first (exact match)
            if email and '@' in email:
                formula = match({"Email": email})
                records = self.leads_table.all(formula=formula)
                if records:
                    return records[0]['fields'], records[0]['id']
            
            # Try by exact name match
            formula = match({"Lead Name": name})
            records = self.leads_table.all(formula=formula)
            if records:
                return records[0]['fields'], records[0]['id']
            
            # Try fuzzy name match
            if HAS_FUZZY_MATCH and name:
                # Get all leads (or filter by company if we have company ID)
                all_leads = self.leads_table.all()
                norm_query = normalize_lead_name(name)
                
                best_match = None
                best_score = 0.0
                
                for record in all_leads:
                    existing_name = record['fields'].get('Lead Name', '')
                    score = similarity_score(name, existing_name, normalize_lead_name)
                    
                    # Also check if same company (if company name provided)
                    if company and score >= 0.85:
                        # Get linked company name
                        lead_company = record['fields'].get('Company', [])
                        # For now, just use the score - company matching is secondary
                        pass
                    
                    if score > best_score:
                        best_score = score
                        best_match = record
                
                # If good fuzzy match found (>= 85% similarity)
                if best_score >= 0.85 and best_match:
                    matched_name = best_match['fields'].get('Lead Name', '')
                    logger.info(f"    Fuzzy matched lead '{name}' -> '{matched_name}' (score: {best_score:.2f})")
                    return best_match['fields'], best_match['id']
            
            return None, None
        except Exception as e:
            logger.error(f"Error looking up lead: {e}")
            return None, None
    
    def create_minimal_lead(self, name: str, title: str, company_record_id: Optional[str]) -> Optional[str]:
        """Create a minimal lead record for enrichment"""
        try:
            fields = {
                'Lead Name': name,
                'Title': title,
                'Enrichment Status': 'Not Enriched'
            }
            if company_record_id:
                fields['Company'] = [company_record_id]
            
            record = self.leads_table.create(fields)
            return record.get('id')
        except Exception as e:
            logger.error(f"Error creating lead: {e}")
            return None
    
    def enrich_lead_record(self, record_id: str, lead_name: str, company_name: str, 
                           title: str, company_id: str = None) -> Optional[Dict]:
        """Run inline lead enrichment with web search"""
        
        # Get company ICP for lead scoring
        company_icp = 50
        if company_id:
            try:
                company_record = self.companies_table.get(company_id)
                company_icp = company_record['fields'].get('ICP Fit Score', 50) or 50
            except:
                pass
        
        prompt = f"""Research this professional for contact information:

NAME: {lead_name}
TITLE: {title}
COMPANY: {company_name}

Find:
1. Email address (search company website, press releases, LinkedIn, conference presentations)
2. Verify current title
3. LinkedIn URL
4. Location (city, country)
5. Any recent news, speaking engagements, or publications

EMAIL FINDING PRIORITY - try these sources:
- Company website team/leadership page
- Press releases with contact info
- Conference speaker lists
- Published papers/patents
- If not found, suggest email pattern based on company format (e.g., firstname.lastname@company.com)

Return ONLY valid JSON:
{{
    "email": "email@company.com or null",
    "email_confidence": "High|Medium|Low|Pattern Suggested",
    "title": "Current verified title",
    "linkedin_url": "https://linkedin.com/in/...",
    "location": "City, Country",
    "recent_activity": "Any recent news or speaking engagements"
}}

Return ONLY JSON."""

        try:
            logger.info(f"    Running inline lead enrichment...")
            
            message = self.anthropic_client.messages.create(
                model=self.config['anthropic']['model'],
                max_tokens=2000,
                tools=[{"type": "web_search_20250305", "name": "web_search"}],
                messages=[{"role": "user", "content": prompt}]
            )
            
            # Extract text
            response_text = ""
            for block in message.content:
                if hasattr(block, 'text'):
                    response_text += block.text
            
            # Parse JSON
            if "```json" in response_text:
                json_str = response_text.split("```json")[1].split("```")[0]
            elif "{" in response_text:
                start = response_text.find("{")
                end = response_text.rfind("}") + 1
                json_str = response_text[start:end]
            else:
                logger.warning(f"    Lead enrichment: no JSON in response")
                return None
            
            data = json.loads(json_str.strip())
            
            # Calculate lead ICP based on title
            lead_icp, lead_icp_justification = self._calculate_lead_icp_with_justification(
                data.get('title') or title, company_icp
            )
            
            # Determine Lead ICP Tier
            if lead_icp >= 85:
                lead_icp_tier = "Perfect Fit (Tier 1)"
            elif lead_icp >= 70:
                lead_icp_tier = "Strong Fit (Tier 2)"
            elif lead_icp >= 55:
                lead_icp_tier = "Good Fit (Tier 3)"
            elif lead_icp >= 40:
                lead_icp_tier = "Acceptable Fit (Tier 4)"
            else:
                lead_icp_tier = "Poor Fit (Tier 5)"
            
            # Calculate Combined Priority
            combined_priority = self._calculate_combined_priority(company_icp, lead_icp)
            
            # Map email confidence
            email_conf = data.get('email_confidence', 'Medium')
            confidence_map = {
                'high': 'High',
                'medium': 'Medium',
                'low': 'Low',
                'pattern suggested': 'Low',
                'pattern': 'Low'
            }
            enrichment_confidence = confidence_map.get(str(email_conf).lower(), 'Medium')
            
            # Update lead record
            update_fields = {
                'Enrichment Status': 'Enriched',
                'Last Enrichment Date': datetime.now().strftime('%Y-%m-%d'),
                'Lead ICP Score': lead_icp,
                'Lead ICP Tier': lead_icp_tier,
                'Lead ICP Justification': lead_icp_justification,
                'Combined Priority': combined_priority,
                'Enrichment Confidence': enrichment_confidence,
                'Lead Source': 'Campaign Leads'
            }
            
            if data.get('email'):
                update_fields['Email'] = data['email']
            if data.get('title'):
                update_fields['Title'] = data['title']
            if data.get('linkedin_url'):
                update_fields['LinkedIn URL'] = data['linkedin_url']
            if data.get('recent_activity'):
                update_fields['Intelligence Notes'] = f"Source: Campaign Leads\n\n{data['recent_activity'][:800]}"
            
            try:
                self.leads_table.update(record_id, update_fields)
                logger.info(f"    ✓ Lead enriched - ICP: {lead_icp} ({lead_icp_tier}), Combined: {combined_priority}")
            except Exception as e:
                logger.warning(f"    Lead update partially failed: {e}")
                # Try updating fields one by one
                for field_name, field_value in update_fields.items():
                    try:
                        self.leads_table.update(record_id, {field_name: field_value})
                    except:
                        pass
            
            return {
                'email': data.get('email'),
                'title': data.get('title') or title,
                'linkedin_url': data.get('linkedin_url'),
                'lead_icp': lead_icp,
                'company_icp': company_icp,
                'combined_priority': combined_priority
            }
            
        except Exception as e:
            logger.error(f"    Error in inline lead enrichment: {e}")
            return None
    
    def _calculate_lead_icp_with_justification(self, title: str, company_icp: int) -> tuple:
        """Calculate lead ICP score with justification based on title and company ICP"""
        if not title:
            return 50, "No title provided - default score"
        
        title_lower = title.lower()
        
        # Title scoring
        title_score = 50
        title_reason = "Standard role"
        
        # High-value titles (70-90)
        if any(t in title_lower for t in ['vp manufacturing', 'vp operations', 'head of cmc', 
                                           'chief operating', 'coo', 'vp supply chain',
                                           'head of manufacturing', 'svp operations']):
            title_score = 85
            title_reason = "Key decision maker for manufacturing/operations - highest priority"
        elif any(t in title_lower for t in ['vp', 'vice president', 'head of', 'director']):
            if any(f in title_lower for f in ['manufacturing', 'operations', 'cmc', 'supply', 'technical']):
                title_score = 75
                title_reason = "Senior leader in relevant functional area"
            else:
                title_score = 60
                title_reason = "Senior leader, may influence CDMO decisions"
        elif any(t in title_lower for t in ['ceo', 'chief executive', 'president', 'founder']):
            title_score = 70
            title_reason = "Top decision maker - strategic influence on partnerships"
        elif any(t in title_lower for t in ['senior director', 'executive director']):
            title_score = 70
            title_reason = "Senior director level - significant influence"
        elif 'director' in title_lower:
            title_score = 60
            title_reason = "Director level - involved in partner selection"
        elif 'manager' in title_lower:
            title_score = 45
            title_reason = "Manager level - may influence but not decide"
        
        # Combine with company ICP (60% title, 40% company)
        combined = int(title_score * 0.6 + company_icp * 0.4)
        combined = min(max(combined, 0), 100)
        
        justification = f"Title score: {title_score}/100 ({title_reason}). Company ICP: {company_icp}/90. Combined: {combined}/100"
        
        return combined, justification
    
    def _calculate_combined_priority(self, company_icp: int, lead_icp: int) -> str:
        """Calculate combined priority based on company and lead ICP scores"""
        if company_icp >= 80 and lead_icp >= 70:
            return "🔥 HOT - Priority 1"
        elif company_icp >= 80 and lead_icp >= 55:
            return "📈 WARM - Priority 2"
        elif company_icp >= 65 and lead_icp >= 70:
            return "📈 WARM - Priority 2"
        elif company_icp >= 65 and lead_icp >= 55:
            return "➡️ MEDIUM - Priority 3"
        elif company_icp >= 50 and lead_icp >= 40:
            return "➡️ MEDIUM - Priority 3"
        elif company_icp >= 35 and lead_icp >= 40:
            return "⬇️ LOW - Priority 4"
        else:
            return "❌ SKIP - Priority 5"
    
    def _generate_lead_generic_outreach(self, lead_id: str, lead_name: str, 
                                        lead_title: str, company_name: str) -> bool:
        """Generate generic outreach messages for the Lead record (not campaign-specific)"""
        
        prompt = f"""Generate professional outreach messages for this lead.

LEAD:
Name: {lead_name}
Title: {lead_title}
Company: {company_name}

YOUR COMPANY: European biologics CDMO specializing in mammalian cell culture manufacturing (mAbs, bispecifics, ADCs)

Generate 4 messages for initial outreach:

1. EMAIL (80-100 words):
Subject: [Professional subject line]
- Brief intro
- Value proposition relevant to their role
- Soft CTA
- Sign: "Best regards, [Your Name]"

2. LINKEDIN CONNECTION (150-180 chars):
- Why you'd like to connect
- Professional tone

3. LINKEDIN SHORT MESSAGE (200-300 chars):
- Relevant to their role
- Brief value mention
- Sign: "Best regards, [Your Name]"

4. LINKEDIN INMAIL (150-200 words):
Subject: [InMail subject]
- More detailed intro
- Clear value proposition
- CTA
- Sign: "Best regards, [Your Name]"

Return ONLY valid JSON:
{{
    "email_subject": "Subject line",
    "email_body": "Email body",
    "linkedin_connection": "Connection request",
    "linkedin_short": "Short message",
    "linkedin_inmail_subject": "InMail subject",
    "linkedin_inmail": "InMail body"
}}

Return ONLY JSON."""

        try:
            message = self.anthropic_client.messages.create(
                model=self.config['anthropic']['model'],
                max_tokens=2000,
                messages=[{"role": "user", "content": prompt}]
            )
            
            response_text = ""
            for block in message.content:
                if hasattr(block, 'text'):
                    response_text += block.text
            
            if "```json" in response_text:
                json_str = response_text.split("```json")[1].split("```")[0]
            elif "{" in response_text:
                start = response_text.find("{")
                end = response_text.rfind("}") + 1
                json_str = response_text[start:end]
            else:
                return False
            
            data = json.loads(json_str.strip())
            
            # Update lead with outreach messages
            outreach_update = {
                'Message Generated Date': datetime.now().strftime('%Y-%m-%d')
            }
            
            if data.get('email_subject'):
                outreach_update['Email Subject'] = data['email_subject']
            if data.get('email_body'):
                outreach_update['Email Body'] = data['email_body']
            if data.get('linkedin_connection'):
                outreach_update['LinkedIn Connection Request'] = data['linkedin_connection']
            if data.get('linkedin_short'):
                outreach_update['LinkedIn Short Message'] = data['linkedin_short']
            if data.get('linkedin_inmail_subject'):
                outreach_update['LinkedIn InMail Subject'] = data['linkedin_inmail_subject']
            if data.get('linkedin_inmail'):
                outreach_update['LinkedIn InMail Body'] = data['linkedin_inmail']
            
            self.leads_table.update(lead_id, outreach_update)
            logger.info(f"    ✓ Generic outreach messages generated for lead")
            return True
            
        except Exception as e:
            logger.warning(f"    Error generating lead outreach: {e}")
            return False
    
    # ==================== CAMPAIGN LEADS OPERATIONS ====================
    
    def get_campaign_leads_to_process(self, enrich_only: bool = False) -> List[Dict]:
        """Get campaign leads that need processing"""
        try:
            all_records = self.campaign_leads_table.all()
            
            if enrich_only:
                # Only get leads with Enrich Lead checked and not yet linked
                return [r for r in all_records 
                        if r['fields'].get('Enrich Lead') 
                        and not r['fields'].get('Linked Lead')]
            else:
                # Get leads with Enrich Lead checked
                return [r for r in all_records if r['fields'].get('Enrich Lead')]
                
        except Exception as e:
            logger.error(f"Error getting campaign leads: {e}")
            return []
    
    def get_campaign_leads_for_outreach(self) -> List[Dict]:
        """Get campaign leads that need outreach generation"""
        try:
            all_records = self.campaign_leads_table.all()
            
            # Get leads with Generate Messages checked and linked
            return [r for r in all_records 
                    if r['fields'].get('Generate Messages')
                    and r['fields'].get('Linked Lead')
                    and not r['fields'].get('Email Body')]
                    
        except Exception as e:
            logger.error(f"Error getting campaign leads for outreach: {e}")
            return []
    
    def update_campaign_lead_links(self, record_id: str, 
                                    lead_record_id: Optional[str],
                                    company_record_id: Optional[str],
                                    lead_data: Optional[Dict] = None) -> bool:
        """Update campaign lead with links to main tables"""
        try:
            update = {}
            
            if lead_record_id:
                update['Linked Lead'] = [lead_record_id]
            if company_record_id:
                update['Linked Company'] = [company_record_id]
            
            # Copy basic enrichment data
            if lead_data:
                email = lead_data.get('Email')
                if email and '@' in str(email):
                    update['Email'] = email
                linkedin = lead_data.get('LinkedIn URL')
                if linkedin and 'linkedin.com' in str(linkedin):
                    update['LinkedIn URL'] = linkedin
            
            if update:
                self.campaign_leads_table.update(record_id, update)
            return True
            
        except Exception as e:
            logger.error(f"Error updating campaign lead: {e}")
            return False
    
    def create_trigger_event(self, lead_record_id: str, company_record_id: Optional[str],
                             campaign_fields: Dict, lead_name: str, company_name: str) -> Optional[str]:
        """Create a trigger event in Trigger History table for campaign lead"""
        
        if not self.trigger_history_table:
            logger.warning("  Trigger History table not available - skipping trigger creation")
            return None
        
        try:
            # Determine trigger type based on campaign type
            campaign_type = campaign_fields.get('Campaign Type', 'Campaign')
            conference_name = campaign_fields.get('Conference Name', '')
            campaign_background = campaign_fields.get('Campaign Background', '')
            campaign_date = campaign_fields.get('Campaign Date', '')
            
            # Map campaign type to existing Airtable trigger types
            # Valid options: CONFERENCE_ATTENDANCE, FUNDING, PROMOTION, JOB_CHANGE, 
            #               PIPELINE, SPEAKING, PAIN_POINT, ROADSHOW, OTHER
            if campaign_type == 'Conference' or conference_name:
                trigger_type = 'CONFERENCE_ATTENDANCE'
            elif campaign_type == 'Roadshow':
                trigger_type = 'ROADSHOW'
            elif campaign_type == 'Funding':
                trigger_type = 'FUNDING'
            elif campaign_type == 'Pipeline':
                trigger_type = 'PIPELINE'
            elif campaign_type == 'Speaking':
                trigger_type = 'SPEAKING'
            else:
                trigger_type = 'OTHER'
            
            # Build description
            description_parts = []
            if campaign_type:
                description_parts.append(f"Campaign: {campaign_type}")
            if conference_name:
                description_parts.append(f"Conference: {conference_name}")
            if campaign_background:
                description_parts.append(campaign_background[:200])
            
            description = " | ".join(description_parts) if description_parts else f"Campaign outreach to {lead_name}"
            
            # Build outreach angle
            if campaign_type == 'Roadshow':
                outreach_angle = f"Roadshow visit - discuss manufacturing partnership opportunities"
            elif conference_name:
                outreach_angle = f"Meeting at {conference_name} - discuss manufacturing needs"
            elif campaign_background:
                outreach_angle = campaign_background[:200]
            else:
                outreach_angle = "Campaign outreach - explore manufacturing partnership"
            
            # Create trigger record
            trigger_fields = {
                'Date Detected': datetime.now().strftime('%Y-%m-%d'),
                'Lead': [lead_record_id],
                'Trigger Type': trigger_type,
                'Trigger Source': 'Campaign Leads',  # New standardized field
                'Urgency': 'MEDIUM',
                'Description': description,
                'Outreach Angle': outreach_angle,
                'Status': 'New',
                'Sources': 'Campaign Leads'
            }
            
            # Add company link if available
            if company_record_id:
                trigger_fields['Company'] = [company_record_id]
            
            # Add conference name if available
            if conference_name:
                trigger_fields['Conference Name'] = conference_name
            
            # Add event date if available
            if campaign_date:
                trigger_fields['Event Date'] = campaign_date
            
            record = self.trigger_history_table.create(trigger_fields)
            trigger_id = record.get('id')
            
            logger.info(f"  ✓ Trigger event created ({trigger_type})")
            return trigger_id
            
        except Exception as e:
            logger.error(f"  Error creating trigger event: {e}")
            return None
    
    # ==================== OUTREACH GENERATION ====================
    
    def generate_outreach_messages(self, lead_fields: Dict, company_fields: Dict, 
                                   campaign_context: Dict = None) -> Dict[str, str]:
        """Generate personalized outreach messages with campaign context"""
        
        # === BASIC INFO ===
        name = lead_fields.get('Lead Name', 'there')
        title = lead_fields.get('Title', '')
        company = company_fields.get('Company Name', '')
        location = company_fields.get('Location/HQ', '')
        
        # === CAMPAIGN CONTEXT ===
        campaign_context = campaign_context or {}
        campaign_type = campaign_context.get('Campaign Type', 'general')
        conference_name = campaign_context.get('Conference Name', '')
        campaign_background = campaign_context.get('Campaign Background', '')
        campaign_date = campaign_context.get('Campaign Date', '')
        
        # === COMPANY CONTEXT (brief, not exhaustive) ===
        # Only include key points that would naturally come up in conversation
        company_context = []
        
        pipeline = company_fields.get('Lead Programs', '')
        if pipeline:
            company_context.append(f"Pipeline: {pipeline[:200]}")  # Truncate
        
        tech_platform = company_fields.get('Technology Platform', [])
        if tech_platform:
            if isinstance(tech_platform, list):
                tech_platform = ', '.join(tech_platform)
            company_context.append(f"Technology: {tech_platform}")
        
        therapeutic = company_fields.get('Therapeutic Areas', [])
        if therapeutic:
            if isinstance(therapeutic, list):
                therapeutic = ', '.join(therapeutic)
            company_context.append(f"Focus: {therapeutic}")
        
        company_context_text = '\n'.join(company_context) if company_context else 'General biotech'
        
        # === BUILD CAMPAIGN-SPECIFIC PROMPT ===
        campaign_section = ""
        if campaign_type == 'Conference' and conference_name:
            campaign_section = f"""
CAMPAIGN: Conference outreach for {conference_name}
Date: {campaign_date}
Background: {campaign_background}

This is conference outreach - the conference is the REASON for reaching out.
- Mention you'll be at {conference_name}
- Suggest meeting at the event
- Keep it natural - like reaching out to someone you'd like to meet
"""
        elif campaign_type == 'Roadshow':
            campaign_section = f"""
CAMPAIGN: Roadshow outreach
Date: {campaign_date}
Background: {campaign_background}

This is roadshow outreach - you're visiting their region/city.
- Mention you'll be in their area for meetings
- Suggest meeting while you're there
- Frame it as convenient timing for both parties
- Keep it natural and low-pressure
"""
        elif campaign_background:
            campaign_section = f"""
CAMPAIGN CONTEXT: {campaign_background}

Use this context as the natural reason for reaching out.
"""
        
        prompt = f"""You are writing business development outreach for a European biologics CDMO.

LEAD: {name}, {title} at {company} ({location})

COMPANY BACKGROUND:
{company_context_text}
{campaign_section}
REZON BIO (your company):
European CDMO specializing in mammalian CHO cell culture for mAbs, bispecifics, and ADCs.
Target: Mid-size biotechs needing manufacturing support.

═══════════════════════════════════════════════════════════
STYLE RULES - CRITICAL:
═══════════════════════════════════════════════════════════
1. **Natural, human language** - slightly imperfect is fine
2. **NO bullet lists** anywhere - weave points into sentences
3. **NO ** for bold** - clean formatting only
4. **Show you know them, don't tell them their situation**
   BAD: "Your company focuses on oncology and has Phase 2 programs"
   GOOD: "Given your work in oncology..."
5. **About THEM, not about us** - lead with their context
6. **Soft CTA** - "would be great to connect" not "let's schedule a call"
7. **Don't overload with intel** - pick 1-2 relevant details max
8. **Sound like a human wrote it** - not an AI that scraped their LinkedIn

═══════════════════════════════════════════════════════════
GENERATE FOUR MESSAGES:
═══════════════════════════════════════════════════════════

MESSAGE 1: EMAIL (100-130 words)
Subject: [Natural, references their context or the conference]
Body: Conversational, references campaign context naturally, soft CTA
Sign: "Best regards, [Your Name]"

MESSAGE 2: LINKEDIN CONNECTION (under 200 chars)
Brief, friendly, reference their role or company or conference
No signature

MESSAGE 3: LINKEDIN INMAIL 
Subject: [Natural, not salesy]
Body: 80-100 words, conversational, references their work
Sign: "Best, [Your Name]"

MESSAGE 4: LINKEDIN SHORT (under 180 chars)
Very brief follow-up style message

Return ONLY valid JSON:
{{
    "email_subject": "Subject",
    "email_body": "Body with signature",
    "linkedin_connection": "Under 200 chars, no signature",
    "linkedin_inmail_subject": "Subject",
    "linkedin_inmail_body": "Body with signature",
    "linkedin_short": "Under 180 chars"
}}"""

        try:
            message = self.anthropic_client.messages.create(
                model=self.config['anthropic']['model'],
                max_tokens=1500,
                messages=[{"role": "user", "content": prompt}]
            )
            
            response_text = message.content[0].text if message.content else ""
            
            # Parse JSON with better error handling
            json_str = ""
            if "```json" in response_text:
                json_str = response_text.split("```json")[1].split("```")[0].strip()
            elif "```" in response_text:
                json_str = response_text.split("```")[1].split("```")[0].strip()
            elif "{" in response_text:
                start = response_text.find("{")
                end = response_text.rfind("}") + 1
                json_str = response_text[start:end]
            
            if not json_str:
                logger.error("No JSON found in response")
                return {}
            
            # Try to fix common JSON issues
            json_str = json_str.replace('\n', ' ').replace('\r', '')
            
            # Handle unterminated strings by trying to close them
            try:
                return json.loads(json_str)
            except json.JSONDecodeError as e:
                # Try to salvage partial JSON
                logger.warning(f"JSON parse error, attempting fix: {str(e)[:50]}")
                
                # Count braces to check if truncated
                open_braces = json_str.count('{')
                close_braces = json_str.count('}')
                
                if open_braces > close_braces:
                    # Truncated - try to close it
                    json_str = json_str.rstrip(',') + '}'
                    try:
                        return json.loads(json_str)
                    except:
                        pass
                
                return {}
            
        except Exception as e:
            logger.error(f"Error generating outreach: {e}")
            return {}
    
    def update_campaign_lead_outreach(self, record_id: str, messages: Dict) -> bool:
        """Update campaign lead with generated outreach messages"""
        try:
            update = {}
            
            if messages.get('email_subject'):
                update['Email Subject'] = messages['email_subject']
            if messages.get('email_body'):
                update['Email Body'] = messages['email_body']
            if messages.get('linkedin_connection'):
                update['LinkedIn Connection Request'] = messages['linkedin_connection']
            if messages.get('linkedin_inmail_subject'):
                update['LinkedIn InMail Subject'] = messages['linkedin_inmail_subject']
            if messages.get('linkedin_inmail_body'):
                update['LinkedIn InMail Body'] = messages['linkedin_inmail_body']
            if messages.get('linkedin_short'):
                update['LinkedIn Short Message'] = messages['linkedin_short']
            
            update['Message Generated Date'] = datetime.now().strftime('%Y-%m-%d')
            
            if update:
                self.campaign_leads_table.update(record_id, update)
            return True
            
        except Exception as e:
            logger.error(f"Error updating outreach: {e}")
            return False
    
    # ==================== MAIN WORKFLOWS ====================
    
    def process_enrichment(self, limit: Optional[int] = None, offset: int = 0):
        """
        Main enrichment workflow:
        1. Get campaign leads with Enrich checked
        2. For each lead, create/enrich company and lead in main tables
        3. Link campaign lead to enriched records
        
        Args:
            limit: Max leads to process
            offset: Skip first N leads (for parallel batch processing)
        """
        leads = self.get_campaign_leads_to_process(enrich_only=True)
        
        # Apply offset first, then limit
        if offset > 0:
            leads = leads[offset:]
            logger.info(f"Skipping first {offset} leads (offset)")
        
        if limit:
            leads = leads[:limit]
        
        total = len(leads)
        logger.info(f"Processing {total} campaign leads for enrichment (offset: {offset})")
        
        if total == 0:
            logger.info("No leads to process in this batch")
            return
        
        success = 0
        rate_limit_delay = self.config.get('web_search', {}).get('rate_limit_delay', 2)
        
        for idx, record in enumerate(leads, 1):
            fields = record['fields']
            record_id = record['id']
            
            name = fields.get('Lead Name', 'Unknown')
            company = fields.get('Company', 'Unknown')
            title = fields.get('Title', '')
            email = fields.get('Email', '')
            
            logger.info(f"\n[{idx}/{total}] {name} @ {company}")
            
            try:
                # ========== PRE-CHECK: Known CDMOs/Service Providers ==========
                pre_exclusion = self._is_known_excluded_company(company)
                if pre_exclusion:
                    logger.warning(f"  ⚠ PRE-EXCLUDED: {pre_exclusion}")
                    try:
                        self.campaign_leads_table.update(record_id, {
                            'Processing Notes': f"PRE-EXCLUDED: {pre_exclusion}",
                            'Enrich': False
                        })
                    except:
                        pass
                    continue
                
                # ========== STEP 1: COMPANY ==========
                logger.info("  Checking Companies table...")
                company_data, company_record_id = self.lookup_company(company)
                newly_created_company = False
                
                if company_data:
                    logger.info(f"  ✓ Found existing company (ICP: {company_data.get('ICP Fit Score', 'N/A')})")
                    
                    # Check if existing company is excluded
                    existing_icp = company_data.get('ICP Fit Score', 0) or 0
                    if existing_icp == 0:
                        logger.warning(f"  ⚠ Existing company has ICP=0, skipping")
                        try:
                            self.campaign_leads_table.update(record_id, {
                                'Processing Notes': "EXCLUDED: Existing company has ICP=0",
                                'Enrich': False
                            })
                        except:
                            pass
                        continue
                else:
                    # ========== QUICK PRE-SCREEN (before creating record) ==========
                    logger.info(f"  ○ Company not found - running quick pre-screen...")
                    prescreen_result = self._quick_prescreen_company(company)
                    
                    if prescreen_result and prescreen_result.get('is_excluded'):
                        exclusion_reason = prescreen_result.get('reason', 'Failed pre-screen')
                        logger.warning(f"  ⚠ PRE-SCREEN EXCLUDED: {exclusion_reason}")
                        try:
                            self.campaign_leads_table.update(record_id, {
                                'Processing Notes': f"PRE-SCREEN EXCLUDED: {exclusion_reason}",
                                'Enrich': False
                            })
                        except:
                            pass
                        continue
                    
                    # Passed pre-screen, now create and enrich
                    logger.info(f"  ✓ Pre-screen passed - creating and enriching...")
                    company_record_id = self.create_minimal_company(company)
                    newly_created_company = True
                    
                    if company_record_id:
                        enrichment_result = self.enrich_company_record(company_record_id, company)
                        if enrichment_result:
                            # Fetch the enriched data
                            company_data = self.companies_table.get(company_record_id)['fields']
                        else:
                            logger.warning(f"  ⚠ Company enrichment failed")
                            company_data = {}
                        
                        time.sleep(rate_limit_delay)
                    else:
                        logger.error(f"  ✗ Failed to create company record")
                        company_data = {}
                
                # ========== POST-ENRICHMENT CHECK ==========
                icp_score = company_data.get('ICP Fit Score', 0) or 0
                is_excluded = self._is_excluded_company(company_data, company)
                
                if is_excluded or icp_score == 0:
                    exclusion_reason = is_excluded or "ICP Score = 0 (CDMO/competitor/service provider)"
                    logger.warning(f"  ⚠ EXCLUDED: {exclusion_reason}")
                    
                    # Delete the company record if we just created it (don't pollute database)
                    if newly_created_company and company_record_id:
                        try:
                            self.companies_table.delete(company_record_id)
                            logger.info(f"    Deleted excluded company record")
                        except Exception as e:
                            logger.warning(f"    Could not delete company: {e}")
                    
                    # Update campaign lead with exclusion status
                    try:
                        self.campaign_leads_table.update(record_id, {
                            'Processing Notes': f"EXCLUDED: {exclusion_reason}",
                            'Enrich': False
                        })
                    except:
                        pass
                    continue
                
                # ========== STEP 2: LEAD ==========
                logger.info("  Checking Leads table...")
                lead_data, lead_record_id = self.lookup_lead(email, name, company)
                
                if lead_data:
                    logger.info(f"  ✓ Found existing lead (ICP: {lead_data.get('Lead ICP Score', 'N/A')})")
                else:
                    # Create and enrich lead
                    logger.info(f"  ○ Lead not found - creating and enriching...")
                    lead_record_id = self.create_minimal_lead(name, title, company_record_id)
                    
                    if lead_record_id:
                        enrichment_result = self.enrich_lead_record(
                            lead_record_id, name, company, title, company_record_id
                        )
                        if enrichment_result:
                            # Fetch the enriched data
                            lead_data = self.leads_table.get(lead_record_id)['fields']
                            if lead_data.get('Email'):
                                logger.info(f"    Email: {lead_data.get('Email')}")
                            
                            # Generate generic outreach messages for the Lead record
                            logger.info(f"  Generating generic outreach for lead...")
                            self._generate_lead_generic_outreach(lead_record_id, name, title, company)
                        else:
                            logger.warning(f"  ⚠ Lead enrichment failed")
                            lead_data = {}
                        
                        time.sleep(rate_limit_delay)
                    else:
                        logger.error(f"  ✗ Failed to create lead record")
                        lead_data = {}
                
                # ========== STEP 3: LINK CAMPAIGN LEAD ==========
                if self.update_campaign_lead_links(record_id, lead_record_id, company_record_id, lead_data):
                    logger.info(f"  ✓ Campaign lead linked")
                    success += 1
                    
                    # ========== STEP 4: CREATE TRIGGER EVENT ==========
                    if lead_record_id:
                        self.create_trigger_event(
                            lead_record_id=lead_record_id,
                            company_record_id=company_record_id,
                            campaign_fields=fields,
                            lead_name=name,
                            company_name=company
                        )
                    
                    # Auto-check Generate Messages
                    try:
                        self.campaign_leads_table.update(record_id, {'Generate Messages': True})
                    except:
                        pass
                
            except Exception as e:
                logger.error(f"  ✗ Error processing: {e}")
                continue
        
        logger.info(f"\n{'='*50}")
        logger.info(f"Enrichment complete: {success}/{total} successful")
    
    def process_outreach(self, limit: Optional[int] = None, campaign_type: str = "general"):
        """
        Generate outreach messages for enriched campaign leads
        """
        leads = self.get_campaign_leads_for_outreach()
        
        if limit:
            leads = leads[:limit]
        
        total = len(leads)
        logger.info(f"Generating outreach for {total} campaign leads")
        
        if total == 0:
            logger.info("No leads need outreach generation")
            return
        
        success = 0
        
        for idx, record in enumerate(leads, 1):
            fields = record['fields']
            record_id = record['id']
            name = fields.get('Lead Name', 'Unknown')
            
            logger.info(f"[{idx}/{total}] Generating outreach for {name}...")
            
            try:
                # Get linked lead and company data
                lead_record_ids = fields.get('Linked Lead', [])
                company_record_ids = fields.get('Linked Company', [])
                
                lead_data = {}
                company_data = {}
                
                if lead_record_ids:
                    lead_data = self.leads_table.get(lead_record_ids[0])['fields']
                if company_record_ids:
                    company_data = self.companies_table.get(company_record_ids[0])['fields']
                
                # Extract campaign context from the Campaign Lead record
                campaign_context = {
                    'Campaign Type': fields.get('Campaign Type', campaign_type),
                    'Conference Name': fields.get('Conference Name', ''),
                    'Campaign Background': fields.get('Campaign Background', ''),
                    'Campaign Date': fields.get('Campaign Date', ''),
                }
                
                # Generate messages with campaign context
                messages = self.generate_outreach_messages(lead_data, company_data, campaign_context)
                
                if messages:
                    if self.update_campaign_lead_outreach(record_id, messages):
                        logger.info(f"  ✓ Outreach generated")
                        success += 1
                else:
                    logger.warning(f"  ⚠ Failed to generate messages")
                    
            except Exception as e:
                logger.error(f"  ✗ Error: {e}")
                continue
        
        logger.info(f"\n{'='*50}")
        logger.info(f"Outreach complete: {success}/{total} successful")
    
    def process_all(self, limit: Optional[int] = None, campaign_type: str = "general", offset: int = 0):
        """Run full workflow: enrichment then outreach"""
        logger.info("="*50)
        logger.info("CAMPAIGN LEADS PROCESSOR - FULL WORKFLOW")
        if offset > 0:
            logger.info(f"Batch offset: {offset}")
        logger.info("="*50)
        
        # Step 1: Enrichment
        logger.info("\n--- PHASE 1: ENRICHMENT ---")
        self.process_enrichment(limit, offset)
        
        # Step 2: Outreach
        logger.info("\n--- PHASE 2: OUTREACH GENERATION ---")
        self.process_outreach(limit, campaign_type)
        
        logger.info("\n" + "="*50)
        logger.info("WORKFLOW COMPLETE")
        logger.info("="*50)
    
    # ==================== BULK PROCESSING (2000+ leads) ====================
    
    def process_bulk(self, batch_size: int = 50, skip_outreach: bool = False,
                     campaign_type: str = "general", resume: bool = True):
        """
        Process large batches of campaign leads (2000+) efficiently.
        
        Features:
        - Batch processing with configurable batch size
        - Progress tracking and logging
        - Resume capability (skips already processed)
        - Cost-efficient: pre-filters before expensive enrichment
        - Detailed statistics
        
        Args:
            batch_size: Number of leads per batch (default 50)
            skip_outreach: If True, only do enrichment (faster)
            campaign_type: Campaign type for outreach
            resume: If True, skip already processed leads
        """
        start_time = datetime.now()
        
        logger.info("="*70)
        logger.info("BULK CAMPAIGN LEADS PROCESSOR")
        logger.info("="*70)
        logger.info(f"Batch size: {batch_size}")
        logger.info(f"Skip outreach: {skip_outreach}")
        logger.info(f"Resume mode: {resume}")
        logger.info("="*70)
        
        # Get all campaign leads
        try:
            all_leads = self.campaign_leads_table.all()
        except Exception as e:
            logger.error(f"Failed to fetch campaign leads: {e}")
            return
        
        # Filter leads that need processing
        if resume:
            leads_to_process = [
                r for r in all_leads 
                if r['fields'].get('Enrich Lead') 
                and not r['fields'].get('Linked Lead')
                and not r['fields'].get('Processing Notes', '').startswith('EXCLUDED')
                and not r['fields'].get('Processing Notes', '').startswith('PRE-')
            ]
        else:
            leads_to_process = [
                r for r in all_leads 
                if r['fields'].get('Enrich Lead')
            ]
        
        total_leads = len(leads_to_process)
        logger.info(f"Total leads to process: {total_leads}")
        logger.info(f"Already processed/excluded: {len(all_leads) - total_leads}")
        
        if total_leads == 0:
            logger.info("No leads to process!")
            return
        
        # Statistics
        stats = {
            'total': total_leads,
            'processed': 0,
            'pre_excluded': 0,
            'prescreen_excluded': 0,
            'enriched': 0,
            'enrichment_failed': 0,
            'existing_company': 0,
            'existing_lead': 0,
            'outreach_generated': 0,
            'errors': 0,
            'batches_completed': 0
        }
        
        # Process in batches
        num_batches = (total_leads + batch_size - 1) // batch_size
        logger.info(f"Processing in {num_batches} batches of {batch_size}")
        logger.info("="*70)
        
        rate_limit_delay = self.config.get('web_search', {}).get('rate_limit_delay', 2)
        
        for batch_num in range(num_batches):
            batch_start = batch_num * batch_size
            batch_end = min(batch_start + batch_size, total_leads)
            batch = leads_to_process[batch_start:batch_end]
            
            logger.info(f"\n{'='*50}")
            logger.info(f"BATCH {batch_num + 1}/{num_batches} (leads {batch_start + 1}-{batch_end})")
            logger.info(f"{'='*50}")
            
            for idx, record in enumerate(batch, batch_start + 1):
                fields = record['fields']
                record_id = record['id']
                
                name = fields.get('Lead Name', 'Unknown')
                company = fields.get('Company', 'Unknown')
                title = fields.get('Title', '')
                email = fields.get('Email', '')
                
                logger.info(f"\n[{idx}/{total_leads}] {name} @ {company}")
                
                try:
                    # ========== TIER 1: INSTANT PRE-EXCLUSION ==========
                    pre_exclusion = self._is_known_excluded_company(company)
                    if pre_exclusion:
                        logger.info(f"  ⚡ PRE-EXCLUDED: {pre_exclusion}")
                        self._update_campaign_lead_status(record_id, f"PRE-EXCLUDED: {pre_exclusion}")
                        stats['pre_excluded'] += 1
                        stats['processed'] += 1
                        continue
                    
                    # ========== CHECK EXISTING COMPANY ==========
                    company_data, company_record_id = self.lookup_company(company)
                    newly_created_company = False
                    
                    if company_data:
                        logger.info(f"  ✓ Found existing company (ICP: {company_data.get('ICP Fit Score', 'N/A')})")
                        stats['existing_company'] += 1
                        
                        # Check if existing company is excluded
                        existing_icp = company_data.get('ICP Fit Score', 0) or 0
                        if existing_icp == 0:
                            logger.info(f"  ⚠ Existing company has ICP=0, skipping")
                            self._update_campaign_lead_status(record_id, "EXCLUDED: Existing company has ICP=0")
                            stats['prescreen_excluded'] += 1
                            stats['processed'] += 1
                            continue
                    else:
                        # ========== TIER 2: QUICK PRE-SCREEN ==========
                        logger.info(f"  🔍 Running quick pre-screen...")
                        prescreen_result = self._quick_prescreen_company(company)
                        
                        if prescreen_result and prescreen_result.get('is_excluded'):
                            exclusion_reason = prescreen_result.get('reason', 'Failed pre-screen')
                            logger.info(f"  ⚠ PRE-SCREEN EXCLUDED: {exclusion_reason}")
                            self._update_campaign_lead_status(record_id, f"PRE-SCREEN EXCLUDED: {exclusion_reason}")
                            stats['prescreen_excluded'] += 1
                            stats['processed'] += 1
                            continue
                        
                        # ========== TIER 3: FULL ENRICHMENT ==========
                        logger.info(f"  ✓ Pre-screen passed - creating and enriching...")
                        company_record_id = self.create_minimal_company(company)
                        newly_created_company = True
                        
                        if company_record_id:
                            enrichment_result = self.enrich_company_record(company_record_id, company)
                            if enrichment_result:
                                company_data = self.companies_table.get(company_record_id)['fields']
                                stats['enriched'] += 1
                            else:
                                logger.warning(f"  ⚠ Company enrichment failed")
                                company_data = {}
                                stats['enrichment_failed'] += 1
                            
                            time.sleep(rate_limit_delay)
                        else:
                            logger.error(f"  ✗ Failed to create company record")
                            stats['errors'] += 1
                            stats['processed'] += 1
                            continue
                    
                    # ========== POST-ENRICHMENT CHECK ==========
                    icp_score = company_data.get('ICP Fit Score', 0) or 0
                    is_excluded = self._is_excluded_company(company_data, company)
                    
                    if is_excluded or icp_score == 0:
                        exclusion_reason = is_excluded or "ICP Score = 0"
                        logger.info(f"  ⚠ EXCLUDED: {exclusion_reason}")
                        
                        if newly_created_company and company_record_id:
                            try:
                                self.companies_table.delete(company_record_id)
                                logger.info(f"    Deleted excluded company record")
                            except:
                                pass
                        
                        self._update_campaign_lead_status(record_id, f"EXCLUDED: {exclusion_reason}")
                        stats['prescreen_excluded'] += 1
                        stats['processed'] += 1
                        continue
                    
                    # ========== LEAD PROCESSING ==========
                    lead_data, lead_record_id = self.lookup_lead(email, name, company)
                    
                    if lead_data:
                        logger.info(f"  ✓ Found existing lead")
                        stats['existing_lead'] += 1
                    else:
                        logger.info(f"  ○ Creating and enriching lead...")
                        lead_record_id = self.create_minimal_lead(name, title, company_record_id)
                        
                        if lead_record_id:
                            enrichment_result = self.enrich_lead_record(
                                lead_record_id, name, company, title, company_record_id
                            )
                            if enrichment_result:
                                lead_data = self.leads_table.get(lead_record_id)['fields']
                                
                                # Generate generic outreach
                                self._generate_lead_generic_outreach(lead_record_id, name, title, company)
                            
                            time.sleep(rate_limit_delay)
                    
                    # ========== LINK CAMPAIGN LEAD ==========
                    if self.update_campaign_lead_links(record_id, lead_record_id, company_record_id, lead_data):
                        logger.info(f"  ✓ Campaign lead linked")
                        
                        # Create trigger
                        if lead_record_id:
                            self.create_trigger_event(
                                lead_record_id=lead_record_id,
                                company_record_id=company_record_id,
                                campaign_fields=fields,
                                lead_name=name,
                                company_name=company
                            )
                        
                        # Mark for outreach generation
                        try:
                            self.campaign_leads_table.update(record_id, {'Generate Messages': True})
                        except:
                            pass
                    
                    stats['processed'] += 1
                    
                except Exception as e:
                    logger.error(f"  ✗ Error: {e}")
                    stats['errors'] += 1
                    stats['processed'] += 1
                    continue
            
            stats['batches_completed'] += 1
            
            # Batch summary
            elapsed = (datetime.now() - start_time).total_seconds()
            rate = stats['processed'] / elapsed * 60 if elapsed > 0 else 0
            remaining = total_leads - stats['processed']
            eta_minutes = remaining / rate if rate > 0 else 0
            
            logger.info(f"\n--- Batch {batch_num + 1} Complete ---")
            logger.info(f"Progress: {stats['processed']}/{total_leads} ({stats['processed']/total_leads*100:.1f}%)")
            logger.info(f"Rate: {rate:.1f} leads/min | ETA: {eta_minutes:.0f} min")
        
        # ========== OUTREACH GENERATION (if not skipped) ==========
        if not skip_outreach:
            logger.info(f"\n{'='*70}")
            logger.info("PHASE 2: GENERATING CAMPAIGN OUTREACH")
            logger.info(f"{'='*70}")
            self.process_outreach(campaign_type=campaign_type)
            stats['outreach_generated'] = True
        
        # ========== FINAL SUMMARY ==========
        elapsed = (datetime.now() - start_time).total_seconds()
        
        logger.info(f"\n{'='*70}")
        logger.info("BULK PROCESSING COMPLETE")
        logger.info(f"{'='*70}")
        logger.info(f"Total time: {elapsed/60:.1f} minutes")
        logger.info(f"Total processed: {stats['processed']}/{total_leads}")
        logger.info(f"")
        logger.info("Breakdown:")
        logger.info(f"  • Pre-excluded (instant): {stats['pre_excluded']}")
        logger.info(f"  • Pre-screen excluded: {stats['prescreen_excluded']}")
        logger.info(f"  • Existing companies found: {stats['existing_company']}")
        logger.info(f"  • New companies enriched: {stats['enriched']}")
        logger.info(f"  • Enrichment failures: {stats['enrichment_failed']}")
        logger.info(f"  • Existing leads found: {stats['existing_lead']}")
        logger.info(f"  • Errors: {stats['errors']}")
        logger.info(f"{'='*70}")
        
        # Cost estimate
        api_calls = stats['enriched'] + stats['prescreen_excluded'] - stats['pre_excluded']
        estimated_cost = api_calls * 0.02  # Rough estimate
        logger.info(f"Estimated API cost: ~${estimated_cost:.2f}")
        logger.info(f"Cost saved by pre-filtering: ~${stats['pre_excluded'] * 0.04 + stats['prescreen_excluded'] * 0.03:.2f}")
        
        return stats
    
    def _update_campaign_lead_status(self, record_id: str, status: str):
        """Helper to update campaign lead processing status"""
        try:
            self.campaign_leads_table.update(record_id, {
                'Processing Notes': status,
                'Enrich Lead': False
            })
        except:
            pass


def main():
    parser = argparse.ArgumentParser(
        description='Campaign Leads Processor',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Standard processing (small batches)
  python process_campaign_leads.py
  
  # Bulk processing for 2000+ leads
  python process_campaign_leads.py --bulk
  
  # Bulk with custom batch size
  python process_campaign_leads.py --bulk --batch-size 100
  
  # Bulk enrichment only (skip outreach for speed)
  python process_campaign_leads.py --bulk --skip-outreach
  
  # Resume interrupted bulk processing
  python process_campaign_leads.py --bulk --resume
  
  # Enrichment only (no outreach)
  python process_campaign_leads.py --enrich-only
  
  # Generate outreach for already enriched leads
  python process_campaign_leads.py --outreach-only
  
  # Parallel batch processing (used by GitHub Actions)
  python process_campaign_leads.py --limit 200 --offset 400
        """
    )
    parser.add_argument('--config', default='config.yaml', help='Config file path')
    parser.add_argument('--enrich-only', action='store_true', 
                        help='Only run enrichment, skip outreach')
    parser.add_argument('--outreach-only', action='store_true', 
                        help='Only generate outreach for already enriched leads')
    parser.add_argument('--campaign-type', type=str, default='general',
                        help='Campaign type for outreach messaging')
    parser.add_argument('--limit', type=int, help='Max leads to process')
    parser.add_argument('--offset', type=int, default=0, 
                        help='Skip first N leads (for parallel batch processing)')
    
    # Bulk processing options
    parser.add_argument('--bulk', action='store_true',
                        help='Enable bulk processing mode for 2000+ leads')
    parser.add_argument('--batch-size', type=int, default=50,
                        help='Batch size for bulk processing (default: 50)')
    parser.add_argument('--skip-outreach', action='store_true',
                        help='Skip outreach generation in bulk mode (faster)')
    parser.add_argument('--resume', action='store_true', default=True,
                        help='Resume from where left off (skip processed leads)')
    parser.add_argument('--no-resume', action='store_true',
                        help='Process all leads, even if already processed')
    
    args = parser.parse_args()
    
    processor = CampaignLeadsProcessor(args.config)
    
    if args.bulk:
        # Bulk processing mode
        resume = not args.no_resume
        processor.process_bulk(
            batch_size=args.batch_size,
            skip_outreach=args.skip_outreach,
            campaign_type=args.campaign_type,
            resume=resume
        )
    elif args.enrich_only:
        processor.process_enrichment(args.limit, args.offset)
    elif args.outreach_only:
        processor.process_outreach(args.limit, args.campaign_type)
    else:
        processor.process_all(args.limit, args.campaign_type, args.offset)


if __name__ == "__main__":
    main()
