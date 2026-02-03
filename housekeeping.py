#!/usr/bin/env python3
"""
Housekeeping — Background quality maintenance for the Lead Intelligence System.

Checks companies, leads, and outreach messages for missing or low data confidence,
re-enriches records that fall below thresholds, and regenerates outreach messages
with version tracking.

Usage:
    # Full housekeeping (screen → re-enrich → regenerate)
    python housekeeping.py

    # Screen only (dry-run — report what needs fixing)
    python housekeeping.py --screen-only

    # Only re-enrich companies
    python housekeeping.py --companies-only

    # Only re-enrich leads  
    python housekeeping.py --leads-only

    # Only regenerate outreach
    python housekeeping.py --outreach-only

    # Custom confidence threshold (default: 85)
    python housekeeping.py --confidence-threshold 70

    # Limit records per category
    python housekeeping.py --limit 50
"""

import json
import logging
import sys
import time
import argparse
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any

import yaml
import anthropic
from pyairtable import Table

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('housekeeping.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class HousekeepingManager:
    """Background quality maintenance for companies, leads, and outreach."""
    
    # Confidence score mapping: text → numeric (0-100)
    CONFIDENCE_SCORES = {
        'high': 100,
        'medium': 70,
        'low': 40,
        'unverified': 10,
        'unknown': 0,
        'failed': 0,
    }
    
    # Valid select field options (shared across the system)
    VALID_COMPANY_SIZE = ['1-10', '11-50', '51-200', '201-500', '501-1000', '1000+']
    VALID_FOCUS_AREAS = ['mAbs', 'Bispecifics', 'ADCs', 'Recombinant Proteins',
                         'Cell Therapy', 'Gene Therapy', 'Vaccines', 'Other']
    VALID_TECH_PLATFORMS = ['Mammalian CHO', 'Mammalian Non-CHO', 'Microbial', 'Cell-Free', 'Other']
    VALID_FUNDING_STAGES = ['Seed', 'Series A', 'Series B', 'Series C', 'Series D+', 'Public', 'Acquired', 'Unknown']
    VALID_PIPELINE_STAGES = ['Preclinical', 'Phase 1', 'Phase 2', 'Phase 3', 'Commercial', 'Unknown']
    VALID_THERAPEUTIC_AREAS = ['Oncology', 'Autoimmune', 'Rare Disease', 'Infectious Disease',
                               'CNS', 'Metabolic', 'Cardiovascular', 'Other']
    VALID_MANUFACTURING_STATUS = ['No Public Partner', 'Has Partner', 'Building In-House', 'Unknown']
    
    def __init__(self, config_path: str = 'config.yaml'):
        with open(config_path) as f:
            self.config = yaml.safe_load(f)
        
        api_key = self.config['airtable']['api_key']
        base_id = self.config['airtable']['base_id']
        
        self.companies_table = Table(api_key, base_id, 'Companies')
        self.leads_table = Table(api_key, base_id, 'Leads')
        
        # Optional tables — graceful if not present
        try:
            self.trigger_history_table = Table(api_key, base_id, 'Trigger History')
            self.has_triggers = True
        except:
            self.has_triggers = False
        
        self.anthropic_client = anthropic.Anthropic(
            api_key=self.config['anthropic']['api_key']
        )
        
        self.rate_limit_delay = self.config.get('web_search', {}).get('rate_limit_delay', 2)
        
        logger.info("✓ HousekeepingManager initialized")
    
    # ═══════════════════════════════════════════════════════════
    # SCREENING — Identify records needing attention
    # ═══════════════════════════════════════════════════════════
    
    def calculate_record_confidence_score(self, data_confidence_raw: str) -> int:
        """Convert Data Confidence JSON to a single numeric score (0-100).
        
        Returns the MINIMUM confidence across all fields — the chain is only
        as strong as the weakest link.
        """
        if not data_confidence_raw:
            return 0  # No confidence data = needs enrichment
        
        try:
            conf = json.loads(data_confidence_raw)
            if not conf:
                return 0
            
            scores = []
            for field, level in conf.items():
                score = self.CONFIDENCE_SCORES.get(str(level).lower(), 0)
                scores.append(score)
            
            return min(scores) if scores else 0
        except (json.JSONDecodeError, AttributeError):
            return 0
    
    def screen_companies(self, threshold: int = 85) -> List[Dict]:
        """Find companies needing re-enrichment.
        
        Returns companies where:
        - Data Confidence is missing entirely
        - Overall confidence score < threshold
        - Enriched but no confidence data (legacy records)
        """
        all_companies = self.companies_table.all(
            formula="OR({Enrichment Status} = 'Enriched', {Enrichment Status} = 'Failed')"
        )
        
        needs_work = []
        for record in all_companies:
            fields = record['fields']
            raw_conf = fields.get('Data Confidence', '')
            conf_score = self.calculate_record_confidence_score(raw_conf)
            
            # Skip excluded companies (ICP = 0)
            icp = fields.get('ICP Fit Score', 0) or 0
            if icp == 0:
                continue
            
            if conf_score < threshold:
                needs_work.append({
                    'record': record,
                    'confidence_score': conf_score,
                    'reason': 'missing' if not raw_conf else f'score {conf_score} < {threshold}'
                })
        
        needs_work.sort(key=lambda x: x['confidence_score'])
        logger.info(f"Companies needing re-enrichment: {len(needs_work)} (threshold: {threshold})")
        return needs_work
    
    def screen_leads(self, threshold: int = 85) -> List[Dict]:
        """Find leads needing re-enrichment."""
        all_leads = self.leads_table.all(
            formula="{Enrichment Status} = 'Enriched'"
        )
        
        needs_work = []
        for record in all_leads:
            fields = record['fields']
            raw_conf = fields.get('Data Confidence', '')
            conf_score = self.calculate_record_confidence_score(raw_conf)
            
            if conf_score < threshold:
                needs_work.append({
                    'record': record,
                    'confidence_score': conf_score,
                    'reason': 'missing' if not raw_conf else f'score {conf_score} < {threshold}'
                })
        
        needs_work.sort(key=lambda x: x['confidence_score'])
        logger.info(f"Leads needing re-enrichment: {len(needs_work)} (threshold: {threshold})")
        return needs_work
    
    def screen_outreach(self, threshold: int = 85) -> Dict[str, List[Dict]]:
        """Find outreach messages needing regeneration.
        
        Returns dict with 'leads' and 'triggers' lists.
        Checks:
        - Missing Outreach Validity Score
        - Outreach Validity Score < threshold
        - Has enrichment data but no outreach
        """
        result = {'leads': [], 'triggers': []}
        
        # Screen lead outreach
        all_leads = self.leads_table.all(
            formula="AND({Enrichment Status} = 'Enriched', {Lead ICP Score} >= 40)"
        )
        
        for record in all_leads:
            fields = record['fields']
            validity_score = fields.get('Outreach Validity Score', None)
            has_email_body = bool(fields.get('Email Body', '').strip())
            
            # Needs outreach if: no messages at all, or low validity score
            if not has_email_body:
                result['leads'].append({
                    'record': record,
                    'validity_score': 0,
                    'reason': 'no outreach generated'
                })
            elif validity_score is not None and validity_score < threshold:
                result['leads'].append({
                    'record': record,
                    'validity_score': validity_score,
                    'reason': f'validity {validity_score} < {threshold}'
                })
            elif validity_score is None:
                result['leads'].append({
                    'record': record,
                    'validity_score': 0,
                    'reason': 'no validity score'
                })
        
        result['leads'].sort(key=lambda x: x['validity_score'])
        
        # Screen trigger outreach
        if self.has_triggers:
            try:
                all_triggers = self.trigger_history_table.all(
                    formula="{Email Body} != ''"
                )
                
                for record in all_triggers:
                    fields = record['fields']
                    validity_score = fields.get('Outreach Validity Score', None)
                    
                    if validity_score is not None and validity_score < threshold:
                        result['triggers'].append({
                            'record': record,
                            'validity_score': validity_score,
                            'reason': f'validity {validity_score} < {threshold}'
                        })
                    elif validity_score is None:
                        result['triggers'].append({
                            'record': record,
                            'validity_score': 0,
                            'reason': 'no validity score'
                        })
            except Exception as e:
                logger.warning(f"Could not screen triggers: {e}")
        
        result['triggers'].sort(key=lambda x: x['validity_score'])
        
        logger.info(f"Outreach needing regeneration: {len(result['leads'])} leads, {len(result['triggers'])} triggers (threshold: {threshold})")
        return result
    
    # ═══════════════════════════════════════════════════════════
    # RE-ENRICHMENT — Company
    # ═══════════════════════════════════════════════════════════
    
    def re_enrich_company(self, record: Dict) -> bool:
        """Re-enrich a single company record."""
        fields = record['fields']
        company_name = fields.get('Company Name', 'Unknown')
        record_id = record['id']
        
        prompt = f"""Research this biotech/pharma company for business intelligence:

COMPANY: {company_name}

═══════════════════════════════════════════════════════════
CRITICAL RULES:
═══════════════════════════════════════════════════════════
1. ONLY report facts you can verify from web search. Return null if not found — do NOT guess.
2. DISAMBIGUATION: If "{company_name}" matches multiple companies, pick the biotech/pharma one.
3. FUNDING: Only from credible sources. NEVER guess amounts.
4. PIPELINE STAGE: Only report explicitly stated stages.
5. CDMO PARTNERSHIPS: Only if confirmed. "None found" is valid.
6. Per-field confidence: "high" (multiple sources), "medium" (single source), "low" (inferred), "unverified" (not found).

Find (return null for unverifiable data):
1. Website URL
2. LinkedIn company page URL  
3. Headquarters (city, country)
4. Company size — one of: {', '.join(self.VALID_COMPANY_SIZE)}
5. Focus areas — from: {', '.join(self.VALID_FOCUS_AREAS)}
6. Technology platform — from: {', '.join(self.VALID_TECH_PLATFORMS)}
7. Funding stage — one of: {', '.join(self.VALID_FUNDING_STAGES)}
8. Total funding USD — ONLY if found
9. Latest funding round — ONLY if found
10. Pipeline stages — from: {', '.join(self.VALID_PIPELINE_STAGES)}
11. Lead programs
12. Therapeutic areas — from: {', '.join(self.VALID_THERAPEUTIC_AREAS)}
13. CDMO partnerships — ONLY if confirmed
14. Manufacturing status — one of: {', '.join(self.VALID_MANUFACTURING_STATUS)}
15. Recent news

Return ONLY valid JSON:
{{
    "website": "URL or null",
    "linkedin_company_page": "URL or null",
    "location": "City, Country or null",
    "company_size": "51-200 or null",
    "focus_areas": ["mAbs"] or [],
    "technology_platforms": ["Mammalian CHO"] or [],
    "funding_stage": "Series B or Unknown",
    "total_funding_usd": null,
    "latest_funding_round": "null or details",
    "pipeline_stages": ["Phase 2"] or [],
    "lead_programs": "description or null",
    "therapeutic_areas": ["Oncology"] or [],
    "cdmo_partnerships": "details or None found",
    "manufacturing_status": "No Public Partner or Unknown",
    "recent_news": "text or null",
    "intelligence_notes": "Key findings",
    "data_confidence": {{
        "funding": "high|medium|low|unverified",
        "pipeline": "high|medium|low|unverified",
        "therapeutic_areas": "high|medium|low|unverified",
        "cdmo_partnerships": "high|medium|low|unverified"
    }}
}}"""
        
        try:
            message = self.anthropic_client.messages.create(
                model=self.config['anthropic']['model'],
                max_tokens=2000,
                tools=[{"type": "web_search_20250305", "name": "web_search"}],
                messages=[{"role": "user", "content": prompt}]
            )
            
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
                return False
            
            data = json.loads(json_str.strip())
            
            # Build update fields
            update_fields = {
                'Last Intelligence Check': datetime.now().strftime('%Y-%m-%d')
            }
            
            # Map enriched fields
            field_map = {
                'website': 'Website',
                'linkedin_company_page': 'LinkedIn Company Page',
                'location': 'Location/HQ',
                'latest_funding_round': 'Latest Funding Round',
                'lead_programs': 'Lead Programs',
                'cdmo_partnerships': 'Current CDMO Partnerships',
            }
            for json_key, airtable_key in field_map.items():
                if data.get(json_key):
                    update_fields[airtable_key] = data[json_key]
            
            # Funding stage
            if data.get('funding_stage'):
                val = data['funding_stage']
                if val in self.VALID_FUNDING_STAGES:
                    update_fields['Funding Stage'] = val
            
            # Total funding
            if data.get('total_funding_usd'):
                try:
                    update_fields['Total Funding'] = float(data['total_funding_usd'])
                except:
                    pass
            
            # Manufacturing status
            if data.get('manufacturing_status'):
                val = data['manufacturing_status']
                if val in self.VALID_MANUFACTURING_STATUS:
                    update_fields['Manufacturing Status'] = val
            
            # Multi-selects
            for json_key, airtable_key, valid_list in [
                ('focus_areas', 'Focus Area', self.VALID_FOCUS_AREAS),
                ('technology_platforms', 'Technology Platform', self.VALID_TECH_PLATFORMS),
                ('therapeutic_areas', 'Therapeutic Areas', self.VALID_THERAPEUTIC_AREAS),
                ('pipeline_stages', 'Pipeline Stage', self.VALID_PIPELINE_STAGES),
            ]:
                values = data.get(json_key, [])
                if values:
                    if isinstance(values, str):
                        values = [values]
                    validated = [v for v in values if v in valid_list]
                    if validated:
                        update_fields[airtable_key] = validated
            
            # Intelligence Notes with confidence
            data_confidence = data.get('data_confidence', {})
            notes_parts = []
            if data.get('intelligence_notes'):
                notes_parts.append(data['intelligence_notes'][:500])
            if data.get('recent_news'):
                notes_parts.append(f"Recent: {data['recent_news'][:300]}")
            if data_confidence:
                low_conf = [f"⚠ {k}: {v}" for k, v in data_confidence.items() if v in ('low', 'unverified')]
                if low_conf:
                    notes_parts.append("Confidence Warnings:\n" + "\n".join(low_conf))
            if notes_parts:
                update_fields['Intelligence Notes'] = f"[Housekeeping {datetime.now().strftime('%Y-%m-%d')}]\n" + "\n\n".join(notes_parts)
            
            # Store confidence
            if data_confidence:
                update_fields['Data Confidence'] = json.dumps(data_confidence)
            
            self.companies_table.update(record_id, update_fields)
            return True
            
        except Exception as e:
            logger.error(f"  Error re-enriching company: {e}")
            return False
    
    # ═══════════════════════════════════════════════════════════
    # RE-ENRICHMENT — Lead
    # ═══════════════════════════════════════════════════════════
    
    def re_enrich_lead(self, record: Dict) -> bool:
        """Re-enrich a single lead record."""
        fields = record['fields']
        lead_name = fields.get('Lead Name', 'Unknown')
        title = fields.get('Title', '')
        record_id = record['id']
        
        # Get company name
        company_name = ''
        company_ids = fields.get('Company', [])
        if company_ids:
            try:
                company = self.companies_table.get(company_ids[0])
                company_name = company['fields'].get('Company Name', '')
            except:
                pass
        
        if not company_name:
            company_name = fields.get('Company Name', 'Unknown')
        
        prompt = f"""Research this professional for contact information:

NAME: {lead_name}
TITLE (from our records): {title}
COMPANY: {company_name}

═══════════════════════════════════════════════════════════
CRITICAL RULES:
═══════════════════════════════════════════════════════════
1. TITLE: KEEP "{title}" unless web clearly shows a different current title at {company_name}.
2. LINKEDIN: Only return if EXACT person found. Do NOT guess URLs.
3. EMAIL: Return null if not found — do NOT fabricate.
4. Only return info about THIS person at THIS company.

Find:
1. Email address
2. Current title (see rule 1)
3. LinkedIn URL (see rule 2)
4. Recent activity

Return ONLY valid JSON:
{{
    "email": "email or null",
    "email_confidence": "High|Medium|Low|Pattern Suggested",
    "title": "{title}",
    "title_changed": false,
    "title_change_reason": "null or reason",
    "linkedin_url": "URL or null",
    "recent_activity": "text or null",
    "data_confidence": {{
        "email": "high|medium|low|unverified",
        "title": "high|medium|low|unverified",
        "linkedin": "high|medium|low|unverified",
        "identity_match": "high|medium|low"
    }}
}}"""
        
        try:
            message = self.anthropic_client.messages.create(
                model=self.config['anthropic']['model'],
                max_tokens=2000,
                tools=[{"type": "web_search_20250305", "name": "web_search"}],
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
            
            update_fields = {
                'Last Enrichment Date': datetime.now().strftime('%Y-%m-%d')
            }
            
            if data.get('email') and '@' in str(data.get('email', '')):
                update_fields['Email'] = data['email']
            if data.get('title') and not data.get('title_changed', False):
                update_fields['Title'] = data['title']
            elif data.get('title') and data.get('title_changed'):
                update_fields['Title'] = data['title']
                logger.info(f"  ⚠ Title changed: '{title}' → '{data['title']}'")
            if data.get('linkedin_url') and 'linkedin.com' in str(data.get('linkedin_url', '')):
                update_fields['LinkedIn URL'] = data['linkedin_url']
            
            # Store confidence
            lead_conf = data.get('data_confidence', {})
            if lead_conf:
                update_fields['Data Confidence'] = json.dumps(lead_conf)
            
            self.leads_table.update(record_id, update_fields)
            return True
            
        except Exception as e:
            logger.error(f"  Error re-enriching lead: {e}")
            return False
    
    # ═══════════════════════════════════════════════════════════
    # OUTREACH REGENERATION — with versioning
    # ═══════════════════════════════════════════════════════════
    
    def get_outreach_version(self, fields: Dict) -> int:
        """Get current outreach version number from record."""
        return int(fields.get('Outreach Version', 0) or 0)
    
    def regenerate_lead_outreach(self, record: Dict, max_version: int = 10) -> bool:
        """Regenerate outreach for a lead with version tracking.
        
        Increments Outreach Version on each regeneration.
        Stops if version >= max_version (prevent infinite loops).
        """
        fields = record['fields']
        lead_name = fields.get('Lead Name', 'Unknown')
        title = fields.get('Title', '')
        record_id = record['id']
        current_version = self.get_outreach_version(fields)
        
        if current_version >= max_version:
            logger.info(f"  ⏭ Skipping {lead_name} — already at version {current_version}")
            return False
        
        new_version = current_version + 1
        
        # Get company context
        company_name = ''
        company_context = {}
        company_ids = fields.get('Company', [])
        if company_ids:
            try:
                company = self.companies_table.get(company_ids[0])
                cf = company['fields']
                company_name = cf.get('Company Name', '')
                company_context = {
                    'technology_platform': ', '.join(cf.get('Technology Platform', [])),
                    'therapeutic_areas': ', '.join(cf.get('Therapeutic Areas', [])),
                    'pipeline_stage': ', '.join(cf.get('Pipeline Stage', [])),
                }
                
                # Filter by confidence
                raw_conf = cf.get('Data Confidence', '')
                if raw_conf:
                    try:
                        conf = json.loads(raw_conf)
                        if conf.get('therapeutic_areas') in ('low', 'unverified'):
                            company_context['therapeutic_areas'] = ''
                        if conf.get('pipeline') in ('low', 'unverified'):
                            company_context['pipeline_stage'] = ''
                    except:
                        pass
            except:
                pass
        
        if not company_name:
            company_name = fields.get('Company Name', 'Unknown')
        
        lead_icp = fields.get('Lead ICP Score', 50)
        company_icp = fields.get('Company ICP Score', None)
        
        # Build context for outreach generation
        context_parts = [f"Title: {title}", f"Company: {company_name}"]
        if company_context.get('technology_platform'):
            context_parts.append(f"Technology: {company_context['technology_platform']}")
        if company_context.get('therapeutic_areas'):
            context_parts.append(f"Focus: {company_context['therapeutic_areas']}")
        
        context_str = "\n".join(context_parts)
        
        prompt = f"""Generate professional outreach messages for this lead.

LEAD: {lead_name}
{context_str}
Lead ICP: {lead_icp}/100

YOUR COMPANY (Rezon Bio):
European CDMO specializing in mammalian cell culture (mAbs, bispecifics, ADCs).

This is VERSION {new_version} of the outreach. {"Previous versions scored below quality threshold — make this one better." if current_version > 0 else ""}

═══════════════════════════════════════════════════════════
CRITICAL RULES:
═══════════════════════════════════════════════════════════
- NEVER mention specific funding amounts or rounds
- NEVER claim specific pipeline stages unless listed above
- NEVER mention CDMO partnerships or manufacturing decisions
- Pick ONE relevant detail max
- Sound human, not AI
- NO bullet lists, NO **bold**
- ALL messages must be SHORT

Generate FOUR messages:

1. EMAIL (60-80 words max)
Subject: [Natural, short]
Body: Natural opener, one detail, soft CTA
Sign: "Best regards, [Your Name], Rezon Bio Business Development"

2. LINKEDIN CONNECTION (under 200 chars)
Brief, friendly. No signature.

3. LINKEDIN SHORT (under 300 chars)
After connection. Conversational.
Sign: "Best regards, [Your Name], Rezon Bio BD"

4. LINKEDIN INMAIL (60-80 words max)
Subject: [Not salesy]
Body: Observation about their work, why connecting makes sense
Sign: "Best regards, [Your Name], Rezon Bio Business Development"

Return ONLY valid JSON:
{{
    "email_subject": "Subject",
    "email_body": "Body with signature",
    "linkedin_connection": "Under 200 chars",
    "linkedin_short": "Under 300 chars with signature",
    "linkedin_inmail_subject": "Subject",
    "linkedin_inmail_body": "Body with signature"
}}"""
        
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
            
            # Parse JSON
            response_text = response_text.strip()
            if response_text.startswith("```json"):
                response_text = response_text[7:]
            if response_text.startswith("```"):
                response_text = response_text[3:]
            if response_text.endswith("```"):
                response_text = response_text[:-3]
            response_text = response_text.strip()
            
            if not response_text.startswith("{"):
                start = response_text.find("{")
                end = response_text.rfind("}") + 1
                if start != -1:
                    response_text = response_text[start:end]
            
            messages_data = json.loads(response_text)
            
            # Update lead with new outreach + version
            update_fields = {
                'Email Subject': messages_data.get('email_subject', ''),
                'Email Body': messages_data.get('email_body', ''),
                'LinkedIn Connection Request': messages_data.get('linkedin_connection', ''),
                'LinkedIn Short Message': messages_data.get('linkedin_short', ''),
                'LinkedIn InMail Subject': messages_data.get('linkedin_inmail_subject', ''),
                'LinkedIn InMail Body': messages_data.get('linkedin_inmail_body', ''),
                'Message Generated Date': datetime.now().strftime('%Y-%m-%d'),
                'Outreach Version': new_version,
                # Reset validation so it gets re-validated
                'Outreach Validity Rating': '',
                'Outreach Validity Score': None,
            }
            
            self.leads_table.update(record_id, update_fields)
            return True
            
        except Exception as e:
            logger.error(f"  Error regenerating outreach: {e}")
            return False
    
    def regenerate_trigger_outreach(self, record: Dict, max_version: int = 10) -> bool:
        """Regenerate outreach for a trigger record with version tracking."""
        if not self.has_triggers:
            return False
        
        fields = record['fields']
        trigger_type = fields.get('Trigger Type', 'Unknown')
        record_id = record['id']
        current_version = self.get_outreach_version(fields)
        
        if current_version >= max_version:
            return False
        
        new_version = current_version + 1
        
        # Get lead context
        lead_name = ''
        company_name = ''
        title = ''
        lead_ids = fields.get('Lead', [])
        if lead_ids:
            try:
                lead = self.leads_table.get(lead_ids[0])
                lead_name = lead['fields'].get('Lead Name', '')
                title = lead['fields'].get('Title', '')
                company_ids = lead['fields'].get('Company', [])
                if company_ids:
                    company = self.companies_table.get(company_ids[0])
                    company_name = company['fields'].get('Company Name', '')
            except:
                pass
        
        trigger_details = fields.get('Trigger Details', fields.get('Description', ''))
        trigger_date = fields.get('Trigger Date', '')
        
        prompt = f"""Generate trigger-based outreach for this lead.

LEAD: {lead_name}
TITLE: {title}
COMPANY: {company_name}
TRIGGER: {trigger_type}
TRIGGER DETAILS: {trigger_details[:500]}
TRIGGER DATE: {trigger_date}

This is VERSION {new_version}.

YOUR COMPANY (Rezon Bio): European CDMO for mammalian cell culture (mAbs, bispecifics, ADCs).

RULES:
- NEVER mention specific funding amounts unless in trigger details above
- Reference the trigger naturally — don't over-explain
- 60-80 words max per message
- ONE relevant detail max
- Sound human, not AI

Generate:
1. EMAIL (60-80 words, Subject + Body, sign as [Your Name], Rezon Bio BD)
2. LINKEDIN CONNECTION (under 200 chars)

Return ONLY valid JSON:
{{
    "email_subject": "Subject referencing trigger",
    "email_body": "Body with signature",
    "linkedin_connection_request": "Under 200 chars",
    "linkedin_short": "Under 300 chars",
    "best_time_to_send": "timing recommendation",
    "follow_up_angle": "alternative angle if no response"
}}"""
        
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
            
            response_text = response_text.strip()
            if "```json" in response_text:
                response_text = response_text.split("```json")[1].split("```")[0]
            elif not response_text.startswith("{"):
                start = response_text.find("{")
                end = response_text.rfind("}") + 1
                if start != -1:
                    response_text = response_text[start:end]
            
            messages_data = json.loads(response_text.strip())
            
            update_fields = {
                'Email Subject': messages_data.get('email_subject', ''),
                'Email Body': messages_data.get('email_body', ''),
                'LinkedIn Connection Request': messages_data.get('linkedin_connection_request', ''),
                'LinkedIn Short Message': messages_data.get('linkedin_short', ''),
                'Outreach Generated Date': datetime.now().strftime('%Y-%m-%d'),
                'Best Time to Send': messages_data.get('best_time_to_send', ''),
                'Follow Up Angle': messages_data.get('follow_up_angle', ''),
                'Outreach Version': new_version,
                'Outreach Validity Rating': '',
                'Outreach Validity Score': None,
            }
            
            self.trigger_history_table.update(record_id, update_fields)
            return True
            
        except Exception as e:
            logger.error(f"  Error regenerating trigger outreach: {e}")
            return False
    
    # ═══════════════════════════════════════════════════════════
    # MAIN ORCHESTRATION
    # ═══════════════════════════════════════════════════════════
    
    def run_screen(self, threshold: int = 85) -> Dict:
        """Screen all records and report what needs work (dry-run)."""
        logger.info("="*70)
        logger.info("HOUSEKEEPING SCREENING REPORT")
        logger.info(f"Confidence threshold: {threshold}")
        logger.info("="*70)
        
        companies = self.screen_companies(threshold)
        leads = self.screen_leads(threshold)
        outreach = self.screen_outreach(threshold)
        
        # Print summary
        logger.info(f"\n{'─'*50}")
        logger.info(f"COMPANIES needing re-enrichment: {len(companies)}")
        if companies:
            for item in companies[:10]:
                name = item['record']['fields'].get('Company Name', '?')
                logger.info(f"  • {name} — {item['reason']} (conf: {item['confidence_score']})")
            if len(companies) > 10:
                logger.info(f"  ... and {len(companies) - 10} more")
        
        logger.info(f"\n{'─'*50}")
        logger.info(f"LEADS needing re-enrichment: {len(leads)}")
        if leads:
            for item in leads[:10]:
                name = item['record']['fields'].get('Lead Name', '?')
                logger.info(f"  • {name} — {item['reason']} (conf: {item['confidence_score']})")
            if len(leads) > 10:
                logger.info(f"  ... and {len(leads) - 10} more")
        
        logger.info(f"\n{'─'*50}")
        logger.info(f"OUTREACH needing regeneration: {len(outreach['leads'])} leads, {len(outreach['triggers'])} triggers")
        if outreach['leads']:
            for item in outreach['leads'][:10]:
                name = item['record']['fields'].get('Lead Name', '?')
                logger.info(f"  • {name} — {item['reason']} (validity: {item['validity_score']})")
            if len(outreach['leads']) > 10:
                logger.info(f"  ... and {len(outreach['leads']) - 10} more")
        
        logger.info(f"\n{'='*70}")
        total = len(companies) + len(leads) + len(outreach['leads']) + len(outreach['triggers'])
        logger.info(f"TOTAL RECORDS NEEDING ATTENTION: {total}")
        logger.info("="*70)
        
        return {
            'companies': companies,
            'leads': leads,
            'outreach': outreach,
            'total': total
        }
    
    def run_full(self, threshold: int = 85, limit: int = None,
                 companies_only: bool = False, leads_only: bool = False,
                 outreach_only: bool = False):
        """Run full housekeeping: screen → re-enrich → regenerate."""
        
        logger.info("="*70)
        logger.info("HOUSEKEEPING — FULL RUN")
        logger.info(f"Threshold: {threshold} | Limit: {limit or 'unlimited'}")
        logger.info(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("="*70)
        
        stats = {
            'companies_screened': 0, 'companies_enriched': 0, 'companies_failed': 0,
            'leads_screened': 0, 'leads_enriched': 0, 'leads_failed': 0,
            'outreach_screened': 0, 'outreach_regenerated': 0, 'outreach_failed': 0,
            'trigger_outreach_regenerated': 0, 'trigger_outreach_failed': 0,
        }
        
        # ─── PHASE 1: Re-enrich companies ───
        if not leads_only and not outreach_only:
            logger.info("\n" + "─"*50)
            logger.info("PHASE 1: RE-ENRICH COMPANIES")
            logger.info("─"*50)
            
            companies = self.screen_companies(threshold)
            if limit:
                companies = companies[:limit]
            stats['companies_screened'] = len(companies)
            
            for idx, item in enumerate(companies, 1):
                name = item['record']['fields'].get('Company Name', '?')
                old_conf = item['confidence_score']
                logger.info(f"[{idx}/{len(companies)}] {name} (conf: {old_conf}, {item['reason']})")
                
                success = self.re_enrich_company(item['record'])
                if success:
                    stats['companies_enriched'] += 1
                    logger.info(f"  ✓ Re-enriched")
                else:
                    stats['companies_failed'] += 1
                    logger.warning(f"  ✗ Failed")
                
                time.sleep(self.rate_limit_delay)
        
        # ─── PHASE 2: Re-enrich leads ───
        if not companies_only and not outreach_only:
            logger.info("\n" + "─"*50)
            logger.info("PHASE 2: RE-ENRICH LEADS")
            logger.info("─"*50)
            
            leads = self.screen_leads(threshold)
            if limit:
                leads = leads[:limit]
            stats['leads_screened'] = len(leads)
            
            for idx, item in enumerate(leads, 1):
                name = item['record']['fields'].get('Lead Name', '?')
                old_conf = item['confidence_score']
                logger.info(f"[{idx}/{len(leads)}] {name} (conf: {old_conf}, {item['reason']})")
                
                success = self.re_enrich_lead(item['record'])
                if success:
                    stats['leads_enriched'] += 1
                    logger.info(f"  ✓ Re-enriched")
                else:
                    stats['leads_failed'] += 1
                    logger.warning(f"  ✗ Failed")
                
                time.sleep(self.rate_limit_delay)
        
        # ─── PHASE 3: Regenerate outreach ───
        if not companies_only and not leads_only:
            logger.info("\n" + "─"*50)
            logger.info("PHASE 3: REGENERATE OUTREACH")
            logger.info("─"*50)
            
            outreach = self.screen_outreach(threshold)
            
            # Lead outreach
            lead_outreach = outreach['leads']
            if limit:
                lead_outreach = lead_outreach[:limit]
            stats['outreach_screened'] = len(lead_outreach)
            
            for idx, item in enumerate(lead_outreach, 1):
                name = item['record']['fields'].get('Lead Name', '?')
                version = self.get_outreach_version(item['record']['fields'])
                logger.info(f"[{idx}/{len(lead_outreach)}] {name} (validity: {item['validity_score']}, v{version} → v{version+1})")
                
                success = self.regenerate_lead_outreach(item['record'])
                if success:
                    stats['outreach_regenerated'] += 1
                    logger.info(f"  ✓ Regenerated (v{version+1})")
                else:
                    stats['outreach_failed'] += 1
                
                time.sleep(self.rate_limit_delay)
            
            # Trigger outreach
            trigger_outreach = outreach['triggers']
            if limit:
                trigger_outreach = trigger_outreach[:limit]
            
            for idx, item in enumerate(trigger_outreach, 1):
                trigger_type = item['record']['fields'].get('Trigger Type', '?')
                version = self.get_outreach_version(item['record']['fields'])
                logger.info(f"[{idx}/{len(trigger_outreach)}] Trigger: {trigger_type} (v{version} → v{version+1})")
                
                success = self.regenerate_trigger_outreach(item['record'])
                if success:
                    stats['trigger_outreach_regenerated'] += 1
                    logger.info(f"  ✓ Regenerated")
                else:
                    stats['trigger_outreach_failed'] += 1
                
                time.sleep(self.rate_limit_delay)
        
        # ─── SUMMARY ───
        logger.info("\n" + "="*70)
        logger.info("HOUSEKEEPING COMPLETE")
        logger.info("="*70)
        logger.info(f"Companies: {stats['companies_enriched']}/{stats['companies_screened']} re-enriched ({stats['companies_failed']} failed)")
        logger.info(f"Leads: {stats['leads_enriched']}/{stats['leads_screened']} re-enriched ({stats['leads_failed']} failed)")
        logger.info(f"Lead Outreach: {stats['outreach_regenerated']}/{stats['outreach_screened']} regenerated ({stats['outreach_failed']} failed)")
        logger.info(f"Trigger Outreach: {stats['trigger_outreach_regenerated']} regenerated ({stats['trigger_outreach_failed']} failed)")
        logger.info("="*70)
        
        return stats


def main():
    parser = argparse.ArgumentParser(description='Housekeeping — Background quality maintenance')
    parser.add_argument('--screen-only', action='store_true',
                       help='Only screen and report (dry-run, no changes)')
    parser.add_argument('--companies-only', action='store_true',
                       help='Only re-enrich companies')
    parser.add_argument('--leads-only', action='store_true',
                       help='Only re-enrich leads')
    parser.add_argument('--outreach-only', action='store_true',
                       help='Only regenerate outreach messages')
    parser.add_argument('--confidence-threshold', type=int, default=85,
                       help='Confidence threshold (default: 85)')
    parser.add_argument('--limit', type=int, default=None,
                       help='Limit records per category')
    parser.add_argument('--config', default='config.yaml',
                       help='Path to config file')
    
    args = parser.parse_args()
    
    try:
        manager = HousekeepingManager(config_path=args.config)
        
        if args.screen_only:
            manager.run_screen(threshold=args.confidence_threshold)
        else:
            manager.run_full(
                threshold=args.confidence_threshold,
                limit=args.limit,
                companies_only=args.companies_only,
                leads_only=args.leads_only,
                outreach_only=args.outreach_only,
            )
    except FileNotFoundError:
        logger.error(f"Config file not found: {args.config}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
