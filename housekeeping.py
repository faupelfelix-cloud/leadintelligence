#!/usr/bin/env python3
"""
Housekeeping — Background quality maintenance for the Lead Intelligence System.

Checks companies, leads, and outreach messages for missing or low data confidence,
re-enriches records that fall below thresholds, and regenerates outreach messages
with version tracking.

Usage:
    # AUDIT FIRST (zero API calls — just read and score existing data)
    python housekeeping.py --audit

    # Full housekeeping (screen → re-enrich → regenerate)
    python housekeeping.py

    # Screen only (report what needs fixing, no changes)
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
import unicodedata
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from collections import Counter

import yaml
import anthropic
from pyairtable import Api
from confidence_utils import calculate_confidence_score

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


def sanitize_string(s: str) -> str:
    """Strip curly quotes, smart quotes, and other unicode quotation artifacts
    that AI models sometimes produce, which Airtable rejects as new select options.
    
    "Cardiovascular" → Cardiovascular
    'mAbs' → mAbs
    """
    if not isinstance(s, str):
        return s
    # Replace all unicode quote variants with straight quotes, then strip them
    quote_chars = '""''‛‟「」『』〝〞＂＇'
    for ch in quote_chars:
        s = s.replace(ch, '')
    # Also strip regular straight quotes at start/end
    s = s.strip('"\'')
    return s.strip()


def safe_update(table, record_id: str, update_fields: Dict, 
                label: str = "record") -> bool:
    """Update Airtable record with per-field fallback.
    
    Tries the full update first. On failure, retries field-by-field
    and logs which fields failed — avoids one bad field killing the
    entire update.
    """
    try:
        table.update(record_id, update_fields)
        return True
    except Exception as e:
        error_str = str(e)
        logger.warning(f"  Full update failed for {label}: {error_str}")
        
        # Identify the failing field from the error message
        # Try field-by-field fallback
        success_count = 0
        failed_fields = []
        for field_name, field_value in update_fields.items():
            try:
                table.update(record_id, {field_name: field_value})
                success_count += 1
            except Exception as field_err:
                failed_fields.append(f"{field_name}: {field_err}")
        
        if failed_fields:
            logger.warning(f"  Partial update: {success_count}/{len(update_fields)} fields succeeded")
            for ff in failed_fields:
                logger.warning(f"    ✗ {ff}")
        else:
            logger.info(f"  ✓ All {success_count} fields updated individually")
        
        return success_count > 0


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
    
    # Valid select field options (must match Airtable exactly)
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
        
        api = Api(api_key)
        self.companies_table = api.table(base_id, 'Companies')
        self.leads_table = api.table(base_id, 'Leads')
        
        # Optional tables — graceful if not present
        try:
            self.trigger_history_table = api.table(base_id, 'Trigger History')
            self.has_triggers = True
        except:
            self.has_triggers = False
        
        self.anthropic_client = anthropic.Anthropic(
            api_key=self.config['anthropic']['api_key']
        )
        
        self.rate_limit_delay = self.config.get('web_search', {}).get('rate_limit_delay', 2)
        
        # Detect which fields exist in Leads table (to avoid UNKNOWN_FIELD_NAME errors)
        self._leads_has_data_confidence = None
        self._leads_has_outreach_version = None
        self._leads_has_validity_rating = None
        
        logger.info("✓ HousekeepingManager initialized")
    
    def _detect_leads_fields(self):
        """Detect which optional fields exist in the Leads table.
        Done lazily on first write attempt to avoid extra API calls."""
        if self._leads_has_data_confidence is not None:
            return  # Already detected
        
        try:
            # Fetch one lead to check available fields
            test_leads = self.leads_table.all(max_records=1)
            if test_leads:
                available_fields = set(test_leads[0]['fields'].keys())
                # Also try to update with each field to check writability
                # For now, use field name presence as a hint (not perfect but avoids writes)
                self._leads_has_data_confidence = True  # Assume yes, catch on write
                self._leads_has_outreach_version = True
                self._leads_has_validity_rating = True
                logger.info("  Field detection: assuming all optional fields exist (will fallback on write)")
            else:
                self._leads_has_data_confidence = True
                self._leads_has_outreach_version = True
                self._leads_has_validity_rating = True
        except:
            self._leads_has_data_confidence = True
            self._leads_has_outreach_version = True
            self._leads_has_validity_rating = True
    
    # ═══════════════════════════════════════════════════════════
    # AUDIT — Zero-cost data quality assessment
    # ═══════════════════════════════════════════════════════════
    
    def run_audit(self) -> Dict:
        """Comprehensive data quality audit — ZERO API calls to Anthropic.
        
        Reads all records from Airtable and scores data completeness and
        confidence across companies, leads, and outreach. Produces a
        detailed report before any money is spent on re-enrichment.
        """
        logger.info("="*70)
        logger.info("DATA QUALITY AUDIT")
        logger.info(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("This audit uses ZERO Anthropic API calls.")
        logger.info("="*70)
        
        audit = {}
        
        # ─── COMPANIES ───
        logger.info("\n" + "─"*50)
        logger.info("AUDITING COMPANIES")
        logger.info("─"*50)
        
        all_companies = self.companies_table.all()
        total_companies = len(all_companies)
        
        company_stats = {
            'total': total_companies,
            'enriched': 0,
            'not_enriched': 0,
            'failed': 0,
            'has_confidence': 0,
            'missing_confidence': 0,
            'confidence_distribution': Counter(),
            'missing_fields': Counter(),
            'icp_distribution': {'0-20': 0, '21-40': 0, '41-60': 0, '61-80': 0, '81-100': 0},
        }
        
        key_company_fields = [
            'Website', 'Location/HQ', 'Funding Stage', 'Pipeline Stage', 
            'Therapeutic Areas', 'Technology Platform', 'Manufacturing Status',
            'Focus Area', 'ICP Fit Score'
        ]
        
        for record in all_companies:
            fields = record['fields']
            status = fields.get('Enrichment Status', '')
            
            if status == 'Enriched':
                company_stats['enriched'] += 1
            elif status == 'Failed':
                company_stats['failed'] += 1
            else:
                company_stats['not_enriched'] += 1
            
            # Confidence
            raw_conf = fields.get('Data Confidence', '')
            if raw_conf:
                company_stats['has_confidence'] += 1
                score = self.calculate_record_confidence_score(raw_conf)
                if score >= 85:
                    company_stats['confidence_distribution']['excellent (85-100)'] += 1
                elif score >= 70:
                    company_stats['confidence_distribution']['good (70-84)'] += 1
                elif score >= 40:
                    company_stats['confidence_distribution']['fair (40-69)'] += 1
                else:
                    company_stats['confidence_distribution']['poor (0-39)'] += 1
            else:
                company_stats['missing_confidence'] += 1
            
            # Missing fields (only for enriched records)
            if status == 'Enriched':
                for field in key_company_fields:
                    val = fields.get(field)
                    if not val or val == 'Unknown' or val == [] or val == ['Unknown']:
                        company_stats['missing_fields'][field] += 1
            
            # ICP distribution
            icp = fields.get('ICP Fit Score')
            if icp is not None:
                if icp <= 20: company_stats['icp_distribution']['0-20'] += 1
                elif icp <= 40: company_stats['icp_distribution']['21-40'] += 1
                elif icp <= 60: company_stats['icp_distribution']['41-60'] += 1
                elif icp <= 80: company_stats['icp_distribution']['61-80'] += 1
                else: company_stats['icp_distribution']['81-100'] += 1
        
        logger.info(f"Total companies: {total_companies}")
        logger.info(f"  Enriched: {company_stats['enriched']}")
        logger.info(f"  Not enriched: {company_stats['not_enriched']}")
        logger.info(f"  Failed: {company_stats['failed']}")
        logger.info(f"\nData Confidence Coverage:")
        logger.info(f"  Has confidence data: {company_stats['has_confidence']} ({company_stats['has_confidence']*100//max(total_companies,1)}%)")
        logger.info(f"  Missing confidence: {company_stats['missing_confidence']} ({company_stats['missing_confidence']*100//max(total_companies,1)}%)")
        if company_stats['has_confidence'] > 0:
            logger.info(f"\nConfidence Distribution (of {company_stats['has_confidence']} with data):")
            for level, count in sorted(company_stats['confidence_distribution'].items()):
                pct = count * 100 // company_stats['has_confidence']
                bar = "█" * (pct // 2)
                logger.info(f"  {level:25s}: {count:5d} ({pct:2d}%) {bar}")
        
        if company_stats['enriched'] > 0:
            logger.info(f"\nMissing Fields (of {company_stats['enriched']} enriched companies):")
            for field, count in company_stats['missing_fields'].most_common():
                pct = count * 100 // company_stats['enriched']
                logger.info(f"  {field:25s}: {count:5d} missing ({pct}%)")
        
        logger.info(f"\nICP Score Distribution:")
        for bucket, count in sorted(company_stats['icp_distribution'].items()):
            bar = "█" * (count // max(total_companies // 100, 1))
            logger.info(f"  {bucket:10s}: {count:5d} {bar}")
        
        audit['companies'] = company_stats
        
        # ─── LEADS ───
        logger.info("\n" + "─"*50)
        logger.info("AUDITING LEADS")
        logger.info("─"*50)
        
        all_leads = self.leads_table.all()
        total_leads = len(all_leads)
        
        lead_stats = {
            'total': total_leads,
            'enriched': 0,
            'not_enriched': 0,
            'has_confidence': 0,
            'missing_confidence': 0,
            'confidence_distribution': Counter(),
            'has_email': 0,
            'has_linkedin': 0,
            'has_title': 0,
            'has_outreach': 0,
            'has_validity_score': 0,
            'validity_distribution': Counter(),
            'outreach_version_distribution': Counter(),
        }
        
        for record in all_leads:
            fields = record['fields']
            status = fields.get('Enrichment Status', '')
            
            if status == 'Enriched':
                lead_stats['enriched'] += 1
            else:
                lead_stats['not_enriched'] += 1
            
            # Confidence
            raw_conf = fields.get('Data Confidence', '')
            if raw_conf:
                lead_stats['has_confidence'] += 1
                score = self.calculate_record_confidence_score(raw_conf)
                if score >= 85:
                    lead_stats['confidence_distribution']['excellent (85-100)'] += 1
                elif score >= 70:
                    lead_stats['confidence_distribution']['good (70-84)'] += 1
                elif score >= 40:
                    lead_stats['confidence_distribution']['fair (40-69)'] += 1
                else:
                    lead_stats['confidence_distribution']['poor (0-39)'] += 1
            else:
                lead_stats['missing_confidence'] += 1
            
            # Contact data
            if fields.get('Email'):
                lead_stats['has_email'] += 1
            if fields.get('LinkedIn URL'):
                lead_stats['has_linkedin'] += 1
            if fields.get('Title'):
                lead_stats['has_title'] += 1
            
            # Outreach
            if fields.get('Email Body', '').strip():
                lead_stats['has_outreach'] += 1
            
            validity = fields.get('Outreach Validity Score')
            if validity is not None:
                lead_stats['has_validity_score'] += 1
                if validity >= 85:
                    lead_stats['validity_distribution']['excellent (85-100)'] += 1
                elif validity >= 70:
                    lead_stats['validity_distribution']['good (70-84)'] += 1
                elif validity >= 50:
                    lead_stats['validity_distribution']['fair (50-69)'] += 1
                else:
                    lead_stats['validity_distribution']['poor (0-49)'] += 1
            
            version = fields.get('Outreach Version', 0) or 0
            lead_stats['outreach_version_distribution'][f'v{int(version)}'] += 1
        
        logger.info(f"Total leads: {total_leads}")
        logger.info(f"  Enriched: {lead_stats['enriched']}")
        logger.info(f"  Not enriched: {lead_stats['not_enriched']}")
        logger.info(f"\nContact Data Coverage (of {total_leads} total):")
        logger.info(f"  Has email:    {lead_stats['has_email']:5d} ({lead_stats['has_email']*100//max(total_leads,1)}%)")
        logger.info(f"  Has LinkedIn: {lead_stats['has_linkedin']:5d} ({lead_stats['has_linkedin']*100//max(total_leads,1)}%)")
        logger.info(f"  Has title:    {lead_stats['has_title']:5d} ({lead_stats['has_title']*100//max(total_leads,1)}%)")
        logger.info(f"\nData Confidence Coverage:")
        logger.info(f"  Has confidence: {lead_stats['has_confidence']} ({lead_stats['has_confidence']*100//max(total_leads,1)}%)")
        logger.info(f"  Missing:        {lead_stats['missing_confidence']} ({lead_stats['missing_confidence']*100//max(total_leads,1)}%)")
        if lead_stats['has_confidence'] > 0:
            logger.info(f"\nConfidence Distribution (of {lead_stats['has_confidence']} with data):")
            for level, count in sorted(lead_stats['confidence_distribution'].items()):
                pct = count * 100 // lead_stats['has_confidence']
                bar = "█" * (pct // 2)
                logger.info(f"  {level:25s}: {count:5d} ({pct:2d}%) {bar}")
        
        logger.info(f"\nOutreach Coverage:")
        logger.info(f"  Has outreach:        {lead_stats['has_outreach']}")
        logger.info(f"  Has validity score:  {lead_stats['has_validity_score']}")
        if lead_stats['has_validity_score'] > 0:
            logger.info(f"\nOutreach Validity Distribution:")
            for level, count in sorted(lead_stats['validity_distribution'].items()):
                pct = count * 100 // lead_stats['has_validity_score']
                bar = "█" * (pct // 2)
                logger.info(f"  {level:25s}: {count:5d} ({pct:2d}%) {bar}")
        
        logger.info(f"\nOutreach Version Distribution:")
        for version, count in sorted(lead_stats['outreach_version_distribution'].items()):
            logger.info(f"  {version:5s}: {count:5d}")
        
        audit['leads'] = lead_stats
        
        # ─── COST ESTIMATE ───
        logger.info("\n" + "─"*50)
        logger.info("ESTIMATED HOUSEKEEPING COST")
        logger.info("─"*50)
        
        companies_needing_work = company_stats['missing_confidence'] + \
            sum(v for k, v in company_stats['confidence_distribution'].items() 
                if 'poor' in k or 'fair' in k)
        leads_needing_work = lead_stats['missing_confidence'] + \
            sum(v for k, v in lead_stats['confidence_distribution'].items() 
                if 'poor' in k or 'fair' in k)
        outreach_needing_work = lead_stats['has_outreach'] - lead_stats['has_validity_score'] + \
            sum(v for k, v in lead_stats['validity_distribution'].items() 
                if 'poor' in k or 'fair' in k)
        
        # Rough cost: ~$0.02 per web search enrichment, ~$0.005 per outreach generation
        est_enrichment_cost = (companies_needing_work + leads_needing_work) * 0.02
        est_outreach_cost = max(outreach_needing_work, 0) * 0.005
        est_total = est_enrichment_cost + est_outreach_cost
        
        logger.info(f"Records below threshold (85):")
        logger.info(f"  Companies: ~{companies_needing_work} need re-enrichment")
        logger.info(f"  Leads:     ~{leads_needing_work} need re-enrichment")
        logger.info(f"  Outreach:  ~{max(outreach_needing_work, 0)} need regeneration")
        logger.info(f"\nEstimated API cost (rough):")
        logger.info(f"  Enrichment: ~${est_enrichment_cost:.2f}")
        logger.info(f"  Outreach:   ~${est_outreach_cost:.2f}")
        logger.info(f"  TOTAL:      ~${est_total:.2f}")
        logger.info(f"\nRecommendation:")
        if companies_needing_work > 500:
            logger.info(f"  ⚠ {companies_needing_work} companies need work — run with --limit 100 first")
        if est_total > 50:
            logger.info(f"  ⚠ Estimated cost >${est_total:.0f} — consider batching with --limit")
        if companies_needing_work < 50 and leads_needing_work < 50:
            logger.info(f"  ✓ Small batch — safe to run full housekeeping")
        
        logger.info("\n" + "="*70)
        logger.info("AUDIT COMPLETE — No records were modified")
        logger.info("="*70)
        
        return audit
    
    # ═══════════════════════════════════════════════════════════
    # SCREENING — Identify records needing attention
    # ═══════════════════════════════════════════════════════════
    
    def calculate_record_confidence_score(self, data_confidence_raw: str) -> int:
        """Convert Data Confidence JSON to a single numeric score (0-100).
        Delegates to the shared confidence_utils module.
        """
        return calculate_confidence_score(data_confidence_raw)
    
    def screen_companies(self, threshold: int = 85) -> List[Dict]:
        """Find companies needing re-enrichment."""
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
        """Find outreach messages needing regeneration."""
        result = {'leads': [], 'triggers': []}
        
        all_leads = self.leads_table.all(
            formula="AND({Enrichment Status} = 'Enriched', {Lead ICP Score} >= 40)"
        )
        
        for record in all_leads:
            fields = record['fields']
            validity_score = fields.get('Outreach Validity Score', None)
            has_email_body = bool(fields.get('Email Body', '').strip())
            
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
    
    def _validate_multi_select(self, values, valid_list: List[str]) -> List[str]:
        """Sanitize and validate multi-select values against allowed options.
        
        Strips curly quotes and other AI artifacts, then filters to valid options only.
        """
        if not values:
            return []
        if isinstance(values, str):
            values = [values]
        
        validated = []
        for v in values:
            clean = sanitize_string(v)
            if clean in valid_list:
                validated.append(clean)
            else:
                # Try case-insensitive match
                for valid in valid_list:
                    if clean.lower() == valid.lower():
                        validated.append(valid)  # Use the canonical casing
                        break
        return validated
    
    def _validate_single_select(self, value: str, valid_list: List[str]) -> Optional[str]:
        """Sanitize and validate a single-select value."""
        if not value:
            return None
        clean = sanitize_string(value)
        if clean in valid_list:
            return clean
        # Case-insensitive fallback
        for valid in valid_list:
            if clean.lower() == valid.lower():
                return valid
        return None
    
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

IMPORTANT: Use EXACTLY these values for select fields — no quotes around values, no extra characters:
- Focus areas: {', '.join(self.VALID_FOCUS_AREAS)}
- Technology platforms: {', '.join(self.VALID_TECH_PLATFORMS)}
- Funding stages: {', '.join(self.VALID_FUNDING_STAGES)}
- Pipeline stages: {', '.join(self.VALID_PIPELINE_STAGES)}
- Therapeutic areas: {', '.join(self.VALID_THERAPEUTIC_AREAS)}
- Manufacturing status: {', '.join(self.VALID_MANUFACTURING_STATUS)}

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
            
            # Simple text fields
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
                    update_fields[airtable_key] = sanitize_string(str(data[json_key]))
            
            # Single-select fields — validated
            funding = self._validate_single_select(
                data.get('funding_stage', ''), self.VALID_FUNDING_STAGES)
            if funding:
                update_fields['Funding Stage'] = funding
            
            mfg = self._validate_single_select(
                data.get('manufacturing_status', ''), self.VALID_MANUFACTURING_STATUS)
            if mfg:
                update_fields['Manufacturing Status'] = mfg
            
            # Total funding
            if data.get('total_funding_usd'):
                try:
                    update_fields['Total Funding'] = float(data['total_funding_usd'])
                except:
                    pass
            
            # Multi-select fields — validated and sanitized
            for json_key, airtable_key, valid_list in [
                ('focus_areas', 'Focus Area', self.VALID_FOCUS_AREAS),
                ('technology_platforms', 'Technology Platform', self.VALID_TECH_PLATFORMS),
                ('therapeutic_areas', 'Therapeutic Areas', self.VALID_THERAPEUTIC_AREAS),
                ('pipeline_stages', 'Pipeline Stage', self.VALID_PIPELINE_STAGES),
            ]:
                validated = self._validate_multi_select(data.get(json_key, []), valid_list)
                if validated:
                    update_fields[airtable_key] = validated
            
            # Intelligence Notes with confidence
            data_confidence = data.get('data_confidence', {})
            notes_parts = []
            if data.get('intelligence_notes'):
                notes_parts.append(sanitize_string(data['intelligence_notes'][:500]))
            if data.get('recent_news'):
                notes_parts.append(f"Recent: {sanitize_string(data['recent_news'][:300])}")
            if data_confidence:
                low_conf = [f"⚠ {k}: {v}" for k, v in data_confidence.items() if v in ('low', 'unverified')]
                if low_conf:
                    notes_parts.append("Confidence Warnings:\n" + "\n".join(low_conf))
            if notes_parts:
                update_fields['Intelligence Notes'] = f"[Housekeeping {datetime.now().strftime('%Y-%m-%d')}]\n" + "\n\n".join(notes_parts)
            
            # Store confidence
            if data_confidence:
                update_fields['Data Confidence'] = json.dumps(data_confidence)
                update_fields['Data Confidence Score'] = calculate_confidence_score(data_confidence)
            
            return safe_update(self.companies_table, record_id, update_fields, label=company_name)
            
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
            
            # Store confidence — use safe_update so missing field doesn't crash
            lead_conf = data.get('data_confidence', {})
            if lead_conf:
                update_fields['Data Confidence'] = json.dumps(lead_conf)
                update_fields['Data Confidence Score'] = calculate_confidence_score(lead_conf)
            
            return safe_update(self.leads_table, record_id, update_fields, label=lead_name)
            
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
        """Regenerate outreach for a lead with version tracking."""
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
                
                # Filter by confidence — omit low/unverified data from prompt
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
        
        # Build context for outreach generation
        context_parts = [f"Title: {title}", f"Company: {company_name}"]
        if company_context.get('technology_platform'):
            context_parts.append(f"Technology: {company_context['technology_platform']}")
        if company_context.get('therapeutic_areas'):
            context_parts.append(f"Focus: {company_context['therapeutic_areas']}")
        
        context_str = "\n".join(context_parts)
        
        version_note = ""
        if current_version > 0:
            version_note = "Previous versions scored below quality threshold — make this one better."
        
        prompt = f"""Generate professional outreach messages for this lead.

LEAD: {lead_name}
{context_str}
Lead ICP: {lead_icp}/100

YOUR COMPANY (Rezon Bio):
European CDMO specializing in mammalian cell culture (mAbs, bispecifics, ADCs).

This is VERSION {new_version} of the outreach. {version_note}

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
            
            # Core outreach fields (always safe)
            update_fields = {
                'Email Subject': messages_data.get('email_subject', ''),
                'Email Body': messages_data.get('email_body', ''),
                'LinkedIn Connection Request': messages_data.get('linkedin_connection', ''),
                'LinkedIn Short Message': messages_data.get('linkedin_short', ''),
                'LinkedIn InMail Subject': messages_data.get('linkedin_inmail_subject', ''),
                'LinkedIn InMail Body': messages_data.get('linkedin_inmail_body', ''),
                'Message Generated Date': datetime.now().strftime('%Y-%m-%d'),
            }
            
            # Optional fields — added separately so they don't crash the whole update
            # Outreach Version (Number field — may not exist yet)
            update_fields['Outreach Version'] = new_version
            
            # Reset validity so next validation run re-scores
            # NOTE: Do NOT set to '' (empty string) — Airtable treats that as
            # creating a new select option. Set to None or omit entirely.
            update_fields['Outreach Validity Score'] = None
            
            return safe_update(self.leads_table, record_id, update_fields, label=lead_name)
            
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
        
        version_note = ""
        if current_version > 0:
            version_note = "Previous versions scored below quality threshold — make this one better."
        
        prompt = f"""Generate trigger-based outreach for this lead.

LEAD: {lead_name}
TITLE: {title}
COMPANY: {company_name}
TRIGGER: {trigger_type}
TRIGGER DETAILS: {trigger_details[:500]}
TRIGGER DATE: {trigger_date}

This is VERSION {new_version}. {version_note}

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
                'Outreach Validity Score': None,
            }
            
            return safe_update(self.trigger_history_table, record_id, update_fields, label=f"trigger-{trigger_type}")
            
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
    
    def run_full(self, threshold: int = 85, limit: int = None, offset: int = 0,
                 companies_only: bool = False, leads_only: bool = False,
                 outreach_only: bool = False):
        """Run full housekeeping: screen → re-enrich → regenerate.
        
        Args:
            threshold: Confidence threshold (0-100)
            limit: Max records per category to process
            offset: Skip this many records (for parallel batching)
            companies_only/leads_only/outreach_only: Run only one phase
        """
        
        logger.info("="*70)
        logger.info("HOUSEKEEPING — FULL RUN")
        logger.info(f"Threshold: {threshold} | Limit: {limit or 'unlimited'} | Offset: {offset}")
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
            total_needing = len(companies)
            # Apply offset + limit for parallel batching
            companies = companies[offset:]
            if limit:
                companies = companies[:limit]
            stats['companies_screened'] = len(companies)
            logger.info(f"Total needing work: {total_needing} | This batch: {len(companies)} (offset {offset})")
            
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
            total_needing = len(leads)
            leads = leads[offset:]
            if limit:
                leads = leads[:limit]
            stats['leads_screened'] = len(leads)
            logger.info(f"Total needing work: {total_needing} | This batch: {len(leads)} (offset {offset})")
            
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
            total_needing = len(lead_outreach)
            lead_outreach = lead_outreach[offset:]
            if limit:
                lead_outreach = lead_outreach[:limit]
            stats['outreach_screened'] = len(lead_outreach)
            logger.info(f"Total needing work: {total_needing} | This batch: {len(lead_outreach)} (offset {offset})")
            
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
            trigger_outreach = trigger_outreach[offset:]
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
    parser.add_argument('--audit', action='store_true',
                       help='Run data quality audit (ZERO API calls to Anthropic — read-only)')
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
    parser.add_argument('--offset', type=int, default=0,
                       help='Skip this many records (for parallel batching)')
    parser.add_argument('--config', default='config.yaml',
                       help='Path to config file')
    
    args = parser.parse_args()
    
    try:
        manager = HousekeepingManager(config_path=args.config)
        
        if args.audit:
            manager.run_audit()
        elif args.screen_only:
            manager.run_screen(threshold=args.confidence_threshold)
        else:
            manager.run_full(
                threshold=args.confidence_threshold,
                limit=args.limit,
                offset=args.offset,
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
