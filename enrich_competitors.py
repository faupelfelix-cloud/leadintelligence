#!/usr/bin/env python3
"""
Competitor Intelligence Enrichment Script

Enriches competitor CDMO profiles with comprehensive data:
- Company basics (employees, revenue, sites)
- Manufacturing capabilities (bioreactors, scales, technologies)
- Service offerings
- Market positioning
- Strengths/weaknesses analysis

Usage:
    python enrich_competitors.py                    # Enrich all unenriched
    python enrich_competitors.py --all              # Re-enrich all
    python enrich_competitors.py --company "Lonza"  # Enrich specific company
    python enrich_competitors.py --limit 5          # Limit to 5 companies
"""

import os
import sys
import yaml
import json
import time
import logging
import argparse
from datetime import datetime
from typing import Dict, List, Optional

import anthropic
from pyairtable import Api

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('competitor_enrichment.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# =============================================================================
# FIELD DEFINITIONS - For Airtable Setup Reference
# =============================================================================

COMPETITOR_FIELDS = """
COMPETITORS TABLE - RECOMMENDED FIELDS
======================================

BASIC INFO:
- Company Name (Single line text) - Required
- Website (URL)
- LinkedIn (URL)
- Headquarters (Single line text) - City, Country
- Founded Year (Number)
- Employees (Number) - Approximate headcount
- Revenue USD Millions (Number) - Annual revenue estimate
- Ownership (Single select): Public, Private, PE-Backed, Subsidiary

CLASSIFICATION:
- Ranking (Single select): Top Tier, Mid Tier, Emerging, Niche
- Threat Level (Single select): High, Medium, Low
- Pricing Tier (Single select): Premium, Mid-Market, Value

MANUFACTURING CAPACITY:
- Number of Sites (Number)
- Site Locations (Long text) - List of locations
- Number of Bioreactors (Number) - Total mammalian bioreactors
- Largest Bioreactor L (Number) - Largest single bioreactor in liters
- Mammalian Scales (Single line text) - e.g., "2L - 20,000L"
- Total Mammalian Capacity L (Number) - Sum of all bioreactor capacity
- Fill Finish Capability (Checkbox)
- Drug Product Capability (Checkbox)

SERVICES (Multi-select):
- Services Offered: 
  * Process Development
  * Cell Line Development
  * Analytical Development
  * Clinical Manufacturing
  * Commercial Manufacturing
  * Fill-Finish
  * Drug Product
  * Drug Substance
  * Formulation Development
  * Tech Transfer Support
  * Regulatory Support
  * Biosimilar Development

TECHNOLOGIES (Multi-select):
- Technologies:
  * Monoclonal Antibodies
  * Bispecific Antibodies
  * ADCs
  * Fusion Proteins
  * Viral Vectors
  * Cell Therapy
  * Gene Therapy
  * mRNA
  * Vaccines
  * Biosimilars
  * Recombinant Proteins

EXPRESSION SYSTEMS (Multi-select):
- Expression Systems:
  * CHO
  * HEK293
  * NS0
  * SP2/0
  * Microbial (E. coli)
  * Yeast
  * Insect Cells

CLIENT FOCUS (Multi-select):
- Client Focus:
  * Big Pharma
  * Mid-size Biotech
  * Emerging Biotech
  * Virtual Biotech
  * Academic/Research

GEOGRAPHIC PRESENCE (Multi-select):
- Geographic Presence:
  * North America
  * Europe
  * Asia Pacific
  * China
  * Japan
  * Global

STRATEGIC ANALYSIS:
- Primary Services (Long text) - Main offerings description
- Key Differentiators (Long text) - What makes them unique
- Market Positioning (Long text) - How they position themselves
- Strengths (Long text) - Competitive advantages
- Weaknesses (Long text) - Known limitations
- Recent Developments (Long text) - News, expansions, deals
- Competitive Notes (Long text) - General intelligence

METADATA:
- Enrichment Status (Single select): Not Enriched, Enriched, Needs Update
- Last Enriched (Date)
- Date Added (Date)
"""


class CompetitorEnricher:
    """Enriches competitor profiles with comprehensive CDMO intelligence"""
    
    def __init__(self, config_path: str = "config.yaml"):
        """Initialize with configuration"""
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        # Initialize APIs
        self.airtable = Api(self.config['airtable']['api_key'])
        self.base = self.airtable.base(self.config['airtable']['base_id'])
        
        # Competitors table
        self.competitors_table = self.base.table('Competitors')
        
        # Company Profile for context
        self.company_profile = self._load_company_profile()
        
        # Anthropic client
        self.anthropic_client = anthropic.Anthropic(
            api_key=self.config['anthropic']['api_key']
        )
        
        logger.info("CompetitorEnricher initialized")
    
    def _load_company_profile(self) -> Dict:
        """Load our company profile for competitive context"""
        try:
            table = self.base.table('Company Profile')
            records = table.all()
            if records:
                return records[0].get('fields', {})
        except:
            pass
        return {}
    
    def get_competitors_to_enrich(self, all_records: bool = False, 
                                   company_name: str = None,
                                   limit: int = None) -> List[Dict]:
        """Get competitors needing enrichment"""
        
        try:
            if company_name:
                # Specific company
                safe_name = company_name.replace("'", "\\'")
                records = self.competitors_table.all(
                    formula=f"LOWER({{Company Name}}) = LOWER('{safe_name}')"
                )
            elif all_records:
                # All competitors
                records = self.competitors_table.all()
            else:
                # Only unenriched
                records = self.competitors_table.all(
                    formula="OR({Enrichment Status}='Not Enriched', {Enrichment Status}=BLANK())"
                )
            
            competitors = []
            for r in records:
                competitors.append({
                    'id': r['id'],
                    'fields': r['fields']
                })
            
            if limit:
                competitors = competitors[:limit]
            
            return competitors
            
        except Exception as e:
            logger.error(f"Error fetching competitors: {e}")
            return []
    
    def enrich_competitor(self, competitor: Dict) -> Dict:
        """Enrich a single competitor with AI-powered research"""
        
        company_name = competitor['fields'].get('Company Name', '')
        existing_info = competitor['fields']
        
        # Build context from existing data
        existing_context = ""
        if existing_info.get('Primary Services'):
            existing_context += f"Known services: {existing_info['Primary Services']}\n"
        if existing_info.get('Ranking'):
            existing_context += f"Tier: {existing_info['Ranking']}\n"
        
        # Our company context for competitive comparison
        our_context = ""
        if self.company_profile:
            our_context = f"""
OUR COMPANY (for competitive comparison):
- Capabilities: {self.company_profile.get('Capabilities', 'European biologics CDMO')}
- Strengths: {self.company_profile.get('Strengths', 'Mammalian cell culture')}
- Focus: {self.company_profile.get('Market Positioning', 'Mid-size biotech clients')}
"""
        
        prompt = f"""Research this CDMO/CMO company comprehensively for competitive intelligence:

COMPANY: {company_name}
{existing_context}
{our_context}

RESEARCH AND PROVIDE:

1. COMPANY BASICS:
   - Website URL
   - LinkedIn company page URL
   - Headquarters (city, country)
   - Founded year
   - Approximate employees
   - Estimated annual revenue (USD millions)
   - Ownership type (Public/Private/PE-Backed/Subsidiary)

2. MANUFACTURING CAPACITY:
   - Number of manufacturing sites
   - Site locations (list cities/countries)
   - Total number of mammalian bioreactors
   - Largest single bioreactor size (liters)
   - Range of mammalian scales available (e.g., "2L - 20,000L")
   - Estimated total mammalian capacity (liters)
   - Has fill-finish capability? (yes/no)
   - Has drug product capability? (yes/no)

3. SERVICES OFFERED (select all that apply):
   - Process Development
   - Cell Line Development
   - Analytical Development
   - Clinical Manufacturing
   - Commercial Manufacturing
   - Fill-Finish
   - Drug Product
   - Drug Substance
   - Formulation Development
   - Tech Transfer Support
   - Regulatory Support
   - Biosimilar Development

4. TECHNOLOGIES (select all that apply):
   - Monoclonal Antibodies
   - Bispecific Antibodies
   - ADCs (Antibody-Drug Conjugates)
   - Fusion Proteins
   - Viral Vectors
   - Cell Therapy
   - Gene Therapy
   - mRNA
   - Vaccines
   - Biosimilars
   - Recombinant Proteins

5. EXPRESSION SYSTEMS (select all that apply):
   - CHO
   - HEK293
   - NS0
   - SP2/0
   - Microbial (E. coli)
   - Yeast
   - Insect Cells

6. CLIENT FOCUS (select all that apply):
   - Big Pharma
   - Mid-size Biotech
   - Emerging Biotech
   - Virtual Biotech
   - Academic/Research

7. GEOGRAPHIC PRESENCE (select all that apply):
   - North America
   - Europe
   - Asia Pacific
   - China
   - Japan
   - Global

8. COMPETITIVE ANALYSIS:
   - Market positioning (how do they position themselves?)
   - Key differentiators (what makes them unique?)
   - Strengths (competitive advantages)
   - Weaknesses (known limitations, gaps)
   - Pricing tier (Premium/Mid-Market/Value)
   - Threat level to us (High/Medium/Low) - based on overlap with our services

9. RECENT DEVELOPMENTS:
   - Any recent expansions, acquisitions, partnerships, or major news

Return ONLY valid JSON:
{{
    "website": "https://...",
    "linkedin": "https://linkedin.com/company/...",
    "headquarters": "City, Country",
    "founded_year": 1990,
    "employees": 5000,
    "revenue_usd_millions": 500,
    "ownership": "Public|Private|PE-Backed|Subsidiary",
    
    "number_of_sites": 5,
    "site_locations": "Basel (CH), Portsmouth (NH), Slough (UK), Singapore, Guangzhou (CN)",
    "number_of_bioreactors": 50,
    "largest_bioreactor_l": 20000,
    "mammalian_scales": "2L - 20,000L",
    "total_mammalian_capacity_l": 500000,
    "has_fill_finish": true,
    "has_drug_product": true,
    
    "services_offered": ["Process Development", "Cell Line Development", "Clinical Manufacturing", "Commercial Manufacturing"],
    "technologies": ["Monoclonal Antibodies", "Bispecific Antibodies", "ADCs"],
    "expression_systems": ["CHO", "HEK293"],
    "client_focus": ["Big Pharma", "Mid-size Biotech"],
    "geographic_presence": ["North America", "Europe", "Asia Pacific"],
    
    "market_positioning": "Premium full-service CDMO with global scale...",
    "key_differentiators": "Largest mammalian capacity globally, integrated drug substance and drug product...",
    "strengths": "Scale, global footprint, regulatory track record, broad technology platform...",
    "weaknesses": "Higher pricing, longer lead times, less flexible for small projects...",
    "pricing_tier": "Premium|Mid-Market|Value",
    "threat_level": "High|Medium|Low",
    
    "recent_developments": "2024: Announced $500M expansion in Singapore...",
    
    "ranking": "Top Tier|Mid Tier|Emerging|Niche",
    "confidence": "high|medium|low"
}}

Be thorough and specific. Use actual data where available, estimates where necessary.
Return ONLY the JSON object."""

        try:
            message = self.anthropic_client.messages.create(
                model=self.config['anthropic']['model'],
                max_tokens=4000,
                tools=[{"type": "web_search_20250305", "name": "web_search"}],
                messages=[{"role": "user", "content": prompt}]
            )
            
            # Extract text from response
            response_text = ""
            for block in message.content:
                if hasattr(block, 'text'):
                    response_text += block.text
            
            # Parse JSON
            if "```json" in response_text:
                json_str = response_text.split("```json")[1].split("```")[0]
            elif "```" in response_text:
                json_str = response_text.split("```")[1].split("```")[0]
            else:
                json_str = response_text
            
            # Clean and parse
            json_str = json_str.strip()
            if not json_str.startswith("{"):
                start = json_str.find("{")
                end = json_str.rfind("}") + 1
                if start >= 0 and end > start:
                    json_str = json_str[start:end]
            
            return json.loads(json_str)
            
        except Exception as e:
            logger.error(f"Error enriching {company_name}: {e}")
            return {'error': str(e)}
    
    def update_competitor_record(self, record_id: str, enrichment_data: Dict) -> bool:
        """Update competitor record in Airtable with smart field handling"""
        
        if enrichment_data.get('error'):
            logger.error(f"Skipping update due to enrichment error")
            return False
        
        # ═══════════════════════════════════════════════════════════════
        # FIELD MAPPINGS - Map AI responses to exact Airtable options
        # ═══════════════════════════════════════════════════════════════
        
        ownership_mapping = {
            'public': 'Public',
            'private': 'Private',
            'pe-backed': 'PE-Backed',
            'pe backed': 'PE-Backed',
            'subsidiary': 'Subsidiary',
            'private (subsidiary)': 'Subsidiary',
        }
        
        threat_mapping = {
            'high': 'High',
            'medium': 'Medium',
            'low': 'Low',
        }
        
        pricing_mapping = {
            'premium': 'Premium',
            'mid-market': 'Mid-Market',
            'mid market': 'Mid-Market',
            'value': 'Value',
        }
        
        ranking_mapping = {
            'top tier': 'Top Tier',
            'mid tier': 'Mid Tier',
            'emerging': 'Emerging',
            'niche': 'Niche',
        }
        
        # Valid multi-select options (add "Other" as fallback)
        valid_services = [
            'Process Development', 'Cell Line Development', 'Analytical Development',
            'Clinical Manufacturing', 'Commercial Manufacturing', 'Fill-Finish',
            'Drug Product', 'Drug Substance', 'Formulation Development',
            'Tech Transfer Support', 'Regulatory Support', 'Biosimilar Development', 'Other'
        ]
        
        valid_technologies = [
            'Monoclonal Antibodies', 'Bispecific Antibodies', 'ADCs', 'Fusion Proteins',
            'Viral Vectors', 'Cell Therapy', 'Gene Therapy', 'mRNA', 'Vaccines',
            'Biosimilars', 'Recombinant Proteins', 'Other'
        ]
        
        valid_expression_systems = [
            'CHO', 'HEK293', 'NS0', 'SP2/0', 'Microbial (E. coli)', 'Yeast', 'Insect Cells', 'Other'
        ]
        
        valid_client_focus = [
            'Big Pharma', 'Mid-size Biotech', 'Emerging Biotech', 'Virtual Biotech', 'Academic/Research'
        ]
        
        valid_geographic = [
            'North America', 'Europe', 'Asia Pacific', 'China', 'Japan', 'Global'
        ]
        
        def map_single_select(value, mapping):
            """Map a value to valid Airtable option"""
            if not value:
                return None
            value_lower = value.lower().strip()
            return mapping.get(value_lower)
        
        def filter_multi_select(values, valid_options):
            """Filter list to only valid options, use 'Other' for unknowns"""
            if not values:
                return None
            filtered = []
            has_unknown = False
            for v in values:
                if v in valid_options:
                    filtered.append(v)
                else:
                    has_unknown = True
            if has_unknown and 'Other' in valid_options and 'Other' not in filtered:
                filtered.append('Other')
            return filtered if filtered else None
        
        # ═══════════════════════════════════════════════════════════════
        # BUILD UPDATE DATA - Separate safe fields from risky ones
        # ═══════════════════════════════════════════════════════════════
        
        # SAFE FIELDS - These almost never fail
        safe_data = {
            'Enrichment Status': 'Enriched',
            'Last Enriched': datetime.now().strftime('%Y-%m-%d')
        }
        
        # Basic info (URLs and text)
        if enrichment_data.get('website'):
            safe_data['Website'] = enrichment_data['website']
        if enrichment_data.get('linkedin'):
            safe_data['LinkedIn'] = enrichment_data['linkedin']
        if enrichment_data.get('headquarters'):
            safe_data['Headquarters'] = enrichment_data['headquarters']
        
        # Number fields
        if enrichment_data.get('founded_year'):
            safe_data['Founded Year'] = enrichment_data['founded_year']
        if enrichment_data.get('employees'):
            safe_data['Employees'] = enrichment_data['employees']
        if enrichment_data.get('revenue_usd_millions'):
            safe_data['Revenue USD Millions'] = enrichment_data['revenue_usd_millions']
        if enrichment_data.get('number_of_sites'):
            safe_data['Number of Sites'] = enrichment_data['number_of_sites']
        if enrichment_data.get('site_locations'):
            safe_data['Site Locations'] = enrichment_data['site_locations']
        if enrichment_data.get('number_of_bioreactors'):
            safe_data['Number of Bioreactors'] = enrichment_data['number_of_bioreactors']
        if enrichment_data.get('largest_bioreactor_l'):
            safe_data['Largest Bioreactor L'] = enrichment_data['largest_bioreactor_l']
        if enrichment_data.get('mammalian_scales'):
            safe_data['Mammalian Scales'] = enrichment_data['mammalian_scales']
        if enrichment_data.get('total_mammalian_capacity_l'):
            safe_data['Total Mammalian Capacity L'] = enrichment_data['total_mammalian_capacity_l']
        
        # Checkboxes
        if enrichment_data.get('has_fill_finish') is not None:
            safe_data['Fill Finish Capability'] = enrichment_data['has_fill_finish']
        if enrichment_data.get('has_drug_product') is not None:
            safe_data['Drug Product Capability'] = enrichment_data['has_drug_product']
        
        # Long text fields
        if enrichment_data.get('market_positioning'):
            safe_data['Market Positioning'] = enrichment_data['market_positioning']
        if enrichment_data.get('key_differentiators'):
            safe_data['Key Differentiators'] = enrichment_data['key_differentiators']
        if enrichment_data.get('strengths'):
            safe_data['Strengths'] = enrichment_data['strengths']
        if enrichment_data.get('weaknesses'):
            safe_data['Weaknesses'] = enrichment_data['weaknesses']
        if enrichment_data.get('recent_developments'):
            safe_data['Recent Developments'] = enrichment_data['recent_developments']
        
        # SINGLE SELECT FIELDS - Map to exact values
        ownership = map_single_select(enrichment_data.get('ownership'), ownership_mapping)
        if ownership:
            safe_data['Ownership'] = ownership
        
        threat_level = map_single_select(enrichment_data.get('threat_level'), threat_mapping)
        if threat_level:
            safe_data['Threat Level'] = threat_level
        
        pricing_tier = map_single_select(enrichment_data.get('pricing_tier'), pricing_mapping)
        if pricing_tier:
            safe_data['Pricing Tier'] = pricing_tier
        
        ranking = map_single_select(enrichment_data.get('ranking'), ranking_mapping)
        if ranking:
            safe_data['Ranking'] = ranking
        
        # RISKY FIELDS - Multi-selects that might fail
        risky_data = {}
        
        services = filter_multi_select(enrichment_data.get('services_offered'), valid_services)
        if services:
            risky_data['Services Offered'] = services
        
        technologies = filter_multi_select(enrichment_data.get('technologies'), valid_technologies)
        if technologies:
            risky_data['Technologies'] = technologies
        
        expression_systems = filter_multi_select(enrichment_data.get('expression_systems'), valid_expression_systems)
        if expression_systems:
            risky_data['Expression Systems'] = expression_systems
        
        client_focus = filter_multi_select(enrichment_data.get('client_focus'), valid_client_focus)
        if client_focus:
            risky_data['Client Focus'] = client_focus
        
        geographic = filter_multi_select(enrichment_data.get('geographic_presence'), valid_geographic)
        if geographic:
            risky_data['Geographic Presence'] = geographic
        
        # ═══════════════════════════════════════════════════════════════
        # UPDATE STRATEGY: Safe fields first, then try risky ones
        # ═══════════════════════════════════════════════════════════════
        
        try:
            # First, always update safe fields
            self.competitors_table.update(record_id, safe_data)
            logger.debug(f"  Safe fields updated successfully")
            
            # Then try risky fields one by one
            for field_name, field_value in risky_data.items():
                try:
                    self.competitors_table.update(record_id, {field_name: field_value})
                    logger.debug(f"  {field_name} updated")
                except Exception as e:
                    if 'INVALID_MULTIPLE_CHOICE_OPTIONS' in str(e):
                        logger.warning(f"  Skipped {field_name} - some options not in Airtable")
                    else:
                        logger.warning(f"  Failed to update {field_name}: {e}")
            
            return True
            
        except Exception as e:
            logger.error(f"  Failed to update safe fields: {e}")
            return False
    
    def enrich_all(self, all_records: bool = False, company_name: str = None, 
                   limit: int = None) -> Dict:
        """Enrich multiple competitors"""
        
        logger.info("="*60)
        logger.info("COMPETITOR ENRICHMENT")
        logger.info("="*60)
        
        competitors = self.get_competitors_to_enrich(all_records, company_name, limit)
        
        if not competitors:
            logger.info("No competitors to enrich")
            return {'total': 0, 'enriched': 0, 'failed': 0}
        
        logger.info(f"Found {len(competitors)} competitors to enrich")
        
        stats = {'total': len(competitors), 'enriched': 0, 'failed': 0}
        
        for idx, competitor in enumerate(competitors, 1):
            name = competitor['fields'].get('Company Name', 'Unknown')
            logger.info(f"\n[{idx}/{len(competitors)}] Enriching: {name}")
            
            # Enrich with AI
            enrichment_data = self.enrich_competitor(competitor)
            
            if enrichment_data.get('error'):
                logger.error(f"  ✗ Enrichment failed: {enrichment_data['error']}")
                stats['failed'] += 1
                continue
            
            # Update record
            success = self.update_competitor_record(competitor['id'], enrichment_data)
            
            if success:
                logger.info(f"  ✓ Enriched successfully")
                # Log key findings
                if enrichment_data.get('employees'):
                    logger.info(f"    Employees: ~{enrichment_data['employees']:,}")
                if enrichment_data.get('number_of_sites'):
                    logger.info(f"    Sites: {enrichment_data['number_of_sites']}")
                if enrichment_data.get('threat_level'):
                    logger.info(f"    Threat Level: {enrichment_data['threat_level']}")
                stats['enriched'] += 1
            else:
                stats['failed'] += 1
            
            # Rate limiting
            time.sleep(2)
        
        # Summary
        logger.info("\n" + "="*60)
        logger.info("ENRICHMENT COMPLETE")
        logger.info("="*60)
        logger.info(f"Total competitors: {stats['total']}")
        logger.info(f"Successfully enriched: {stats['enriched']}")
        logger.info(f"Failed: {stats['failed']}")
        
        return stats


def print_field_guide():
    """Print field setup guide"""
    print(COMPETITOR_FIELDS)


def main():
    parser = argparse.ArgumentParser(description='Competitor Intelligence Enrichment')
    parser.add_argument('--config', default='config.yaml', help='Config file path')
    parser.add_argument('--all', action='store_true', help='Re-enrich all competitors')
    parser.add_argument('--company', type=str, help='Enrich specific company by name')
    parser.add_argument('--limit', type=int, help='Limit number of companies to enrich')
    parser.add_argument('--fields', action='store_true', help='Print field setup guide')
    
    args = parser.parse_args()
    
    if args.fields:
        print_field_guide()
        return
    
    try:
        enricher = CompetitorEnricher(args.config)
        enricher.enrich_all(
            all_records=args.all,
            company_name=args.company,
            limit=args.limit
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
