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

# Configure logging FIRST (before using logger)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('conference_intelligence.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Import fuzzy matching utilities
try:
    from fuzzy_match import normalize_company_name, normalize_lead_name, similarity_score, check_company_alias
    HAS_FUZZY_MATCH = True
    logger.info("✓ Fuzzy matching module loaded successfully")
except ImportError as e:
    HAS_FUZZY_MATCH = False
    logger.warning(f"⚠ Fuzzy matching not available: {e}")
    normalize_company_name = lambda x: x.lower().strip() if x else ""
    normalize_lead_name = lambda x: x.lower().strip() if x else ""
    similarity_score = lambda x, y, f: 1.0 if f(x) == f(y) else 0.0
    check_company_alias = lambda x, y: False


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
        
        # Initialize dynamic ICP scorer
        from complete_icp_scorer import CompleteICPScorer
        try:
            self.icp_scorer = CompleteICPScorer(self.config)
            logger.info(f"✓ ICP Scorer loaded: {len(self.icp_scorer.criteria)} criteria, {self.icp_scorer.get_total_score()} points total")
        except Exception as e:
            logger.warning(f"Could not load ICP scorer: {str(e)}. Using fallback scoring.")
            self.icp_scorer = None
        
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

SEARCH THESE SOURCES (in order of priority):

1. OFFICIAL CONFERENCE SOURCES:
   - Conference website speaker list / agenda
   - Conference program PDF
   - Exhibitor list on conference website
   - Conference press releases

2. LINKEDIN (critical source!):
   - Search: "{conference_name}" site:linkedin.com
   - Search: "attending {conference_name}" site:linkedin.com
   - Search: "speaking at {conference_name}" site:linkedin.com
   - Look for posts with conference hashtag mentions
   - People who updated their profiles mentioning the conference

3. TWITTER/X:
   - Search: #{conference_name.replace(' ', '')} (conference hashtag)
   - Search: "{conference_name}" from:biotech OR from:pharma
   - Company announcements about attending

4. COMPANY PRESS RELEASES:
   - "[Company] to present at {conference_name}"
   - "[Company] attending {conference_name}"
   - Search company investor relations pages

5. INDUSTRY NEWS:
   - Endpoints News, Fierce Biotech, BioPharma Dive
   - Conference preview articles
   - "Companies to watch at {conference_name}"

WHO TO FIND (our ideal customer profile):
- People from BIOTECH/PHARMA companies developing therapeutics
- Companies focused on BIOLOGICS: mAbs, bispecifics, ADCs, fusion proteins, biosimilars
- Decision-maker titles: CEO, COO, CSO, CTO, VP, SVP, Head of, Director
- Functions: Manufacturing, Operations, CMC, Supply Chain, Procurement, Business Development

WHO TO EXCLUDE (not our customers):
- Cell & gene therapy companies (CAR-T, gene therapy) - NOT our technology
- Technology/equipment providers (Sartorius, Cytiva, etc.) - They sell TO us
- Small molecule only companies
- Diagnostics/medical devices only
- Pure service providers, consultants
- Junior roles (Associate, Manager, Scientist, Researcher)
- HR, Marketing, Legal (unless C-level)

For each person found, provide:
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
  "sources_checked": ["List of sources you searched"],
  "notes": "Any relevant notes about search results"
}}

CRITICAL INSTRUCTIONS:
1. ALWAYS return valid JSON in the format above - no exceptions
2. If you find NO attendees, return: {{"attendees": [], "total_found": 0, "sources_checked": [...], "notes": "reason why no attendees found"}}
3. Do NOT write explanatory text before or after the JSON
4. Do NOT say "I couldn't find" - just return empty attendees array with notes

Search thoroughly across all sources and return all relevant people you find."""

        max_retries = 3
        retry_delay = 5  # seconds
        
        for attempt in range(max_retries + 1):
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
                
                data = json.loads(json_str)
                attendees = data.get('attendees', [])
                
                if attendees:
                    logger.info(f"  Found {len(attendees)} potential attendees")
                else:
                    logger.warning(f"  No attendees in parsed JSON")
                    # Show notes if available (explains why no attendees)
                    notes = data.get('notes', '')
                    if notes:
                        logger.info(f"  Search notes: {notes}")
                    sources = data.get('sources_checked', [])
                    if sources:
                        logger.info(f"  Sources checked: {', '.join(sources[:5])}")
                    else:
                        # Log a sample of the response for debugging
                        logger.info(f"  Response sample: {result_text[:200]}...")
                
                return attendees
                
            except json.JSONDecodeError as e:
                logger.error(f"  JSON parse error: {str(e)}")
                if json_str:
                    logger.info(f"  Attempted to parse: {json_str[:200]}...")
                return []
            
            except Exception as e:
                error_str = str(e)
                # Check if it's a retryable error (500, 529, 503, etc.)
                if any(code in error_str for code in ['500', '529', '503', '502', 'overloaded', 'Internal server error']):
                    if attempt < max_retries:
                        logger.warning(f"  API error, retrying in {retry_delay}s ({attempt + 1}/{max_retries})...")
                        import time
                        time.sleep(retry_delay)
                        retry_delay *= 2  # Exponential backoff
                        continue
                    else:
                        logger.error(f"  API error after {max_retries} retries: {error_str}")
                        return []
                else:
                    logger.error(f"  Error searching for attendees: {error_str}")
                    return []
        
        # If we get here, all retries failed
        return []
    
    def quick_company_icp_with_pharma_flag(self, company_name: str) -> tuple:
        """
        Quick ICP assessment that also returns whether company is big pharma
        Returns: (icp_score, is_big_pharma)
        """
        
        # Use dynamic scorer if available
        if self.icp_scorer:
            try:
                score, breakdown = self.icp_scorer.score_company(company_name)
                
                # Ensure score is an integer
                if not isinstance(score, (int, float)):
                    logger.warning(f"    Invalid score type: {type(score)}, defaulting to 0")
                    score = 0
                else:
                    score = int(score)
                
                # Check if big pharma
                is_big_pharma = breakdown.get('is_big_pharma', False)
                
                logger.info(f"    Quick ICP for {company_name}: {score}")
                if breakdown.get('is_competitor'):
                    logger.info(f"    → CDMO Competitor (excluded)")
                if is_big_pharma:
                    logger.info(f"    → Big Pharma (lower threshold applied)")
                    
                return score, is_big_pharma
            except Exception as e:
                logger.error(f"    Error with dynamic ICP scorer: {str(e)}")
                logger.info(f"    Falling back to hardcoded scoring")
        
        # Fallback: use old method and check company name for big pharma
        score = self.quick_company_icp(company_name)
        
        # Simple big pharma detection
        big_pharma_names = [
            'pfizer', 'sanofi', 'novartis', 'roche', 'merck', 'msd', 'astrazeneca',
            'johnson & johnson', 'j&j', 'abbvie', 'bristol-myers', 'bms', 'eli lilly',
            'lilly', 'gsk', 'glaxosmithkline', 'takeda', 'daiichi', 'astellas',
            'bayer', 'boehringer', 'amgen', 'gilead', 'regeneron', 'biogen'
        ]
        is_big_pharma = any(bp in company_name.lower() for bp in big_pharma_names)
        
        return score, is_big_pharma
    
    def quick_company_icp(self, company_name: str) -> int:
        """
        Quick ICP assessment for unknown company
        Uses dynamic ICP scorer if available, otherwise fallback to hardcoded
        """
        
        # Use dynamic scorer if available
        if self.icp_scorer:
            try:
                score, breakdown = self.icp_scorer.score_company(company_name)
                
                # Ensure score is an integer
                if not isinstance(score, (int, float)):
                    logger.warning(f"    Invalid score type: {type(score)}, defaulting to 0")
                    score = 0
                else:
                    score = int(score)
                
                logger.info(f"    Quick ICP for {company_name}: {score}")
                if breakdown.get('is_competitor'):
                    logger.info(f"    → CDMO Competitor (excluded)")
                return score
            except Exception as e:
                logger.error(f"    Error with dynamic ICP scorer: {str(e)}")
                logger.info(f"    Falling back to hardcoded scoring")
        
        # Fallback: Use hardcoded prompt (your improved version)
        prompt = f"""Quick ICP assessment for: {company_name}

You are assessing fit for Rezon Bio, a European biologics CDMO specializing in mammalian cell culture (mAbs, bispecifics, ADCs, biosimilars).

CRITICAL EXCLUSIONS - Automatic ICP 0 (DO NOT partner with):
- CDMOs/CMOs (competitors): Fujifilm Diosynth, Lonza, Samsung Biologics, Thermo Fisher Biologics, WuXi, Catalent Biologics, etc.
- Contract manufacturers offering similar services
- Pure service providers (CROs without manufacturing needs)

IDEAL CUSTOMERS - High ICP (75-100):
- Biosimilar developers (we have biosimilar expertise!)
- Mid-size biotechs (50-1000 employees) with biologics programs
- Pharma companies with biologics pipelines (any size if biologics focus)
- Biotechs in Phase 2/3 or commercial stage
- Companies with mAbs, bispecifics, ADCs, fusion proteins
- European companies (preferred but not required)

SCORING CRITERIA (0-100):

1. COMPANY TYPE (Critical - can be 0 or high):
   - CDMO/CMO competitor = 0 (automatic exclusion)
   - Biosimilar developer = 30 pts (HIGH value!)
   - Biotech with biologics = 30 pts
   - Pharma with biologics = 25 pts
   - Academic/research only = 0 pts

2. FOCUS AREA (0-25):
   - Pure biologics (mAbs, bispecifics, ADCs) = 25 pts
   - Biosimilars = 25 pts (we have expertise!)
   - Biologics + cell/gene therapy = 20 pts
   - Biologics + small molecules = 15 pts
   - No biologics = 0 pts

3. DEVELOPMENT STAGE (0-20):
   - Commercial or Phase 3 = 20 pts
   - Phase 2 = 18 pts
   - Phase 1 = 12 pts
   - Preclinical with funding = 8 pts
   - Research only = 0 pts

4. COMPANY SIZE (0-15):
   - 50-500 employees = 15 pts (sweet spot)
   - 500-1000 employees = 12 pts
   - 20-50 employees = 10 pts (smaller but viable)
   - >1000 employees = 8 pts (big pharma - still good if biologics)
   - <20 employees = 5 pts (too small)

5. GEOGRAPHIC LOCATION (0-10):
   - Europe (Germany, Switzerland, UK, France, etc.) = 10 pts
   - US = 8 pts
   - Other = 5 pts

EXAMPLES:
- BioMarin (large pharma with biologics) = 85-90 (biologics focus, commercial, big pharma)
- Sandoz (biosimilars) = 90-95 (biosimilar expertise match!)
- Sanofi (large pharma with biologics) = 80-85 (size, biologics programs)
- MSD/Merck (large pharma with biologics) = 80-85 (if biologics division)
- Small biotech (30 employees, Phase 1 biologics) = 65-70 (small but biologics)
- Fujifilm Diosynth = 0 (CDMO competitor)
- Thermo Fisher Biologics = 0 (CDMO competitor)

Search for {company_name} and return JSON:
{{
  "company_type": "Biotech/Pharma/Biosimilar/CDMO",
  "focus_area": "Biologics/Biosimilars/Mixed/Other",
  "stage": "Commercial/Phase 3/Phase 2/etc",
  "size": "X employees",
  "location": "Country",
  "is_competitor": true/false,
  "icp_score": 85,
  "reasoning": "Brief explanation of score"
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
            
            # Parse JSON - handle multiple formats (same as search_conference_attendees)
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
                logger.warning(f"    No JSON found in ICP response for {company_name}")
                return 0
            
            try:
                data = json.loads(json_str)
                icp_score = data.get('icp_score', 0)
                
                logger.info(f"    Quick ICP for {company_name}: {icp_score}")
                return icp_score
            except json.JSONDecodeError as e:
                logger.error(f"    JSON parse error for {company_name}: {str(e)}")
                logger.debug(f"    Attempted to parse: {json_str[:150]}...")
                return 0
            
        except Exception as e:
            logger.error(f"    Error assessing ICP for {company_name}: {str(e)}")
            return 0
    
    def find_company(self, company_name: str) -> Optional[Dict]:
        """Find company in Companies table with fuzzy matching"""
        try:
            # First try exact match
            safe_name = company_name.replace("'", "\\'")
            formula = f"{{Company Name}} = '{safe_name}'"
            records = self.companies_table.all(formula=formula)
            
            if records:
                logger.debug(f"    Exact match found: {company_name}")
                return records[0]
            
            # Try normalized exact match (e.g., "Sandoz" matches "Sandoz Group")
            if HAS_FUZZY_MATCH:
                norm_query = normalize_company_name(company_name)
                all_companies = self.companies_table.all()
                
                # First pass: look for exact normalized match
                for record in all_companies:
                    existing_name = record['fields'].get('Company Name', '')
                    if normalize_company_name(existing_name) == norm_query:
                        logger.info(f"    Normalized match: '{company_name}' -> '{existing_name}'")
                        return record
                
                # Second pass: fuzzy match
                best_match = None
                best_score = 0.0
                
                for record in all_companies:
                    existing_name = record['fields'].get('Company Name', '')
                    score = similarity_score(company_name, existing_name, normalize_company_name)
                    
                    if score > best_score:
                        best_score = score
                        best_match = record
                
                # If good fuzzy match (>= 85%)
                if best_score >= 0.85 and best_match:
                    matched_name = best_match['fields'].get('Company Name', '')
                    logger.info(f"    Fuzzy matched company '{company_name}' -> '{matched_name}' (score: {best_score:.2f})")
                    return best_match
                
                logger.debug(f"    No match for '{company_name}' (best score: {best_score:.2f})")
            
            return None
        except Exception as e:
            logger.warning(f"    Error in find_company for '{company_name}': {e}")
            return None
    
    def create_company(self, company_name: str, icp_score: int) -> Dict:
        """Create a new company record and enrich it immediately"""
        try:
            company_data = {
                'Company Name': company_name,
                'ICP Fit Score': icp_score,
                'Enrichment Status': 'Not Enriched'
            }
            
            record = self.companies_table.create(company_data)
            company_id = record['id']
            logger.info(f"    ✓ Created company: {company_name} (ICP: {icp_score})")
            
            # ENRICH IMMEDIATELY - inline enrichment
            logger.info(f"    Enriching company...")
            enriched = self._enrich_company_inline(company_id, company_name)
            
            if enriched:
                # Update ICP score if enrichment found a better one
                new_icp = enriched.get('icp_score')
                if new_icp:
                    logger.info(f"    ✓ Company enriched - ICP updated to: {new_icp}")
                else:
                    logger.info(f"    ✓ Company enriched")
            else:
                logger.warning(f"    ⚠ Could not enrich company")
            
            # Refresh the record to get updated data
            record = self.companies_table.get(company_id)
            return record
            
        except Exception as e:
            logger.error(f"    ✗ Failed to create company {company_name}: {str(e)}")
            return None
    
    def _enrich_company_inline(self, company_id: str, company_name: str) -> Optional[Dict]:
        """Enrich company with web search - inline implementation"""
        
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

═══════════════════════════════════════════════════════════
CRITICAL RULES — READ BEFORE RESEARCHING:
═══════════════════════════════════════════════════════════
1. ONLY report facts you can verify from web search results. If you cannot find a specific data point, return null — do NOT guess or infer.
2. DISAMBIGUATION: If "{company_name}" matches multiple companies, pick the biotech/pharma one. If ambiguous, note in disambiguation_note.
3. FUNDING: Only report funding from credible sources. If not found, return null. NEVER guess amounts.
4. PIPELINE STAGE: Only report stages explicitly stated. If unclear, use "Unknown".
5. THERAPEUTIC AREAS: Only include areas explicitly mentioned in company materials or credible news.
6. CDMO PARTNERSHIPS: Only report if confirmed. "None found" is valid.
7. For every key field, include confidence: "high" (multiple sources), "medium" (single source), "low" (inferred), "unverified" (not found).

Find the following (return null for anything you CANNOT verify):
1. Website URL
2. LinkedIn company page URL
3. Headquarters location (city, country)
4. Company size — MUST be one of: {', '.join(VALID_COMPANY_SIZE)}, or null
5. Focus areas — MUST be from: {', '.join(VALID_FOCUS_AREAS)}, or empty list
6. Technology platform — MUST be from: {', '.join(VALID_TECH_PLATFORMS)}, or empty list
7. Funding stage — MUST be one of: {', '.join(VALID_FUNDING_STAGES)}
8. Total funding raised (USD) — ONLY if found, otherwise null
9. Latest funding round — ONLY if found, otherwise null
10. Pipeline stages — MUST be from: {', '.join(VALID_PIPELINE_STAGES)}
11. Lead programs/products
12. Therapeutic areas — MUST be from: {', '.join(VALID_THERAPEUTIC_AREAS)}, or empty list
13. Current CDMO partnerships — ONLY if confirmed, otherwise "None found"
14. Manufacturing status — MUST be one of: {', '.join(VALID_MANUFACTURING_STATUS)}
15. Recent news or developments
16. ICP Score (0-90) with justification — score 0 for CDMO/CMO/service providers
17. Urgency Score (0-100)

Return ONLY valid JSON:
{{
    "website": "https://... or null",
    "linkedin_company_page": "https://linkedin.com/company/... or null",
    "location": "City, Country",
    "company_size": "51-200",
    "focus_areas": ["mAbs"],
    "technology_platforms": ["Mammalian CHO"],
    "funding_stage": "Series B",
    "total_funding_usd": 75000000,
    "latest_funding_round": "Series B - $50M - Jan 2024 or null",
    "pipeline_stages": ["Phase 2"],
    "lead_programs": "Program description or null",
    "therapeutic_areas": ["Oncology"],
    "cdmo_partnerships": "Partner name or None found",
    "manufacturing_status": "No Public Partner",
    "recent_news": "Recent news or null",
    "icp_score": 65,
    "icp_justification": "Justification text",
    "urgency_score": 75,
    "disambiguation_note": "null or explanation if ambiguous",
    "data_confidence": {{
        "funding": "high|medium|low|unverified",
        "pipeline": "high|medium|low|unverified",
        "therapeutic_areas": "high|medium|low|unverified",
        "cdmo_partnerships": "high|medium|low|unverified"
    }}
}}

Return ONLY JSON."""

        try:
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
                return None
            
            data = json.loads(json_str.strip())
            
            # Helper function to validate single select
            def validate_single(value, valid_options, default='Unknown'):
                if not value:
                    return default
                if value in valid_options:
                    return value
                for opt in valid_options:
                    if opt.lower() == value.lower():
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
            
            # Intelligence Notes with confidence
            data_confidence = data.get('data_confidence', {})
            disambiguation = data.get('disambiguation_note')
            notes_parts = []
            if data.get('recent_news'):
                notes_parts.append(f"Recent News: {data['recent_news'][:500]}")
            if data_confidence:
                low_conf = [f"⚠ {k}: {v}" for k, v in data_confidence.items() if v in ('low', 'unverified')]
                if low_conf:
                    notes_parts.append("Data Confidence Warnings:\n" + "\n".join(low_conf))
            if disambiguation:
                notes_parts.append(f"Disambiguation: {disambiguation}")
            if notes_parts:
                update_fields['Intelligence Notes'] = "Discovered via Conference Intelligence\n\n" + "\n\n".join(notes_parts)
            
            # Store raw confidence for downstream use
            if data_confidence:
                try:
                    update_fields['Data Confidence'] = json.dumps(data_confidence)
                except:
                    pass
            
            # Scores and justifications
            if data.get('icp_score'):
                update_fields['ICP Fit Score'] = min(max(int(data['icp_score']), 0), 90)
            if data.get('icp_justification'):
                update_fields['ICP Score Justification'] = data['icp_justification']
            if data.get('urgency_score'):
                update_fields['Urgency Score'] = min(max(int(data['urgency_score']), 0), 100)
            
            # Update record
            try:
                self.companies_table.update(company_id, update_fields)
            except Exception as e:
                logger.debug(f"Full company update failed: {e}")
                # Try with minimal fields
                minimal = {
                    'Enrichment Status': 'Enriched',
                    'Last Intelligence Check': datetime.now().strftime('%Y-%m-%d')
                }
                if data.get('website'):
                    minimal['Website'] = data['website']
                if data.get('location'):
                    minimal['Location/HQ'] = data['location']
                if data.get('icp_score'):
                    minimal['ICP Fit Score'] = min(max(int(data['icp_score']), 0), 90)
                try:
                    self.companies_table.update(company_id, minimal)
                except:
                    self.companies_table.update(company_id, {'Enrichment Status': 'Enriched'})
            
            return data
            
        except Exception as e:
            logger.error(f"    Error in inline company enrichment: {e}")
            return None
    
    def find_lead(self, name: str, company_name: str) -> Optional[Dict]:
        """Find lead in Leads table with fuzzy matching"""
        try:
            # First try exact name match
            safe_name = name.replace("'", "\\'")
            formula = f"{{Lead Name}} = '{safe_name}'"
            records = self.leads_table.all(formula=formula)
            
            # Check if company matches
            for record in records:
                company_field = record['fields'].get('Company Name')
                if company_field and company_name.lower() in company_field.lower():
                    return record
            
            # If exact name matches found but different company, still return first match
            if records:
                return records[0]
            
            # Try fuzzy match if enabled
            if HAS_FUZZY_MATCH:
                all_leads = self.leads_table.all()
                norm_query = normalize_lead_name(name)
                
                best_match = None
                best_score = 0.0
                
                for record in all_leads:
                    existing_name = record['fields'].get('Lead Name', '')
                    score = similarity_score(name, existing_name, normalize_lead_name)
                    
                    # Boost score if company also matches
                    company_field = record['fields'].get('Company Name', '')
                    if company_field and company_name:
                        company_score = similarity_score(company_name, company_field, normalize_company_name)
                        if company_score >= 0.85:
                            score = min(score + 0.1, 1.0)  # Boost for company match
                    
                    if score > best_score:
                        best_score = score
                        best_match = record
                
                # If good fuzzy match (>= 85%)
                if best_score >= 0.85 and best_match:
                    matched_name = best_match['fields'].get('Lead Name', '')
                    logger.info(f"    Fuzzy matched lead '{name}' -> '{matched_name}' (score: {best_score:.2f})")
                    return best_match
            
            return None
        except:
            return None
    
    def create_lead(self, name: str, title: str, company_id: str, source: str) -> Dict:
        """Create a new lead record and enrich it immediately"""
        try:
            # Get company info for enrichment
            company_name = "Unknown Company"
            company_icp = 50
            try:
                company_record = self.companies_table.get(company_id)
                company_name = company_record['fields'].get('Company Name', 'Unknown Company')
                company_icp = company_record['fields'].get('ICP Fit Score', 50)
            except:
                pass
            
            # Required fields
            lead_data = {
                'Lead Name': name,
                'Title': title,
                'Company': [company_id],
                'Enrichment Status': 'Not Enriched',
                'Lead Source': 'Conference Intelligence',
                'Intelligence Notes': f"Discovered via Conference Intelligence\nSource: {source}"
            }
            
            record = self.leads_table.create(lead_data)
            lead_id = record['id']
            logger.info(f"    ✓ Created lead: {name}")
            
            # ENRICH IMMEDIATELY - inline enrichment
            logger.info(f"    Enriching lead...")
            enriched = self._enrich_lead_inline(
                lead_id=lead_id,
                lead_name=name,
                lead_title=title,
                company_id=company_id,
                company_name=company_name,
                company_icp=company_icp
            )
            
            if enriched:
                logger.info(f"    ✓ Lead enriched - ICP: {enriched.get('lead_icp', 'N/A')}")
                
                # Generate generic outreach messages for the lead
                logger.info(f"    Generating generic outreach messages...")
                self._generate_lead_outreach(lead_id, name, title, company_name)
            else:
                logger.warning(f"    ⚠ Could not enrich lead")
            
            # Refresh the record to get updated data
            record = self.leads_table.get(lead_id)
            return record
            
        except Exception as e:
            logger.error(f"    ✗ Failed to create lead {name}: {str(e)}")
            return None
    
    def _enrich_lead_inline(self, lead_id: str, lead_name: str, lead_title: str,
                           company_id: str, company_name: str, company_icp: int,
                           linkedin_url: str = None) -> Optional[Dict]:
        """Enrich lead with web search - inline implementation"""
        
        prompt = f"""Research this professional for contact information:

NAME: {lead_name}
TITLE (from our records): {lead_title}
COMPANY: {company_name}
{f"LINKEDIN: {linkedin_url}" if linkedin_url else ""}

═══════════════════════════════════════════════════════════
CRITICAL RULES:
═══════════════════════════════════════════════════════════
1. TITLE: The title above is from our records. KEEP this title unless web search clearly shows a DIFFERENT current title at the SAME company. If changed, set title_changed to true.
2. LINKEDIN: Only return a LinkedIn URL if you find the EXACT person (matching name AND company). Do NOT guess URLs.
3. EMAIL: Search thoroughly but return null if not found — do NOT fabricate.
4. Only return information about THIS specific person at THIS company.

Find:
1. Email address (search company website, press releases, LinkedIn, conference presentations)
2. Verify current title (see rule 1)
3. LinkedIn URL (if not provided — see rule 2)
4. Location (city, country)
5. Any recent news, speaking engagements, or publications

EMAIL FINDING PRIORITY:
- Company website team/leadership page
- Press releases with contact info
- Conference speaker lists
- Published papers/patents
- If not found, suggest email pattern based on company format

Return ONLY valid JSON:
{{
    "email": "email@company.com or null",
    "email_confidence": "High|Medium|Low|Pattern Suggested",
    "title": "{lead_title}",
    "title_changed": false,
    "title_change_reason": "null or reason",
    "linkedin_url": "https://linkedin.com/in/... or null",
    "location": "City, Country or null",
    "recent_activity": "Any recent news or speaking engagements, or null",
    "data_confidence": {{
        "email": "high|medium|low|unverified",
        "title": "high|medium|low|unverified",
        "linkedin": "high|medium|low|unverified",
        "identity_match": "high|medium|low"
    }}
}}

Return ONLY JSON."""

        try:
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
                return None
            
            data = json.loads(json_str.strip())
            
            # Calculate lead ICP based on title
            lead_icp, lead_icp_justification = self._calculate_lead_icp_with_justification(
                data.get('title') or lead_title, company_icp
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
            enrichment_confidence = confidence_map.get(email_conf.lower(), 'Medium')
            
            # Update lead record
            update_fields = {
                'Enrichment Status': 'Enriched',
                'Last Enrichment Date': datetime.now().strftime('%Y-%m-%d'),
                'Lead ICP Score': lead_icp,
                'Lead ICP Tier': lead_icp_tier,
                'Lead ICP Justification': lead_icp_justification,
                'Combined Priority': combined_priority,
                'Enrichment Confidence': enrichment_confidence
            }
            
            if data.get('email'):
                update_fields['Email'] = data['email']
            if data.get('title'):
                update_fields['Title'] = data['title']
            if data.get('linkedin_url'):
                update_fields['LinkedIn URL'] = data['linkedin_url']
            if data.get('recent_activity'):
                existing_notes = f"Discovered via Conference Intelligence\n\n"
                update_fields['Intelligence Notes'] = existing_notes + f"Recent Activity: {data['recent_activity'][:800]}"
            
            # Store data confidence for downstream use
            lead_data_confidence = data.get('data_confidence', {})
            if lead_data_confidence:
                try:
                    update_fields['Data Confidence'] = json.dumps(lead_data_confidence)
                except:
                    pass
            
            try:
                self.leads_table.update(lead_id, update_fields)
            except Exception as e:
                logger.warning(f"    Lead update partially failed: {e}")
                # Try updating fields one by one
                for field_name, field_value in update_fields.items():
                    try:
                        self.leads_table.update(lead_id, {field_name: field_value})
                    except:
                        pass
            
            return {
                'email': data.get('email'),
                'title': data.get('title') or lead_title,
                'linkedin_url': data.get('linkedin_url') or linkedin_url,
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
    
    def _generate_lead_outreach(self, lead_id: str, lead_name: str, lead_title: str, 
                                company_name: str) -> bool:
        """Generate generic outreach messages for a lead"""
        
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
            logger.info(f"    ✓ Generic outreach messages generated")
            return True
            
        except Exception as e:
            logger.warning(f"    Error generating lead outreach: {e}")
            return False
    
    def create_conference_trigger(self, lead_id: str, company_id: str, conference_name: str, 
                                  conference_date: str, role_at_conference: str,
                                  session_topic: str = None, source_url: str = None) -> bool:
        """Create CONFERENCE_ATTENDANCE trigger with outreach messages"""
        try:
            # Build description
            description = f"Conference: {conference_name}\nRole: {role_at_conference}"
            if session_topic:
                description += f"\nTopic: {session_topic}"
            if source_url:
                description += f"\nSource URL: {source_url}"
            
            # Build outreach angle
            if session_topic:
                outreach_angle = f"Speaking at {conference_name} about '{session_topic}'. Great opportunity to connect before or at the event."
            else:
                outreach_angle = f"Attending/speaking at {conference_name}. Great opportunity to connect before or at the event."
            
            # Timing recommendation based on conference date
            try:
                conf_date = datetime.strptime(conference_date, '%Y-%m-%d')
                days_until = (conf_date - datetime.now()).days
                if days_until > 30:
                    timing = f"Contact now - {days_until} days until conference. Ideal time to arrange meeting."
                elif days_until > 14:
                    timing = f"Contact within 1 week - {days_until} days until conference."
                elif days_until > 0:
                    timing = f"Urgent: Only {days_until} days until conference. Reach out immediately."
                else:
                    timing = "Conference has passed - follow up on experience at the event."
            except:
                timing = "Contact 2-4 weeks before conference to arrange meeting"
            
            # Build sources list
            sources = ["Conference Intelligence System"]
            if source_url:
                sources.append(source_url)
            
            trigger_data = {
                'Date Detected': datetime.now().strftime('%Y-%m-%d'),
                'Lead': [lead_id],
                'Company': [company_id],
                'Trigger Type': 'CONFERENCE_ATTENDANCE',
                'Trigger Source': 'Conference Monitor',
                'Conference Name': conference_name,
                'Urgency': 'HIGH',
                'Description': description,
                'Outreach Angle': outreach_angle,
                'Timing Recommendation': timing,
                'Event Date': conference_date,
                'Status': 'New',
                'Sources': ', '.join(sources)
            }
            
            # Generate trigger-specific outreach messages
            logger.info(f"    Generating conference outreach messages...")
            outreach = self._generate_conference_outreach(
                lead_id=lead_id,
                conference_name=conference_name,
                conference_date=conference_date,
                role_at_conference=role_at_conference,
                session_topic=session_topic
            )
            
            if outreach:
                if outreach.get('email_subject'):
                    trigger_data['Email Subject'] = outreach['email_subject']
                if outreach.get('email_body'):
                    trigger_data['Email Body'] = outreach['email_body']
                if outreach.get('linkedin_connection'):
                    trigger_data['LinkedIn Connection Request'] = outreach['linkedin_connection']
                if outreach.get('linkedin_short'):
                    trigger_data['LinkedIn Short Message'] = outreach['linkedin_short']
                trigger_data['Outreach Generated Date'] = datetime.now().strftime('%Y-%m-%d')
                logger.info(f"    ✓ Outreach messages generated")
            else:
                logger.warning(f"    ⚠ Could not generate outreach messages")
            
            self.trigger_history_table.create(trigger_data)
            return True
            
        except Exception as e:
            logger.error(f"    ✗ Failed to create trigger: {str(e)}")
            return False
    
    def _generate_conference_outreach(self, lead_id: str, conference_name: str,
                                      conference_date: str, role_at_conference: str,
                                      session_topic: str = None) -> Optional[Dict]:
        """Generate conference-specific outreach messages"""
        
        # Get lead info
        lead_name = "there"
        lead_title = ""
        company_name = ""
        try:
            lead_record = self.leads_table.get(lead_id)
            lead_name = lead_record['fields'].get('Lead Name', 'there')
            lead_title = lead_record['fields'].get('Title', '')
            company_ids = lead_record['fields'].get('Company', [])
            if company_ids:
                company_record = self.companies_table.get(company_ids[0])
                company_name = company_record['fields'].get('Company Name', '')
        except:
            pass
        
        # Build context
        if session_topic:
            context = f"Speaking at {conference_name} on '{session_topic}'"
        else:
            context = f"{role_at_conference} at {conference_name}"
        
        prompt = f"""Generate conference-specific outreach messages.

CONFERENCE: {conference_name}
DATE: {conference_date}
THEIR ROLE: {role_at_conference}
{f"SESSION TOPIC: {session_topic}" if session_topic else ""}

LEAD:
Name: {lead_name}
Title: {lead_title}
Company: {company_name}

YOUR COMPANY: European biologics CDMO specializing in mammalian cell culture manufacturing

Generate 3 SHORT messages to connect before/at the conference:

1. EMAIL (80-100 words):
Subject: [Reference conference naturally]
- Mention you'll also be at the conference
- Reference their session/role if applicable
- Propose meeting for coffee/chat
- Sign: "Best regards, [Your Name]"

2. LINKEDIN CONNECTION (150-180 chars):
- Reference the conference
- Express interest in connecting there

3. LINKEDIN SHORT MESSAGE (200-300 chars):
- Reference their presence at conference
- Mention interest in their work/session
- Propose meeting up
- Sign: "Best regards, [Your Name]"

Return ONLY valid JSON:
{{
    "email_subject": "Subject line",
    "email_body": "Email body",
    "linkedin_connection": "Connection request",
    "linkedin_short": "Short message"
}}

Return ONLY JSON."""

        try:
            message = self.anthropic_client.messages.create(
                model=self.config['anthropic']['model'],
                max_tokens=1500,
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
                return None
            
            return json.loads(json_str.strip())
            
        except Exception as e:
            logger.debug(f"Error generating conference outreach: {e}")
            return None
    
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
        
        # Validate name and title are different (sometimes AI returns title as name)
        if name and title and name.lower() == title.lower():
            logger.warning(f"  Skipping - name and title are the same: {name}")
            return {'status': 'skipped', 'reason': 'invalid_name'}
        
        if not all([name, title, company_name]):
            logger.warning(f"  Incomplete data for attendee: {name}")
            return {'status': 'skipped', 'reason': 'incomplete_data'}
        
        logger.info(f"  Processing: {name} ({title}) at {company_name}")
        
        # Step 1: Check company and get ICP
        company_record = self.find_company(company_name)
        company_icp = None
        is_big_pharma = False
        
        if company_record:
            company_icp = company_record['fields'].get('ICP Fit Score', 0)
            logger.info(f"    Company exists (ICP: {company_icp})")
        else:
            # Quick ICP assessment
            logger.info(f"    Company not found - assessing ICP...")
            company_icp, is_big_pharma = self.quick_company_icp_with_pharma_flag(company_name)
            
            # Different thresholds:
            # - Big pharma: Include even with lower ICP (they're still valuable)
            # - Regular companies: Need ICP >= 40
            # - Pure CDMOs: ICP = 0 (excluded)
            
            min_threshold = 20 if is_big_pharma else 40
            
            if company_icp >= min_threshold:
                company_record = self.create_company(company_name, company_icp)
                if is_big_pharma:
                    logger.info(f"    ✓ Big Pharma included (ICP: {company_icp})")
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
                company_id=company_record['id'],
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
                company_id=company_record['id'],
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
        logger.info("="*60)
        
        # Get conferences to monitor
        conferences = self.get_conferences_to_monitor()
        
        if not conferences:
            logger.info("No conferences to monitor at this time")
            logger.info("(All conferences are either too far out, already happened, or were recently monitored)")
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
