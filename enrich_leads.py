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
        
        # Initialize dynamic ICP scorer (optional - for companies without ICP)
        try:
            from complete_icp_scorer import CompleteICPScorer
            self.icp_scorer = CompleteICPScorer(self.config)
            logger.info(f"✓ ICP Scorer loaded: {len(self.icp_scorer.criteria)} criteria")
        except Exception as e:
            logger.warning(f"ICP Scorer not available: {str(e)}")
            self.icp_scorer = None
        
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
            justification_text += f"\n\nCOMPANY ICP: {company_icp}/90\n→ COMBINED: {combined_priority}"
        
        return (score, tier, justification_text, combined_priority)
    
    def normalize_title(self, title: str) -> str:
        """
        Normalize title for fuzzy matching.
        Handles variations like:
        - "Chief Strategy Officer" → includes "cso"
        - "Vice President" → includes "vp"
        - "Senior Vice President" → includes "svp"
        - "& " or " and " variations
        - Common abbreviations and expansions
        """
        if not title:
            return ""
        
        title_lower = title.lower().strip()
        
        # Store original for combined matching
        normalized = title_lower
        
        # ═══════════════════════════════════════════════════════════════
        # EXPAND ABBREVIATIONS (so we can match both ways)
        # ═══════════════════════════════════════════════════════════════
        expansions = {
            # C-Suite abbreviations
            'ceo': 'ceo chief executive officer',
            'coo': 'coo chief operating officer',
            'cfo': 'cfo chief financial officer',
            'cso': 'cso chief strategy officer chief scientific officer',
            'cto': 'cto chief technology officer chief technical officer',
            'cmo': 'cmo chief marketing officer chief medical officer',
            'cbo': 'cbo chief business officer',
            'cpo': 'cpo chief product officer chief procurement officer',
            'cro': 'cro chief revenue officer chief research officer',
            
            # VP variations
            'svp': 'svp senior vice president',
            'evp': 'evp executive vice president',
            'vp': 'vp vice president',
            'avp': 'avp assistant vice president',
            
            # Other abbreviations
            'gm': 'gm general manager',
            'md': 'md managing director',
            'bd': 'bd business development',
            'r&d': 'r&d research and development research development',
            'cmc': 'cmc chemistry manufacturing controls',
            'qa': 'qa quality assurance',
            'qc': 'qc quality control',
            'ops': 'ops operations',
            'mfg': 'mfg manufacturing',
            'tech ops': 'tech ops technical operations',
            'corp dev': 'corp dev corporate development business development bd',
            'bus dev': 'bus dev business development bd',
        }
        
        # ═══════════════════════════════════════════════════════════════
        # NORMALIZE FULL TITLES TO INCLUDE ABBREVIATIONS
        # ═══════════════════════════════════════════════════════════════
        title_mappings = [
            # C-Suite full titles
            ('chief executive officer', ' ceo '),
            ('chief operating officer', ' coo '),
            ('chief financial officer', ' cfo '),
            ('chief strategy officer', ' cso '),
            ('chief scientific officer', ' cso '),
            ('chief technology officer', ' cto '),
            ('chief technical officer', ' cto '),
            ('chief marketing officer', ' cmo '),
            ('chief medical officer', ' cmo '),
            ('chief business officer', ' cbo '),
            ('chief commercial officer', ' cco '),
            ('chief product officer', ' cpo '),
            ('chief procurement officer', ' cpo '),
            ('chief revenue officer', ' cro '),
            ('chief research officer', ' cro '),
            ('chief people officer', ' cpo '),
            ('chief human resources officer', ' chro '),
            
            # VP variations
            ('senior vice president', ' svp vp '),
            ('executive vice president', ' evp vp '),
            ('vice president', ' vp '),
            ('vice-president', ' vp '),
            ('assistant vice president', ' avp vp '),
            
            # Director variations
            ('senior director', ' sr director director '),
            ('associate director', ' assoc director ad '),
            ('executive director', ' exec director director '),
            ('managing director', ' md director '),
            
            # Manager variations
            ('senior manager', ' sr manager manager '),
            ('general manager', ' gm manager '),
            
            # Function expansions
            ('business development', ' bd business '),
            ('corporate development', ' corp dev corporate business development bd '),
            ('research and development', ' r&d research development '),
            ('research & development', ' r&d research development '),
            ('technical operations', ' tech ops operations '),
            ('chemistry manufacturing controls', ' cmc manufacturing '),
            ('chemistry, manufacturing, and controls', ' cmc manufacturing '),
            ('chemistry, manufacturing and controls', ' cmc manufacturing '),
            ('supply chain', ' supply chain sourcing procurement '),
            ('quality assurance', ' qa quality '),
            ('quality control', ' qc quality '),
            ('process development', ' process dev development '),
            ('drug development', ' drug dev development '),
            ('program management', ' program mgmt management '),
            ('project management', ' project mgmt management '),
            
            # Common variations
            (' & ', ' and '),
            (' / ', ' '),
            ('-', ' '),
            (',', ' '),
        ]
        
        # Apply mappings to create expanded title
        for pattern, expansion in title_mappings:
            if pattern in normalized:
                normalized = normalized + expansion
        
        # Also check if abbreviations are present and expand them
        words = title_lower.split()
        for word in words:
            if word in expansions:
                normalized = normalized + ' ' + expansions[word]
        
        return normalized
    
    def has_word(self, text: str, word: str) -> bool:
        """Check if word exists as a complete word (not substring) in text.
        
        Example: has_word("director of cmc", "cto") returns False
                 has_word("cto of operations", "cto") returns True
        """
        import re
        # Use word boundaries to match complete words only
        pattern = r'\b' + re.escape(word) + r'\b'
        return bool(re.search(pattern, text))
    
    def has_any_word(self, text: str, words: list) -> bool:
        """Check if any word from list exists as complete word in text."""
        return any(self.has_word(text, w) for w in words)
    
    def has_phrase(self, text: str, phrase: str) -> bool:
        """Check if phrase exists in text (phrase matching, not word boundary)."""
        return phrase in text
    
    def has_any_phrase(self, text: str, phrases: list) -> bool:
        """Check if any phrase exists in text."""
        return any(phrase in text for phrase in phrases)
    
    def score_title_relevance(self, title: str) -> int:
        """Score title relevance (0-25 points)
        
        For a CDMO, relevant contacts include:
        - C-Suite (all - they make strategic decisions)
        - Manufacturing/Ops/Supply Chain (direct users)
        - Strategy/BD/Corp Dev (partnership decision makers)
        - R&D/Process Dev (influence technology selection)
        - Finance (involved in make vs buy decisions)
        """
        if not title:
            return 3  # Unknown title gets base points
        
        # Use normalized title for fuzzy matching
        title_lower = self.normalize_title(title)
        
        # ═══════════════════════════════════════════════════════════════
        # TIER 1: C-SUITE - All Chiefs are relevant (25 pts)
        # Use word boundary matching to avoid "direCTOr" matching "cto"
        # ═══════════════════════════════════════════════════════════════
        c_suite_words = ['ceo', 'coo', 'cfo', 'cso', 'cto', 'cmo', 'cbo', 'cpo', 'cro']
        c_suite_phrases = ['chief', 'president', 'founder', 'co-founder', 'managing director']
        
        if self.has_any_word(title_lower, c_suite_words) or self.has_any_phrase(title_lower, c_suite_phrases):
            return 25
        
        # ═══════════════════════════════════════════════════════════════
        # TIER 2: VP/SVP - PRIMARY CONTACTS (20-22 pts)
        # ═══════════════════════════════════════════════════════════════
        # Direct manufacturing/ops VPs
        vp_primary = ['vp manufacturing', 'vp technical operations', 'vp operations',
                      'vp supply chain', 'vp cmc', 'svp manufacturing', 'svp operations',
                      'vp production', 'vp tech ops', 'vp process']
        if self.has_any_phrase(title_lower, vp_primary):
            return 22
        
        # Strategy/BD VPs - they decide on partnerships
        vp_strategic = ['vp strategy', 'vp business development', 'vp corporate development',
                        'vp strategic', 'svp strategy', 'svp business', 'vp partnerships',
                        'vp alliances', 'vp external']
        if self.has_any_phrase(title_lower, vp_strategic):
            return 20
        
        # R&D/Science VPs
        vp_rd = ['vp r&d', 'vp research', 'vp development', 'vp science', 'vp preclinical',
                 'svp r&d', 'svp research', 'vp drug development', 'vp biologics']
        if self.has_any_phrase(title_lower, vp_rd):
            return 18
        
        # General VP/SVP
        if 'vp' in title_lower or 'vice president' in title_lower or 'svp' in title_lower:
            return 16
        
        # ═══════════════════════════════════════════════════════════════
        # TIER 3: DIRECTORS / HEADS (14-18 pts)
        # ═══════════════════════════════════════════════════════════════
        # Head of anything relevant
        head_primary = ['head of manufacturing', 'head of operations', 'head of supply',
                        'head of cmc', 'head of tech', 'head of production', 'head of process']
        if any(h in title_lower for h in head_primary):
            return 18
        
        head_strategic = ['head of strategy', 'head of business', 'head of corporate',
                          'head of partnerships', 'head of alliances', 'head of external']
        if any(h in title_lower for h in head_strategic):
            return 16
        
        # Directors - manufacturing/ops
        if 'director' in title_lower and 'associate' not in title_lower:
            if any(d in title_lower for d in ['manufacturing', 'operations', 'supply', 'cmc', 'production', 'process']):
                return 16
            if any(d in title_lower for d in ['strategy', 'business', 'corporate', 'r&d', 'research', 'development']):
                return 14
            return 12  # Other director
        
        # General "Head of"
        if 'head of' in title_lower:
            return 14
        
        # ═══════════════════════════════════════════════════════════════
        # TIER 4: SENIOR MANAGERS / ASSOCIATE DIRECTORS (8-10 pts)
        # ═══════════════════════════════════════════════════════════════
        if 'associate director' in title_lower:
            return 10
        if 'senior manager' in title_lower:
            return 10
        if 'principal' in title_lower:
            return 8
        
        # ═══════════════════════════════════════════════════════════════
        # TIER 5: MANAGERS (5-6 pts)
        # ═══════════════════════════════════════════════════════════════
        if 'manager' in title_lower:
            if any(m in title_lower for m in ['manufacturing', 'operations', 'supply', 'cmc', 'production']):
                return 6
            return 5
        
        # ═══════════════════════════════════════════════════════════════
        # TIER 6: OTHER (3-4 pts)
        # ═══════════════════════════════════════════════════════════════
        if any(s in title_lower for s in ['scientist', 'engineer', 'specialist', 'analyst', 'coordinator']):
            return 4
        
        return 3  # Unknown/other
    
    def score_seniority(self, title: str) -> int:
        """Score seniority (0-20 points)"""
        if not title:
            return 5  # Unknown gets base points
        
        # Use normalized title for fuzzy matching
        title_lower = self.normalize_title(title)
        
        # C-Level
        if any(c in title_lower for c in ['chief', 'ceo', 'coo', 'cfo', 'cso', 'cto', 'cmo', 'president', 'founder']):
            return 20
        
        # VP/SVP
        if any(v in title_lower for v in ['vp', 'vice president', 'svp', 'evp']):
            return 18
        
        # Head of / Director
        if 'head of' in title_lower:
            return 16
        if 'director' in title_lower and 'associate' not in title_lower:
            return 15
        
        # Senior Manager / Associate Director
        if 'senior manager' in title_lower or 'associate director' in title_lower or 'principal' in title_lower:
            return 10
        
        # Manager
        if 'manager' in title_lower:
            return 6
        
        # Other
        return 4
    
    def score_function_fit(self, title: str) -> int:
        """Score function fit (0-20 points)
        
        For a CDMO, relevant functions:
        - Manufacturing/CMC/Supply Chain = PERFECT (directly use our services)
        - Operations/Production/Quality = EXCELLENT
        - Strategy/BD/Corp Dev = VERY GOOD (make partnership decisions!)
        - R&D/Process Dev = GOOD (influence technology choices)
        - Finance = RELEVANT (make vs buy decisions)
        - Clinical/Regulatory = USEFUL
        """
        if not title:
            return 5  # Unknown gets base points
        
        # Use normalized title for fuzzy matching
        title_lower = self.normalize_title(title)
        
        # PERFECT FIT - Manufacturing/CMC/Supply Chain (20 pts)
        if any(p in title_lower for p in ['manufacturing', 'cmc', 'supply chain', 'technical operations', 
                                           'tech ops', 'production', 'bioprocessing']):
            return 20
        
        # EXCELLENT - Operations/Quality (18 pts)
        if any(o in title_lower for o in ['operations', 'quality', 'gmp', 'compliance']):
            return 18
        
        # VERY GOOD - Strategy/BD/Partnerships (16 pts)
        # These people DECIDE on CDMO partnerships!
        if any(s in title_lower for s in ['strategy', 'strategic', 'business development', 'corporate development',
                                           'partnerships', 'alliances', 'external', 'sourcing', 'procurement']):
            return 16
        
        # GOOD - R&D/Process Development (14 pts)
        if any(r in title_lower for r in ['r&d', 'research', 'development', 'process development', 
                                           'drug development', 'biologics', 'science', 'scientific']):
            return 14
        
        # RELEVANT - Finance (make vs buy decisions) (12 pts)
        if any(f in title_lower for f in ['finance', 'financial', 'cfo', 'controller', 'treasurer']):
            return 12
        
        # USEFUL - Clinical/Regulatory (10 pts)
        if any(c in title_lower for c in ['clinical', 'regulatory', 'medical', 'pharmacovigilance']):
            return 10
        
        # GENERAL C-SUITE - still relevant (14 pts)
        if any(c in title_lower for c in ['chief', 'ceo', 'coo', 'president', 'founder']):
            return 14
        
        # Commercial roles - less directly relevant but still contact (8 pts)
        if any(m in title_lower for m in ['marketing', 'commercial', 'sales', 'market access']):
            return 8
        
        # Other (5 pts)
        return 5
    
    def score_decision_power(self, title: str) -> int:
        """Score decision power (0-15 points)"""
        if not title:
            return 4  # Unknown gets base points
        
        # Use normalized title for fuzzy matching
        title_lower = self.normalize_title(title)
        
        # Budget authority - C-suite and VPs (15 pts)
        if any(b in title_lower for b in ['chief', 'ceo', 'coo', 'cfo', 'cso', 'cto', 'president', 'founder']):
            return 15
        if any(v in title_lower for v in ['vp', 'vice president', 'svp', 'evp']):
            return 15
        
        # Strong influence - Head of / Director (12 pts)
        if 'head of' in title_lower:
            return 12
        if 'director' in title_lower and 'associate' not in title_lower:
            return 12
        
        # Moderate influence - Senior Manager / AD (8 pts)
        if 'senior manager' in title_lower or 'associate director' in title_lower or 'principal' in title_lower:
            return 8
        
        # Some influence - Manager (5 pts)
        if 'manager' in title_lower:
            return 5
        
        return 3
    
    def score_geography(self, location: str) -> int:
        """Score geography (0-5 points)"""
        if not location:
            return 3  # Unknown gets base points
        
        location_lower = location.lower()
        
        # Europe - priority (5 pts)
        europe = ['germany', 'poland', 'uk', 'united kingdom', 'france', 'netherlands', 'switzerland',
                 'belgium', 'sweden', 'denmark', 'austria', 'italy', 'spain', 'ireland', 'norway',
                 'finland', 'portugal', 'czech', 'hungary', 'europe']
        if any(e in location_lower for e in europe):
            return 5
        
        # US - priority (5 pts)
        us = ['usa', 'united states', 'california', 'massachusetts', 'new york', 'new jersey',
              'maryland', 'north carolina', 'texas', 'boston', 'san francisco', 'san diego']
        if any(u in location_lower for u in us):
            return 5
        
        # Korea/Japan - strong markets (4 pts)
        asia_priority = ['korea', 'south korea', 'japan', 'tokyo', 'seoul']
        if any(a in location_lower for a in asia_priority):
            return 4
        
        # Other Asia (3 pts)
        other_asia = ['china', 'singapore', 'taiwan', 'hong kong', 'australia', 'india']
        if any(o in location_lower for o in other_asia):
            return 3
        
        return 2  # ROW
    
    def calculate_combined_priority(self, company_icp: int, lead_icp: int) -> str:
        """Calculate combined priority based on company and lead ICP scores"""
        # Company tiers: 80+ (T1), 65+ (T2), 50+ (T3), 35+ (T4)
        # Lead tiers: 85+ (T1), 70+ (T2), 55+ (T3), 40+ (T4)
        
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
    
    def get_leads_to_enrich(self, status: str = "Not Enriched") -> List[Dict]:
        """Fetch leads that need enrichment"""
        formula = f"{{Enrichment Status}} = '{status}'"
        records = self.leads_table.all(formula=formula)
        logger.info(f"Found {len(records)} leads with status '{status}'")
        return records
    
    def get_leads_needing_refresh(self, months: int = 6) -> List[Dict]:
        """Fetch leads that need re-enrichment (6+ months old or missing data)"""
        from datetime import datetime, timedelta
        
        cutoff_date = (datetime.now() - timedelta(days=months * 30)).strftime('%Y-%m-%d')
        
        # Get all enriched leads
        all_leads = self.leads_table.all(formula="{Enrichment Status} = 'Enriched'")
        
        needs_refresh = []
        for lead in all_leads:
            fields = lead['fields']
            
            # Check if last enrichment date is old or missing
            last_enrichment = fields.get('Last Enrichment Date')
            
            # Reason 1: Last enrichment was 6+ months ago
            if last_enrichment and last_enrichment < cutoff_date:
                needs_refresh.append(lead)
                continue
            
            # Reason 2: Missing critical fields
            missing_fields = []
            if not fields.get('Email'):
                missing_fields.append('Email')
            if not fields.get('LinkedIn URL'):
                missing_fields.append('LinkedIn')
            if not fields.get('Lead ICP Score'):
                missing_fields.append('ICP Score')
            if not fields.get('Email Subject'):  # Outreach not generated
                missing_fields.append('Outreach')
            
            if missing_fields:
                needs_refresh.append(lead)
        
        logger.info(f"Found {len(needs_refresh)} leads needing refresh")
        return needs_refresh
    
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
            
            # Check if we got any response
            if not result_text:
                logger.warning(f"Empty response for {lead_name}")
                return {
                    "overall_confidence": "Failed",
                    "error": "Empty response from AI"
                }
            
            # Handle markdown code blocks
            if result_text.startswith("```json"):
                result_text = result_text[7:]
            if result_text.startswith("```"):
                result_text = result_text[3:]
            if result_text.endswith("```"):
                result_text = result_text[:-3]
            
            result_text = result_text.strip()
            
            # Find JSON object in response
            if not result_text.startswith("{"):
                # Try to find JSON in the response
                start = result_text.find("{")
                if start != -1:
                    end = result_text.rfind("}") + 1
                    result_text = result_text[start:end]
                else:
                    logger.warning(f"No JSON found in response for {lead_name}")
                    return {
                        "overall_confidence": "Failed",
                        "error": "No JSON in response"
                    }
            
            result = json.loads(result_text.strip())
            logger.info(f"Successfully enriched {lead_name}")
            return result
            
        except Exception as e:
            logger.error(f"Error enriching {lead_name}: {str(e)}")
            return {
                "overall_confidence": "Failed",
                "error": str(e)
            }
    
    def generate_general_outreach(self, lead_name: str, title: str, company_name: str,
                                  lead_icp: int, company_icp: int = None) -> Dict:
        """Generate general introduction outreach messages during enrichment"""
        
        prompt = f"""Generate professional outreach messages for this lead.

LEAD INFORMATION:
Name: {lead_name}
Title: {title}
Company: {company_name}
Lead ICP Score: {lead_icp}/100
Company ICP Score: {company_icp if company_icp else 'Unknown'}/90

YOUR COMPANY (Rezon Bio):
- European CDMO specializing in mammalian cell culture
- Focus: mAbs, bispecifics, ADCs
- Target: Mid-size biotechs in Phase 2/3 or commercial
- Positioning: Cost-efficient European quality vs. Western CDMOs
- Strengths: Biosimilar track record, Sandoz-qualified, agile mid-size partner

Generate FOUR brief, natural outreach messages:

═══════════════════════════════════════════════════════════
MESSAGE 1: EMAIL (120-150 words)
═══════════════════════════════════════════════════════════
Subject: [Natural subject line]

Requirements:
- Natural, conversational opener
- Reference their role/company naturally
- Suggest why Rezon might be relevant (no bullet lists)
- Soft CTA - question or suggestion to connect
- Sign as: "Best regards, [Your Name], Rezon Bio Business Development"

═══════════════════════════════════════════════════════════
MESSAGE 2: LINKEDIN CONNECTION REQUEST (180-200 chars)
═══════════════════════════════════════════════════════════
Requirements:
- Very brief, friendly
- Reference their role or company
- No signature needed

═══════════════════════════════════════════════════════════
MESSAGE 3: LINKEDIN SHORT MESSAGE (300-400 chars)
═══════════════════════════════════════════════════════════
For after connection accepted.

Requirements:
- Conversational opener
- Reference their background or role
- Suggest why connecting makes sense
- End with: "Best regards, [Your Name], Rezon Bio BD"

═══════════════════════════════════════════════════════════
MESSAGE 4: LINKEDIN INMAIL (250-350 words)
═══════════════════════════════════════════════════════════
Subject: [Natural, not salesy]

Requirements:
- Open with observation about their work/company
- Share relevant perspective (not sales pitch)
- Sound like industry peer conversation
- Weave in why Rezon might be relevant
- Natural next step suggestion
- NO bullet lists - paragraphs only
- Sign as: "Best regards, [Your Name], Rezon Bio Business Development"

CRITICAL STYLE RULES:
- Natural, human language (slightly imperfect is fine)
- NO bullet lists anywhere
- NO ** for emphasis
- Show knowledge, don't tell them their situation
- About THEM, not about us
- Conversational, not corporate
- Use [Your Name] as placeholder

Return in this JSON format:
{{
  "email_subject": "Subject here",
  "email_body": "Body with signature using [Your Name] placeholder",
  "linkedin_connection": "Connection request (under 200 chars, no signature)",
  "linkedin_short": "Short message ending with 'Best regards, [Your Name], Rezon Bio BD'",
  "linkedin_inmail_subject": "InMail subject",
  "linkedin_inmail_body": "InMail body ending with 'Best regards, [Your Name], Rezon Bio Business Development'"
}}

Only return valid JSON."""

        try:
            message = self.anthropic_client.messages.create(
                model=self.config['anthropic']['model'],
                max_tokens=2000,
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
            
            # Check for empty response
            if not result_text.strip():
                logger.warning("Empty response for outreach generation")
                return None
            
            # Clean JSON
            result_text = result_text.strip()
            if result_text.startswith("```json"):
                result_text = result_text[7:]
            elif result_text.startswith("```"):
                result_text = result_text[3:]
            if result_text.endswith("```"):
                result_text = result_text[:-3]
            result_text = result_text.strip()
            
            # Find JSON if not at start
            if not result_text.startswith("{"):
                start = result_text.find("{")
                if start != -1:
                    end = result_text.rfind("}") + 1
                    result_text = result_text[start:end]
                else:
                    logger.warning("No JSON found in outreach response")
                    return None
            
            # Parse
            messages_data = json.loads(result_text)
            return messages_data
            
        except Exception as e:
            logger.error(f"Failed to generate outreach: {str(e)}")
            return None
    
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
            'Enrichment Confidence': confidence,
            'Last Enrichment Date': datetime.now().strftime('%Y-%m-%d')
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
        lead_icp_score = None
        try:
            existing_record = self.leads_table.get(record_id)
            company_ids = existing_record['fields'].get('Company', [])
            company_icp = None
            
            if company_ids:
                try:
                    company = self.companies_table.get(company_ids[0])
                    company_icp = company['fields'].get('ICP Fit Score')
                    
                    # If company doesn't have ICP score yet, calculate it
                    if company_icp is None and self.icp_scorer:
                        company_name = company['fields'].get('Company Name', '')
                        if company_name:
                            logger.info(f"  Company '{company_name}' missing ICP - calculating now...")
                            try:
                                new_icp, breakdown = self.icp_scorer.score_company(company_name)
                                
                                # Update company with new ICP
                                self.companies_table.update(company_ids[0], {
                                    'ICP Fit Score': new_icp
                                })
                                company_icp = new_icp
                                logger.info(f"  ✓ Updated company ICP: {new_icp}")
                            except Exception as e:
                                logger.warning(f"  Could not calculate company ICP: {str(e)}")
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
        
        # Generate General Outreach Messages
        # Only generate if Lead ICP is acceptable (40+)
        if lead_icp_score and lead_icp_score >= 40:
            try:
                logger.info(f"  Generating general outreach messages...")
                outreach_messages = self.generate_general_outreach(
                    lead_name=existing_record['fields'].get('Lead Name', ''),
                    title=enriched_data.get('title', ''),
                    company_name=existing_record['fields'].get('Company Name', ''),
                    lead_icp=lead_icp_score,
                    company_icp=company_icp
                )
                
                if outreach_messages:
                    update_fields['Email Subject'] = outreach_messages.get('email_subject', '')
                    update_fields['Email Body'] = outreach_messages.get('email_body', '')
                    update_fields['LinkedIn Connection Request'] = outreach_messages.get('linkedin_connection', '')
                    update_fields['LinkedIn Short Message'] = outreach_messages.get('linkedin_short', '')
                    update_fields['LinkedIn InMail Subject'] = outreach_messages.get('linkedin_inmail_subject', '')
                    update_fields['LinkedIn InMail Body'] = outreach_messages.get('linkedin_inmail_body', '')
                    update_fields['Message Generated Date'] = datetime.now().strftime('%Y-%m-%d')
                    logger.info(f"  ✓ Outreach messages generated")
            except Exception as e:
                logger.warning(f"  Could not generate outreach: {str(e)}")
        else:
            logger.info(f"  Skipping outreach (Lead ICP too low: {lead_icp_score})")
        
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
    
    def enrich_leads(self, status: str = "Not Enriched", limit: Optional[int] = None, 
                     refresh: bool = False, refresh_months: int = 6, offset: int = 0):
        """
        Main enrichment workflow
        
        Args:
            status: Enrichment status to filter by (default: "Not Enriched")
            limit: Max number of leads to process
            refresh: If True, re-enrich old leads or those with missing data
            refresh_months: Consider leads older than this many months for refresh (default: 6)
            offset: Skip first N leads (for batch processing)
        """
        if refresh:
            logger.info(f"Refresh mode: Finding leads needing re-enrichment (>{refresh_months} months or missing data)")
            leads = self.get_leads_needing_refresh(months=refresh_months)
        else:
            leads = self.get_leads_to_enrich(status)
        
        # Apply offset first, then limit
        if offset > 0:
            leads = leads[offset:]
            logger.info(f"Batch mode: skipping first {offset} leads")
        
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
    parser.add_argument('--offset', type=int, default=0,
                       help='Skip first N leads (for batch processing)')
    parser.add_argument('--refresh', action='store_true',
                       help='Re-enrich leads that are 6+ months old or have missing data')
    parser.add_argument('--refresh-months', type=int, default=6,
                       help='Re-enrich leads older than this many months (default: 6)')
    parser.add_argument('--config', default='config.yaml',
                       help='Path to config file')
    
    args = parser.parse_args()
    
    try:
        enricher = LeadEnricher(config_path=args.config)
        enricher.enrich_leads(
            status=args.status, 
            limit=args.limit,
            offset=args.offset,
            refresh=args.refresh,
            refresh_months=args.refresh_months
        )
    except FileNotFoundError:
        logger.error(f"Config file not found: {args.config}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
