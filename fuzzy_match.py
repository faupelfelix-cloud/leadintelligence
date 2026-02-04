#!/usr/bin/env python3
"""
Fuzzy Matching Utilities for Lead Intelligence Platform

Provides consistent fuzzy matching across all scripts to prevent duplicate
records due to minor variations in company/lead names.

Features:
- Company name normalization (removes Inc., Ltd., GmbH, etc.)
- Fuzzy string matching with configurable threshold
- Multiple matching strategies (exact, normalized, fuzzy)
- Caching for performance

Usage:
    from fuzzy_match import FuzzyMatcher, normalize_company_name, normalize_lead_name
    
    matcher = FuzzyMatcher(airtable_base)
    company_id = matcher.find_company("Pfizer Inc.")  # Finds "Pfizer"
    lead_id = matcher.find_lead("John Smith", company_id)
"""

import re
import logging
from typing import Optional, List, Dict, Tuple
from difflib import SequenceMatcher

logger = logging.getLogger(__name__)


# =============================================================================
# NORMALIZATION FUNCTIONS
# =============================================================================

# Company suffixes to remove (order matters - longer first)
COMPANY_SUFFIXES = [
    # Full forms
    'incorporated', 'corporation', 'limited', 'company', 'holdings',
    'pharmaceuticals', 'therapeutics', 'biotherapeutics', 'biotechnology',
    'biopharmaceuticals', 'biopharma', 'biotech', 'pharma', 'sciences',
    'biosciences', 'lifesciences', 'life sciences', 'healthcare',
    'laboratories', 'laboratory', 'research', 'development',
    # Abbreviations
    'inc.', 'inc', 'corp.', 'corp', 'ltd.', 'ltd', 'llc', 'l.l.c.',
    'plc', 'p.l.c.', 'ag', 'a.g.', 'sa', 's.a.', 'nv', 'n.v.',
    'gmbh', 'g.m.b.h.', 'co.', 'co', 'kg', 'k.g.',
    'pty', 'pty.', 'pvt', 'pvt.',
    # Common endings
    'group', 'international', 'global', 'worldwide',
    # Registered marks
    '®', '™', '©',
]

# Words to normalize in company names
COMPANY_WORD_MAPPINGS = {
    '&': 'and',
    '+': 'and',
    'intl': 'international',
    'int\'l': 'international',
    'pharm': 'pharma',
    'bio': 'bio',
    'tech': 'technology',
    'mgmt': 'management',
    'mfg': 'manufacturing',
    'svcs': 'services',
    'svc': 'service',
}

# Known company aliases/abbreviations - maps alias to canonical name
# Both directions are checked during matching
COMPANY_ALIASES = {
    # Big Pharma
    'msd': ['merck sharp dohme', 'merck', 'merck and co', 'merck co'],
    'merck': ['msd', 'merck sharp dohme', 'merck and co'],
    'merck sharp dohme': ['msd', 'merck'],
    'jnj': ['johnson johnson', 'johnson and johnson', 'janssen'],
    'j&j': ['johnson johnson', 'johnson and johnson', 'janssen', 'jnj'],
    'j and j': ['johnson johnson', 'johnson and johnson', 'janssen', 'jnj'],
    'johnson johnson': ['jnj', 'j&j', 'janssen', 'j and j'],
    'johnson and johnson': ['jnj', 'j&j', 'janssen', 'j and j'],
    'janssen': ['jnj', 'j&j', 'johnson johnson', 'johnson and johnson'],
    'gsk': ['glaxosmithkline', 'glaxo smith kline', 'glaxo'],
    'glaxosmithkline': ['gsk', 'glaxo'],
    'az': ['astrazeneca', 'astra zeneca'],
    'astrazeneca': ['az', 'astra zeneca'],
    'bms': ['bristol myers squibb', 'bristol-myers squibb'],
    'bristol myers squibb': ['bms'],
    'abbvie': ['abbott', 'abbv'],
    'lly': ['eli lilly', 'lilly'],
    'eli lilly': ['lly', 'lilly'],
    'lilly': ['eli lilly', 'lly'],
    'pfe': ['pfizer'],
    'pfizer': ['pfe'],
    'nvs': ['novartis'],
    'novartis': ['nvs'],  # Sandoz spun off as independent company in 2023
    'sandoz': ['sandoz group', 'sandoz ag'],
    'sandoz group': ['sandoz', 'sandoz ag'],
    'roche': ['rhhby', 'hoffmann la roche', 'f hoffmann la roche', 'genentech'],
    'genentech': ['roche'],
    'sny': ['sanofi', 'sanofi aventis'],
    'sanofi': ['sny', 'sanofi aventis'],
    'tak': ['takeda'],
    'takeda': ['tak'],
    'amgn': ['amgen'],
    'amgen': ['amgn'],
    'gild': ['gilead', 'gilead sciences'],
    'gilead': ['gild', 'gilead sciences'],
    'regn': ['regeneron'],
    'regeneron': ['regn'],
    'vrtx': ['vertex', 'vertex pharmaceuticals'],
    'vertex': ['vrtx', 'vertex pharmaceuticals'],
    'biib': ['biogen'],
    'biogen': ['biib'],
    'mrna': ['moderna'],
    'moderna': ['mrna'],
    'bntx': ['biontech'],
    'biontech': ['bntx'],
    # CDMOs
    'lonza': ['lonza group', 'lonza biologics'],
    'wuxi': ['wuxi biologics', 'wuxi apptec', 'wuxi app tec'],
    'wuxi biologics': ['wuxi'],
    'samsung biologics': ['samsung bio', 'samsung'],
    'fuji': ['fujifilm', 'fujifilm diosynth', 'fuji diosynth'],
    'fujifilm': ['fuji', 'fujifilm diosynth'],
    'catalent': ['catalent pharma', 'catalent biologics'],
    'boehringer': ['boehringer ingelheim', 'bi'],
    'boehringer ingelheim': ['boehringer', 'bi'],
}

# Title variations for lead matching
TITLE_ABBREVIATIONS = {
    'vp': 'vice president',
    'svp': 'senior vice president',
    'evp': 'executive vice president',
    'ceo': 'chief executive officer',
    'coo': 'chief operating officer',
    'cfo': 'chief financial officer',
    'cto': 'chief technology officer',
    'cmo': 'chief medical officer',
    'cso': 'chief scientific officer',
    'cbo': 'chief business officer',
    'dir': 'director',
    'sr': 'senior',
    'jr': 'junior',
    'mgr': 'manager',
    'mfg': 'manufacturing',
    'ops': 'operations',
    'dev': 'development',
    'r&d': 'research and development',
}


def normalize_company_name(name: str) -> str:
    """
    Normalize company name for matching.
    
    Transformations:
    - Lowercase
    - Remove punctuation (except hyphens in compound names)
    - Remove common suffixes (Inc., Ltd., GmbH, etc.)
    - Normalize common abbreviations
    - Collapse whitespace
    
    Examples:
        "Pfizer Inc." -> "pfizer"
        "Johnson & Johnson" -> "johnson and johnson"
        "F. Hoffmann-La Roche Ltd" -> "f hoffmann la roche"
        "BioNTech SE" -> "biontech"
        "R-Pharm" -> "r-pharm" (preserved because too short otherwise)
    """
    if not name:
        return ""
    
    # Lowercase
    normalized = name.lower().strip()
    original_normalized = normalized  # Keep for fallback
    
    # Replace special characters
    for old, new in COMPANY_WORD_MAPPINGS.items():
        normalized = normalized.replace(old, f' {new} ')
    
    # Remove punctuation except hyphens and apostrophes in names
    # Keep hyphens for compound names like "Hoffmann-La Roche"
    normalized = re.sub(r'[^\w\s\-\']', ' ', normalized)
    
    # Remove suffixes (from longest to shortest)
    for suffix in COMPANY_SUFFIXES:
        # Match suffix at end of string or followed by space/punctuation
        pattern = rf'\b{re.escape(suffix)}\b'
        normalized = re.sub(pattern, '', normalized, flags=re.IGNORECASE)
    
    # Collapse multiple spaces
    normalized = ' '.join(normalized.split())
    
    # Remove trailing/leading hyphens
    normalized = normalized.strip('-').strip()
    
    # SAFEGUARD: If we stripped too much, use a more conservative normalization
    # A valid company name should be at least 3 characters
    if len(normalized) < 3:
        # Fall back to just removing obvious suffixes and punctuation
        fallback = original_normalized
        # Only remove the most common corporate suffixes
        for suffix in ['inc.', 'inc', 'ltd.', 'ltd', 'llc', 'corp.', 'corp', 'plc', 'ag', 'sa', 'gmbh']:
            pattern = rf'\s*\b{re.escape(suffix)}\b\.?\s*$'
            fallback = re.sub(pattern, '', fallback, flags=re.IGNORECASE)
        fallback = re.sub(r'[^\w\s\-]', '', fallback)  # Remove punctuation
        fallback = ' '.join(fallback.split()).strip()
        if len(fallback) >= 3:
            return fallback
        # If still too short, return original lowercase
        return name.lower().strip()
    
    return normalized


def normalize_lead_name(name: str) -> str:
    """
    Normalize lead/person name for matching.
    
    Transformations:
    - Lowercase
    - Remove titles (Dr., Prof., Mr., Mrs., etc.)
    - Remove suffixes (Jr., Sr., III, PhD, etc.)
    - Normalize unicode characters
    - Handle "Last, First" format
    
    Examples:
        "Dr. John Smith" -> "john smith"
        "Smith, John" -> "john smith"
        "María García" -> "maria garcia"
    """
    if not name:
        return ""
    
    # Lowercase
    normalized = name.lower().strip()
    
    # Handle "Last, First" format
    if ',' in normalized:
        parts = normalized.split(',', 1)
        if len(parts) == 2:
            normalized = f"{parts[1].strip()} {parts[0].strip()}"
    
    # Remove titles
    titles = ['dr.', 'dr', 'prof.', 'prof', 'mr.', 'mr', 'mrs.', 'mrs', 
              'ms.', 'ms', 'miss', 'sir', 'dame', 'lord', 'lady']
    for title in titles:
        normalized = re.sub(rf'^{re.escape(title)}\s+', '', normalized)
    
    # Remove suffixes
    suffixes = ['jr.', 'jr', 'sr.', 'sr', 'ii', 'iii', 'iv', 'v',
                'phd', 'ph.d.', 'md', 'm.d.', 'mba', 'm.b.a.',
                'esq', 'esq.']
    for suffix in suffixes:
        normalized = re.sub(rf'\s+{re.escape(suffix)}$', '', normalized)
        normalized = re.sub(rf',\s*{re.escape(suffix)}$', '', normalized)
    
    # Normalize unicode (é -> e, ü -> u, etc.)
    import unicodedata
    normalized = unicodedata.normalize('NFKD', normalized)
    normalized = ''.join(c for c in normalized if not unicodedata.combining(c))
    
    # Remove punctuation
    normalized = re.sub(r'[^\w\s]', '', normalized)
    
    # Collapse whitespace
    normalized = ' '.join(normalized.split())
    
    return normalized


def normalize_title(title: str) -> str:
    """
    Normalize job title for matching.
    
    Examples:
        "VP, Manufacturing" -> "vice president manufacturing"
        "Sr. Director of Operations" -> "senior director operations"
    """
    if not title:
        return ""
    
    normalized = title.lower().strip()
    
    # Expand abbreviations
    for abbrev, full in TITLE_ABBREVIATIONS.items():
        # Match as whole word
        normalized = re.sub(rf'\b{re.escape(abbrev)}\b', full, normalized)
    
    # Remove filler words
    filler = ['of', 'the', 'and', 'for', 'in', 'at', '-', ',', '&']
    for word in filler:
        normalized = re.sub(rf'\b{re.escape(word)}\b', ' ', normalized)
    
    # Collapse whitespace
    normalized = ' '.join(normalized.split())
    
    return normalized


# =============================================================================
# SIMILARITY FUNCTIONS
# =============================================================================

def similarity_ratio(s1: str, s2: str) -> float:
    """
    Calculate similarity ratio between two strings (0.0 to 1.0).
    Uses SequenceMatcher which handles insertions, deletions, substitutions.
    """
    if not s1 or not s2:
        return 0.0
    return SequenceMatcher(None, s1, s2).ratio()


def check_company_alias(name1: str, name2: str) -> bool:
    """
    Check if two company names are known aliases of each other.
    Returns True if they are aliases, False otherwise.
    """
    norm1 = normalize_company_name(name1)
    norm2 = normalize_company_name(name2)
    raw1 = name1.lower().strip()
    raw2 = name2.lower().strip()
    
    # Minimum length for partial matching (avoid matching single letters)
    MIN_MATCH_LENGTH = 3
    
    # Direct match
    if norm1 == norm2:
        return True
    
    # Skip alias checking if either name is too short (likely over-normalized)
    # But still allow exact matches against known alias keys
    if len(norm1) < MIN_MATCH_LENGTH or len(norm2) < MIN_MATCH_LENGTH:
        # Special case: check if short name is an EXACT alias key
        short_name = norm1 if len(norm1) < MIN_MATCH_LENGTH else norm2
        long_name = norm2 if len(norm1) < MIN_MATCH_LENGTH else norm1
        long_raw = raw2 if len(norm1) < MIN_MATCH_LENGTH else raw1
        
        if short_name in COMPANY_ALIASES:
            for alias in COMPANY_ALIASES[short_name]:
                alias_norm = normalize_company_name(alias)
                if alias_norm == long_name or alias == long_raw or alias_norm == long_raw:
                    return True
        # Also check if long name has short name as an alias
        if long_name in COMPANY_ALIASES:
            if short_name in COMPANY_ALIASES[long_name]:
                return True
            for alias in COMPANY_ALIASES[long_name]:
                if normalize_company_name(alias) == short_name:
                    return True
        return False
    
    # Check both raw and normalized versions against aliases
    names_to_check = [raw1, norm1]
    targets_to_match = [raw2, norm2]
    
    # Helper function to check if names match (with length check)
    def safe_contains(haystack: str, needle: str) -> bool:
        return len(needle) >= MIN_MATCH_LENGTH and needle in haystack
    
    for name in names_to_check:
        if name in COMPANY_ALIASES:
            for alias in COMPANY_ALIASES[name]:
                alias_norm = normalize_company_name(alias)
                if alias in targets_to_match or alias_norm in targets_to_match:
                    return True
                # Check partial match only if lengths are reasonable
                for t in targets_to_match:
                    if safe_contains(alias, t) or safe_contains(t, alias):
                        return True
    
    for name in targets_to_match:
        if name in COMPANY_ALIASES:
            for alias in COMPANY_ALIASES[name]:
                alias_norm = normalize_company_name(alias)
                if alias in names_to_check or alias_norm in names_to_check:
                    return True
                for n in names_to_check:
                    if safe_contains(alias, n) or safe_contains(n, alias):
                        return True
    
    # Check all aliases for partial matches (only with sufficient length)
    for key, aliases in COMPANY_ALIASES.items():
        # Only check if key or normalized names are long enough
        if len(key) < MIN_MATCH_LENGTH:
            continue
            
        # Exact key match (not substring)
        key_matches_1 = key == norm1 or key == raw1
        key_matches_2 = key == norm2 or key == raw2
        
        # Substring match only if both are long enough
        if not key_matches_1 and len(norm1) >= MIN_MATCH_LENGTH:
            key_matches_1 = (key in norm1 and len(key) >= MIN_MATCH_LENGTH) or \
                           (norm1 in key and len(norm1) >= MIN_MATCH_LENGTH)
        if not key_matches_2 and len(norm2) >= MIN_MATCH_LENGTH:
            key_matches_2 = (key in norm2 and len(key) >= MIN_MATCH_LENGTH) or \
                           (norm2 in key and len(norm2) >= MIN_MATCH_LENGTH)
        
        if key_matches_1:
            for a in aliases:
                a_norm = normalize_company_name(a)
                if a == norm2 or a == raw2 or a_norm == norm2:
                    return True
                if len(a) >= MIN_MATCH_LENGTH and len(norm2) >= MIN_MATCH_LENGTH:
                    if a in norm2 or norm2 in a or a in raw2 or raw2 in a:
                        return True
        if key_matches_2:
            for a in aliases:
                a_norm = normalize_company_name(a)
                if a == norm1 or a == raw1 or a_norm == norm1:
                    return True
                if len(a) >= MIN_MATCH_LENGTH and len(norm1) >= MIN_MATCH_LENGTH:
                    if a in norm1 or norm1 in a or a in raw1 or raw1 in a:
                        return True
    
    return False


def similarity_score(name1: str, name2: str, normalize_func=normalize_company_name) -> float:
    """
    Calculate similarity score with normalization.
    
    Returns a score from 0.0 to 1.0 where:
    - 1.0 = exact match (after normalization) or known alias
    - 0.9+ = very likely the same
    - 0.8+ = probably the same
    - 0.7+ = possibly the same
    - <0.7 = likely different
    """
    # Normalize both strings
    norm1 = normalize_func(name1)
    norm2 = normalize_func(name2)
    
    # Exact match after normalization
    if norm1 == norm2:
        return 1.0
    
    # Check for known company aliases (only for company names)
    if normalize_func == normalize_company_name:
        if check_company_alias(norm1, norm2):
            return 1.0  # Known alias = perfect match
    
    # Check if one contains the other (common for name variations)
    if norm1 in norm2 or norm2 in norm1:
        # Length ratio to penalize very different lengths
        len_ratio = min(len(norm1), len(norm2)) / max(len(norm1), len(norm2))
        return 0.9 * len_ratio + 0.1
    
    # Fuzzy match
    return similarity_ratio(norm1, norm2)


def find_best_match(query: str, candidates: List[str], 
                   threshold: float = 0.8,
                   normalize_func=normalize_company_name) -> Optional[Tuple[str, float]]:
    """
    Find the best matching candidate above threshold.
    
    Args:
        query: The string to match
        candidates: List of potential matches
        threshold: Minimum similarity score (0.0 to 1.0)
        normalize_func: Function to normalize strings
    
    Returns:
        Tuple of (best_match, score) or None if no match above threshold
    """
    if not query or not candidates:
        return None
    
    best_match = None
    best_score = 0.0
    
    norm_query = normalize_func(query)
    
    for candidate in candidates:
        norm_candidate = normalize_func(candidate)
        
        # Exact match after normalization
        if norm_query == norm_candidate:
            return (candidate, 1.0)
        
        score = similarity_score(query, candidate, normalize_func)
        
        if score > best_score:
            best_score = score
            best_match = candidate
    
    if best_score >= threshold:
        return (best_match, best_score)
    
    return None


# =============================================================================
# FUZZY MATCHER CLASS
# =============================================================================

class FuzzyMatcher:
    """
    Fuzzy matcher for Airtable records.
    
    Caches records for efficient repeated lookups.
    """
    
    def __init__(self, airtable_base, company_table_name: str = "Companies",
                 leads_table_name: str = "Leads",
                 company_threshold: float = 0.85,
                 lead_threshold: float = 0.85):
        """
        Initialize FuzzyMatcher.
        
        Args:
            airtable_base: PyAirtable base object
            company_table_name: Name of companies table
            leads_table_name: Name of leads table
            company_threshold: Minimum similarity for company matching
            lead_threshold: Minimum similarity for lead matching
        """
        self.base = airtable_base
        self.company_table = airtable_base.table(company_table_name)
        self.leads_table = airtable_base.table(leads_table_name)
        self.company_threshold = company_threshold
        self.lead_threshold = lead_threshold
        
        # Caches
        self._company_cache: Dict[str, Dict] = {}  # normalized_name -> record
        self._company_list_loaded = False
        self._lead_cache: Dict[str, List[Dict]] = {}  # company_id -> leads
    
    def _load_companies(self, force_refresh: bool = False):
        """Load all companies into cache, including alias mappings."""
        if self._company_list_loaded and not force_refresh:
            return
        
        logger.info("Loading companies for fuzzy matching...")
        self._company_cache.clear()
        
        try:
            records = self.company_table.all()
            for record in records:
                name = record['fields'].get('Company Name', '')
                if name:
                    norm_name = normalize_company_name(name)
                    company_data = {
                        'id': record['id'],
                        'name': name,
                        'fields': record['fields']
                    }
                    
                    # Index by normalized name
                    self._company_cache[norm_name] = company_data
                    
                    # Also index by known aliases
                    name_lower = name.lower().strip()
                    if name_lower in COMPANY_ALIASES:
                        for alias in COMPANY_ALIASES[name_lower]:
                            alias_norm = normalize_company_name(alias)
                            if alias_norm not in self._company_cache:
                                self._company_cache[alias_norm] = company_data
                    
                    # Check if any alias key matches this company
                    for alias_key, aliases in COMPANY_ALIASES.items():
                        if alias_key in norm_name or norm_name in alias_key:
                            # Add all related aliases
                            for alias in aliases:
                                alias_norm = normalize_company_name(alias)
                                if alias_norm not in self._company_cache:
                                    self._company_cache[alias_norm] = company_data
            
            self._company_list_loaded = True
            logger.info(f"Loaded {len(records)} companies ({len(self._company_cache)} including aliases)")
            
        except Exception as e:
            logger.error(f"Error loading companies: {e}")
    
    def find_company(self, company_name: str, 
                    threshold: float = None) -> Optional[Dict]:
        """
        Find a company by fuzzy matching.
        
        Args:
            company_name: Company name to search for
            threshold: Override default threshold
        
        Returns:
            Dict with 'id', 'name', 'fields', 'match_score' or None
        """
        if not company_name:
            return None
        
        threshold = threshold or self.company_threshold
        self._load_companies()
        
        norm_query = normalize_company_name(company_name)
        
        # Exact match (after normalization)
        if norm_query in self._company_cache:
            result = self._company_cache[norm_query].copy()
            result['match_score'] = 1.0
            result['match_type'] = 'exact'
            logger.debug(f"Exact match: '{company_name}' -> '{result['name']}'")
            return result
        
        # Fuzzy match
        best_match = None
        best_score = 0.0
        
        for norm_name, record in self._company_cache.items():
            score = similarity_score(company_name, record['name'], normalize_company_name)
            
            if score > best_score:
                best_score = score
                best_match = record
        
        if best_score >= threshold:
            result = best_match.copy()
            result['match_score'] = best_score
            result['match_type'] = 'fuzzy'
            logger.info(f"Fuzzy match ({best_score:.2f}): '{company_name}' -> '{result['name']}'")
            return result
        
        logger.debug(f"No match for '{company_name}' (best score: {best_score:.2f})")
        return None
    
    def find_lead(self, lead_name: str, company_id: str = None,
                 threshold: float = None) -> Optional[Dict]:
        """
        Find a lead by fuzzy matching.
        
        Args:
            lead_name: Lead name to search for
            company_id: Optional company ID to scope search
            threshold: Override default threshold
        
        Returns:
            Dict with 'id', 'name', 'fields', 'match_score' or None
        """
        if not lead_name:
            return None
        
        threshold = threshold or self.lead_threshold
        
        # Get leads to search
        leads = []
        
        if company_id:
            # Search within company
            if company_id not in self._lead_cache:
                try:
                    records = self.leads_table.all(
                        formula=f"FIND('{company_id}', ARRAYJOIN({{Company}}))"
                    )
                    self._lead_cache[company_id] = [
                        {'id': r['id'], 'name': r['fields'].get('Lead Name', ''), 'fields': r['fields']}
                        for r in records
                    ]
                except Exception as e:
                    logger.error(f"Error loading leads for company: {e}")
                    self._lead_cache[company_id] = []
            
            leads = self._lead_cache[company_id]
        else:
            # Search all leads (expensive - avoid if possible)
            try:
                records = self.leads_table.all()
                leads = [
                    {'id': r['id'], 'name': r['fields'].get('Lead Name', ''), 'fields': r['fields']}
                    for r in records
                ]
            except Exception as e:
                logger.error(f"Error loading all leads: {e}")
                return None
        
        # Find best match
        norm_query = normalize_lead_name(lead_name)
        best_match = None
        best_score = 0.0
        
        for lead in leads:
            if not lead['name']:
                continue
            
            norm_name = normalize_lead_name(lead['name'])
            
            # Exact match
            if norm_query == norm_name:
                result = lead.copy()
                result['match_score'] = 1.0
                result['match_type'] = 'exact'
                return result
            
            # Fuzzy match
            score = similarity_ratio(norm_query, norm_name)
            
            if score > best_score:
                best_score = score
                best_match = lead
        
        if best_score >= threshold:
            result = best_match.copy()
            result['match_score'] = best_score
            result['match_type'] = 'fuzzy'
            logger.info(f"Fuzzy lead match ({best_score:.2f}): '{lead_name}' -> '{result['name']}'")
            return result
        
        return None
    
    def find_or_create_company(self, company_name: str, 
                               default_fields: Dict = None,
                               threshold: float = None) -> Tuple[str, bool]:
        """
        Find existing company or create new one.
        
        Args:
            company_name: Company name
            default_fields: Fields for new company if created
            threshold: Match threshold
        
        Returns:
            Tuple of (company_id, is_new)
        """
        # Try to find existing
        match = self.find_company(company_name, threshold)
        
        if match:
            return (match['id'], False)
        
        # Create new
        fields = {'Company Name': company_name}
        if default_fields:
            fields.update(default_fields)
        
        try:
            record = self.company_table.create(fields)
            company_id = record['id']
            
            # Add to cache
            norm_name = normalize_company_name(company_name)
            self._company_cache[norm_name] = {
                'id': company_id,
                'name': company_name,
                'fields': fields
            }
            
            logger.info(f"Created new company: {company_name}")
            return (company_id, True)
            
        except Exception as e:
            logger.error(f"Error creating company: {e}")
            raise
    
    def find_or_create_lead(self, lead_name: str, company_id: str,
                           default_fields: Dict = None,
                           threshold: float = None) -> Tuple[str, bool]:
        """
        Find existing lead or create new one.
        
        Args:
            lead_name: Lead name
            company_id: Company ID to link
            default_fields: Fields for new lead if created
            threshold: Match threshold
        
        Returns:
            Tuple of (lead_id, is_new)
        """
        # Try to find existing
        match = self.find_lead(lead_name, company_id, threshold)
        
        if match:
            return (match['id'], False)
        
        # Create new
        fields = {
            'Lead Name': lead_name,
            'Company': [company_id]
        }
        if default_fields:
            fields.update(default_fields)
        
        # Auto-classify persona from title if present
        title = fields.get('Title', '')
        if title and 'Persona Category' not in fields:
            try:
                from company_profile_utils import classify_persona
                fields['Persona Category'] = classify_persona(title)
            except ImportError:
                pass
        
        try:
            record = self.leads_table.create(fields)
            lead_id = record['id']
            
            # Invalidate lead cache for this company
            if company_id in self._lead_cache:
                del self._lead_cache[company_id]
            
            logger.info(f"Created new lead: {lead_name}")
            return (lead_id, True)
            
        except Exception as e:
            logger.error(f"Error creating lead: {e}")
            raise
    
    def clear_cache(self):
        """Clear all caches."""
        self._company_cache.clear()
        self._company_list_loaded = False
        self._lead_cache.clear()
        logger.info("Fuzzy matcher cache cleared")


# =============================================================================
# STANDALONE FUNCTIONS FOR QUICK USE
# =============================================================================

def companies_match(name1: str, name2: str, threshold: float = 0.85) -> bool:
    """Quick check if two company names likely refer to the same company."""
    return similarity_score(name1, name2, normalize_company_name) >= threshold


def leads_match(name1: str, name2: str, threshold: float = 0.85) -> bool:
    """Quick check if two lead names likely refer to the same person."""
    return similarity_score(name1, name2, normalize_lead_name) >= threshold


# =============================================================================
# TESTING
# =============================================================================

if __name__ == "__main__":
    # Test company normalization
    test_companies = [
        ("Pfizer Inc.", "Pfizer"),
        ("Johnson & Johnson", "Johnson and Johnson"),
        ("F. Hoffmann-La Roche Ltd", "F. Hoffmann La Roche"),
        ("BioNTech SE", "BioNTech"),
        ("Eli Lilly and Company", "Eli Lilly"),
        ("AbbVie Inc.", "AbbVie"),
        ("Novartis AG", "Novartis"),
        ("Sanofi S.A.", "Sanofi"),
        ("GlaxoSmithKline plc", "GlaxoSmithKline"),
        ("Moderna, Inc.", "Moderna"),
        ("Regeneron Pharmaceuticals, Inc.", "Regeneron"),
    ]
    
    print("Company Normalization Tests:")
    print("=" * 60)
    for original, expected in test_companies:
        normalized = normalize_company_name(original)
        match = normalized == expected.lower()
        status = "✓" if match else "✗"
        print(f"{status} '{original}' -> '{normalized}' (expected: '{expected.lower()}')")
    
    print("\nCompany Matching Tests:")
    print("=" * 60)
    match_tests = [
        ("Pfizer Inc.", "Pfizer", True),
        ("Pfizer Inc.", "Pfizer Corporation", True),
        ("BioNTech SE", "BioNTech", True),
        ("Moderna Inc", "Moderna, Inc.", True),
        ("Pfizer", "Novartis", False),
        ("Eli Lilly", "Eli Lily", True),  # Typo
        ("Johnson & Johnson", "Johnson and Johnson", True),
        ("AbbVie", "Abbvie Inc.", True),  # Case difference
    ]
    
    for name1, name2, expected_match in match_tests:
        score = similarity_score(name1, name2, normalize_company_name)
        actual_match = score >= 0.85
        status = "✓" if actual_match == expected_match else "✗"
        print(f"{status} '{name1}' vs '{name2}' -> {score:.2f} (match: {actual_match})")
    
    print("\nLead Name Normalization Tests:")
    print("=" * 60)
    lead_tests = [
        ("Dr. John Smith", "john smith"),
        ("Smith, John", "john smith"),
        ("John Smith Jr.", "john smith"),
        ("Prof. María García", "maria garcia"),
        ("John Smith, PhD", "john smith"),
    ]
    
    for original, expected in lead_tests:
        normalized = normalize_lead_name(original)
        match = normalized == expected
        status = "✓" if match else "✗"
        print(f"{status} '{original}' -> '{normalized}' (expected: '{expected}')")
