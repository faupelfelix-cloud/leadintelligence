#!/usr/bin/env python3
"""
Company Profile utilities for outreach generation.

Loads the Company Profile table from Airtable and builds context-aware
value propositions matched to each prospect's situation.

Also provides confidence-based field filtering so that low-confidence
data never reaches outreach prompts.
"""

import json
import logging
from typing import Dict, Optional, List, Tuple

logger = logging.getLogger(__name__)

# === CONFIDENCE FIELD MAPPING ===
# Maps Data Confidence JSON keys → Airtable field names that should be suppressed
# when confidence is low/unverified
CONFIDENCE_FIELD_MAP = {
    # Company Data Confidence keys → Company table fields
    'funding': ['Funding Stage', 'Total Funding', 'Last Funding Round'],
    'pipeline': ['Pipeline Stage', 'Lead Programs', 'Pipeline Details'],
    'therapeutic_areas': ['Therapeutic Areas'],
    'cdmo_partnerships': ['CDMO Partner', 'Manufacturing Partner', 'Manufacturing Status'],
    'employees': ['Employee Count', 'Company Size'],
    'revenue': ['Revenue', 'Annual Revenue'],
}

# Lead Data Confidence keys → Lead table fields
LEAD_CONFIDENCE_FIELD_MAP = {
    'email': ['Email'],
    'title': ['Title'],
    'linkedin': ['LinkedIn URL'],
}


def filter_by_confidence(fields: Dict, min_confidence: str = 'medium') -> Tuple[Dict, List[str]]:
    """Filter Airtable record fields based on Data Confidence scores.
    
    Returns a COPY of the fields dict with low-confidence fields cleared,
    plus a list of suppressed field names (for DO NOT MENTION warnings).
    
    Args:
        fields: Raw Airtable record fields
        min_confidence: Minimum confidence level to keep ('high' or 'medium')
        
    Returns:
        (filtered_fields, suppressed_topics) tuple
    """
    # Parse confidence JSON
    raw_conf = fields.get('Data Confidence', '')
    if not raw_conf:
        return fields, []  # No confidence data → pass through as-is
    
    try:
        confidence = json.loads(raw_conf)
    except (json.JSONDecodeError, TypeError):
        return fields, []
    
    # Determine which confidence levels are acceptable
    if min_confidence == 'high':
        acceptable = {'high'}
    else:
        acceptable = {'high', 'medium'}
    
    # Build filtered copy
    filtered = dict(fields)
    suppressed = []
    
    # Check company-level confidence fields
    for conf_key, field_names in CONFIDENCE_FIELD_MAP.items():
        conf_level = confidence.get(conf_key, '')
        if conf_level and conf_level not in acceptable:
            for field_name in field_names:
                if field_name in filtered and filtered[field_name]:
                    filtered[field_name] = '' if isinstance(filtered[field_name], str) else []
            suppressed.append(conf_key)
    
    # Check lead-level confidence fields
    for conf_key, field_names in LEAD_CONFIDENCE_FIELD_MAP.items():
        conf_level = confidence.get(conf_key, '')
        if conf_level and conf_level not in acceptable:
            for field_name in field_names:
                if field_name in filtered and filtered[field_name]:
                    filtered[field_name] = ''
            suppressed.append(conf_key)
    
    if suppressed:
        logger.debug(f"  Confidence filter suppressed: {', '.join(suppressed)}")
    
    return filtered, suppressed


def suppressed_to_do_not_mention(suppressed: List[str]) -> str:
    """Convert suppressed confidence keys to human-readable DO NOT MENTION text.
    
    Args:
        suppressed: List of confidence keys that were filtered out
        
    Returns:
        Warning string for the prompt, or empty string if nothing suppressed
    """
    if not suppressed:
        return ''
    
    topic_map = {
        'funding': 'specific funding rounds or amounts',
        'pipeline': 'specific pipeline stages (Phase 1/2/3)',
        'therapeutic_areas': 'specific therapeutic areas',
        'cdmo_partnerships': 'CDMO partnerships or manufacturing partners',
        'employees': 'specific employee counts or company size',
        'revenue': 'specific revenue figures',
        'email': 'email address (unverified)',
        'title': 'job title (unverified)',
    }
    
    topics = [topic_map.get(s, s) for s in suppressed]
    return "\n⚠️ DO NOT MENTION (low confidence / unverified): " + ", ".join(topics)


def load_company_profile(base) -> Dict:
    """Load Company Profile from Airtable.
    
    Args:
        base: pyairtable Base object
        
    Returns:
        Dict of profile fields, or empty dict if not found
    """
    try:
        table = base.table('Company Profile')
        records = table.all()
        if records:
            logger.info("✓ Company Profile loaded for outreach context")
            return records[0]['fields']
        else:
            logger.warning("Company Profile table is empty")
            return {}
    except Exception as e:
        logger.debug(f"Could not load Company Profile: {e}")
        return {}


def build_value_proposition(profile: Dict, company_fields: Dict = None, 
                            lead_title: str = '', campaign_type: str = '') -> str:
    """Build a targeted value proposition section for the outreach prompt.
    
    Matches Rezon's specific strengths to the prospect's situation based on
    their pipeline stage, funding, technology, geography, and the lead's role.
    
    Args:
        profile: Company Profile fields from Airtable
        company_fields: Prospect's company data (enriched)
        lead_title: The lead's job title
        campaign_type: Campaign type (Conference, Roadshow, general)
        
    Returns:
        String to inject into the outreach prompt
    """
    if not profile:
        # Fallback if Company Profile not loaded
        return """
YOUR COMPANY (Rezon Bio):
European biologics CDMO specializing in mammalian CHO cell culture for mAbs, bispecifics, and ADCs.
Target: Mid-size biotechs needing cost-efficient manufacturing support.
"""
    
    company_fields = company_fields or {}
    
    # === CONFIDENCE FILTERING ===
    # Strip low-confidence fields so value matching doesn't use unreliable data
    safe_fields, suppressed = filter_by_confidence(company_fields)
    
    # === CORE POSITIONING ===
    positioning = profile.get('Positioning Statement', 
        'EU/US cost leader for New Biological Entities (NBEs)')
    
    # === DETERMINE PROSPECT SEGMENT ===
    # Use pipeline stage and funding to pick the right value angle
    segment_pitch = _match_segment_pitch(profile, safe_fields)
    
    # === PERSONA-SPECIFIC ANGLE ===
    persona_angle = _match_persona_angle(profile, lead_title)
    
    # === SELECT PROOF POINTS ===
    # Pick 2-3 most relevant proof points based on prospect context
    proof_points = _select_proof_points(profile, safe_fields)
    
    # === PAIN POINTS THEY LIKELY HAVE ===
    pain_points = _match_pain_points(profile, safe_fields)
    
    # === KEY MESSAGING THEMES ===
    messaging = profile.get('Key Messaging Themes', '')
    # Extract just the primary messages, not the full block
    primary_msgs = []
    if messaging:
        for line in messaging.split('\n'):
            line = line.strip()
            if line and line[0].isdigit() and '.' in line[:3]:
                primary_msgs.append(line.split('.', 1)[1].strip())
    
    # === DIFFERENTIATION ===
    diff = profile.get('Differentiation vs Competitors', '')
    # Pick the most relevant competitive angle based on prospect geography
    diff_angle = _match_differentiation(diff, safe_fields)
    
    # === WEAKNESSES / HONESTY ===
    weaknesses = profile.get('Key Weaknesses', '')
    
    # === BUILD THE PROMPT SECTION ===
    section = f"""
YOUR COMPANY — REZON BIO:
{positioning}

WHAT'S IN IT FOR THEM — match ONE of these to their situation:
{segment_pitch}

{persona_angle}

PROOF POINTS (weave ONE in naturally, don't list them):
{proof_points}

{pain_points}

COMPETITIVE ANGLE (use if relevant):
{diff_angle}

KEY MESSAGE (pick the most fitting one):
{chr(10).join('- ' + m for m in primary_msgs[:4]) if primary_msgs else '- European quality and trust at competitive pricing'}

⚠️ HONESTY GUARDRAILS — do NOT overpromise on:
{weaknesses[:300] if weaknesses else '- We are building our CDMO track record — be authentic about this'}
"""
    
    return section


def build_outreach_philosophy() -> str:
    """Return the outreach philosophy section for prompts.
    
    Based on research into what makes CDMO outreach convert:
    - Lead with THEIR situation, not your capabilities
    - Answer "why should I take this meeting?" from their perspective
    - Use timeline hooks (tied to their stage/milestone)
    - One concrete proof point, not a features list
    - Low-barrier CTA
    """
    return """
═══════════════════════════════════════════════════════════
OUTREACH PHILOSOPHY — CRITICAL:
═══════════════════════════════════════════════════════════

Your #1 job: answer "Why should I take this meeting?" from THEIR perspective.

1. LEAD WITH THEIR WORLD — don't start with "We are a CDMO..."
   Start with an observation about their work, stage, or challenge.
   
2. CONNECT ONE REZON STRENGTH TO THEIR SITUATION
   BAD: "We offer cost-efficient manufacturing" (generic)
   GOOD: "Scaling a bispecific for Phase 3 in the EU? That's exactly the stage where our setup makes the biggest cost difference" (specific + relevant)

3. ONE PROOF POINT, NOT A FEATURES LIST
   BAD: "We have FDA/EMA approval, Sandoz qualification, state-of-art facilities, 2000L bioreactors..."
   GOOD: "Our facilities were qualified by Sandoz, so the regulatory bar is already set" (one credible signal)

4. TIMELINE HOOK > PROBLEM HOOK
   BAD: "Are you facing manufacturing challenges?"
   GOOD: "With your Phase 3 readout approaching, locking in manufacturing timelines becomes critical"

5. LOW-BARRIER CTA — suggest a conversation, not a capabilities presentation
   BAD: "Can we schedule a 60-minute capabilities review?"
   GOOD: "Would it make sense to compare notes over a quick call?"

6. SOUND LIKE A KNOWLEDGEABLE PEER, NOT A SALESPERSON
   Write as if you're an industry colleague who spotted something relevant.

═══════════════════════════════════════════════════════════
STYLE RULES:
═══════════════════════════════════════════════════════════
- Natural, human language — slightly imperfect is fine
- NO bullet lists — weave points into sentences
- NO **bold** markup — clean formatting only
- Show you know them, don't tell them their situation
- Pick ONE relevant detail max from their company data
- Keep messages SHORT — less is more
- NEVER mention specific funding amounts or rounds
- NEVER claim specific pipeline stages unless provided as verified
- NEVER mention CDMO partnerships or manufacturing decisions
"""


# === INTERNAL HELPERS ===

def _match_segment_pitch(profile: Dict, company_fields: Dict) -> str:
    """Pick the right value angle based on prospect's stage."""
    
    # Try to determine their segment from enriched data
    pipeline_stages = company_fields.get('Pipeline Stage', [])
    if isinstance(pipeline_stages, str):
        pipeline_stages = [pipeline_stages]
    pipeline_str = ' '.join(pipeline_stages).lower() if pipeline_stages else ''
    
    funding = (company_fields.get('Funding Stage', '') or '').lower()
    
    # Value Proposition by Segment from Company Profile
    segments = []
    
    if any(x in pipeline_str for x in ['phase 3', 'phase iii', 'commercial', 'marketed', 'approved']):
        segments.append("LATE-STAGE / COMMERCIAL: Fast tech transfer, competitive commercial pricing, reliable batch execution. They need a manufacturing partner who can deliver on timeline and cost for commercialization.")
    
    if any(x in pipeline_str for x in ['phase 2', 'phase ii', 'phase 1', 'phase i', 'clinical']):
        segments.append("CLINICAL STAGE: Cost-efficient clinical supply services + scaling expertise. They need a partner who can grow with them from clinical through commercial without switching CDMOs.")
    
    if any(x in funding for x in ['series b', 'series c', 'ipo', 'public']):
        segments.append("FUNDED & SCALING: They have capital to invest in manufacturing. Cost optimization matters because they're deploying capital efficiently. EU manufacturing can be significantly cheaper than US alternatives.")
    
    if not segments:
        # Default based on general profile
        segments.append("GENERAL: Cost-competitive European manufacturing for mid-size biotechs. Lower cost than Western CDMOs, higher trust than APAC. Agile mid-size partner, not a bureaucratic big CDMO.")
    
    return '\n'.join(segments)


def _match_persona_angle(profile: Dict, lead_title: str) -> str:
    """Pick messaging angle based on the lead's role."""
    title_lower = (lead_title or '').lower()
    
    if any(x in title_lower for x in ['ceo', 'founder', 'chief executive', 'president', 'coo']):
        return """PERSONA: C-SUITE / FOUNDER
They care about: cost efficiency, speed to market, reliable partnership, risk mitigation
They DON'T want: technical deep-dives, capability lists
Approach: Business impact, strategic fit, trustworthiness"""
    
    elif any(x in title_lower for x in ['cmc', 'manufacturing', 'technical', 'process', 'production']):
        return """PERSONA: CMC / MANUFACTURING LEADER
They care about: tech transfer speed, regulatory compliance, facility quality, scale capability
They DON'T want: vague promises, marketing fluff
Approach: Practical specifics, regulatory credentials (FDA/EMA), Sandoz qualification"""
    
    elif any(x in title_lower for x in ['supply', 'procurement', 'sourcing']):
        return """PERSONA: SUPPLY CHAIN / PROCUREMENT
They care about: cost, capacity availability, lead times, supply reliability, dual sourcing
They DON'T want: technical jargon, capability presentations
Approach: Cost competitiveness, EU supply security, capacity availability"""
    
    elif any(x in title_lower for x in ['vp', 'svp', 'evp', 'director', 'head']):
        return """PERSONA: VP / DIRECTOR LEVEL
They care about: proven track record, practical solutions, data-driven decisions
They DON'T want: fluff, overpromising, aggressive sales tactics
Approach: Evidence-based, reference relevant experience, respect their expertise"""
    
    elif any(x in title_lower for x in ['program', 'project', 'lead']):
        return """PERSONA: PROGRAM / PROJECT LEAD
They care about: timelines, deliverables, hands-on support, communication quality
They DON'T want: high-level strategy talk, corporate presentations
Approach: Practical, specific to their program needs, offer to discuss details"""
    
    else:
        return """PERSONA: BIOTECH PROFESSIONAL
They appreciate: Data, facts, evidence, practical solutions
They dislike: Fluff, excessive marketing speak, overpromising
Approach: Professional, specific, value-driven"""


def _select_proof_points(profile: Dict, company_fields: Dict) -> str:
    """Select 2-3 most relevant proof points for this prospect."""
    
    tech = company_fields.get('Technology Platform', [])
    if isinstance(tech, str):
        tech = [tech]
    tech_str = ' '.join(tech).lower() if tech else ''
    
    therapeutic = company_fields.get('Therapeutic Areas', [])
    if isinstance(therapeutic, str):
        therapeutic = [therapeutic]
    therapeutic_str = ' '.join(therapeutic).lower() if therapeutic else ''
    
    location = (company_fields.get('Location/HQ', '') or '').lower()
    
    proof_points = []
    
    # Always relevant
    proof_points.append("Qualified by Sandoz — sets a high regulatory bar for facility and process standards")
    
    # Regulatory — pick based on geography
    if any(x in location for x in ['us', 'united states', 'america', 'boston', 'san francisco', 'california']):
        proof_points.append("FDA and EMA approved facilities — dual filing capability for US+EU")
    elif any(x in location for x in ['europ', 'germany', 'france', 'uk', 'swiss', 'netherlands', 'belgium']):
        proof_points.append("EMA approved with FDA track record — strong European regulatory foundation")
    else:
        proof_points.append("Global regulatory compliance — FDA, EMA, and Anvisa approved")
    
    # Technology match
    if any(x in tech_str for x in ['biosimilar']):
        proof_points.append("Proven biosimilar development track record — gene to market experience")
    elif any(x in tech_str for x in ['bispecific', 'adc', 'antibody-drug']):
        proof_points.append("Specialized in complex mammalian molecules including bispecifics and ADCs")
    elif any(x in tech_str for x in ['mab', 'monoclonal', 'antibod']):
        proof_points.append("Core expertise in monoclonal antibody manufacturing at 500-2000L scale")
    else:
        proof_points.append("State-of-the-art mammalian cell culture infrastructure (CHO platform)")
    
    return '\n'.join(f"- {pp}" for pp in proof_points[:3])


def _match_pain_points(profile: Dict, company_fields: Dict) -> str:
    """Match likely pain points based on prospect profile."""
    
    location = (company_fields.get('Location/HQ', '') or '').lower()
    mfg_status = (company_fields.get('Manufacturing Status', '') or '').lower()
    
    pain_points = []
    
    if any(x in location for x in ['us', 'united states', 'america']):
        pain_points.append("US biotechs face high manufacturing costs domestically — EU manufacturing can cut costs significantly without sacrificing quality")
    
    if any(x in location for x in ['europ', 'germany', 'france', 'uk', 'swiss']):
        pain_points.append("EU biotechs need regulatory-aligned manufacturing close to home — avoids geopolitical supply chain risks from APAC outsourcing")
    
    if 'no public partner' in mfg_status or 'no partner' in mfg_status:
        pain_points.append("They likely don't have a manufacturing partner yet — first-mover opportunity to become their CDMO of choice")
    elif 'has partner' in mfg_status:
        pain_points.append("They have a partner but may be looking for alternatives or second source — approach as complementary option")
    
    if not pain_points:
        pain_points.append("Mid-size biotechs often get deprioritized by large CDMOs — Rezon offers the attention and agility of a mid-size partner")
    
    return "THEIR LIKELY PAIN POINTS (reference indirectly, don't state them bluntly):\n" + '\n'.join(f"- {pp}" for pp in pain_points[:2])


def _match_differentiation(diff_text: str, company_fields: Dict) -> str:
    """Pick the most relevant competitive differentiation angle."""
    
    location = (company_fields.get('Location/HQ', '') or '').lower()
    
    if any(x in location for x in ['china', 'india', 'korea', 'japan', 'asia', 'singapore']):
        return "VS. APAC: Higher trust (EU location), proximity to US/EU markets, no geopolitical supply chain risk, easier communication"
    
    if any(x in location for x in ['us', 'united states', 'america']):
        return "VS. US CDMOs: Significantly lower cost for equivalent quality, EU regulatory footprint for global filing, agile mid-size partner"
    
    if any(x in location for x in ['switzerland', 'germany', 'france', 'uk', 'netherlands', 'belgium']):
        return "VS. Western EU CDMOs (Lonza, Samsung, Fujifilm): Lower cost without compromising quality, faster decision-making, more personalized service"
    
    return "KEY: European quality and trust at competitive pricing — lower cost than Western CDMOs, higher trust than APAC"
