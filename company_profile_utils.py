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
Best cost-for-value in the EU: lower half of cost benchmarks, multinational pharma validated, 
FDA approved, 95% batch success rate. Quality is uncompromised.
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
    - Give a SPECIFIC, DIFFERENTIATED reason — not a vague offer
    - Sound like a real person, not a polished AI
    """
    return """
═══════════════════════════════════════════════════════════
OUTREACH PHILOSOPHY — CRITICAL:
═══════════════════════════════════════════════════════════

Your #1 job: answer "Why should I take this meeting?" from THEIR perspective.
This means two things: (a) sound like a real human, AND (b) give a SPECIFIC 
reason why Rezon Bio is interesting to THEM specifically, not just "we're an EU CDMO".

1. OPEN CASUALLY — don't narrate their company back to them
   
   BAD: "With Merck's expanding oncology portfolio requiring global supply resilience..."
   (Sounds like you copied from their annual report. Presumptuous and stiff.)
   
   GOOD: "Saw your team's been busy with Phase 3 readouts — exciting times."
   (Simple. Human. Shows you pay attention without showing off.)
   
   BAD: "I noticed your company has raised a Series B to advance your bispecific pipeline..."
   (Stacking facts to prove you did homework. Feels robotic.)
   
   GOOD: "Congrats on the pipeline progress — sounds like things are moving fast."
   (Light touch. One reference. Move on to the point.)

2. MAKE THE VALUE PROPOSITION SHARP AND SPECIFIC — this is the core of the message
   The opener should be humble and casual, but the VALUE PROP should be concrete
   and differentiated. Don't water it down to "we're EU-based, might be worth a look."
   
   BAD: "We're an EU-based option that might be worth a look" (so what? There are dozens)
   BAD: "We offer cost-efficient manufacturing" (generic, every CDMO says this)
   
   GOOD: "We sit in the lower half of EU cost benchmarks but our facilities were 
   qualified by Sandoz and we hold FDA approvals — so it's pharma-grade quality 
   without the premium CDMO price tag."
   (This answers: why YOU specifically? What makes you DIFFERENT from other EU CDMOs?)
   
   GOOD: "Most EU CDMOs either charge a premium or cut corners. We managed to get 
   to competitive pricing while keeping a 95% batch success rate and multinational 
   pharma validation — that's the bit that's hard to find."
   (Positions against the category, explains WHY the cost is interesting.)
   
   The key: casual opener + sharp value prop. Humble about how you open,
   confident about what you actually bring to the table.

3. ONE PROOF POINT, WOVEN IN NATURALLY
   BAD: "We have FDA/EMA approval, Sandoz qualification, state-of-art facilities, 2000L bioreactors..."
   GOOD: "Sandoz qualified our facilities early on, so the regulatory groundwork is done"
   (Casual delivery of a strong credential. Don't stack multiple proof points.)

4. TIMELINE HOOK > GENERIC PROBLEM HOOK
   BAD: "Are you facing manufacturing challenges?"
   GOOD: "With Phase 3 coming up, manufacturing timelines start to get very real"

5. LOW-BARRIER CTA — suggest a conversation, not a pitch
   BAD: "Can we schedule a 60-minute capabilities review?"
   BAD: "Would it make sense to compare notes on EU supply chain strategies?" (consultant-speak)
   GOOD: "Happy to chat if useful — no agenda, just a conversation."
   GOOD: "Would a quick call make sense? Even 15 minutes to see if there's a fit."

═══════════════════════════════════════════════════════════
STYLE RULES:
═══════════════════════════════════════════════════════════
- The OPENER should be casual and human — slightly humble, not over-polished
- The VALUE PROP should be specific and confident — this is where you earn the meeting
- Combination: "Hey, noticed [light reference]. [Sharp, specific reason Rezon is different]. [Easy CTA]."
- Write like a real person typing an email, not a marketing team crafting copy
- NO bullet lists — weave points into sentences
- NO **bold** markup — clean formatting only
- DON'T over-demonstrate your research — one light reference is enough
  BAD: "With your company's recent Series C and three programs advancing into late-stage..."
  GOOD: "Saw your Phase 3 news — congrats."
- NEVER mention specific funding amounts or rounds
- NEVER claim specific pipeline stages unless provided as verified
- NEVER mention CDMO partnerships or manufacturing decisions
- NEVER claim we compete with APAC on cost — we don't. Our positioning is best cost-for-value IN THE EU.
- NEVER use "cost-competitive with APAC/Asia" or similar — this is factually wrong

═══════════════════════════════════════════════════════════
BANNED PHRASES — classic AI signatures, never use these:
═══════════════════════════════════════════════════════════
These phrases instantly signal the message was AI-generated. NEVER use them:
- "I hope this message finds you well"
- "I wanted to reach out because..."
- "I'd love to explore..."
- "I'd love to connect..."  
- "I came across your..." / "I came across [company]..."
- "I was impressed by..."
- "Let's explore synergies"
- "Explore potential synergies"
- "I believe there's a great opportunity..."
- "Leverage" (as a verb in outreach)
- "Streamline your..."
- "Take your [X] to the next level"
- "In today's rapidly evolving..."
- "Navigating the complexities of..."
- "At the forefront of..."
- "Cutting-edge" / "Best-in-class" / "World-class"
- "Unlock" / "Unleash" / "Empower"
- "Seamless" / "Seamlessly"
- "Circle back" / "Touch base"
- "Deep dive" (in outreach context)
- "I imagine" (as opener — presumptuous)
- "Increasingly complex" / "Expanding portfolio"
- "Supply resilience" / "Dual sourcing strategy"
- "Compare notes on [buzzword] strategies"
- "Discuss alignment" / "Explore alignment"

Instead, write like you'd actually talk to someone at a conference bar.

Words/phrases that WORK: "noticed", "seems like", "might be worth", "happy to chat",
"if it's useful", "curious whether", "no pressure", "even 15 minutes"
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
    
    # Check if Company Profile has custom segment messaging
    segment_messaging = profile.get('Value Proposition by Segment', '') if profile else ''
    
    # Value Proposition by Segment
    segments = []
    
    if any(x in pipeline_str for x in ['phase 3', 'phase iii', 'commercial', 'marketed', 'approved']):
        segments.append("LATE-STAGE / COMMERCIAL: Fast tech transfer, competitive commercial pricing, reliable batch execution. They need a manufacturing partner who delivers on timeline and cost — validated by multinational pharma, not just another mid-tier CDMO.")
    
    if any(x in pipeline_str for x in ['phase 2', 'phase ii', 'phase 1', 'phase i', 'clinical']):
        segments.append("CLINICAL STAGE: Cost-efficient clinical supply + scaling expertise. They need a partner who can grow with them from clinical through commercial without switching CDMOs. Our cost structure means their runway stretches further.")
    
    if any(x in funding for x in ['series b', 'series c', 'ipo', 'public']):
        segments.append("FUNDED & SCALING: They have capital to deploy efficiently. Our position in the lower half of EU cost benchmarks means they get pharma-grade quality without overpaying for a premium CDMO brand name.")
    
    if not segments:
        # Default — pull from Company Profile if available
        if segment_messaging:
            segments.append(f"GENERAL:\n{segment_messaging[:400]}")
        else:
            segments.append("GENERAL: Best cost-for-value in the EU — lower half of cost benchmarks, yet multinational pharma validated, FDA approved, 95% batch success rate. Agile mid-size partner, not a bureaucratic big CDMO.")
    
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
        pain_points.append("US biotechs often overpay for manufacturing — EU-based CDMOs like Rezon can offer the same quality at a significantly lower cost point")
    
    if any(x in location for x in ['europ', 'germany', 'france', 'uk', 'swiss']):
        pain_points.append("EU biotechs benefit from an EU-based CDMO: regulatory alignment, proximity, and no cross-border complexity for EMA filings")
    
    if 'no public partner' in mfg_status or 'no partner' in mfg_status:
        pain_points.append("They likely don't have a manufacturing partner yet — first-mover opportunity to become their CDMO of choice")
    elif 'has partner' in mfg_status:
        pain_points.append("They have a partner but may be looking for alternatives or second source — approach as complementary option")
    
    if not pain_points:
        pain_points.append("Mid-size biotechs often get deprioritized by large CDMOs — Rezon offers the attention and agility of a mid-size partner with pharma-validated quality")
    
    return "THEIR LIKELY PAIN POINTS (reference indirectly, don't state them bluntly):\n" + '\n'.join(f"- {pp}" for pp in pain_points[:2])


def _match_differentiation(diff_text: str, company_fields: Dict) -> str:
    """Pick the most relevant competitive differentiation angle."""
    
    location = (company_fields.get('Location/HQ', '') or '').lower()
    
    # If Company Profile has differentiation text, use it directly
    if diff_text:
        # Try to pick relevant section based on prospect geography
        if any(x in location for x in ['us', 'united states', 'america']):
            for line in diff_text.split('\n'):
                if any(x in line.lower() for x in ['us cdmo', 'american', 'us-based']):
                    return f"COMPETITIVE ANGLE:\n{line.strip()}"
        if any(x in location for x in ['switzerland', 'germany', 'france', 'uk']):
            for line in diff_text.split('\n'):
                if any(x in line.lower() for x in ['lonza', 'samsung', 'fuji', 'western eu', 'premium']):
                    return f"COMPETITIVE ANGLE:\n{line.strip()}"
        # Return first meaningful section if no geo match
        return f"COMPETITIVE ANGLE:\n{diff_text[:400]}"
    
    # Fallback differentiation based on geography
    if any(x in location for x in ['us', 'united states', 'america']):
        return "VS. US CDMOs: Lower half of EU cost benchmarks — significantly cheaper than US manufacturing for equivalent pharma-validated quality. FDA approved, so dual filing is straightforward."
    
    if any(x in location for x in ['switzerland', 'germany', 'france', 'uk', 'netherlands', 'belgium']):
        return "VS. Premium EU CDMOs (Lonza, Samsung, Fujifilm): Same pharma-grade quality, but lower half of cost benchmarks. Faster decision-making, more personalized service, no 'small fish in a big pond' problem."
    
    return "KEY DIFFERENTIATOR: Best cost-for-value in the EU — lower half of cost benchmarks, yet multinational pharma validated, FDA approved, 95% batch success rate. Quality is uncompromised; it's the overhead and pricing model that's different."
