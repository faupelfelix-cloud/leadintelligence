#!/usr/bin/env python3
"""
Company Profile utilities for outreach generation.

Loads the Company Profile table from Airtable and builds context-aware
value propositions matched to each prospect's situation.

Also provides:
- Confidence-based field filtering (low-confidence data never reaches prompts)
- Persona classification (maps lead titles to persona buckets)
- Persona-specific messaging (loads value drivers from Persona Messaging table)
"""

import json
import logging
import re
from typing import Dict, Optional, List, Tuple

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
# PERSONA CLASSIFICATION
# ═══════════════════════════════════════════════════════════════
# Maps lead titles to persona buckets. The buckets determine which
# value drivers, proof points, and tone to use in outreach.

PERSONA_BUCKETS = {
    'C-Level / Owner': {
        'keywords': [
            'ceo', 'chief executive', 'founder', 'co-founder', 'cofounder',
            'owner', 'managing director', 'president', 'chairman', 'chairwoman',
            'general manager', 'coo', 'chief operating',
            'chief strategy', 'chief corporate', 'chief technology', 'cto',
            'chief people', 'chief human', 'chro', 'chief legal', 'clo',
            'chief patient', 'chief information', 'cio', 'cido',
            'board director', 'evp', 'executive vice president',
            'svp', 'senior vice president',
            'head of development', 'head of technical', 'head of pipeline',
            'vp,', 'vp ', 'vice president',
        ],
        'description': 'Top decision-makers who care about strategic fit, risk, and business impact',
    },
    'Operations / Manufacturing': {
        'keywords': [
            'cmc', 'manufacturing', 'production', 'operations', 'process development',
            'process science', 'upstream', 'downstream', 'bioprocess', 'tech transfer',
            'technology transfer', 'technical operations', 'plant manager', 'site director',
            'fill finish', 'drug substance', 'drug product', 'ppq', 'validation', 'gmp',
            'plant director', 'site leader', 'plant network', 'site head',
            'sterile', 'aseptic', 'formulation', 'lyophilization',
            'maintenance', 'engineering manager', 'industrial engineer',
            'continuous improvement', 'lean', 'six sigma',
            'packaging', 'device assembly', 'msat',
            'process expert', 'process and procedures', 'process engineer',
            'technical director', 'technical manager', 'technical design',
            'technical development', 'technology excellence', 'inspection readiness',
            'chemist', 'api ', 'active pharmaceutical',
            'dsp development', 'usp ', 'small molecule',
            'biologics manufacturing', 'biomanufacturing',
            'engineer', 'engineering', 'materials science',
        ],
        'description': 'Hands-on leaders who run manufacturing and care about execution capability',
    },
    'Quality / Regulatory': {
        'keywords': [
            'quality', 'qa', 'qc', 'quality assurance', 'quality control',
            'regulatory', 'compliance', 'pharmacovigilance', 'gxp',
            'qualified person', 'regulatory affairs', 'cqo', 'chief quality',
            'inspection', 'auditor', 'audit', 'gmp compliance',
            'deviation', 'capa', 'batch release',
        ],
        'description': 'Guardians of compliance who need to trust your regulatory track record',
    },
    'Supply Chain / Procurement': {
        'keywords': [
            'supply chain', 'procurement', 'sourcing', 'purchasing', 'vendor',
            'supplier', 'logistics', 'supply management', 'category manager',
            'strategic sourcing', 'cpo', 'chief procurement',
            'external supply', 'supply planning', 'supply base',
            'supply security', 'supply strategy', 'supply network',
            'category strategy', 'category director', 'category lead',
            'global category', 'sr. category', 'commodity',
            'warehousing', 'warehouse', 'partnership & external',
            'co-development',
        ],
        'description': 'Cost and reliability focused — they evaluate CDMOs on price, capacity, and risk',
    },
    'Business Development / Commercial': {
        'keywords': [
            'business development', 'commercial', 'marketing', 'sales',
            'partnerships', 'alliance', 'licensing', 'market access',
            'chief commercial', 'cbo', 'chief business',
            'account manager', 'account director', 'account executive',
            'account specialist', 'key account', 'national account',
            'global account', 'territory manager', 'territory representative',
            'brand manager', 'communications', 'corporate communications',
            'product launch', 'new product', 'launch lead',
            'strategy office', 'enterprise strategy',
        ],
        'description': 'Growth-focused leaders thinking about partnerships and market strategy',
    },
    'R&D / Scientific': {
        'keywords': [
            'r&d', 'research', 'scientific', 'scientist', 'cso', 'chief scientific',
            'discovery', 'preclinical', 'biology', 'pharmacology', 'head of research',
            'clinical development', 'cmo', 'chief medical', 'medical director',
            'analytical', 'bioanalytical', 'analytical sciences',
            'immunology', 'virology', 'oncology', 'biotherapeutics',
            'protein', 'antibody', 'biologics development', 'biologic',
            'biosimilar', 'flow cytometry', 'bioinformatics', 'data science',
            'pharm sci', 'pharm.', 'pharmaceutical science',
            'allosteric', 'modulation', 'drug development',
            'early phase', 'toxicolog', 'study director',
            'cell line', 'cell culture', 'assay',
            'medical affairs', 'medical advisor', 'heor',
            'health economics', 'value & evidence',
            'clinical trial', 'clinical supply', 'clinical systems',
            'post-doc', 'postdoc', 'fellow',
        ],
        'description': 'Science-driven leaders — less focused on cost, more on capability and innovation',
    },
    'Program / Project Management': {
        'keywords': [
            'program', 'project', 'portfolio', 'pmo',
            'project manager', 'program director', 'program lead',
            'implementation manager', 'planning manager',
            'end-to-end', 'product planning',
        ],
        'description': 'Execution-focused — timelines, deliverables, hands-on coordination',
    },
    'Finance / Investment': {
        'keywords': [
            'cfo', 'chief financial', 'finance', 'investor relations',
            'controller', 'treasurer', 'financial planning',
            'accounting', 'accounts payable', 'accounts receivable',
            'investment', 'venture', 'analyst', 'financial analyst',
        ],
        'description': 'Numbers-driven — cost efficiency, capital deployment, ROI',
    },
}

# Hardcoded fallback persona messaging — used when Persona Messaging table
# is not available. This is the MINIMUM; the Airtable table should have
# much richer content.
DEFAULT_PERSONA_MESSAGING = {
    'C-Level / Owner': {
        'Value Drivers': 'Strategic fit, risk mitigation, speed to market, reliable partnership, cost efficiency at scale, partner who treats them as priority (not small fish at big CDMO)',
        'Proof Points': 'Multinational pharma validated (Sandoz), 95% batch success rate, FDA+EMA+Anvisa approved, fast decision-making as mid-size partner, lower half of EU cost benchmarks',
        'Tone': 'Strategic, concise, peer-to-peer. Lead with business impact. Respect their time — these are the busiest people. Get to the point fast.',
        'What They Dont Want': 'Technical deep-dives, capability lists, feature dumps, aggressive sales tactics, long emails',
        'Example Angles': """- Strategic fit: "For a biotech at your stage, having a manufacturing partner who can scale with you matters — we're set up for exactly that"
- De-risking: "Locking in a validated manufacturing partner before Phase 3 removes a big variable from the equation"
- Speed: "Our mid-size setup means faster decisions — no 6-month onboarding process"
- Partner priority: "At a big CDMO, a program your size might not get priority. With us, every client gets direct access to leadership"
- Cost efficiency: "We sit in the lower half of EU cost benchmarks — your runway stretches further without compromising quality"
- Capital efficiency: "For a funded biotech, manufacturing spend is one of the biggest line items — getting it right matters"
Keep messages SHORT for C-level. They skim.""",
    },
    'Operations / Manufacturing': {
        'Value Drivers': 'PPQ experience and campaign execution, tech transfer speed and methodology, facility fit (500-2000L bioreactors, mammalian CHO), batch success rate (95%), GMP track record, operational flexibility, ability to handle complex molecules',
        'Proof Points': 'Sandoz-qualified facilities (sets operational bar), 95% batch success rate, proven PPQ campaign execution, 500-2000L single-use and stainless steel bioreactors, structured tech transfer process with clear milestones, experience with mAbs/bispecifics/ADCs',
        'Tone': 'Technical and practical. These people live in the details — they want specifics, not marketing. Lead with execution capability and relevant operational experience. Be concrete about what your facility can do.',
        'What They Dont Want': 'Marketing fluff, vague promises, cost-only arguments, generic "we are a CDMO" pitches. They need to trust your EXECUTION capability before anything else.',
        'Example Angles': """- PPQ: "We've run PPQ campaigns for mammalian products — happy to walk through our approach and timelines"
- Tech transfer: "Our tech transfer process is structured with clear milestones — typically X months from kick-off to GMP"
- Facility fit: "Our 500-2000L setup handles [their molecule type] well — we've done similar molecules"
- Batch success: "95% batch success rate across our GMP campaigns — we track it closely"
- Scale-up: "If you're moving from pilot to GMP, that transition is something we've done multiple times"
- Complex molecules: "Bispecifics/ADCs have specific manufacturing challenges — our team has hands-on experience with those"
- Operational flexibility: "As a mid-size CDMO, we can adapt to your process rather than forcing you into a platform"
Pick the angle that best matches their specific operational situation.""",
    },
    'Quality / Regulatory': {
        'Value Drivers': 'Regulatory track record (FDA/EMA/Anvisa approvals), inspection history, quality systems maturity, deviation handling capabilities, qualified person availability, documentation standards, comparability study experience',
        'Proof Points': 'FDA approved, EMA approved, Anvisa approved, Sandoz qualification (multinational pharma-level quality bar), clean inspection history, 95% batch success rate as quality indicator, established quality management system',
        'Tone': 'Precise, evidence-based, no exaggeration. Quality people are allergic to overclaiming. Understate rather than overstate. They respect facts and track records, not promises.',
        'What They Dont Want': 'Marketing language, unsubstantiated claims, cost arguments (cost is irrelevant if quality is not proven first), aggressive sales language',
        'Example Angles': """- Regulatory approvals: "We hold FDA, EMA, and Anvisa approvals — happy to share our inspection history"
- Quality systems: "Our quality systems were built to Sandoz standards — the bar was set high from day one"
- Inspection readiness: "We've had clean regulatory inspections — can walk you through our track record"
- Documentation: "Our documentation and batch record standards are built for multinational pharma audits"
- Deviation handling: "We have a mature CAPA system — deviation rates and resolution times are something we track closely"
- Comparability: "If you need comparability studies for tech transfer, we've done that for several programs"
- Qualified Person: "We have experienced QPs in-house for EU batch release"
Pick the angle that best matches their specific quality/regulatory concern.""",
    },
    'Supply Chain / Procurement': {
        'Value Drivers': 'Cost competitiveness (lower half of EU benchmarks), capacity availability and flexibility, lead times, supply reliability and continuity, dual sourcing value, transparent pricing',
        'Proof Points': 'Lower half of EU cost benchmarks with pharma-grade quality, reliable capacity, EU-based manufacturing, flexible batch scheduling, long-term partnership pricing, multinational pharma validated (not a cheap/risky option)',
        'Tone': 'Data-driven, commercial, straightforward. Procurement speaks numbers and risk. Give concrete cost positioning and capacity facts.',
        'What They Dont Want': 'Technical jargon, capability presentations, vague "partnership" language, science deep-dives. They want facts on cost, capacity, and reliability.',
        'Example Angles': """- Cost position: "We sit in the lower half of EU CDMO cost benchmarks — pharma-grade quality without the premium brand markup"
- Dual sourcing: "If you're looking to diversify your manufacturing base, an EU-based second source could make sense"
- Capacity: "We have capacity available for [timeframe] — happy to discuss batch scheduling"
- Total cost: "When you factor in logistics, regulatory alignment, and communication overhead, our total cost of ownership is very competitive"
- Pricing transparency: "Our pricing model is transparent — no hidden costs or surprise upcharges"
- Supply security: "EU-based manufacturing means shorter supply chains and less exposure to shipping disruptions"
This is the ONE persona where leading with cost makes sense. Be specific about cost positioning.""",
    },
    'Business Development / Commercial': {
        'Value Drivers': 'Speed to market, manufacturing scalability for commercial launch, global supply strategy (EU base + FDA/EMA/Anvisa for global filing), partnership flexibility, COGS competitiveness for market positioning',
        'Proof Points': 'FDA+EMA+Anvisa for global filing, clinical-to-commercial continuity, competitive pricing for market-competitive COGS, agile mid-size partner for fast decisions',
        'Tone': 'Strategic and forward-looking. Think about their commercial goals and how manufacturing fits into their launch and partnership strategy.',
        'What They Dont Want': 'Deep technical details, quality system descriptions, operational specifics. They want to know how you help them get to market and compete.',
        'Example Angles': """- Global filing: "With FDA, EMA, and Anvisa approvals, your manufacturing is already set up for global filing — no second facility needed"
- COGS: "Getting competitive COGS early helps your commercial positioning — our cost structure supports that"
- Speed to market: "Manufacturing shouldn't be the bottleneck — our lead times and decision speed are built for biotech timelines"
- Partnership flexibility: "If you're in licensing discussions, having a flexible manufacturing partner makes deal structuring easier"
- Clinical to commercial: "We can take you from clinical supply through commercial without switching CDMOs — one tech transfer, not two"
Pick the angle that matches their commercial stage and goals.""",
    },
    'R&D / Scientific': {
        'Value Drivers': 'Scientific expertise in mammalian cell culture, process development capabilities, molecule-specific experience (mAbs, bispecifics, ADCs), analytical development, cell line development support, scientific collaboration model',
        'Proof Points': 'CHO platform expertise, experience with complex molecules (bispecifics, ADCs), process development from gene to GMP, analytical method development and transfer, biosimilar development track record',
        'Tone': 'Scientific, substantive. R&D people respect depth and specificity. Show you understand their molecule type and development challenges. Be a scientist talking to a scientist.',
        'What They Dont Want': 'Cost-first arguments (irrelevant to them), generic CDMO pitches, marketing language, Sandoz name-dropping. Lead with SCIENCE, not price or credentials.',
        'Example Angles': """- Process development: "Our PD team has experience developing processes for [their molecule type] — happy to discuss approach"
- CHO platform: "We run a well-established CHO platform with good clone selection and cell line development capabilities"
- Analytical: "We have in-house analytical development — method development and transfer is something we handle end-to-end"
- Complex molecules: "Bispecifics/ADCs come with specific development challenges — our team has worked through those before"
- Biosimilar: "If it's a biosimilar program, we have gene-to-market experience in that space"
- Scientific collaboration: "We prefer to work as a scientific partner, not just execute a fixed protocol — our PD team engages deeply"
- Scale-up science: "Moving from bench to GMP scale is where a lot of programs stumble — our PD team focuses on de-risking that transition"
Pick the angle that matches their specific development stage and molecule type.""",
    },
    'Program / Project Management': {
        'Value Drivers': 'Clear timelines and milestones, communication quality and frequency, project management methodology, hands-on support, transparency on progress and issues, dedicated project team',
        'Proof Points': 'Dedicated project managers per program, proactive communication style, structured tech transfer process with clear milestones, agile mid-size organization means fast decisions and escalation',
        'Tone': 'Practical and organized. They want to know you will deliver on time and communicate proactively. Show you understand project execution.',
        'What They Dont Want': 'High-level strategy talk, corporate presentations, vague timelines, cost-first arguments. Be specific about HOW you work.',
        'Example Angles': """- Project structure: "Every program gets a dedicated PM and defined milestone plan — no guessing where things stand"
- Communication: "We do regular progress updates and flag issues proactively — no surprises"
- Timelines: "Tech transfer to first GMP batch typically takes X months — we can walk through the milestone plan"
- Escalation: "As a mid-size organization, escalation paths are short — decisions happen in days, not weeks"
- Transparency: "We share real-time progress dashboards — you always know where your program stands"
These people are your day-to-day interface. Show them you're easy to work with.""",
    },
    'Finance / Investment': {
        'Value Drivers': 'Cost efficiency and capital preservation, predictable pricing, transparent cost structure, long-term cost trajectory, manufacturing COGS impact on company valuation and fundraising narrative',
        'Proof Points': 'Lower half of EU cost benchmarks, transparent pricing model, no hidden costs, capital-efficient manufacturing for funded biotechs, competitive COGS for commercial viability',
        'Tone': 'Numbers-focused, ROI-oriented. Finance people want to understand financial impact. Be concrete about cost positioning. They think in spreadsheets.',
        'What They Dont Want': 'Technical jargon, vague "value" claims without numbers, science deep-dives, marketing language. Lead with financial impact.',
        'Example Angles': """- Cost positioning: "We're in the lower half of EU CDMO cost benchmarks — meaningful savings vs premium CDMOs"
- Capital efficiency: "For a biotech deploying VC capital, manufacturing is a major line item — getting competitive pricing without sacrificing quality directly impacts runway"
- COGS: "Competitive manufacturing COGS strengthens your commercial case and valuation narrative"
- Predictability: "Our pricing is transparent and predictable — no hidden costs or surprise change orders"
- Comparison: "If you benchmark us against Lonza/Samsung/Fujifilm tier, you'll see a significant cost delta for equivalent quality standards"
This persona responds to concrete numbers and comparisons.""",
    },
}

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


# ═══════════════════════════════════════════════════════════════
# PERSONA CLASSIFICATION & MESSAGING
# ═══════════════════════════════════════════════════════════════

def classify_persona(lead_title: str) -> str:
    """Classify a lead's job title into a persona bucket.
    
    Uses keyword matching against PERSONA_BUCKETS. Falls back to 
    'General' if no match found. Handles compound titles like
    'VP Manufacturing & Quality' by picking the most specific match.
    
    Args:
        lead_title: The lead's job title string
        
    Returns:
        Persona bucket name (e.g., 'Operations / Manufacturing')
    """
    if not lead_title:
        return 'General'
    
    title_lower = lead_title.lower().strip()
    
    # Skip unverified titles — these are enrichment artifacts, not real titles
    unverified_markers = [
        'unable to verify', 'not verified', 'cannot verify', 'could not verify',
        'no verification', 'no record found', 'not found', 'no match found',
        'no evidence found', 'not confirmed', 'no current employee',
        'position not verified', 'does not appear', 'does not exist',
    ]
    if any(marker in title_lower for marker in unverified_markers):
        return 'General'
    
    # Score each bucket by how many keywords match
    scores = {}
    for bucket_name, bucket_info in PERSONA_BUCKETS.items():
        score = 0
        for keyword in bucket_info['keywords']:
            # Use word boundary matching for short keywords to avoid false positives
            if len(keyword) <= 4:
                # Short keywords like 'qa', 'qc', 'cfo', 'cto', 'api' — need boundary
                if re.search(r'\b' + re.escape(keyword) + r'\b', title_lower):
                    score += 2  # Exact short match is strong signal
            elif keyword in title_lower:
                score += 1
        if score > 0:
            scores[bucket_name] = score
    
    if not scores:
        return 'General'
    
    # Return the bucket with the highest score
    return max(scores, key=scores.get)


def load_persona_messaging(base) -> Dict:
    """Load Persona Messaging table from Airtable.
    
    The table should have rows for each persona bucket with columns:
    - Persona (text) — matches bucket name from PERSONA_BUCKETS
    - Value Drivers (long text) — what this persona cares about
    - Proof Points (long text) — Rezon proof points relevant to this persona
    - Tone (long text) — how to write to this persona
    - What They Dont Want (long text) — what to avoid
    - Example Angles (long text) — specific talking point ideas
    
    Falls back to DEFAULT_PERSONA_MESSAGING if table doesn't exist.
    
    Args:
        base: pyairtable Base object
        
    Returns:
        Dict keyed by persona name → fields dict
    """
    try:
        table = base.table('Persona Messaging')
        records = table.all()
        if records:
            messaging = {}
            for record in records:
                fields = record['fields']
                persona_name = fields.get('Persona', '').strip()
                if persona_name:
                    messaging[persona_name] = fields
            logger.info(f"✓ Persona Messaging loaded: {len(messaging)} personas ({', '.join(messaging.keys())})")
            return messaging
        else:
            logger.info("Persona Messaging table is empty — using defaults")
            return DEFAULT_PERSONA_MESSAGING
    except Exception as e:
        logger.debug(f"Could not load Persona Messaging table: {e} — using defaults")
        return DEFAULT_PERSONA_MESSAGING


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
                            lead_title: str = '', campaign_type: str = '',
                            persona_messaging: Dict = None) -> str:
    """Build a targeted value proposition section for the outreach prompt.
    
    Matches Rezon's specific strengths to the prospect's situation based on
    their pipeline stage, funding, technology, geography, and the lead's role.
    
    The persona messaging is the PRIMARY driver — it determines which value
    drivers and proof points the AI should lead with. The segment/geography
    data is SECONDARY context.
    
    Args:
        profile: Company Profile fields from Airtable
        company_fields: Prospect's company data (enriched)
        lead_title: The lead's job title
        campaign_type: Campaign type (Conference, Roadshow, general)
        persona_messaging: Dict from load_persona_messaging()
        
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
    
    # === PERSONA-SPECIFIC ANGLE (PRIMARY) ===
    # This is the MAIN driver of the message — determines what to lead with
    persona_angle = _match_persona_angle(profile, lead_title, persona_messaging)
    persona_bucket = classify_persona(lead_title)
    
    # === DETERMINE PROSPECT SEGMENT (SECONDARY CONTEXT) ===
    # Adds context about their stage, but persona angle takes priority
    segment_pitch = _match_segment_pitch(profile, safe_fields)
    
    # === SELECT PROOF POINTS ===
    # Pick 2-3 from Company Profile Key Strengths, ordered by PERSONA relevance
    proof_points = _select_proof_points(profile, safe_fields, persona_bucket, persona_messaging)
    
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
    # Persona is PRIMARY — it goes first and determines the message angle.
    # Segment, geography, and proof points are secondary context.
    section = f"""
═══════════════════════════════════════════════════════════
⚡ CRITICAL: YOUR MESSAGE MUST BE SHAPED BY THIS PERSONA
═══════════════════════════════════════════════════════════
{persona_angle}

YOUR COMPANY — REZON BIO:
{positioning}

RULES FOR USING THE ABOVE:
- The PERSONA section determines WHAT you lead with and which proof points you use
- The REZON BIO section is background context, NOT the default message
- If the persona says "PPQ experience matters" → lead with PPQ, NOT cost
- If the persona says "regulatory track record" → lead with FDA/EMA approvals, NOT cost
- If the persona says "scientific expertise" → lead with CHO platform and molecule experience, NOT Sandoz
- Only use cost/pricing as the lead angle for Procurement, Finance, or C-Level personas
- NEVER use the same angle (cost + Sandoz + quality) for every persona — this is the #1 mistake

THEIR STAGE (add context if it sharpens your angle):
{segment_pitch}

ADDITIONAL PROOF POINTS (use ONLY if persona proof points don't already cover it):
{proof_points}

{pain_points}

COMPETITIVE ANGLE (use if it fits what THIS persona cares about):
{diff_angle}

KEY MESSAGE (pick the one that fits THIS PERSONA best):
{chr(10).join('- ' + m for m in primary_msgs[:4]) if primary_msgs else '- Best cost-for-value in the EU: pharma-grade quality without the premium CDMO price tag'}

⚠️ SELF-CHECK: Before writing, ask yourself: "Would a {persona_bucket} person 
actually care about what I'm leading with?" If you're leading with cost for an 
Operations VP, START OVER. If you're leading with Sandoz for an R&D Director, START OVER.

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

═══════════════════════════════════════════════════════════
ANTI-REPETITION — CRITICAL RULE:
═══════════════════════════════════════════════════════════
If two recipients at the same company compared messages, they should NOT look
like the same template with names swapped. Each message must feel individually
written. To achieve this:

RULE 1: Pick ONE proof point per message. Not two, not three. ONE.
  - An Operations person gets: PPQ execution or tech transfer speed
  - An R&D person gets: CHO platform depth or analytical capabilities  
  - A Procurement person gets: cost positioning or supply reliability
  - A C-level gets: strategic fit or partner agility
  They should NEVER all get "Sandoz + cost + batch success rate" — that's lazy.

RULE 2: Vary your sentence structures across messages.
  - DON'T always write "We sit in the lower half of EU cost benchmarks while maintaining..."
  - DON'T always write "Our facilities were qualified by Sandoz"  
  - DON'T always write "pharma-grade quality without the premium price tag"
  - These phrases must NOT appear in every message. Rewrite the same idea in fresh words.

RULE 3: The value prop paragraph must match the PERSONA's priorities.
  - For R&D: process development approach, molecule experience, scientific depth
  - For Operations: execution track record, milestones, facility specs, batch success
  - For Quality: regulatory approvals, inspection history, documentation standards
  - For Procurement: cost, capacity, dual sourcing, pricing transparency
  - For C-level: strategic fit, scaling with them, partner priority
  - NEVER write a generic "we're a quality EU CDMO at good cost" paragraph for everyone

RULE 4: The opening line must be genuinely different per person.
  - Reference something specific to THEIR work, not just "Saw you're a DCAT member"
  - If you can't find something specific, at least vary the opener structure
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


def _match_persona_angle(profile: Dict, lead_title: str, 
                         persona_messaging: Dict = None) -> str:
    """Build persona-specific messaging section for the outreach prompt.
    
    Uses classify_persona() to bucket the title, then pulls specific
    value drivers, proof points, and tone from the Persona Messaging data.
    
    Args:
        profile: Company Profile fields (unused currently, reserved for future)
        lead_title: The lead's job title
        persona_messaging: Dict from load_persona_messaging()
    
    Returns:
        Rich persona messaging block for the prompt
    """
    persona = classify_persona(lead_title)
    
    # Get messaging for this persona
    if persona_messaging and persona in persona_messaging:
        msg = persona_messaging[persona]
    elif persona in DEFAULT_PERSONA_MESSAGING:
        msg = DEFAULT_PERSONA_MESSAGING[persona]
    else:
        msg = DEFAULT_PERSONA_MESSAGING.get('C-Level / Owner', {})
    
    value_drivers = msg.get('Value Drivers', 'Strategic fit, reliability, quality')
    proof_points = msg.get('Proof Points', 'Sandoz-qualified, FDA approved, 95% batch success rate')
    tone = msg.get('Tone', 'Professional and direct')
    dont_want = msg.get('What They Dont Want', 'Marketing fluff, overpromising')
    example_angles = msg.get('Example Angles', '')
    
    section = f"""PERSONA: {persona.upper()}
Lead title: {lead_title}

WHAT THIS PERSON VALUES — shape your message around THESE (not generic cost/Sandoz talking points):
{value_drivers}

PROOF POINTS TO USE FOR THIS PERSONA (pick the 1-2 most relevant):
{proof_points}

TONE GUIDANCE:
{tone}

DO NOT lead with or emphasize:
{dont_want}"""
    
    if example_angles:
        section += f"""

SPECIFIC ANGLE IDEAS for this persona (pick the best fit for their company situation):
{example_angles}"""
    
    return section


def _select_proof_points(profile: Dict, company_fields: Dict, 
                         persona: str = 'General',
                         persona_messaging: Dict = None) -> str:
    """Select 2-3 most relevant proof points from Company Profile, ordered by persona.
    
    Reads the 'Key Strengths' field from the Company Profile table and selects
    which strengths to lead with based on the persona. The persona messaging
    'Proof Points' field determines the priority order.
    
    This replaces the old hardcoded proof point pool — everything now comes
    from your Company Profile table in Airtable.
    
    Args:
        profile: Company Profile fields from Airtable
        company_fields: Prospect's company data (for geography/tech matching)
        persona: Classified persona bucket name
        persona_messaging: Dict from load_persona_messaging() (optional)
    """
    
    # === PULL PROOF POINTS FROM COMPANY PROFILE ===
    key_strengths = profile.get('Key Strengths', '') if profile else ''
    
    # Parse Key Strengths into individual items
    # Handles formats like "- item" or "1. item" or "item\nitem"
    strength_items = []
    if key_strengths:
        for line in key_strengths.split('\n'):
            line = line.strip()
            if not line:
                continue
            # Strip leading bullet/number markers
            cleaned = line.lstrip('-•·').strip()
            if cleaned and cleaned[0].isdigit() and '.' in cleaned[:4]:
                cleaned = cleaned.split('.', 1)[1].strip()
            if cleaned and len(cleaned) > 5:
                strength_items.append(cleaned)
    
    # === GET PERSONA-SPECIFIC PROOF POINT PRIORITIES ===
    # The Persona Messaging table's 'Proof Points' field tells us which 
    # strengths THIS persona cares about most
    persona_proof_text = ''
    if persona_messaging and persona in persona_messaging:
        persona_proof_text = persona_messaging[persona].get('Proof Points', '').lower()
    elif persona in DEFAULT_PERSONA_MESSAGING:
        persona_proof_text = DEFAULT_PERSONA_MESSAGING[persona].get('Proof Points', '').lower()
    
    # === SCORE EACH STRENGTH BY PERSONA RELEVANCE ===
    # Match words from the persona's Proof Points against each strength
    scored = []
    for strength in strength_items:
        strength_lower = strength.lower()
        score = 0
        
        # Check how many persona proof point keywords appear in this strength
        if persona_proof_text:
            # Extract meaningful words (skip common words)
            skip_words = {'the', 'and', 'for', 'with', 'from', 'our', 'we', 'are', 'is', 
                          'at', 'in', 'of', 'to', 'a', 'an', 'as', 'by', 'not', 'no', 'but'}
            proof_words = [w for w in persona_proof_text.split() if len(w) > 2 and w not in skip_words]
            
            for word in proof_words:
                if word in strength_lower:
                    score += 1
        
        scored.append((strength, score))
    
    # Sort by score (highest = most relevant to this persona), then original order
    scored.sort(key=lambda x: -x[1])
    
    # === GEOGRAPHY-AWARE FILTERING ===
    # If we have geography info, boost relevant strengths
    location = (company_fields.get('Location/HQ', '') or '').lower()
    tech = company_fields.get('Technology Platform', [])
    if isinstance(tech, str):
        tech = [tech]
    tech_str = ' '.join(tech).lower() if tech else ''
    
    # Re-score with geography/tech bonuses
    final_scored = []
    for strength, persona_score in scored:
        s_lower = strength.lower()
        geo_bonus = 0
        
        # Boost FDA mentions for US prospects
        if any(x in location for x in ['us', 'united states', 'america', 'boston', 'california']):
            if 'fda' in s_lower:
                geo_bonus += 2
        
        # Boost EMA mentions for EU prospects
        if any(x in location for x in ['europ', 'germany', 'france', 'uk', 'swiss', 'netherlands']):
            if 'ema' in s_lower or 'european' in s_lower:
                geo_bonus += 2
        
        # Boost tech-matching strengths
        if any(x in tech_str for x in ['biosimilar']) and 'biosimilar' in s_lower:
            geo_bonus += 2
        if any(x in tech_str for x in ['bispecific', 'adc']) and any(x in s_lower for x in ['bispecific', 'adc', 'complex']):
            geo_bonus += 2
        
        final_scored.append((strength, persona_score + geo_bonus))
    
    final_scored.sort(key=lambda x: -x[1])
    
    # === SELECT TOP 2-3 ===
    selected = [s for s, _ in final_scored[:3]]
    
    # === FALLBACK if Company Profile Key Strengths is empty ===
    if not selected:
        # Use persona messaging Proof Points directly as a fallback
        if persona_proof_text:
            return f"PROOF POINTS (from persona messaging):\n{persona_proof_text}"
        else:
            return "PROOF POINTS: European biologics CDMO with pharma-grade quality standards"
    
    return '\n'.join(f"- {pp}" for pp in selected)


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
