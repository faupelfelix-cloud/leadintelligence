#!/usr/bin/env python3
"""
Complete Dynamic ICP Scoring System

This reads from BOTH:
1. ICP Scoring Criteria table (scoring rules)
2. Company Profile table (your positioning, strengths, ideal customer)

Then builds intelligent prompts that use YOUR criteria to score companies.
"""

import os
import yaml
import json
import logging
from typing import Dict, List, Tuple
from pyairtable import Api
from anthropic import Anthropic

logger = logging.getLogger(__name__)


class CompleteICPScorer:
    """
    Complete ICP scorer using your Airtable criteria and company profile
    """
    
    def __init__(self, config: Dict):
        """Initialize with config"""
        self.config = config
        
        # Initialize Airtable
        self.airtable = Api(config['airtable']['api_key'])
        self.base = self.airtable.base(config['airtable']['base_id'])
        
        # Initialize Anthropic
        self.anthropic_client = Anthropic(api_key=config['anthropic']['api_key'])
        
        # Load your company profile
        self.company_profile = self.load_company_profile()
        
        # Load ICP scoring criteria
        self.criteria = self.load_icp_criteria()
        
        logger.info(f"✓ Loaded company profile")
        logger.info(f"✓ Loaded {len(self.criteria)} ICP criteria (total: {self.get_total_score()} points)")
    
    def load_company_profile(self) -> Dict:
        """Load company profile from Airtable"""
        try:
            # Try to find company profile table
            profile_table = self.base.table('Company Profile')
            records = profile_table.all()
            
            if not records:
                logger.warning("Company Profile table is empty")
                return {}
            
            # Get first record (should only be one)
            profile = records[0]['fields']
            
            return {
                'positioning': profile.get('Value Proposition', ''),
                'ideal_customer': profile.get('ICP Definition', ''),
                'geographic_focus': profile.get('Geographic Priority', ''),
                'product_focus': profile.get('Product Focus', ''),
                'technology_focus': profile.get('Technology Platforms', ''),
                'services': profile.get('Core Services', ''),
                'competitive_advantages': profile.get('Competitive Advantages', ''),
                'limitations': profile.get('Honest Limitations', ''),
                'perfect_project': profile.get('Perfect Fit Project Profile', ''),
                'competitors': profile.get('Competitive Positioning', ''),
                'avoid': profile.get('What to Avoid', '')
            }
        except Exception as e:
            logger.error(f"Error loading company profile: {str(e)}")
            return {}
    
    def load_icp_criteria(self) -> List[Dict]:
        """
        Load ICP scoring criteria from Airtable
        
        Returns list of criteria with scoring rules
        """
        try:
            criteria_table = self.base.table('ICP Scoring Criteria')
            records = criteria_table.all()
            
            criteria = []
            
            for record in records:
                fields = record['fields']
                
                # Parse criterion name and max points
                criterion_text = fields.get('Criterion', '')
                points_text = fields.get('Points (Max X)', '')
                
                # Extract criterion name (before "Points")
                if ':' in criterion_text:
                    criterion_name = criterion_text.split(':')[1].strip()
                else:
                    criterion_name = criterion_text.replace('Criterion', '').strip()
                
                # Extract max points from the points text
                max_points = 0
                if 'Max' in points_text:
                    # Extract number after "Max"
                    import re
                    match = re.search(r'Max (\d+)', points_text)
                    if match:
                        max_points = int(match.group(1))
                
                # Parse rules from the points text
                rules = []
                lines = points_text.split('\n')
                for line in lines:
                    line = line.strip()
                    if ':' in line and 'point' in line.lower():
                        # Parse lines like "- <50: 3 points (startup - low priority)"
                        parts = line.split(':')
                        if len(parts) >= 2:
                            condition = parts[0].replace('-', '').strip()
                            rest = parts[1]
                            
                            # Extract points
                            point_match = re.search(r'(\d+)\s*point', rest)
                            if point_match:
                                points = int(point_match.group(1))
                                
                                # Extract label (text in parentheses)
                                label = ''
                                label_match = re.search(r'\((.*?)\)', rest)
                                if label_match:
                                    label = label_match.group(1)
                                
                                rules.append({
                                    'condition': condition,
                                    'points': points,
                                    'label': label
                                })
                
                if criterion_name and rules:
                    criteria.append({
                        'name': criterion_name,
                        'max_points': max_points,
                        'rules': rules
                    })
            
            return criteria
            
        except Exception as e:
            logger.error(f"Error loading ICP criteria: {str(e)}")
            return []
    
    def get_total_score(self) -> int:
        """Get total possible ICP score"""
        return sum(c['max_points'] for c in self.criteria)
    
    def build_scoring_prompt(self, company_name: str) -> str:
        """
        Build comprehensive ICP scoring prompt using:
        - Your company profile (who you are, what you do)
        - Your ICP criteria (how to score companies)
        """
        
        profile = self.company_profile
        
        prompt = f"""ICP Assessment for: {company_name}

ABOUT REZON BIO:
{profile.get('positioning', 'European biologics CDMO specializing in mammalian cell culture')}

YOUR IDEAL CUSTOMER:
{profile.get('ideal_customer', 'Mid-size biotech (50-1,000 employees) with biologics programs')}

YOUR COMPETITIVE ADVANTAGES:
{profile.get('competitive_advantages', 'Cost-competitive, biosimilar expertise, EU location')}

WHAT TO AVOID (CRITICAL EXCLUSIONS):
{profile.get('avoid', 'Cell/gene therapy, non-mammalian, CDMOs (competitors)')}

---

ICP SCORING CRITERIA (Use these EXACT rules to score):

"""
        
        # Add each criterion with exact rules
        for idx, criterion in enumerate(self.criteria, 1):
            prompt += f"{idx}. {criterion['name'].upper()} (0-{criterion['max_points']} points):\n"
            
            for rule in criterion['rules']:
                prompt += f"   - {rule['condition']}: {rule['points']} pts"
                if rule['label']:
                    prompt += f" ({rule['label']})"
                prompt += "\n"
            
            prompt += "\n"
        
        prompt += f"""TOTAL POSSIBLE SCORE: {self.get_total_score()} points

CRITICAL: Check if CDMO/CMO competitor first!
Known competitors: Lonza, Samsung Biologics, Fujifilm Diosynth, Thermo Fisher Biologics, WuXi, Catalent Biologics, etc.
→ If CDMO competitor: Automatic score = 0

Search for {company_name} and assess using the criteria above.

Return JSON with scores for each criterion:
{{
  "company_info": {{
    "name": "{company_name}",
    "size_employees": X,
    "revenue": "$XM-$XM",
    "stage": "Description",
    "technology": "Description",
    "location": "Country",
    "funding": "Series X / Public",
    "manufacturing_status": "Description",
    "product_focus": "NBE/Biosimilar/etc"
  }},
  "is_cdmo_competitor": false,
  "scores": {{
    "company_size": {{"value": "50-300", "points": 15}},
    "annual_revenue": {{"value": "$20M-$100M", "points": 15}},
    "pipeline_stage": {{"value": "3-10 Early-Late Clinical", "points": 20}},
    "production_technology": {{"value": "Purely mammalian", "points": 20}},
    "geography": {{"value": "US", "points": 10}},
    "funding": {{"value": "Series C+", "points": 10}},
    "manufacturing_status": {{"value": "No public partner", "points": 10}},
    "product_focus": {{"value": "NBEs", "points": 5}}
  }},
  "total_score": X,
  "tier": "Perfect Fit / Strong Fit / etc",
  "reasoning": "Brief explanation of why this score"
}}

Search and assess now."""
        
        return prompt
    
    def score_company(self, company_name: str) -> Tuple[int, Dict]:
        """
        Score a company using your ICP criteria
        
        Returns: (icp_score, detailed_breakdown)
        """
        
        # Build prompt using your criteria
        prompt = self.build_scoring_prompt(company_name)
        
        try:
            # Call Claude with web search
            message = self.anthropic_client.messages.create(
                model=self.config['anthropic']['model'],
                max_tokens=2000,
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
            
            # Parse JSON (robust)
            json_str = self.extract_json(result_text)
            
            if not json_str:
                logger.error("No JSON found in response")
                return 0, {}
            
            data = json.loads(json_str)
            
            # Check if CDMO competitor
            if data.get('is_cdmo_competitor', False):
                logger.info(f"  ⚠️  {company_name} is a CDMO competitor - score = 0")
                return 0, {
                    'is_competitor': True,
                    'reason': 'CDMO/CMO competitor'
                }
            
            # Get total score
            total_score = data.get('total_score', 0)
            
            # Build detailed breakdown
            breakdown = {
                'total': total_score,
                'tier': data.get('tier', ''),
                'reasoning': data.get('reasoning', ''),
                'company_info': data.get('company_info', {}),
                'scores': data.get('scores', {}),
                'is_competitor': False
            }
            
            return total_score, breakdown
            
        except Exception as e:
            logger.error(f"Error scoring company {company_name}: {str(e)}")
            return 0, {'error': str(e)}
    
    def extract_json(self, text: str) -> str:
        """Extract JSON from Claude's response"""
        text = text.strip()
        
        # Try markdown code blocks
        if "```json" in text:
            start = text.find("```json") + 7
            end = text.find("```", start)
            if end > start:
                return text[start:end].strip()
        elif "```" in text:
            start = text.find("```") + 3
            end = text.find("```", start)
            if end > start:
                return text[start:end].strip()
        
        # Try to find JSON object
        if "{" in text:
            start = text.find("{")
            depth = 0
            end = start
            for i in range(start, len(text)):
                if text[i] == "{":
                    depth += 1
                elif text[i] == "}":
                    depth -= 1
                    if depth == 0:
                        end = i + 1
                        break
            if end > start:
                return text[start:end].strip()
        
        return text


def test_scorer():
    """Test the complete ICP scorer"""
    
    logging.basicConfig(level=logging.INFO)
    
    # Load config
    with open('config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    # Initialize scorer
    scorer = CompleteICPScorer(config)
    
    print("="*70)
    print("COMPLETE ICP SCORER - READY")
    print("="*70)
    print()
    print(f"Company Profile Loaded: {bool(scorer.company_profile)}")
    print(f"ICP Criteria Loaded: {len(scorer.criteria)} criteria")
    print(f"Total Possible Score: {scorer.get_total_score()} points")
    print()
    
    # Test with a few companies
    test_companies = [
        "BioMarin",
        "Sandoz",
        "Fujifilm Diosynth",
        "Arcellx"
    ]
    
    print("TESTING ON SAMPLE COMPANIES:")
    print("-" * 70)
    
    for company in test_companies:
        print(f"\n{company}:")
        score, breakdown = scorer.score_company(company)
        print(f"  Score: {score}/{scorer.get_total_score()}")
        if breakdown.get('is_competitor'):
            print(f"  Status: CDMO Competitor - Excluded")
        else:
            print(f"  Tier: {breakdown.get('tier', 'Unknown')}")
            print(f"  Reasoning: {breakdown.get('reasoning', 'N/A')[:100]}...")


if __name__ == "__main__":
    test_scorer()
