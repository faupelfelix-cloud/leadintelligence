#!/usr/bin/env python3
"""
Market News Intelligence System - Integrated with Lead Intelligence Platform

This system:
1. Collects news from configurable sources (RSS feeds, NewsAPI, ClinicalTrials.gov)
2. Analyzes against ICP criteria and existing company list
3. Discovers new companies and enriches them
4. Links leads and creates NEWS trigger events
5. Generates outreach for opportunities

Integrates with existing:
- Company Profile (for context)
- Companies table (ICP scoring, discovery)
- Leads table (linking, creation)
- Trigger History (NEWS events)

New tables:
- News Sources (configurable sources)
- News Articles (raw articles)
- News Analysis (AI output)
- Trigger Points Library (expanded triggers)
- Competitors (competitor tracking)
"""

import os
import sys
import yaml
import json
import time
import logging
import argparse
import feedparser
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from dateutil import parser as date_parser

import anthropic
from pyairtable import Api
from pyairtable.formulas import match

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('market_news.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# =============================================================================
# DEFAULT NEWS SOURCES
# =============================================================================

DEFAULT_RSS_SOURCES = [
    {
        'name': 'Fierce Pharma',
        'url': 'https://www.fiercepharma.com/rss/xml',
        'type': 'RSS Feed',
        'category': 'Pharma News',
        'quality_score': 9,
        'check_frequency': 'Daily'
    },
    {
        'name': 'Fierce Biotech',
        'url': 'https://www.fiercebiotech.com/rss/xml',
        'type': 'RSS Feed',
        'category': 'Biotech News',
        'quality_score': 9,
        'check_frequency': 'Daily'
    },
    {
        'name': 'Endpoints News',
        'url': 'https://endpts.com/feed/',
        'type': 'RSS Feed',
        'category': 'Biotech News',
        'quality_score': 9,
        'check_frequency': 'Daily'
    },
    {
        'name': 'BioPharma Dive',
        'url': 'https://www.biopharmadive.com/feeds/news/',
        'type': 'RSS Feed',
        'category': 'Pharma News',
        'quality_score': 8,
        'check_frequency': 'Daily'
    },
    {
        'name': 'European Biotechnology',
        'url': 'https://european-biotechnology.com/rss',
        'type': 'RSS Feed',
        'category': 'European Biotech',
        'quality_score': 8,
        'check_frequency': 'Daily'
    },
    {
        'name': 'GEN - Genetic Engineering News',
        'url': 'https://www.genengnews.com/feed/',
        'type': 'RSS Feed',
        'category': 'Biotech News',
        'quality_score': 8,
        'check_frequency': 'Daily'
    },
    {
        'name': 'BioSpace',
        'url': 'https://www.biospace.com/rss/',
        'type': 'RSS Feed',
        'category': 'Biotech News',
        'quality_score': 7,
        'check_frequency': 'Daily'
    },
    {
        'name': 'Pharma Manufacturing',
        'url': 'https://www.pharmamanufacturing.com/rss/',
        'type': 'RSS Feed',
        'category': 'Manufacturing',
        'quality_score': 8,
        'check_frequency': 'Daily'
    },
    {
        'name': 'Contract Pharma',
        'url': 'https://www.contractpharma.com/rss/',
        'type': 'RSS Feed',
        'category': 'CDMO/CMO',
        'quality_score': 9,
        'check_frequency': 'Daily'
    },
    {
        'name': 'ClinicalTrials - mAbs Phase 2-3',
        'url': 'https://clinicaltrials.gov/ct2/results/rss.xml?term=monoclonal+antibodies&phase=1&phase=2&lup_d=14&count=50',
        'type': 'Clinical Trials',
        'category': 'Clinical Trials',
        'quality_score': 10,
        'check_frequency': 'Daily'
    },
    {
        'name': 'ClinicalTrials - Biologics Europe',
        'url': 'https://clinicaltrials.gov/ct2/results/rss.xml?term=biologics&cntry=DE&cntry=FR&cntry=GB&cntry=CH&lup_d=14&count=50',
        'type': 'Clinical Trials',
        'category': 'Clinical Trials',
        'quality_score': 10,
        'check_frequency': 'Daily'
    }
]

# Default trigger points for CDMO business
DEFAULT_TRIGGER_POINTS = [
    {
        'name': 'Series B+ Funding',
        'category': 'Funding',
        'description': 'Company raises $30M+ Series B or later funding round',
        'timeline': '3-6 months post-funding',
        'signal_strength': 'Strong',
        'keywords': 'series b, series c, series d, raised, funding, financing, million, investment',
        'action': 'Reach out within 2 weeks offering manufacturing assessment'
    },
    {
        'name': 'IPO or Public Offering',
        'category': 'Funding',
        'description': 'Company files or completes IPO',
        'timeline': '6-12 months',
        'signal_strength': 'Strong',
        'keywords': 'ipo, nasdaq, nyse, public offering, went public',
        'action': 'Commercial scale manufacturing discussion'
    },
    {
        'name': 'Phase 2 Advancement',
        'category': 'Clinical',
        'description': 'Program advances from Phase 1 to Phase 2',
        'timeline': '6-12 months',
        'signal_strength': 'Strong',
        'keywords': 'phase 2, phase ii, dose escalation complete, pivotal, iib',
        'action': 'Discuss clinical supply and scale-up needs'
    },
    {
        'name': 'Phase 3 Initiation',
        'category': 'Clinical',
        'description': 'Company initiates Phase 3 clinical trial',
        'timeline': '3-6 months',
        'signal_strength': 'Strong',
        'keywords': 'phase 3, phase iii, pivotal trial, registration, late-stage',
        'action': 'Commercial manufacturing capacity discussion'
    },
    {
        'name': 'Regulatory Approval',
        'category': 'Regulatory',
        'description': 'FDA, EMA, or other major regulatory approval',
        'timeline': '1-3 months',
        'signal_strength': 'Strong',
        'keywords': 'fda approval, ema approval, bla, market authorization, approved',
        'action': 'Commercial supply discussion - urgent'
    },
    {
        'name': 'Partnership/Licensing Deal',
        'category': 'Strategic',
        'description': 'Company announces licensing or partnership deal',
        'timeline': '3-9 months',
        'signal_strength': 'Medium',
        'keywords': 'partnership, collaboration, license, agreement, alliance, deal',
        'action': 'Discuss manufacturing support for expanded programs'
    },
    {
        'name': 'Capacity Expansion Announcement',
        'category': 'Operational',
        'description': 'Company announces manufacturing expansion or seeks partner',
        'timeline': '1-3 months',
        'signal_strength': 'Strong',
        'keywords': 'capacity, expansion, manufacturing partner, cdmo, outsourc, scale-up',
        'action': 'Immediate outreach - they are actively looking'
    },
    {
        'name': 'Executive Hire - Manufacturing',
        'category': 'Leadership',
        'description': 'Company hires VP/Head of Manufacturing, CMC, or Operations',
        'timeline': '3-6 months',
        'signal_strength': 'Medium',
        'keywords': 'hired, appointed, joined, vp manufacturing, head of, cmo, coo, cmc',
        'action': 'Congratulate and introduce capabilities'
    },
    {
        'name': 'Clinical Hold/Setback',
        'category': 'Clinical',
        'description': 'Company faces clinical hold or setback',
        'timeline': '6-12 months',
        'signal_strength': 'Weak',
        'keywords': 'clinical hold, setback, delay, pause, safety, discontinued',
        'action': 'Monitor - may need to reformulate or adjust manufacturing'
    },
    {
        'name': 'Acquisition Target',
        'category': 'M&A',
        'description': 'Company being acquired or exploring strategic options',
        'timeline': 'Variable',
        'signal_strength': 'Medium',
        'keywords': 'acquisition, acquired, merger, strategic options, buyout',
        'action': 'Monitor relationship - may change manufacturing strategy'
    },
    {
        'name': 'IND Filing',
        'category': 'Regulatory',
        'description': 'Company files IND with FDA or equivalent',
        'timeline': '6-12 months',
        'signal_strength': 'Medium',
        'keywords': 'ind, investigational new drug, clinical trial application, cta, first-in-human',
        'action': 'Discuss clinical manufacturing needs'
    },
    {
        'name': 'Positive Clinical Data',
        'category': 'Clinical',
        'description': 'Company announces positive clinical trial results',
        'timeline': '3-6 months',
        'signal_strength': 'Strong',
        'keywords': 'positive data, met endpoint, efficacy, successful, promising results',
        'action': 'Discuss scale-up for next phase'
    },
    {
        'name': 'Manufacturing Challenge',
        'category': 'Operational',
        'description': 'Company mentions manufacturing challenges or supply issues',
        'timeline': '1-3 months',
        'signal_strength': 'Strong',
        'keywords': 'supply issue, manufacturing challenge, production delay, capacity constraint',
        'action': 'Immediate outreach - position as solution provider'
    },
    {
        'name': 'European Expansion',
        'category': 'Strategic',
        'description': 'US company expanding to Europe or seeking EU manufacturing',
        'timeline': '3-6 months',
        'signal_strength': 'Strong',
        'keywords': 'european expansion, eu market, ema submission, european partner',
        'action': 'Position as European manufacturing solution'
    },
    {
        'name': 'Pipeline Addition',
        'category': 'Clinical',
        'description': 'Company adds new program to pipeline',
        'timeline': '12-18 months',
        'signal_strength': 'Weak',
        'keywords': 'pipeline, new program, added, portfolio, candidate',
        'action': 'Monitor for manufacturing timing'
    }
]

# Default competitors to track
DEFAULT_COMPETITORS = [
    {'name': 'Lonza', 'tier': 'Top Tier', 'focus': 'Full service CDMO, mAbs, cell/gene'},
    {'name': 'Samsung Biologics', 'tier': 'Top Tier', 'focus': 'Large scale mAbs'},
    {'name': 'WuXi Biologics', 'tier': 'Top Tier', 'focus': 'Asia-based, full service'},
    {'name': 'Boehringer Ingelheim BioXcellence', 'tier': 'Top Tier', 'focus': 'European, premium'},
    {'name': 'Catalent', 'tier': 'Top Tier', 'focus': 'US-based, broad services'},
    {'name': 'Fujifilm Diosynth', 'tier': 'Top Tier', 'focus': 'Gene therapy, mAbs'},
    {'name': 'Rentschler Biopharma', 'tier': 'Mid Tier', 'focus': 'European, mid-scale'},
    {'name': 'AGC Biologics', 'tier': 'Mid Tier', 'focus': 'Global, cell/gene'},
    {'name': 'CMIC', 'tier': 'Mid Tier', 'focus': 'Asia-Pacific focused'},
    {'name': 'KBI Biopharma', 'tier': 'Mid Tier', 'focus': 'US-based, mammalian'},
    {'name': 'Hovione', 'tier': 'Mid Tier', 'focus': 'European, small molecule + bio'},
    {'name': 'Ajinomoto Bio-Pharma', 'tier': 'Mid Tier', 'focus': 'ADCs, conjugation'},
    {'name': 'Batavia Biosciences', 'tier': 'Emerging', 'focus': 'Netherlands, viral vectors'},
    {'name': 'Biomeva', 'tier': 'Emerging', 'focus': 'German, mammalian'},
    {'name': 'Richter-Helm', 'tier': 'Emerging', 'focus': 'German, mAbs'},
]


# =============================================================================
# MAIN CLASS
# =============================================================================

class MarketNewsIntelligence:
    """Comprehensive market news collection and analysis system"""
    
    def __init__(self, config_path: str = "config.yaml"):
        """Initialize with configuration"""
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        # Initialize APIs
        self.airtable = Api(self.config['airtable']['api_key'])
        self.base = self.airtable.base(self.config['airtable']['base_id'])
        
        # Core tables
        self.companies_table = self.base.table(self.config['airtable']['tables']['companies'])
        self.leads_table = self.base.table(self.config['airtable']['tables']['leads'])
        self.trigger_history_table = self.base.table('Trigger History')
        
        # News tables
        self.news_sources_table = self._init_table('News Sources')
        self.news_articles_table = self._init_table('News Articles')
        self.news_analysis_table = self._init_table('News Analysis')
        self.trigger_points_table = self._init_table('Trigger Points Library')
        self.competitors_table = self._init_table('Competitors')
        
        # Company Profile
        self.company_profile = self._load_company_profile()
        
        # API client
        self.anthropic_client = anthropic.Anthropic(
            api_key=self.config['anthropic']['api_key']
        )
        
        # NewsAPI key (optional)
        self.news_api_key = os.getenv('NEWS_API_KEY', self.config.get('news_api', {}).get('api_key'))
        
        # Load context data
        self.trigger_points = self._load_trigger_points()
        self.competitors = self._load_competitors()
        self.high_icp_companies = self._load_high_icp_companies()
        
        logger.info("MarketNewsIntelligence initialized")
        logger.info(f"  - {len(self.trigger_points)} trigger points loaded")
        logger.info(f"  - {len(self.competitors)} competitors tracked")
        logger.info(f"  - {len(self.high_icp_companies)} high-ICP companies to monitor")
    
    def _init_table(self, table_name: str):
        """Initialize table, return None if not found"""
        try:
            return self.base.table(table_name)
        except:
            logger.warning(f"Table '{table_name}' not found")
            return None
    
    def _load_company_profile(self) -> Dict:
        """Load company profile for context"""
        try:
            table = self.base.table('Company Profile')
            records = table.all()
            if records:
                profile = records[0].get('fields', {})
                logger.info("Company Profile loaded")
                return profile
        except Exception as e:
            logger.warning(f"Could not load Company Profile: {e}")
        return {}
    
    def _load_trigger_points(self) -> List[Dict]:
        """Load trigger points from table or use defaults"""
        if self.trigger_points_table:
            try:
                records = self.trigger_points_table.all(formula="{Active}=TRUE()")
                if records:
                    return [r['fields'] for r in records]
            except:
                pass
        return DEFAULT_TRIGGER_POINTS
    
    def _load_competitors(self) -> List[Dict]:
        """Load competitors from table or use defaults"""
        if self.competitors_table:
            try:
                records = self.competitors_table.all()
                if records:
                    return [r['fields'] for r in records]
            except:
                pass
        return DEFAULT_COMPETITORS
    
    def _load_high_icp_companies(self) -> List[Dict]:
        """Load companies with ICP > 50 for monitoring"""
        try:
            records = self.companies_table.all(formula="{ICP Fit Score}>=50")
            companies = []
            for r in records:
                fields = r.get('fields', {})
                companies.append({
                    'id': r['id'],
                    'name': fields.get('Company Name', ''),
                    'icp_score': fields.get('ICP Fit Score', 0),
                    'pipeline': fields.get('Pipeline Stage', []),
                    'technology': fields.get('Technology Platform', [])
                })
            return companies
        except Exception as e:
            logger.error(f"Error loading high-ICP companies: {e}")
            return []
    
    # =========================================================================
    # NEWS SOURCE MANAGEMENT
    # =========================================================================
    
    def get_active_sources(self) -> List[Dict]:
        """Get all active news sources from table or defaults"""
        if self.news_sources_table:
            try:
                records = self.news_sources_table.all(formula="{Active}=TRUE()")
                if records:
                    sources = []
                    for r in records:
                        f = r['fields']
                        sources.append({
                            'name': f.get('Source Name', ''),
                            'url': f.get('URL', ''),
                            'type': f.get('Source Type', 'RSS Feed'),
                            'category': f.get('Category', ''),
                            'quality_score': f.get('Quality Score', 5),
                            'id': r['id']
                        })
                    logger.info(f"Loaded {len(sources)} active sources from Airtable")
                    return sources
            except Exception as e:
                logger.warning(f"Could not load sources from table: {e}")
        
        logger.info("Using default RSS sources")
        return DEFAULT_RSS_SOURCES
    
    def initialize_news_sources(self):
        """Populate News Sources table with defaults if empty"""
        if not self.news_sources_table:
            logger.warning("News Sources table not found")
            return
        
        existing = self.news_sources_table.all()
        if existing:
            logger.info(f"News Sources table already has {len(existing)} records")
            return
        
        logger.info("Initializing News Sources table with defaults...")
        for source in DEFAULT_RSS_SOURCES:
            try:
                self.news_sources_table.create({
                    'Source Name': source['name'],
                    'URL': source['url'],
                    'Source Type': source['type'],
                    'Category': source.get('category', ''),
                    'Quality Score': source.get('quality_score', 5),
                    'Check Frequency': source.get('check_frequency', 'Daily'),
                    'Active': True,
                    'Date Added': datetime.now().strftime('%Y-%m-%d')
                })
            except Exception as e:
                logger.error(f"Error adding source {source['name']}: {e}")
        
        logger.info(f"Added {len(DEFAULT_RSS_SOURCES)} default sources")
    
    def initialize_trigger_points(self):
        """Populate Trigger Points Library with defaults if empty"""
        if not self.trigger_points_table:
            logger.warning("Trigger Points Library table not found")
            return
        
        existing = self.trigger_points_table.all()
        if existing:
            logger.info(f"Trigger Points Library already has {len(existing)} records")
            return
        
        logger.info("Initializing Trigger Points Library...")
        for trigger in DEFAULT_TRIGGER_POINTS:
            try:
                self.trigger_points_table.create({
                    'Trigger Name': trigger['name'],
                    'Category': trigger['category'],
                    'Description': trigger['description'],
                    'Typical Timeline': trigger['timeline'],
                    'Buying Signal Strength': trigger['signal_strength'],
                    'Keywords': trigger['keywords'],
                    'Recommended Action': trigger['action'],
                    'Active': True
                })
            except Exception as e:
                logger.error(f"Error adding trigger {trigger['name']}: {e}")
        
        logger.info(f"Added {len(DEFAULT_TRIGGER_POINTS)} trigger points")
    
    def initialize_competitors(self):
        """Populate Competitors table with defaults if empty"""
        if not self.competitors_table:
            logger.warning("Competitors table not found")
            return
        
        existing = self.competitors_table.all()
        if existing:
            logger.info(f"Competitors table already has {len(existing)} records")
            return
        
        logger.info("Initializing Competitors table...")
        for comp in DEFAULT_COMPETITORS:
            try:
                self.competitors_table.create({
                    'Company Name': comp['name'],
                    'Ranking': comp['tier'],
                    'Primary Services': comp['focus'],
                    'Date Added': datetime.now().strftime('%Y-%m-%d')
                })
            except Exception as e:
                logger.error(f"Error adding competitor {comp['name']}: {e}")
        
        logger.info(f"Added {len(DEFAULT_COMPETITORS)} competitors")
    
    # =========================================================================
    # NEWS COLLECTION
    # =========================================================================
    
    def collect_rss_articles(self, sources: List[Dict]) -> List[Dict]:
        """Collect articles from RSS feeds"""
        articles = []
        
        for source in sources:
            if source.get('type') not in ['RSS Feed', 'Clinical Trials']:
                continue
            
            try:
                logger.info(f"Collecting from {source['name']}...")
                feed = feedparser.parse(source['url'])
                
                for entry in feed.entries[:15]:  # Top 15 per source
                    # Parse date
                    pub_date = entry.get('published', '')
                    try:
                        parsed = date_parser.parse(pub_date)
                        pub_date = parsed.strftime('%Y-%m-%d')
                    except:
                        pub_date = datetime.now().strftime('%Y-%m-%d')
                    
                    article = {
                        'headline': entry.get('title', '')[:500],
                        'url': entry.get('link', ''),
                        'source': source['name'],
                        'source_type': source.get('type', 'RSS Feed'),
                        'published_date': pub_date,
                        'content': entry.get('summary', entry.get('description', ''))[:2000],
                        'author': entry.get('author', ''),
                        'category': source.get('category', '')
                    }
                    
                    # Check if already collected
                    if not self._article_exists(article['url']):
                        articles.append(article)
                
                time.sleep(0.5)  # Rate limiting
                
            except Exception as e:
                logger.error(f"Error collecting from {source['name']}: {e}")
        
        return articles
    
    def collect_newsapi_articles(self, keywords: List[str] = None) -> List[Dict]:
        """Collect from NewsAPI (optional)"""
        if not self.news_api_key:
            return []
        
        keywords = keywords or [
            'biologics CDMO',
            'monoclonal antibody manufacturing',
            'biopharmaceutical outsourcing',
            'European biotech funding'
        ]
        
        articles = []
        base_url = "https://newsapi.org/v2/everything"
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        for keyword in keywords:
            try:
                logger.info(f"NewsAPI search: {keyword}")
                
                params = {
                    'q': keyword,
                    'from': yesterday,
                    'sortBy': 'relevancy',
                    'language': 'en',
                    'apiKey': self.news_api_key
                }
                
                response = requests.get(base_url, params=params, timeout=10)
                data = response.json()
                
                if data.get('status') == 'ok':
                    for item in data.get('articles', [])[:5]:
                        article = {
                            'headline': item.get('title', '')[:500],
                            'url': item.get('url', ''),
                            'source': item.get('source', {}).get('name', 'NewsAPI'),
                            'source_type': 'NewsAPI',
                            'published_date': item.get('publishedAt', '')[:10],
                            'content': item.get('description', '')[:2000],
                            'author': item.get('author', ''),
                            'category': 'NewsAPI'
                        }
                        
                        if not self._article_exists(article['url']):
                            articles.append(article)
                
                time.sleep(1)  # Rate limiting
                
            except Exception as e:
                logger.error(f"NewsAPI error for '{keyword}': {e}")
        
        return articles
    
    def _article_exists(self, url: str) -> bool:
        """Check if article already in database"""
        if not self.news_articles_table or not url:
            return False
        
        try:
            # Escape URL for formula
            safe_url = url.replace("'", "\\'")
            records = self.news_articles_table.all(formula=f"{{URL}}='{safe_url}'")
            return len(records) > 0
        except:
            return False
    
    def save_raw_article(self, article: Dict) -> Optional[str]:
        """Save raw article to News Articles table"""
        if not self.news_articles_table:
            return None
        
        try:
            record = self.news_articles_table.create({
                'Headline': article.get('headline', '')[:500],
                'URL': article.get('url', ''),
                'Source': article.get('source', ''),
                'Published Date': article.get('published_date', ''),
                'Collected Date': datetime.now().strftime('%Y-%m-%d'),
                'Raw Content': article.get('content', '')[:5000],
                'Author': article.get('author', ''),
                'Processing Status': 'New'
            })
            return record['id']
        except Exception as e:
            logger.error(f"Error saving article: {e}")
            return None
    
    def collect_all_news(self) -> List[Dict]:
        """Collect from all active sources"""
        logger.info("="*60)
        logger.info("NEWS COLLECTION STARTING")
        logger.info("="*60)
        
        sources = self.get_active_sources()
        logger.info(f"Active sources: {len(sources)}")
        
        # Collect RSS
        rss_articles = self.collect_rss_articles(sources)
        logger.info(f"RSS articles collected: {len(rss_articles)}")
        
        # Collect NewsAPI
        newsapi_articles = self.collect_newsapi_articles()
        logger.info(f"NewsAPI articles collected: {len(newsapi_articles)}")
        
        all_articles = rss_articles + newsapi_articles
        
        # Deduplicate by URL
        seen_urls = set()
        unique_articles = []
        for article in all_articles:
            if article['url'] not in seen_urls:
                seen_urls.add(article['url'])
                unique_articles.append(article)
        
        logger.info(f"Total unique articles: {len(unique_articles)}")
        return unique_articles
    
    # =========================================================================
    # AI ANALYSIS
    # =========================================================================
    
    def build_analysis_context(self) -> str:
        """Build context for AI analysis"""
        
        # Company profile context
        profile_ctx = ""
        if self.company_profile:
            profile_ctx = f"""
YOUR COMPANY (Rezon Bio):
- Capabilities: {self.company_profile.get('Capabilities', 'European biologics CDMO')}
- Strengths: {self.company_profile.get('Strengths', 'Mammalian cell culture, mAbs, bispecifics, ADCs')}
- Target: {self.company_profile.get('Market Positioning', 'Mid-size biotech companies')}
"""
        
        # High-ICP companies to watch
        watch_companies = [c['name'] for c in self.high_icp_companies[:30]]
        companies_ctx = f"COMPANIES TO MONITOR (ICP>50): {', '.join(watch_companies)}"
        
        # Competitors
        competitor_names = [c.get('name', c.get('Company Name', '')) for c in self.competitors[:20]]
        competitors_ctx = f"COMPETITORS (track their news): {', '.join(competitor_names)}"
        
        # Trigger points
        triggers_ctx = "TRIGGER EVENTS TO DETECT:\n"
        for t in self.trigger_points[:10]:
            name = t.get('name', t.get('Trigger Name', ''))
            keywords = t.get('keywords', t.get('Keywords', ''))
            triggers_ctx += f"- {name}: {keywords[:100]}\n"
        
        return f"""
{profile_ctx}

{companies_ctx}

{competitors_ctx}

{triggers_ctx}

ICP SCORING (0-90 points):
- Company Size: 0 to -20 (penalties only for large companies)
- Funding: 0-15 (Series C+ = 15, B = 12, A = 8)
- Pipeline: 0-25 (Phase 2-3 = 25, Commercial = 20)
- Technology: 0-20 (Mammalian/mAbs = 20, Cell/Gene = 0)
- Location: 0-10 (US/Europe = 10)
- Manufacturing Need: 0-15 (No partner = 15)
- Product Type: 0-5 (Bispecifics/ADCs = 5)

TIERS: 80+ = Tier 1, 65-79 = Tier 2, 50-64 = Tier 3, 35-49 = Tier 4, <35 = Tier 5

EXCLUSIONS (score = 0):
- Gene therapy / viral vectors ONLY
- Cell therapy ONLY (CAR-T, iPSC)
- Other CDMOs/CMOs
"""
    
    def analyze_article(self, article: Dict) -> Dict:
        """Analyze single article with AI"""
        
        context = self.build_analysis_context()
        
        prompt = f"""Analyze this biotech/pharma news article for a European biologics CDMO.

{context}

ARTICLE TO ANALYZE:
Title: {article['headline']}
Source: {article['source']}
Date: {article['published_date']}
Content: {article['content'][:3000]}

ANALYSIS REQUIRED:
1. Is this relevant to a biologics CDMO? Score relevance 0-10
2. What company/companies are mentioned?
3. Is this an existing monitored company or a NEW potential client?
4. What trigger events are present?
5. Is there a business opportunity?
6. What's the ICP score estimate for the company?
7. Who should we contact?

Return ONLY valid JSON:
{{
    "relevance_score": 7,
    "summary": "2-3 sentence summary",
    "why_important": "Why this matters for BD",
    "primary_category": "Client News|Competitor News|Industry Trend|Regulatory|Technology|Other",
    "importance_level": "Critical|High|Medium|Low",
    
    "companies_mentioned": [
        {{
            "name": "Company Name",
            "is_new": true,
            "is_competitor": false,
            "is_monitored": false,
            "location": "Country",
            "modality": "mAbs|Bispecifics|ADCs|Other",
            "estimated_icp_score": 65,
            "icp_tier": "Tier 2"
        }}
    ],
    
    "trigger_events": [
        {{
            "type": "FUNDING|PIPELINE|PARTNERSHIP|EXPANSION|LEADERSHIP|M&A|CLINICAL_NEWS|NEWS",
            "details": "Specific details",
            "urgency": "HIGH|MEDIUM|LOW"
        }}
    ],
    
    "opportunity_flag": true,
    "opportunity_assessment": "Description of the opportunity",
    "recommended_contacts": ["VP Manufacturing", "Head of CMC"],
    "recommended_action": "Specific next step",
    "timing": "When to act",
    
    "competitor_intelligence": "Any competitor insights (if applicable)"
}}

SCORING GUIDANCE:
- 9-10: Direct opportunity with named company needing CDMO
- 7-8: Strong client/competitor news with actionable insights
- 5-6: Relevant industry news
- 3-4: Tangentially relevant
- 0-2: Not relevant

Set opportunity_flag=true ONLY if:
- Specific company identified
- Clear manufacturing need signal
- Company fits ICP (not gene/cell therapy only)
- Actionable timing

Return ONLY JSON."""

        try:
            message = self.anthropic_client.messages.create(
                model=self.config['anthropic']['model'],
                max_tokens=2000,
                messages=[{"role": "user", "content": prompt}]
            )
            
            response_text = message.content[0].text
            
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
            logger.error(f"Analysis error: {e}")
            return {
                'relevance_score': 0,
                'summary': 'Analysis failed',
                'error': str(e)
            }
    
    def save_analysis(self, article_id: str, article: Dict, analysis: Dict) -> Optional[str]:
        """Save analysis to News Analysis table"""
        if not self.news_analysis_table:
            return None
        
        try:
            # Validate category
            valid_categories = ['Client News', 'Competitor News', 'Industry Trend', 'Regulatory', 'Technology', 'Other']
            category = analysis.get('primary_category', 'Other')
            if category not in valid_categories:
                category = 'Other'
            
            # Validate importance
            valid_importance = ['Critical', 'High', 'Medium', 'Low']
            importance = analysis.get('importance_level', 'Medium')
            if importance not in valid_importance:
                importance = 'Medium'
            
            data = {
                'Article': [article_id] if article_id else None,
                'Analysis Date': datetime.now().strftime('%Y-%m-%d'),
                'Short Summary': analysis.get('summary', '')[:1000],
                'Why Important': analysis.get('why_important', '')[:1000],
                'Opportunity Assessment': analysis.get('opportunity_assessment', '')[:1000],
                'Relevance Score': min(max(analysis.get('relevance_score', 0), 0), 10),
                'Primary Category': category,
                'Importance Level': importance,
                'Opportunity Flag': analysis.get('opportunity_flag', False),
                'Action Items': analysis.get('recommended_action', '')[:1000],
                'Competitor Intelligence': analysis.get('competitor_intelligence', '')[:1000]
            }
            
            # Remove None values
            data = {k: v for k, v in data.items() if v is not None}
            
            record = self.news_analysis_table.create(data)
            return record['id']
            
        except Exception as e:
            logger.error(f"Error saving analysis: {e}")
            return None
    
    # =========================================================================
    # COMPANY & LEAD INTEGRATION
    # =========================================================================
    
    def process_company_from_news(self, company_info: Dict, article: Dict, analysis: Dict) -> Optional[str]:
        """Process company discovered in news - create/update as needed"""
        
        company_name = company_info.get('name', '')
        if not company_name:
            return None
        
        # Skip competitors
        if company_info.get('is_competitor'):
            logger.info(f"  Skipping competitor: {company_name}")
            return None
        
        # Check if exists
        try:
            safe_name = company_name.replace("'", "\\'")
            matches = self.companies_table.all(
                formula=f"LOWER({{Company Name}}) = LOWER('{safe_name}')"
            )
            
            if matches:
                company_record = matches[0]
                logger.info(f"  Found existing company: {company_name} (ICP: {company_record['fields'].get('ICP Fit Score', 'N/A')})")
                return company_record['id']
            
        except Exception as e:
            logger.debug(f"Company lookup error: {e}")
        
        # New company - create if ICP is promising
        estimated_icp = company_info.get('estimated_icp_score', 0)
        if estimated_icp < 40:
            logger.info(f"  Skipping low-ICP company: {company_name} (est: {estimated_icp})")
            return None
        
        # Create new company
        logger.info(f"  Creating new company: {company_name} (est ICP: {estimated_icp})")
        
        try:
            new_company = self.companies_table.create({
                'Company Name': company_name,
                'Location/HQ': company_info.get('location', ''),
                'Enrichment Status': 'Not Enriched',
                'Intelligence Notes': f"Discovered from news: {article['headline'][:200]}"
            })
            
            # TODO: Trigger full enrichment via enrich_companies.py
            # For now, mark for enrichment queue
            
            return new_company['id']
            
        except Exception as e:
            logger.error(f"Error creating company: {e}")
            return None
    
    def find_or_create_lead(self, company_id: str, company_name: str, 
                           recommended_contacts: List[str]) -> Optional[str]:
        """Find existing lead or create placeholder for company"""
        
        # Check for existing leads at this company
        try:
            leads = self.leads_table.all(formula=f"{{Company}}='{company_id}'")
            
            if leads:
                # Return best lead (highest ICP score)
                best_lead = max(leads, key=lambda l: l['fields'].get('Lead ICP Score', 0))
                lead_icp = best_lead['fields'].get('Lead ICP Score', 0)
                
                if lead_icp >= 50:
                    logger.info(f"  Found existing lead at {company_name} (ICP: {lead_icp})")
                    return best_lead['id']
                else:
                    logger.info(f"  Existing lead has low ICP ({lead_icp}) - may need better contact")
            
        except Exception as e:
            logger.debug(f"Lead lookup error: {e}")
        
        # No good lead - create placeholder with recommended title
        if recommended_contacts:
            target_title = recommended_contacts[0]
            
            try:
                new_lead = self.leads_table.create({
                    'Lead Name': f"[Find] {target_title}",
                    'Company': [company_id],
                    'Title': target_title,
                    'Enrichment Status': 'Not Enriched',
                    'Intelligence Notes': f"Contact needed for news opportunity at {company_name}"
                })
                
                logger.info(f"  Created lead placeholder: {target_title} at {company_name}")
                return new_lead['id']
                
            except Exception as e:
                logger.error(f"Error creating lead: {e}")
        
        return None
    
    def create_news_trigger(self, lead_id: str, company_id: str, 
                           article: Dict, analysis: Dict, trigger_info: Dict) -> Optional[str]:
        """Create NEWS trigger event in Trigger History"""
        
        try:
            trigger_type = trigger_info.get('type', 'NEWS')
            
            # Map to valid Airtable options
            valid_types = ['CONFERENCE_ATTENDANCE', 'FUNDING', 'PIPELINE', 'PROMOTION', 
                          'JOB_CHANGE', 'SPEAKING', 'PAIN_POINT', 'ROADSHOW', 'NEWS', 'OTHER']
            
            if trigger_type not in valid_types:
                # Map common types
                type_mapping = {
                    'PARTNERSHIP': 'NEWS',
                    'EXPANSION': 'NEWS',
                    'M&A': 'NEWS',
                    'CLINICAL_NEWS': 'PIPELINE',
                    'LEADERSHIP': 'JOB_CHANGE'
                }
                trigger_type = type_mapping.get(trigger_type, 'NEWS')
            
            trigger_data = {
                'Date Detected': datetime.now().strftime('%Y-%m-%d'),
                'Lead': [lead_id],
                'Trigger Type': trigger_type,
                'Urgency': trigger_info.get('urgency', 'MEDIUM'),
                'Description': f"NEWS: {article['headline'][:200]}",
                'Outreach Angle': analysis.get('opportunity_assessment', trigger_info.get('details', ''))[:500],
                'Status': 'New',
                'Sources': f"News: {article['source']}"
            }
            
            if company_id:
                trigger_data['Company'] = [company_id]
            
            record = self.trigger_history_table.create(trigger_data)
            logger.info(f"  ✓ Created NEWS trigger ({trigger_type})")
            return record['id']
            
        except Exception as e:
            logger.error(f"Error creating trigger: {e}")
            return None
    
    # =========================================================================
    # MAIN PROCESSING FLOW
    # =========================================================================
    
    def process_articles(self, articles: List[Dict], relevance_threshold: int = 5) -> Dict:
        """Process all articles through analysis pipeline"""
        
        logger.info("="*60)
        logger.info(f"ANALYZING {len(articles)} ARTICLES")
        logger.info("="*60)
        
        stats = {
            'total': len(articles),
            'analyzed': 0,
            'relevant': 0,
            'opportunities': 0,
            'new_companies': 0,
            'triggers_created': 0
        }
        
        for idx, article in enumerate(articles, 1):
            logger.info(f"\n[{idx}/{len(articles)}] {article['headline'][:60]}...")
            
            # Save raw article
            article_id = self.save_raw_article(article)
            
            # Analyze
            analysis = self.analyze_article(article)
            stats['analyzed'] += 1
            
            relevance = analysis.get('relevance_score', 0)
            
            if relevance < relevance_threshold:
                logger.info(f"  Score: {relevance} - below threshold, skipping")
                continue
            
            stats['relevant'] += 1
            logger.info(f"  Score: {relevance} - {analysis.get('primary_category', 'N/A')}")
            
            # Save analysis
            analysis_id = self.save_analysis(article_id, article, analysis)
            
            # Process opportunities
            if analysis.get('opportunity_flag'):
                stats['opportunities'] += 1
                logger.info(f"  🎯 OPPORTUNITY DETECTED")
                
                # Process each company mentioned
                for company_info in analysis.get('companies_mentioned', []):
                    if company_info.get('is_competitor'):
                        continue
                    
                    # Create/find company
                    company_id = self.process_company_from_news(company_info, article, analysis)
                    
                    if company_id:
                        if company_info.get('is_new'):
                            stats['new_companies'] += 1
                        
                        # Find/create lead
                        lead_id = self.find_or_create_lead(
                            company_id, 
                            company_info.get('name', ''),
                            analysis.get('recommended_contacts', [])
                        )
                        
                        if lead_id:
                            # Create trigger for each trigger event
                            for trigger in analysis.get('trigger_events', []):
                                self.create_news_trigger(
                                    lead_id, company_id, article, analysis, trigger
                                )
                                stats['triggers_created'] += 1
            
            # Rate limiting
            time.sleep(1)
        
        return stats
    
    def run_full_cycle(self, relevance_threshold: int = 5):
        """Run complete news collection and analysis cycle"""
        
        logger.info("="*70)
        logger.info("MARKET NEWS INTELLIGENCE - FULL CYCLE")
        logger.info(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("="*70)
        
        # Step 1: Collect news
        articles = self.collect_all_news()
        
        if not articles:
            logger.info("No new articles found")
            return
        
        # Step 2: Analyze and process
        stats = self.process_articles(articles, relevance_threshold)
        
        # Summary
        logger.info("\n" + "="*70)
        logger.info("CYCLE COMPLETE - SUMMARY")
        logger.info("="*70)
        logger.info(f"Total articles collected: {stats['total']}")
        logger.info(f"Articles analyzed: {stats['analyzed']}")
        logger.info(f"Relevant articles (score >= {relevance_threshold}): {stats['relevant']}")
        logger.info(f"Opportunities identified: {stats['opportunities']}")
        logger.info(f"New companies discovered: {stats['new_companies']}")
        logger.info(f"Trigger events created: {stats['triggers_created']}")
        logger.info(f"Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("="*70)
        
        return stats


# =============================================================================
# CLI
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description='Market News Intelligence System')
    parser.add_argument('--config', default='config.yaml', help='Config file path')
    parser.add_argument('--init', action='store_true', help='Initialize tables with defaults')
    parser.add_argument('--threshold', type=int, default=5, help='Relevance threshold (0-10)')
    parser.add_argument('--collect-only', action='store_true', help='Only collect, no analysis')
    parser.add_argument('--list-sources', action='store_true', help='List active news sources')
    
    args = parser.parse_args()
    
    try:
        system = MarketNewsIntelligence(args.config)
        
        if args.init:
            logger.info("Initializing tables with defaults...")
            system.initialize_news_sources()
            system.initialize_trigger_points()
            system.initialize_competitors()
            logger.info("Initialization complete")
            return
        
        if args.list_sources:
            sources = system.get_active_sources()
            print("\nActive News Sources:")
            print("-"*60)
            for s in sources:
                print(f"  • {s['name']} ({s.get('type', 'RSS')})")
            print(f"\nTotal: {len(sources)} sources")
            return
        
        if args.collect_only:
            articles = system.collect_all_news()
            print(f"\nCollected {len(articles)} articles")
            return
        
        # Full cycle
        system.run_full_cycle(args.threshold)
        
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
