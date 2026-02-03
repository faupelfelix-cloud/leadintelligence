#!/usr/bin/env python3
"""
Market News Intelligence System - Integrated with Lead Intelligence Platform

This system:
1. Collects news from configurable sources (RSS feeds, NewsAPI, ClinicalTrials.gov)
2. Analyzes against ICP criteria and existing company list
3. Discovers new companies and ENRICHES them immediately (inline)
4. Finds REAL leads (not placeholders), ENRICHES them (inline), generates OUTREACH
5. Creates NEWS trigger events with full context

All enrichment is done INLINE within this script - no external dependencies.

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
from confidence_utils import calculate_confidence_score
from company_profile_utils import (load_company_profile, build_value_proposition, 
                                   build_outreach_philosophy, filter_by_confidence,
                                   suppressed_to_do_not_mention)

# Import fuzzy matching utilities
try:
    from fuzzy_match import FuzzyMatcher, normalize_company_name, normalize_lead_name, similarity_score
    HAS_FUZZY_MATCH = True
except ImportError:
    HAS_FUZZY_MATCH = False
    FuzzyMatcher = None
    normalize_company_name = lambda x: x.lower().strip() if x else ""
    normalize_lead_name = lambda x: x.lower().strip() if x else ""
    similarity_score = lambda x, y, f: 1.0 if f(x) == f(y) else 0.0

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
        self.config_path = config_path
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
        self.company_profile = load_company_profile(self.base)
        
        # API client
        self.anthropic_client = anthropic.Anthropic(
            api_key=self.config['anthropic']['api_key']
        )
        
        # NewsAPI key (optional)
        self.news_api_key = os.getenv('NEWS_API_KEY', self.config.get('news_api', {}).get('api_key'))
        
        # Initialize fuzzy matcher for duplicate detection
        self.fuzzy_matcher = None
        if HAS_FUZZY_MATCH:
            try:
                self.fuzzy_matcher = FuzzyMatcher(
                    self.base,
                    company_table_name=self.config['airtable']['tables']['companies'],
                    leads_table_name=self.config['airtable']['tables']['leads'],
                    company_threshold=0.85,
                    lead_threshold=0.85
                )
                logger.info("✓ Fuzzy matching enabled")
            except Exception as e:
                logger.warning(f"Could not initialize FuzzyMatcher: {e}")
        
        # Load context data
        self.trigger_points = self._load_trigger_points()
        self.competitors = self._load_competitors()
        self.high_icp_companies = self._load_high_icp_companies()
        
        logger.info("MarketNewsIntelligence initialized")
        logger.info(f"  - {len(self.trigger_points)} trigger points loaded")
        logger.info(f"  - {len(self.competitors)} competitors tracked")
        logger.info(f"  - {len(self.high_icp_companies)} companies to monitor (ICP>=30)")
        logger.info(f"  - Inline enrichment: ENABLED")
    
    def _init_table(self, table_name: str):
        """Initialize table, return None if not found"""
        try:
            return self.base.table(table_name)
        except:
            logger.warning(f"Table '{table_name}' not found")
            return None
    
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
        """Load companies with ICP >= 30 for monitoring"""
        try:
            records = self.companies_table.all(formula="{ICP Fit Score}>=30")
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
        success_count = 0
        for source in DEFAULT_RSS_SOURCES:
            try:
                # Required fields only
                data = {
                    'Source Name': source['name'],
                    'URL': source['url'],
                    'Active': True
                }
                
                # Optional fields - try to add, ignore if they don't exist
                optional_fields = {
                    'Source Type': source.get('type', 'RSS Feed'),
                    'Category': source.get('category', ''),
                    'Quality Score': source.get('quality_score', 5),
                    'Check Frequency': source.get('check_frequency', 'Daily'),
                    'Date Added': datetime.now().strftime('%Y-%m-%d')
                }
                
                # Try with all fields first
                try:
                    data.update(optional_fields)
                    self.news_sources_table.create(data)
                    success_count += 1
                except Exception as e:
                    # If it fails, try with minimal fields
                    if 'UNKNOWN_FIELD_NAME' in str(e):
                        minimal_data = {
                            'Source Name': source['name'],
                            'URL': source['url'],
                            'Active': True
                        }
                        self.news_sources_table.create(minimal_data)
                        success_count += 1
                        logger.warning(f"  Added {source['name']} with minimal fields")
                    else:
                        raise e
                        
            except Exception as e:
                logger.error(f"Error adding source {source['name']}: {e}")
        
        logger.info(f"Added {success_count} default sources")
    
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
    
    def save_raw_article(self, article: Dict, analysis: Dict = None) -> Optional[str]:
        """Save raw article to News Articles table with optional analysis score"""
        if not self.news_articles_table:
            return None
        
        try:
            data = {
                'Headline': article.get('headline', '')[:500],
                'URL': article.get('url', ''),
                'Source': article.get('source', ''),
                'Published Date': article.get('published_date', ''),
                'Collected Date': datetime.now().strftime('%Y-%m-%d'),
                'Raw Content': article.get('content', '')[:5000],
                'Author': article.get('author', ''),
                'Processing Status': 'New'
            }
            
            # Add analysis data if provided
            if analysis:
                data['Processing Status'] = 'Analyzed'
                # Add score if the field exists
                if analysis.get('relevance_score') is not None:
                    data['Relevance Score'] = analysis.get('relevance_score', 0)
                # Add scoring explanation if the field exists
                scoring_explanation = []
                if analysis.get('summary'):
                    scoring_explanation.append(f"Summary: {analysis['summary']}")
                if analysis.get('why_important'):
                    scoring_explanation.append(f"Why Important: {analysis['why_important']}")
                if analysis.get('relevance_reasoning'):
                    scoring_explanation.append(f"Reasoning: {analysis['relevance_reasoning']}")
                if scoring_explanation:
                    data['Scoring Explanation'] = '\n\n'.join(scoring_explanation)
            
            record = self.news_articles_table.create(data)
            return record['id']
        except Exception as e:
            logger.error(f"Error saving article: {e}")
            # Try minimal save
            try:
                minimal_data = {
                    'Headline': article.get('headline', '')[:500],
                    'URL': article.get('url', ''),
                    'Source': article.get('source', ''),
                    'Collected Date': datetime.now().strftime('%Y-%m-%d'),
                    'Processing Status': 'New'
                }
                record = self.news_articles_table.create(minimal_data)
                return record['id']
            except:
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
        companies_ctx = f"COMPANIES TO MONITOR (ICP>=30): {', '.join(watch_companies)}"
        
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
        """Save analysis to News Analysis table with graceful field handling"""
        if not self.news_analysis_table:
            return None
        
        if not analysis or not isinstance(analysis, dict):
            logger.error("Invalid analysis data")
            return None
        
        try:
            # Build data with only fields that have values
            data = {
                'Analysis Date': datetime.now().strftime('%Y-%m-%d'),
            }
            
            # Link to article if we have an ID
            if article_id:
                data['Article'] = [article_id]
            
            # Text fields - safe to add
            if analysis.get('summary'):
                data['Short Summary'] = str(analysis['summary'])[:1000]
            if analysis.get('why_important'):
                data['Why Important'] = str(analysis['why_important'])[:1000]
            if analysis.get('opportunity_assessment'):
                data['Opportunity Assessment'] = str(analysis['opportunity_assessment'])[:1000]
            if analysis.get('recommended_action'):
                data['Action Items'] = str(analysis['recommended_action'])[:1000]
            if analysis.get('competitor_intelligence'):
                data['Competitor Intelligence'] = str(analysis['competitor_intelligence'])[:1000]
            
            # Number field
            if analysis.get('relevance_score') is not None:
                score = analysis.get('relevance_score', 0)
                if isinstance(score, (int, float)):
                    data['Relevance Score'] = min(max(int(score), 0), 10)
            
            # Boolean field
            if analysis.get('opportunity_flag') is not None:
                data['Opportunity Flag'] = bool(analysis.get('opportunity_flag', False))
            
            # Create record with safe fields first
            record = self.news_analysis_table.create(data)
            analysis_id = record['id']
            
            # Try to add single-select fields separately (they might fail)
            select_fields = {}
            
            # Map category to valid options
            category = analysis.get('primary_category', '')
            if category:
                valid_categories = ['Client News', 'Competitor News', 'Industry Trend', 'Regulatory', 'Technology', 'Other']
                if category in valid_categories:
                    select_fields['Primary Category'] = category
                else:
                    # Try to match
                    category_lower = category.lower()
                    for vc in valid_categories:
                        if vc.lower() in category_lower or category_lower in vc.lower():
                            select_fields['Primary Category'] = vc
                            break
            
            # Map importance to valid options
            importance = analysis.get('importance_level', '')
            if importance:
                valid_importance = ['Critical', 'High', 'Medium', 'Low']
                if importance in valid_importance:
                    select_fields['Importance Level'] = importance
                else:
                    importance_lower = importance.lower()
                    for vi in valid_importance:
                        if vi.lower() in importance_lower:
                            select_fields['Importance Level'] = vi
                            break
            
            # Try to update with select fields
            if select_fields:
                try:
                    self.news_analysis_table.update(analysis_id, select_fields)
                except Exception as e:
                    logger.warning(f"Could not update select fields: {e}")
            
            return analysis_id
            
        except Exception as e:
            logger.error(f"Error saving analysis: {e}")
            return None
    
    # =========================================================================
    # COMPANY & LEAD INTEGRATION - Using Existing Enrichment Flows
    # =========================================================================
    
    def process_company_from_news(self, company_info: Dict, article: Dict, analysis: Dict) -> Optional[Tuple[str, bool]]:
        """
        Process company discovered in news - create/update and enrich using existing flow.
        Returns (company_id, is_new) or None
        """
        
        company_name = company_info.get('name', '')
        if not company_name:
            return None
        
        # Skip competitors
        if company_info.get('is_competitor'):
            logger.info(f"  Skipping competitor: {company_name}")
            return None
        
        # Check if exists using fuzzy matching
        company_id = None
        is_new = False
        
        try:
            if self.fuzzy_matcher:
                # Use fuzzy matching
                match_result = self.fuzzy_matcher.find_company(company_name)
                if match_result:
                    company_id = match_result['id']
                    matched_name = match_result['name']
                    match_score = match_result.get('match_score', 1.0)
                    icp_score = match_result['fields'].get('ICP Fit Score', 'N/A')
                    
                    if match_score < 1.0:
                        logger.info(f"  Fuzzy matched '{company_name}' -> '{matched_name}' (score: {match_score:.2f}, ICP: {icp_score})")
                    else:
                        logger.info(f"  Found existing company: {matched_name} (ICP: {icp_score})")
                    return (company_id, False)
            else:
                # Fallback to exact match (case-insensitive)
                safe_name = company_name.replace("'", "\\'")
                matches = self.companies_table.all(
                    formula=f"LOWER({{Company Name}}) = LOWER('{safe_name}')"
                )
                
                if matches:
                    company_record = matches[0]
                    company_id = company_record['id']
                    icp_score = company_record['fields'].get('ICP Fit Score', 'N/A')
                    logger.info(f"  Found existing company: {company_name} (ICP: {icp_score})")
                    return (company_id, False)
            
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
                'Intelligence Notes': f"Discovered from news: {article.get('headline', '')[:200]}"
            })
            company_id = new_company['id']
            is_new = True
            
            # ENRICH IMMEDIATELY - inline enrichment (don't rely on external enricher config)
            logger.info(f"  Enriching company: {company_name}...")
            try:
                enriched_data = self._enrich_company_inline(company_id, company_name)
                if enriched_data:
                    logger.info(f"  ✓ Company enriched - ICP Score: {enriched_data.get('icp_score', 'N/A')}")
            except Exception as e:
                logger.warning(f"  Company enrichment failed: {e}")
            
            return (company_id, True)
            
        except Exception as e:
            logger.error(f"Error creating company: {e}")
            return None
    
    def find_existing_lead(self, company_id: str, company_name: str) -> Optional[Tuple[str, Dict]]:
        """
        Find existing lead at company with good ICP score.
        Uses fuzzy matching to find leads with similar names.
        Returns (lead_id, lead_fields) or None
        """
        try:
            # Get all leads at this company
            leads = self.leads_table.all(formula=f"FIND('{company_id}', ARRAYJOIN({{Company}}))")
            
            if leads:
                # Return best lead (highest ICP score)
                best_lead = max(leads, key=lambda l: l['fields'].get('Lead ICP Score', 0))
                lead_icp = best_lead['fields'].get('Lead ICP Score', 0)
                lead_name = best_lead['fields'].get('Lead Name', 'Unknown')
                
                if lead_icp >= 50:
                    logger.info(f"  Found existing lead: {lead_name} (ICP: {lead_icp})")
                    return (best_lead['id'], best_lead['fields'])
                else:
                    logger.info(f"  Existing lead {lead_name} has low ICP ({lead_icp}) - will search for better contact")
            
        except Exception as e:
            logger.debug(f"Lead lookup error: {e}")
        
        return None
    
    def _lead_exists_fuzzy(self, lead_name: str, company_id: str) -> Optional[Tuple[str, Dict]]:
        """
        Check if a lead with similar name exists at company using fuzzy matching.
        Returns (lead_id, lead_fields) or None
        """
        if self.fuzzy_matcher:
            match_result = self.fuzzy_matcher.find_lead(lead_name, company_id)
            if match_result:
                match_score = match_result.get('match_score', 1.0)
                matched_name = match_result['name']
                if match_score < 1.0:
                    logger.info(f"  Fuzzy matched lead '{lead_name}' -> '{matched_name}' (score: {match_score:.2f})")
                return (match_result['id'], match_result['fields'])
        else:
            # Fallback: check by normalized name
            try:
                leads = self.leads_table.all(formula=f"FIND('{company_id}', ARRAYJOIN({{Company}}))")
                norm_query = normalize_lead_name(lead_name)
                
                for lead in leads:
                    existing_name = lead['fields'].get('Lead Name', '')
                    norm_existing = normalize_lead_name(existing_name)
                    
                    if norm_query == norm_existing:
                        logger.info(f"  Found existing lead (normalized match): {existing_name}")
                        return (lead['id'], lead['fields'])
                    
                    # Simple fuzzy check
                    score = similarity_score(lead_name, existing_name, normalize_lead_name)
                    if score >= 0.85:
                        logger.info(f"  Fuzzy matched lead '{lead_name}' -> '{existing_name}' (score: {score:.2f})")
                        return (lead['id'], lead['fields'])
            except Exception as e:
                logger.debug(f"Lead fuzzy lookup error: {e}")
        
        return None
    
    def search_and_create_lead(self, company_id: str, company_name: str, 
                                company_info: Dict, recommended_titles: List[str],
                                article: Dict, analysis: Dict) -> Optional[Tuple[str, Dict]]:
        """
        Search for a REAL lead at the company, create and enrich, generate outreach.
        Uses existing LeadEnricher flow.
        Returns (lead_id, lead_fields) or None if no lead found.
        """
        
        # Default target titles for CDMO outreach
        if not recommended_titles:
            recommended_titles = ['VP Manufacturing', 'Head of CMC', 'VP Operations', 'COO', 'VP Supply Chain']
        
        # Search for a real person at the company
        logger.info(f"  Searching for lead at {company_name}...")
        
        # Use AI to find a specific person with thorough search
        search_prompt = f"""Find a specific person at this company who would be the right contact for a biologics CDMO.

COMPANY: {company_name}
LOCATION: {company_info.get('location', 'Unknown')}
MODALITY: {company_info.get('modality', 'Biologics')}

TARGET ROLES (in order of preference):
{chr(10).join(f"- {title}" for title in recommended_titles[:5])}

SEARCH INSTRUCTIONS:
1. Search LinkedIn for "{company_name}" and look for employees with manufacturing, operations, CMC, or supply chain roles
2. Search the company website for leadership/team page
3. Search news articles for executive quotes or announcements
4. Look for VP, Director, or Head level positions
5. Get their LinkedIn URL - this is critical

REQUIRED INFORMATION:
- Full name (first and last)
- Current title at {company_name}
- LinkedIn profile URL

Return ONLY valid JSON:
{{
    "found": true,
    "name": "John Smith",
    "title": "VP, Manufacturing Operations",
    "linkedin_url": "https://linkedin.com/in/johnsmith",
    "email": "jsmith@company.com or null if not found",
    "confidence": "high|medium|low"
}}

If you absolutely cannot find anyone in manufacturing/operations/CMC roles after thorough search, return:
{{
    "found": false,
    "reason": "Detailed explanation of what was searched and why no one was found"
}}

Be thorough - search LinkedIn, company website, and news. Return ONLY JSON."""

        try:
            message = self.anthropic_client.messages.create(
                model=self.config['anthropic']['model'],
                max_tokens=2000,
                tools=[{"type": "web_search_20250305", "name": "web_search"}],
                messages=[{"role": "user", "content": search_prompt}]
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
                logger.info(f"  Could not parse lead search response")
                return None
            
            result = json.loads(json_str.strip())
            
            if not result.get('found') or not result.get('name'):
                logger.info(f"  No lead found at {company_name}: {result.get('reason', 'Unknown')}")
                return None
            
            lead_name = result.get('name')
            lead_title = result.get('title', '')
            linkedin_url = result.get('linkedin_url', '')
            lead_email = result.get('email', '')
            
            logger.info(f"  Found: {lead_name} ({lead_title})")
            
            # Check if this lead already exists (fuzzy match)
            existing_lead = self._lead_exists_fuzzy(lead_name, company_id)
            if existing_lead:
                lead_id, lead_fields = existing_lead
                logger.info(f"  Lead already exists (fuzzy match) - using existing record")
                return (lead_id, lead_fields)
            
            # Create the lead record - use only fields that exist in your table
            lead_data = {
                'Lead Name': lead_name,
                'Company': [company_id],
                'Title': lead_title,
                'Enrichment Status': 'Not Enriched',
                'Lead Source': 'News Intelligence',
                'Intelligence Notes': f"Found via news: {article.get('headline', '')[:150]}"
            }
            
            # Add optional fields if they have values
            if linkedin_url:
                lead_data['LinkedIn URL'] = linkedin_url
            if lead_email:
                lead_data['Email'] = lead_email
            
            try:
                lead_record = self.leads_table.create(lead_data)
                lead_id = lead_record['id']
                logger.info(f"  ✓ Created lead: {lead_name}")
            except Exception as e:
                # If create fails, try with minimal fields
                logger.warning(f"  Lead creation failed with full data: {e}")
                try:
                    minimal_data = {
                        'Lead Name': lead_name,
                        'Company': [company_id],
                        'Title': lead_title
                    }
                    lead_record = self.leads_table.create(minimal_data)
                    lead_id = lead_record['id']
                    logger.info(f"  ✓ Created lead with minimal fields: {lead_name}")
                except Exception as e2:
                    logger.error(f"  Lead creation failed completely: {e2}")
                    return None
            
            # ENRICH THE LEAD - inline enrichment
            logger.info(f"  Enriching lead...")
            try:
                enriched_lead = self._enrich_lead_inline(
                    lead_id=lead_id,
                    lead_name=lead_name,
                    lead_title=lead_title,
                    company_id=company_id,
                    company_name=company_name,
                    linkedin_url=linkedin_url
                )
                if enriched_lead:
                    logger.info(f"  ✓ Lead enriched - ICP: {enriched_lead.get('lead_icp', 'N/A')}")
                else:
                    logger.warning(f"  Lead enrichment returned None - will still generate outreach")
                    enriched_lead = {'lead_icp': 50, 'company_icp': 50, 'title': lead_title}
                
                # GENERATE OUTREACH - always try if we have a lead_id
                logger.info(f"  Generating outreach messages...")
                try:
                    outreach_result = self._generate_outreach_inline(
                        lead_id=lead_id,
                        lead_name=lead_name,
                        lead_title=enriched_lead.get('title') or lead_title,
                        company_name=company_name,
                        lead_icp=enriched_lead.get('lead_icp', 50),
                        company_icp=enriched_lead.get('company_icp', 50)
                    )
                    if outreach_result:
                        logger.info(f"  ✓ Outreach messages generated")
                    else:
                        logger.warning(f"  Outreach generation returned None")
                except Exception as e:
                    logger.warning(f"  Outreach generation failed: {e}")
                        
            except Exception as e:
                logger.warning(f"  Lead enrichment failed: {e}")
            
            # Get updated lead record
            try:
                updated_lead = self.leads_table.get(lead_id)
                return (lead_id, updated_lead['fields'])
            except:
                return (lead_id, {'Lead Name': lead_name, 'Title': lead_title})
                
        except Exception as e:
            logger.error(f"Error searching for lead: {e}")
            return None
    
    # =========================================================================
    # INLINE ENRICHMENT METHODS - Self-contained, no external dependencies
    # =========================================================================
    
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
            
            # Helper function to sanitize AI output (curly quotes, smart quotes)
            def sanitize_val(s):
                if not isinstance(s, str):
                    return s
                for ch in '\u201c\u201d\u2018\u2019\u201a\u201b\u201f\u300c\u300d\u300e\u300f\u301d\u301e\uff02\uff07':
                    s = s.replace(ch, '')
                return s.strip('"\'').strip()
            
            # Helper function to validate single select
            def validate_single(value, valid_options, default='Unknown'):
                if not value:
                    return default
                value = sanitize_val(value)
                if value in valid_options:
                    return value
                # Try case-insensitive match
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
            
            # Company Size - validate
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
            
            # Funding Stage - single select
            if data.get('funding_stage'):
                update_fields['Funding Stage'] = validate_single(data['funding_stage'], VALID_FUNDING_STAGES, 'Unknown')
            
            # Total Funding - number
            if data.get('total_funding_usd'):
                try:
                    update_fields['Total Funding'] = float(data['total_funding_usd'])
                except:
                    pass
            
            # Latest Funding Round - text
            if data.get('latest_funding_round'):
                update_fields['Latest Funding Round'] = data['latest_funding_round']
            
            # Pipeline Stage - multi-select
            if data.get('pipeline_stages'):
                validated = validate_multi(data['pipeline_stages'], VALID_PIPELINE_STAGES)
                if validated:
                    update_fields['Pipeline Stage'] = validated
            
            # Lead Programs - text
            if data.get('lead_programs'):
                update_fields['Lead Programs'] = data['lead_programs']
            
            # Therapeutic Areas - multi-select
            if data.get('therapeutic_areas'):
                validated = validate_multi(data['therapeutic_areas'], VALID_THERAPEUTIC_AREAS)
                if validated:
                    update_fields['Therapeutic Areas'] = validated
            
            # Current CDMO Partnerships - text
            if data.get('cdmo_partnerships'):
                update_fields['Current CDMO Partnerships'] = data['cdmo_partnerships']
            
            # Manufacturing Status - single select
            if data.get('manufacturing_status'):
                update_fields['Manufacturing Status'] = validate_single(data['manufacturing_status'], VALID_MANUFACTURING_STATUS, 'Unknown')
            
            # Intelligence Notes with confidence
            data_confidence = data.get('data_confidence', {})
            disambiguation = data.get('disambiguation_note')
            notes_parts = []
            if data.get('recent_news'):
                notes_parts.append(data['recent_news'][:500])
            if data_confidence:
                low_conf = [f"⚠ {k}: {v}" for k, v in data_confidence.items() if v in ('low', 'unverified')]
                if low_conf:
                    notes_parts.append("Data Confidence Warnings:\n" + "\n".join(low_conf))
            if disambiguation:
                notes_parts.append(f"Disambiguation: {disambiguation}")
            if notes_parts:
                update_fields['Intelligence Notes'] = "\n\n".join(notes_parts)
            
            # Store raw confidence for downstream use
            if data_confidence:
                try:
                    update_fields['Data Confidence'] = json.dumps(data_confidence)
                    update_fields['Data Confidence Score'] = calculate_confidence_score(data_confidence)
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
                logger.info(f"  ✓ Company enriched - ICP: {data.get('icp_score', 'N/A')}, Urgency: {data.get('urgency_score', 'N/A')}")
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
                except Exception as e2:
                    logger.debug(f"Minimal update also failed: {e2}")
                    self.companies_table.update(company_id, {'Enrichment Status': 'Enriched'})
            
            return data
            
        except Exception as e:
            logger.error(f"Error in inline company enrichment: {e}")
            return None
    
    def _enrich_lead_inline(self, lead_id: str, lead_name: str, lead_title: str,
                           company_id: str, company_name: str, 
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
            
            # Get company ICP for lead scoring
            company_icp = 50
            try:
                company_record = self.companies_table.get(company_id)
                company_icp = company_record['fields'].get('ICP Fit Score', 50)
            except:
                pass
            
            # Calculate lead ICP based on title
            lead_icp, lead_icp_justification = self._calculate_lead_icp_with_justification(
                data.get('title') or lead_title, company_icp
            )
            
            # Determine Lead ICP Tier - MUST match exact Airtable options
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
            
            # Calculate Combined Priority - returns a STRING, not a number
            combined_priority = self._calculate_combined_priority(company_icp, lead_icp)
            
            # Map email confidence to valid options (High, Medium, Low only)
            email_conf = data.get('email_confidence', 'Medium')
            confidence_map = {
                'high': 'High',
                'medium': 'Medium',
                'low': 'Low',
                'pattern suggested': 'Low',  # Map to valid option
                'pattern': 'Low'
            }
            enrichment_confidence = confidence_map.get(email_conf.lower(), 'Medium')
            
            # Update lead record with all fields
            update_fields = {
                'Enrichment Status': 'Enriched',
                'Last Enrichment Date': datetime.now().strftime('%Y-%m-%d')
            }
            
            # Add ICP fields
            update_fields['Lead ICP Score'] = lead_icp
            update_fields['Lead ICP Tier'] = lead_icp_tier
            update_fields['Lead ICP Justification'] = lead_icp_justification
            update_fields['Combined Priority'] = combined_priority
            update_fields['Enrichment Confidence'] = enrichment_confidence
            
            if data.get('email'):
                update_fields['Email'] = data['email']
            if data.get('title'):
                update_fields['Title'] = data['title']
            if data.get('linkedin_url'):
                update_fields['LinkedIn URL'] = data['linkedin_url']
            # Note: Location field doesn't exist in Leads table - skip it
            if data.get('recent_activity'):
                update_fields['Intelligence Notes'] = data['recent_activity'][:1000]
            
            # Store data confidence for downstream use
            lead_data_confidence = data.get('data_confidence', {})
            if lead_data_confidence:
                try:
                    update_fields['Data Confidence'] = json.dumps(lead_data_confidence)
                    update_fields['Data Confidence Score'] = calculate_confidence_score(lead_data_confidence)
                except:
                    pass
            
            # Try full update first
            try:
                self.leads_table.update(lead_id, update_fields)
                logger.info(f"  ✓ Lead enriched - ICP: {lead_icp} ({lead_icp_tier}), Combined: {combined_priority}")
            except Exception as e:
                logger.warning(f"  Full lead update failed: {e}")
                # Try updating fields one by one to identify which one fails
                for field_name, field_value in update_fields.items():
                    try:
                        self.leads_table.update(lead_id, {field_name: field_value})
                        logger.debug(f"    ✓ Updated {field_name}")
                    except Exception as field_error:
                        logger.warning(f"    ✗ Failed to update {field_name}: {field_error}")
            
            return {
                'email': data.get('email'),
                'title': data.get('title') or lead_title,
                'linkedin_url': data.get('linkedin_url') or linkedin_url,
                'lead_icp': lead_icp,
                'company_icp': company_icp,
                'combined_priority': combined_priority
            }
            
        except Exception as e:
            logger.error(f"Error in inline lead enrichment: {e}")
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
        
        justification = f"Title score: {title_score}/100 ({title_reason}). Company ICP: {company_icp}/90. Combined (60% title + 40% company): {combined}/100"
        
        return combined, justification
    
    def _calculate_combined_priority(self, company_icp: int, lead_icp: int) -> str:
        """Calculate combined priority based on company and lead ICP scores - returns STRING"""
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
    
    def _calculate_lead_icp(self, title: str, company_icp: int) -> int:
        """Calculate lead ICP score based on title and company ICP"""
        if not title:
            return 50
        
        title_lower = title.lower()
        
        # Title scoring
        title_score = 50
        
        # High-value titles (70-90)
        if any(t in title_lower for t in ['vp manufacturing', 'vp operations', 'head of cmc', 
                                           'chief operating', 'coo', 'vp supply chain',
                                           'head of manufacturing', 'svp operations']):
            title_score = 85
        elif any(t in title_lower for t in ['vp', 'vice president', 'head of', 'director']):
            if any(f in title_lower for f in ['manufacturing', 'operations', 'cmc', 'supply', 'technical']):
                title_score = 75
            else:
                title_score = 60
        elif any(t in title_lower for t in ['ceo', 'chief executive', 'president', 'founder']):
            title_score = 70  # Decision maker but not always technical
        elif any(t in title_lower for t in ['senior director', 'executive director']):
            title_score = 70
        elif 'director' in title_lower:
            title_score = 60
        elif 'manager' in title_lower:
            title_score = 45
        
        # Combine with company ICP (60% title, 40% company)
        combined = int(title_score * 0.6 + company_icp * 0.4)
        return min(max(combined, 0), 100)
    
    def _generate_outreach_inline(self, lead_id: str, lead_name: str, lead_title: str,
                                  company_name: str, lead_icp: int, company_icp: int):
        """Generate outreach messages - inline implementation"""
        
        # Fetch and filter company data
        company_fields = {}
        do_not_mention_text = ""
        try:
            lead_record = self.leads_table.get(lead_id)
            company_ids = lead_record['fields'].get('Company', [])
            if company_ids:
                raw_fields = self.companies_table.get(company_ids[0])['fields']
                company_fields, suppressed = filter_by_confidence(raw_fields)
                do_not_mention_text = suppressed_to_do_not_mention(suppressed)
        except:
            pass
        
        # Build value proposition and philosophy
        value_prop = build_value_proposition(self.company_profile, company_fields, lead_title)
        outreach_rules = build_outreach_philosophy()
        
        prompt = f"""Generate professional outreach messages for this lead.

LEAD:
Name: {lead_name}
Title: {lead_title}
Company: {company_name}
Lead ICP Score: {lead_icp}/100
Company ICP Score: {company_icp}/90
{do_not_mention_text}

{value_prop}

{outreach_rules}

Generate 4 messages:

1. EMAIL (100-120 words):
Subject: [Natural subject line]
- Start with THEIR world, connect ONE Rezon strength to their situation
- Soft CTA
- Sign: "Best regards, [Your Name], Business Development"

2. LINKEDIN CONNECTION (under 200 chars):
- Brief, friendly, reference their role

3. LINKEDIN SHORT MESSAGE (under 300 chars):
- For after connection accepted
- Conversational, reference their background
- End: "Best regards, [Your Name]"

4. LINKEDIN INMAIL (100-120 words):
Subject: [Natural subject]
- Observation about their work, why connecting makes sense
- NO bullet lists
- Sign: "Best regards, [Your Name], Business Development"

Return ONLY valid JSON:
{{
    "email_subject": "Subject line",
    "email_message": "Full email body",
    "linkedin_connection": "Connection request text",
    "linkedin_short": "Short message text",
    "linkedin_inmail_subject": "InMail subject",
    "linkedin_inmail": "Full InMail body"
}}

Return ONLY JSON."""

        try:
            message = self.anthropic_client.messages.create(
                model=self.config['anthropic']['model'],
                max_tokens=3000,
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
            
            # Update lead record with outreach using correct field names
            outreach_update = {
                'Message Generated Date': datetime.now().strftime('%Y-%m-%d')
            }
            
            if data.get('email_subject'):
                outreach_update['Email Subject'] = data['email_subject']
            
            if data.get('email_message'):
                outreach_update['Email Body'] = data['email_message']
            
            if data.get('linkedin_connection'):
                outreach_update['LinkedIn Connection Request'] = data['linkedin_connection']
            
            if data.get('linkedin_short'):
                outreach_update['LinkedIn Short Message'] = data['linkedin_short']
            
            if data.get('linkedin_inmail_subject'):
                outreach_update['LinkedIn InMail Subject'] = data['linkedin_inmail_subject']
            
            if data.get('linkedin_inmail'):
                outreach_update['LinkedIn InMail Body'] = data['linkedin_inmail']
            
            logger.info(f"  Outreach fields to update: {list(outreach_update.keys())}")
            
            if outreach_update and len(outreach_update) > 1:  # More than just the date
                try:
                    self.leads_table.update(lead_id, outreach_update)
                    logger.info(f"  ✓ Outreach messages saved to lead record")
                except Exception as e:
                    logger.error(f"  ✗ Outreach update failed: {e}")
                    # Try fields one by one
                    for field_name, field_value in outreach_update.items():
                        try:
                            self.leads_table.update(lead_id, {field_name: field_value})
                            logger.debug(f"    ✓ Updated {field_name}")
                        except Exception as field_error:
                            logger.warning(f"    ✗ Failed to update {field_name}: {field_error}")
            else:
                logger.warning(f"  No outreach content generated")
            
            return data
            
        except Exception as e:
            logger.error(f"Error generating outreach: {e}")
            return None
    
    def create_news_trigger(self, lead_id: str, company_id: str, 
                           article: Dict, analysis: Dict, trigger_info: Dict) -> Optional[str]:
        """Create NEWS trigger event in Trigger History with article URL and outreach"""
        
        # Validate lead_id
        if not lead_id or not lead_id.startswith('rec'):
            logger.error(f"Invalid lead_id for trigger creation: {lead_id}")
            return None
        
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
            
            # Build Sources field with article URL
            article_url = article.get('url', '')
            article_source = article.get('source', 'News')
            sources_text = f"News: {article_source}"
            if article_url:
                sources_text += f"\nURL: {article_url}"
            
            # Build description with article headline
            headline = article.get('headline', '')[:150]
            description = f"NEWS: {headline}"
            if trigger_info.get('details'):
                description += f"\n\nDetails: {trigger_info['details'][:300]}"
            
            # Build outreach angle
            outreach_angle = analysis.get('opportunity_assessment', '') or trigger_info.get('details', '')
            if not outreach_angle:
                outreach_angle = f"Follow up on news: {headline}"
            
            # Determine timing recommendation based on trigger type and urgency
            urgency = trigger_info.get('urgency', 'MEDIUM')
            if urgency == 'HIGH':
                timing_recommendation = "Reach out within 24-48 hours while news is fresh"
                best_time_to_send = "Morning (9-10 AM) - catch them at start of day"
            elif urgency == 'MEDIUM':
                timing_recommendation = "Reach out within 1 week, reference the recent news"
                best_time_to_send = "Tuesday-Thursday, mid-morning (10-11 AM)"
            else:
                timing_recommendation = "Add to nurture sequence, reach out within 2 weeks"
                best_time_to_send = "Any weekday, avoid Mondays and Fridays"
            
            # Build follow-up angle based on trigger type
            follow_up_angle = ""
            if trigger_type == 'FUNDING':
                follow_up_angle = "Reference their recent funding and how it might accelerate their pipeline/manufacturing needs"
            elif trigger_type == 'PIPELINE':
                follow_up_angle = "Congratulate on clinical progress and discuss manufacturing scale-up requirements"
            elif trigger_type == 'PROMOTION' or trigger_type == 'JOB_CHANGE':
                follow_up_angle = "Congratulate on new role and offer to discuss how you can support their objectives"
            elif trigger_type == 'CONFERENCE_ATTENDANCE':
                follow_up_angle = "Reference meeting at conference and continue the conversation"
            else:
                follow_up_angle = f"Reference the recent news about {headline[:50]}... and explore potential collaboration"
            
            # Event date (article publication date or today)
            event_date = article.get('published', datetime.now().strftime('%Y-%m-%d'))
            if isinstance(event_date, str) and 'T' in event_date:
                event_date = event_date.split('T')[0]
            
            trigger_data = {
                'Date Detected': datetime.now().strftime('%Y-%m-%d'),
                'Event Date': event_date,
                'Lead': [lead_id],
                'Trigger Type': trigger_type,
                'Trigger Source': 'News Intelligence',  # New standardized field
                'Urgency': urgency,
                'Description': description[:1000],
                'Outreach Angle': outreach_angle[:500],
                'Follow Up Angle': follow_up_angle,
                'Timing Recommendation': timing_recommendation,
                'Best Time to Send': best_time_to_send,
                'Status': 'New',
                'Sources': sources_text
            }
            
            if company_id:
                trigger_data['Company'] = [company_id]
            
            # Generate trigger-specific outreach
            trigger_outreach = self._generate_trigger_outreach(
                lead_id=lead_id,
                trigger_type=trigger_type,
                headline=headline,
                outreach_angle=outreach_angle
            )
            
            if trigger_outreach:
                if trigger_outreach.get('email_subject'):
                    trigger_data['Email Subject'] = trigger_outreach['email_subject']
                if trigger_outreach.get('email_body'):
                    trigger_data['Email Body'] = trigger_outreach['email_body']
                if trigger_outreach.get('linkedin_connection'):
                    trigger_data['LinkedIn Connection Request'] = trigger_outreach['linkedin_connection']
                if trigger_outreach.get('linkedin_short'):
                    trigger_data['LinkedIn Short Message'] = trigger_outreach['linkedin_short']
                trigger_data['Outreach Generated Date'] = datetime.now().strftime('%Y-%m-%d')
            
            record = self.trigger_history_table.create(trigger_data)
            logger.info(f"  ✓ Created NEWS trigger ({trigger_type}) with outreach")
            return record['id']
            
        except Exception as e:
            logger.error(f"Error creating trigger: {e}")
            return None
    
    def _generate_trigger_outreach(self, lead_id: str, trigger_type: str, 
                                   headline: str, outreach_angle: str) -> Optional[Dict]:
        """Generate trigger-specific outreach messages"""
        
        # Get lead info and filtered company data
        lead_name = "there"
        lead_title = ""
        company_name = ""
        company_fields = {}
        do_not_mention_text = ""
        try:
            lead_record = self.leads_table.get(lead_id)
            lead_name = lead_record['fields'].get('Lead Name', 'there')
            lead_title = lead_record['fields'].get('Title', '')
            company_ids = lead_record['fields'].get('Company', [])
            if company_ids:
                raw_fields = self.companies_table.get(company_ids[0])['fields']
                company_fields, suppressed = filter_by_confidence(raw_fields)
                company_name = company_fields.get('Company Name', '')
                do_not_mention_text = suppressed_to_do_not_mention(suppressed)
        except:
            pass
        
        # Build value proposition and philosophy
        value_prop = build_value_proposition(self.company_profile, company_fields, lead_title)
        outreach_rules = build_outreach_philosophy()
        
        prompt = f"""Generate trigger-specific outreach messages referencing recent news.

NEWS HEADLINE: {headline}

OUTREACH ANGLE: {outreach_angle}

LEAD:
Name: {lead_name}
Title: {lead_title}
Company: {company_name}
{do_not_mention_text}

{value_prop}

{outreach_rules}

Lead with the trigger event — this news is WHY you're reaching out NOW.

Generate 3 SHORT messages that reference this specific news:

1. EMAIL (100-120 words):
Subject: [Reference the news naturally]
- Open by referencing the news
- Connect ONE Rezon strength to their situation
- Soft CTA
- Sign: "Best regards, [Your Name], Rezon Bio Business Development"

2. LINKEDIN CONNECTION (under 200 chars):
- Reference the news briefly
- Why you'd like to connect

3. LINKEDIN SHORT MESSAGE (under 300 chars):
- Reference the news naturally
- Brief mention of relevance
- Sign: "Best regards, [Your Name]"

Return ONLY valid JSON:
{{
    "email_subject": "Subject referencing news",
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
            logger.debug(f"Error generating trigger outreach: {e}")
            return None
    
    # =========================================================================
    # MAIN PROCESSING FLOW - FULLY INTEGRATED
    # =========================================================================
    
    def process_articles(self, articles: List[Dict], relevance_threshold: int = 5) -> Dict:
        """
        Process articles with fully integrated enrichment flow:
        
        PHASE 1: Screen and analyze all articles
        PHASE 2: For opportunities - Create/enrich companies, find/enrich leads, generate outreach, create triggers
        """
        
        logger.info("="*60)
        logger.info(f"PHASE 1: ANALYZING {len(articles)} ARTICLES")
        logger.info("="*60)
        
        stats = {
            'total': len(articles),
            'analyzed': 0,
            'relevant': 0,
            'opportunities': 0,
            'companies_created': 0,
            'companies_enriched': 0,
            'leads_found': 0,
            'leads_enriched': 0,
            'outreach_generated': 0,
            'triggers_created': 0
        }
        
        # Collect opportunities for processing
        opportunities = []
        
        # PHASE 1: Screen and analyze all articles
        for idx, article in enumerate(articles, 1):
            headline = article.get('headline', '')[:60]
            logger.info(f"\n[{idx}/{len(articles)}] {headline}...")
            
            # Analyze article
            analysis = self.analyze_article(article)
            stats['analyzed'] += 1
            
            if not analysis or not isinstance(analysis, dict):
                logger.warning("  Analysis failed - skipping")
                continue
            
            relevance = analysis.get('relevance_score', 0)
            
            # Save article with analysis score
            article_id = self.save_raw_article(article, analysis)
            
            if relevance < relevance_threshold:
                logger.info(f"  Score: {relevance} - below threshold")
                continue
            
            stats['relevant'] += 1
            category = analysis.get('primary_category', 'N/A')
            logger.info(f"  Score: {relevance} - {category}")
            
            # Save analysis
            analysis_id = self.save_analysis(article_id, article, analysis)
            
            # Collect opportunities for phase 2
            if analysis.get('opportunity_flag'):
                stats['opportunities'] += 1
                logger.info(f"  🎯 OPPORTUNITY FLAGGED")
                
                opportunities.append({
                    'article': article,
                    'article_id': article_id,
                    'analysis': analysis,
                    'analysis_id': analysis_id
                })
            
            # Rate limiting
            time.sleep(0.5)
        
        if not opportunities:
            logger.info("\nNo opportunities found - done")
            return stats
        
        # PHASE 2: Process each opportunity fully (company → lead → outreach → trigger)
        logger.info("\n" + "="*60)
        logger.info(f"PHASE 2: PROCESSING {len(opportunities)} OPPORTUNITIES")
        logger.info("="*60)
        
        for opp_idx, opp in enumerate(opportunities, 1):
            analysis = opp['analysis']
            article = opp['article']
            
            companies_mentioned = analysis.get('companies_mentioned', [])
            if not companies_mentioned:
                continue
            
            recommended_contacts = analysis.get('recommended_contacts', [])
            trigger_events = analysis.get('trigger_events', [])
            
            for company_info in companies_mentioned:
                if not isinstance(company_info, dict):
                    continue
                    
                company_name = company_info.get('name', '')
                if not company_name or company_info.get('is_competitor'):
                    continue
                
                logger.info(f"\n--- Processing: {company_name} ---")
                
                # STEP 1: Create/find and enrich company
                result = self.process_company_from_news(company_info, article, analysis)
                if not result:
                    logger.info(f"  Skipped - company not processable")
                    continue
                
                company_id, is_new_company = result
                if is_new_company:
                    stats['companies_created'] += 1
                    stats['companies_enriched'] += 1  # Inline enrichment runs automatically
                
                # STEP 2: Find existing lead OR search for new lead
                lead_result = self.find_existing_lead(company_id, company_name)
                
                if not lead_result:
                    # Search for and create a REAL lead (with enrichment + outreach)
                    lead_result = self.search_and_create_lead(
                        company_id=company_id,
                        company_name=company_name,
                        company_info=company_info,
                        recommended_titles=recommended_contacts,
                        article=article,
                        analysis=analysis
                    )
                    
                    if lead_result:
                        stats['leads_found'] += 1
                        stats['leads_enriched'] += 1
                        stats['outreach_generated'] += 1
                
                if not lead_result:
                    logger.info(f"  No lead found - skipping trigger creation")
                    continue
                
                lead_id, lead_fields = lead_result
                logger.info(f"  Lead ID for trigger: {lead_id}")
                
                # STEP 3: Create trigger(s)
                if trigger_events:
                    for trigger in trigger_events:
                        if isinstance(trigger, dict):
                            trigger_id = self.create_news_trigger(lead_id, company_id, article, analysis, trigger)
                            if trigger_id:
                                logger.info(f"  ✓ Trigger {trigger_id} linked to Lead {lead_id}")
                            stats['triggers_created'] += 1
                else:
                    # Create generic NEWS trigger
                    generic_trigger = {'type': 'NEWS', 'urgency': 'MEDIUM', 'details': ''}
                    trigger_id = self.create_news_trigger(lead_id, company_id, article, analysis, generic_trigger)
                    if trigger_id:
                        logger.info(f"  ✓ Trigger {trigger_id} linked to Lead {lead_id}")
                    stats['triggers_created'] += 1
                
                # Rate limiting between companies
                time.sleep(1)
        
        return stats
    
    def run_full_cycle(self, relevance_threshold: int = 5):
        """Run complete news collection and analysis cycle"""
        
        logger.info("="*70)
        logger.info("MARKET NEWS INTELLIGENCE - FULL INTEGRATED CYCLE")
        logger.info(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("="*70)
        
        # Step 1: Collect news
        articles = self.collect_all_news()
        
        if not articles:
            logger.info("No new articles found")
            return
        
        # Step 2: Analyze and process with integrated enrichment
        stats = self.process_articles(articles, relevance_threshold)
        
        # Summary
        logger.info("\n" + "="*70)
        logger.info("CYCLE COMPLETE - SUMMARY")
        logger.info("="*70)
        logger.info(f"Total articles collected: {stats['total']}")
        logger.info(f"Articles analyzed: {stats['analyzed']}")
        logger.info(f"Relevant articles (score >= {relevance_threshold}): {stats['relevant']}")
        logger.info(f"Opportunities identified: {stats['opportunities']}")
        logger.info(f"---")
        logger.info(f"Companies created: {stats.get('companies_created', 0)}")
        logger.info(f"Companies enriched: {stats.get('companies_enriched', 0)}")
        logger.info(f"Leads found: {stats.get('leads_found', 0)}")
        logger.info(f"Leads enriched: {stats.get('leads_enriched', 0)}")
        logger.info(f"Outreach generated: {stats.get('outreach_generated', 0)}")
        logger.info(f"Triggers created: {stats['triggers_created']}")
        logger.info(f"---")
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
