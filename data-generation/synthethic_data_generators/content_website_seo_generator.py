import json
import random
import uuid
from datetime import datetime, timedelta
import math
from typing import Dict, List, Tuple, Any, Optional
from dataclasses import dataclass, asdict
from enum import Enum
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
import uvicorn
from pydantic import BaseModel

class TrafficSource(Enum):
    ORGANIC_SEARCH = "organic"
    DIRECT = "direct"
    REFERRAL = "referral"
    SOCIAL = "social"
    EMAIL = "email"
    PAID_SEARCH = "cpc"

class DeviceCategory(Enum):
    DESKTOP = "desktop"
    MOBILE = "mobile"
    TABLET = "tablet"

class UserType(Enum):
    NEW_VISITOR = "New Visitor"
    RETURNING_VISITOR = "Returning Visitor"

class ContentCategory(Enum):
    BANKING_FEATURES = "banking_features"
    FINANCIAL_EDUCATION = "financial_education"
    COMPANY_INFO = "company_info"
    SUPPORT_HELP = "support_help"
    BLOG_ARTICLES = "blog_articles"
    PRICING = "pricing"
    SIGNUP_ONBOARDING = "signup_onboarding"
    LEGAL_COMPLIANCE = "legal_compliance"

class CustomerSegment(Enum):
    B2C_WORKING_AGE = "B2C_WORKING_AGE"
    B2C_STUDENTS = "B2C_STUDENTS"
    B2C_NON_WORKING = "B2C_NON_WORKING"
    B2B_SMALL = "B2B_SMALL"
    B2B_MEDIUM = "B2B_MEDIUM"
    B2B_LARGE = "B2B_LARGE"

@dataclass
class GoogleAnalyticsRecord:
    """Google Analytics-style record structure"""
    # GA Core Dimensions
    date: str
    session_id: str
    client_id: str  # GA client ID for cross-session tracking
    user_id: Optional[str]  # Cross-device user ID when available
    
    # Session Metrics
    sessions: int
    users: int
    new_users: int
    pageviews: int
    session_duration: int  # seconds
    bounce_rate: float
    
    # Traffic Source Dimensions
    source: str
    medium: str
    campaign: Optional[str]
    keyword: Optional[str]
    landing_page: str
    exit_page: str
    
    # Page/Content Dimensions
    page_path: str
    page_title: str
    content_category: str
    
    # Technical Dimensions
    device_category: str
    browser: str
    operating_system: str
    
    # Geographic Dimensions
    country: str
    region: str
    city: str
    
    # Custom Dimensions (Banking-specific)
    user_type: str
    customer_segment: Optional[str]
    engagement_score: float
    lead_score: Optional[float]
    
    # Goal/Conversion Metrics
    goal_completions: int
    goal_value: float
    ecommerce_revenue: float
    
    # Attribution Fields
    attribution_touchpoint_id: str
    cross_channel_customer_id: Optional[str]
    session_timestamp: str

@dataclass
class CampaignConfig:
    """Content campaign configuration structure"""
    name: str
    start_date: datetime
    end_date: datetime
    content_focus: List[ContentCategory]
    target_segments: List[CustomerSegment]
    organic_multiplier: float
    seasonal_boost: float

class DataRequest(BaseModel):
    """API request model for GA-style data generation"""
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    metrics: Optional[List[str]] = None
    dimensions: Optional[List[str]] = None
    traffic_sources: Optional[List[str]] = None
    device_categories: Optional[List[str]] = None
    content_categories: Optional[List[str]] = None
    customer_segments: Optional[List[str]] = None
    max_records: Optional[int] = 10000

class ContentWebsiteSEOGenerator:
    """
    Content/Website/SEO synthetic data API service for Dutch market
    Generates Google Analytics-style data (Jan 2024 - June 2025)
    """
    
    def __init__(self):
        self.total_customers = 200000
        self.website_penetration = 0.78  # 78% of customers visit website
        
        # Dutch cities for geographic data
        self.dutch_cities = {
            "Amsterdam": 0.25,
            "Rotterdam": 0.18,
            "The Hague": 0.15,
            "Utrecht": 0.12,
            "Eindhoven": 0.08,
            "Tilburg": 0.06,
            "Groningen": 0.05,
            "Almere": 0.04,
            "Breda": 0.04,
            "Nijmegen": 0.03
        }
        
        # Traffic source distribution
        self.traffic_source_distribution = {
            TrafficSource.ORGANIC_SEARCH: 0.45,  # SEO is primary
            TrafficSource.DIRECT: 0.25,          # Brand searches
            TrafficSource.REFERRAL: 0.12,        # Other sites
            TrafficSource.SOCIAL: 0.08,          # Social media links
            TrafficSource.EMAIL: 0.06,           # Email campaigns
            TrafficSource.PAID_SEARCH: 0.04      # Some paid overlap
        }
        
        # Device distribution for web traffic
        self.device_distribution = {
            DeviceCategory.MOBILE: 0.62,
            DeviceCategory.DESKTOP: 0.33,
            DeviceCategory.TABLET: 0.05
        }
        
        # Browser distribution for Netherlands
        self.browser_distribution = {
            "Chrome": 0.65,
            "Safari": 0.18,
            "Firefox": 0.08,
            "Edge": 0.06,
            "Opera": 0.02,
            "Other": 0.01
        }
        
        # Operating system distribution
        self.os_distribution = {
            "Windows": 0.45,
            "macOS": 0.22,
            "iOS": 0.18,
            "Android": 0.12,
            "Linux": 0.02,
            "Other": 0.01
        }
        
        # Customer segment distribution
        self.segment_distribution = {
            CustomerSegment.B2C_WORKING_AGE: 0.42,
            CustomerSegment.B2C_STUDENTS: 0.25,
            CustomerSegment.B2C_NON_WORKING: 0.18,
            CustomerSegment.B2B_SMALL: 0.10,
            CustomerSegment.B2B_MEDIUM: 0.04,
            CustomerSegment.B2B_LARGE: 0.01
        }
        
        self.customer_pool = self._generate_customer_pool()
        self._campaign_configs = None
        self._content_pages = self._define_content_pages()
        
    def _generate_customer_pool(self) -> List[Dict]:
        """Generate realistic customer pool with web analytics attributes"""
        customers = []
        active_customers = int(self.total_customers * self.website_penetration)
        
        for i in range(active_customers):
            segment = self._weighted_choice(list(self.segment_distribution.keys()), 
                                          list(self.segment_distribution.values()))
            
            # Generate GA-style client ID
            client_id = f"{random.randint(1000000000, 9999999999)}.{random.randint(1000000000, 9999999999)}"
            
            # User ID when customer is logged in (30% of sessions)
            user_id = f"user_{str(uuid.uuid4())[:8]}" if random.random() < 0.3 else None
            
            customer = {
                'customer_id': f"cust_{str(uuid.uuid4())[:8]}",
                'client_id': client_id,
                'user_id': user_id,
                'segment': segment,
                'device_preference': self._weighted_choice(list(self.device_distribution.keys()),
                                                         list(self.device_distribution.values())),
                'browser': self._weighted_choice(list(self.browser_distribution.keys()),
                                               list(self.browser_distribution.values())),
                'operating_system': self._weighted_choice(list(self.os_distribution.keys()),
                                                        list(self.os_distribution.values())),
                'city': self._weighted_choice(list(self.dutch_cities.keys()),
                                            list(self.dutch_cities.values())),
                'engagement_score': random.uniform(0.2, 0.95),
                'content_affinity': {
                    ContentCategory.BANKING_FEATURES: random.uniform(0.3, 0.9),
                    ContentCategory.FINANCIAL_EDUCATION: random.uniform(0.2, 0.8),
                    ContentCategory.PRICING: random.uniform(0.4, 0.9),
                    ContentCategory.BLOG_ARTICLES: random.uniform(0.1, 0.7)
                },
                'session_frequency': random.uniform(0.5, 8.0),  # sessions per week
                'conversion_probability': random.uniform(0.02, 0.15),
                'returning_visitor_probability': random.uniform(0.3, 0.8)
            }
            customers.append(customer)
            
        return customers
    
    def _weighted_choice(self, choices: List, weights: List) -> Any:
        """Select random choice based on weights"""
        return random.choices(choices, weights=weights)[0]
    
    def _define_content_pages(self) -> Dict[ContentCategory, List[Dict]]:
        """Define realistic website content structure"""
        return {
            ContentCategory.BANKING_FEATURES: [
                {"path": "/features/mobile-banking", "title": "Mobile Banking App - Bunq", "goal_value": 25},
                {"path": "/features/business-banking", "title": "Business Banking Solutions", "goal_value": 45},
                {"path": "/features/savings-accounts", "title": "High-Yield Savings Accounts", "goal_value": 20},
                {"path": "/features/payment-cards", "title": "Debit & Credit Cards", "goal_value": 18},
                {"path": "/features/international-transfers", "title": "International Money Transfers", "goal_value": 22},
                {"path": "/features/budgeting-tools", "title": "Smart Budgeting Tools", "goal_value": 15}
            ],
            ContentCategory.FINANCIAL_EDUCATION: [
                {"path": "/learn/banking-basics", "title": "Banking Basics for Beginners", "goal_value": 8},
                {"path": "/learn/saving-strategies", "title": "Smart Saving Strategies", "goal_value": 12},
                {"path": "/learn/business-finance", "title": "Small Business Finance Guide", "goal_value": 35},
                {"path": "/learn/investment-intro", "title": "Introduction to Investing", "goal_value": 18},
                {"path": "/learn/dutch-banking-system", "title": "Understanding Dutch Banking", "goal_value": 10}
            ],
            ContentCategory.COMPANY_INFO: [
                {"path": "/about", "title": "About Bunq - Dutch Mobile Bank", "goal_value": 8},
                {"path": "/careers", "title": "Careers at Bunq", "goal_value": 5},
                {"path": "/press", "title": "Press & Media", "goal_value": 3},
                {"path": "/sustainability", "title": "Sustainable Banking", "goal_value": 6}
            ],
            ContentCategory.SUPPORT_HELP: [
                {"path": "/help/getting-started", "title": "Getting Started with Bunq", "goal_value": 12},
                {"path": "/help/account-setup", "title": "Account Setup Guide", "goal_value": 15},
                {"path": "/help/troubleshooting", "title": "Troubleshooting Common Issues", "goal_value": 8},
                {"path": "/help/contact", "title": "Contact Support", "goal_value": 10}
            ],
            ContentCategory.BLOG_ARTICLES: [
                {"path": "/blog/dutch-fintech-trends", "title": "Dutch FinTech Trends 2024", "goal_value": 12},
                {"path": "/blog/mobile-banking-security", "title": "Mobile Banking Security Tips", "goal_value": 8},
                {"path": "/blog/business-growth-banking", "title": "Banking for Business Growth", "goal_value": 25},
                {"path": "/blog/student-banking-guide", "title": "Banking Guide for Students", "goal_value": 10},
                {"path": "/blog/kings-day-banking", "title": "King's Day: Banking on the Go", "goal_value": 6}
            ],
            ContentCategory.PRICING: [
                {"path": "/pricing", "title": "Bunq Pricing Plans", "goal_value": 40},
                {"path": "/pricing/personal", "title": "Personal Banking Pricing", "goal_value": 35},
                {"path": "/pricing/business", "title": "Business Banking Pricing", "goal_value": 55},
                {"path": "/pricing/compare", "title": "Compare Banking Plans", "goal_value": 30}
            ],
            ContentCategory.SIGNUP_ONBOARDING: [
                {"path": "/signup", "title": "Open Your Bunq Account", "goal_value": 100},
                {"path": "/signup/personal", "title": "Personal Account Registration", "goal_value": 85},
                {"path": "/signup/business", "title": "Business Account Registration", "goal_value": 120},
                {"path": "/onboarding/welcome", "title": "Welcome to Bunq", "goal_value": 75}
            ],
            ContentCategory.LEGAL_COMPLIANCE: [
                {"path": "/legal/privacy-policy", "title": "Privacy Policy", "goal_value": 2},
                {"path": "/legal/terms-conditions", "title": "Terms & Conditions", "goal_value": 3},
                {"path": "/legal/gdpr", "title": "GDPR Compliance", "goal_value": 4},
                {"path": "/legal/deposit-guarantee", "title": "Deposit Guarantee Scheme", "goal_value": 8}
            ]
        }
    
    def get_campaign_configs(self) -> List[CampaignConfig]:
        """Define content campaigns based on Dutch market calendar"""
        if self._campaign_configs is not None:
            return self._campaign_configs
            
        campaigns = []
        
        # 2024 Campaigns
        campaigns.extend([
            # January 2024 - New Year Financial Education
            CampaignConfig(
                name="Content_New_Year_Financial_Education",
                start_date=datetime(2024, 1, 2),
                end_date=datetime(2024, 1, 31),
                content_focus=[ContentCategory.FINANCIAL_EDUCATION, ContentCategory.BANKING_FEATURES],
                target_segments=[CustomerSegment.B2C_WORKING_AGE, CustomerSegment.B2C_STUDENTS],
                organic_multiplier=1.2,
                seasonal_boost=0.9
            ),
            
            # February-March 2024 - Spring Banking Content
            CampaignConfig(
                name="Content_Spring_Banking_Education",
                start_date=datetime(2024, 2, 20),
                end_date=datetime(2024, 3, 31),
                content_focus=[ContentCategory.BANKING_FEATURES, ContentCategory.PRICING, ContentCategory.BLOG_ARTICLES],
                target_segments=[CustomerSegment.B2C_WORKING_AGE, CustomerSegment.B2B_SMALL],
                organic_multiplier=1.4,
                seasonal_boost=1.3
            ),
            
            # April 2024 - King's Day Content
            CampaignConfig(
                name="Content_Kings_Day_Banking_Freedom",
                start_date=datetime(2024, 4, 15),
                end_date=datetime(2024, 5, 5),
                content_focus=[ContentCategory.BLOG_ARTICLES, ContentCategory.BANKING_FEATURES],
                target_segments=[CustomerSegment.B2C_WORKING_AGE, CustomerSegment.B2C_STUDENTS],
                organic_multiplier=1.6,
                seasonal_boost=1.25
            ),
            
            # May-June 2024 - Summer Preparation
            CampaignConfig(
                name="Content_Summer_Banking_Preparation",
                start_date=datetime(2024, 5, 10),
                end_date=datetime(2024, 6, 30),
                content_focus=[ContentCategory.BANKING_FEATURES, ContentCategory.FINANCIAL_EDUCATION],
                target_segments=[CustomerSegment.B2C_WORKING_AGE, CustomerSegment.B2C_STUDENTS],
                organic_multiplier=1.1,
                seasonal_boost=1.0
            ),
            
            # July-August 2024 - Summer Content (Lower activity)
            CampaignConfig(
                name="Content_Summer_Banking_Tips",
                start_date=datetime(2024, 7, 1),
                end_date=datetime(2024, 8, 31),
                content_focus=[ContentCategory.BLOG_ARTICLES, ContentCategory.SUPPORT_HELP],
                target_segments=[CustomerSegment.B2C_NON_WORKING, CustomerSegment.B2C_STUDENTS],
                organic_multiplier=0.7,
                seasonal_boost=0.6
            ),
            
            # September 2024 - Back to School/Work
            CampaignConfig(
                name="Content_Autumn_Financial_Planning",
                start_date=datetime(2024, 9, 1),
                end_date=datetime(2024, 10, 31),
                content_focus=[ContentCategory.FINANCIAL_EDUCATION, ContentCategory.PRICING, ContentCategory.BANKING_FEATURES],
                target_segments=[CustomerSegment.B2C_WORKING_AGE, CustomerSegment.B2C_STUDENTS, CustomerSegment.B2B_SMALL],
                organic_multiplier=1.3,
                seasonal_boost=1.1
            ),
            
            # November-December 2024 - Holiday Content
            CampaignConfig(
                name="Content_Holiday_Banking_Smart",
                start_date=datetime(2024, 11, 1),
                end_date=datetime(2024, 12, 20),
                content_focus=[ContentCategory.FINANCIAL_EDUCATION, ContentCategory.BANKING_FEATURES, ContentCategory.BLOG_ARTICLES],
                target_segments=[CustomerSegment.B2C_WORKING_AGE, CustomerSegment.B2C_NON_WORKING],
                organic_multiplier=1.2,
                seasonal_boost=1.15
            )
        ])
        
        # 2025 Campaigns (January - June)
        campaigns.extend([
            # January 2025
            CampaignConfig(
                name="Content_New_Year_Financial_Education",
                start_date=datetime(2025, 1, 2),
                end_date=datetime(2025, 1, 31),
                content_focus=[ContentCategory.FINANCIAL_EDUCATION, ContentCategory.BANKING_FEATURES],
                target_segments=[CustomerSegment.B2C_WORKING_AGE, CustomerSegment.B2C_STUDENTS],
                organic_multiplier=1.25,
                seasonal_boost=0.95
            ),
            
            # February-March 2025
            CampaignConfig(
                name="Content_Spring_Banking_Education",
                start_date=datetime(2025, 2, 20),
                end_date=datetime(2025, 3, 31),
                content_focus=[ContentCategory.BANKING_FEATURES, ContentCategory.PRICING, ContentCategory.BLOG_ARTICLES],
                target_segments=[CustomerSegment.B2C_WORKING_AGE, CustomerSegment.B2B_SMALL],
                organic_multiplier=1.45,
                seasonal_boost=1.35
            ),
            
            # April 2025
            CampaignConfig(
                name="Content_Kings_Day_Banking_Freedom",
                start_date=datetime(2025, 4, 15),
                end_date=datetime(2025, 5, 5),
                content_focus=[ContentCategory.BLOG_ARTICLES, ContentCategory.BANKING_FEATURES],
                target_segments=[CustomerSegment.B2C_WORKING_AGE, CustomerSegment.B2C_STUDENTS],
                organic_multiplier=1.65,
                seasonal_boost=1.3
            ),
            
            # May-June 2025
            CampaignConfig(
                name="Content_Summer_Banking_Preparation",
                start_date=datetime(2025, 5, 10),
                end_date=datetime(2025, 6, 30),
                content_focus=[ContentCategory.BANKING_FEATURES, ContentCategory.FINANCIAL_EDUCATION],
                target_segments=[CustomerSegment.B2C_WORKING_AGE, CustomerSegment.B2C_STUDENTS],
                organic_multiplier=1.15,
                seasonal_boost=1.05
            )
        ])
        
        self._campaign_configs = campaigns
        return campaigns
    
    def _generate_search_keywords(self, content_category: ContentCategory, segment: CustomerSegment) -> List[str]:
        """Generate realistic Dutch search keywords based on content and segment"""
        base_keywords = {
            ContentCategory.BANKING_FEATURES: [
                "mobiel bankieren", "online banking nederland", "bankrekening openen",
                "spaarrekening", "betaalkaart", "internetbankieren", "banking app"
            ],
            ContentCategory.FINANCIAL_EDUCATION: [
                "sparen tips", "financieel advies", "beleggen beginners", "budget maken",
                "geld besparen", "financiële planning", "spaargeld"
            ],
            ContentCategory.PRICING: [
                "bankkosten vergelijken", "gratis bankrekening", "bank tarieven",
                "goedkoopste bank", "bankkosten nederland"
            ],
            ContentCategory.COMPANY_INFO: [
                "bunq bank", "bunq review", "bunq ervaringen", "nederlandse bank",
                "fintech nederland", "mobiele bank"
            ],
            ContentCategory.BLOG_ARTICLES: [
                "fintech trends", "banking nieuws", "financieel nieuws nederland",
                "mobiel bankieren tips", "digitaal bankieren"
            ]
        }
        
        # Adjust keywords based on segment
        if segment.name.startswith("B2B"):
            business_keywords = [
                "zakelijk bankieren", "bedrijfsrekening", "business banking",
                "mkb financiering", "bedrijf bank", "zakelijke rekening"
            ]
            return business_keywords + base_keywords.get(content_category, [])
        
        return base_keywords.get(content_category, ["bunq", "mobile banking"])
    
    def _generate_utm_campaign(self, content_category: ContentCategory, date: datetime) -> Optional[str]:
        """Generate UTM campaign parameters for tracked traffic"""
        if random.random() < 0.3:  # 30% of organic traffic has UTM tracking
            campaigns = {
                ContentCategory.BANKING_FEATURES: "feature_education",
                ContentCategory.FINANCIAL_EDUCATION: "financial_literacy",
                ContentCategory.PRICING: "pricing_awareness",
                ContentCategory.BLOG_ARTICLES: "content_marketing",
                ContentCategory.SIGNUP_ONBOARDING: "conversion_funnel"
            }
            base_campaign = campaigns.get(content_category, "organic_content")
            return f"{base_campaign}_{date.strftime('%Y%m')}"
        return None
    
    def _calculate_session_metrics(self, customer: Dict, page: Dict, 
                                 traffic_source: TrafficSource) -> Tuple[int, float, int]:
        """Calculate realistic session duration, bounce rate, and pageviews"""
        # Base metrics
        base_duration = 180  # 3 minutes
        base_bounce_rate = 0.45
        base_pageviews = 2.5
        
        # Traffic source impact
        source_multipliers = {
            TrafficSource.ORGANIC_SEARCH: {"duration": 1.2, "bounce": 0.9, "pages": 1.3},
            TrafficSource.DIRECT: {"duration": 1.5, "bounce": 0.7, "pages": 1.8},
            TrafficSource.REFERRAL: {"duration": 0.9, "bounce": 1.1, "pages": 1.1},
            TrafficSource.SOCIAL: {"duration": 0.8, "bounce": 1.3, "pages": 0.9},
            TrafficSource.EMAIL: {"duration": 1.4, "bounce": 0.8, "pages": 1.6},
            TrafficSource.PAID_SEARCH: {"duration": 1.1, "bounce": 0.95, "pages": 1.2}
        }
        
        # Content category impact
        category_multipliers = {
            ContentCategory.SIGNUP_ONBOARDING: {"duration": 2.0, "bounce": 0.4, "pages": 3.0},
            ContentCategory.PRICING: {"duration": 1.8, "bounce": 0.5, "pages": 2.5},
            ContentCategory.BANKING_FEATURES: {"duration": 1.5, "bounce": 0.6, "pages": 2.2},
            ContentCategory.FINANCIAL_EDUCATION: {"duration": 2.2, "bounce": 0.5, "pages": 1.8},
            ContentCategory.BLOG_ARTICLES: {"duration": 1.3, "bounce": 0.7, "pages": 1.4},
            ContentCategory.SUPPORT_HELP: {"duration": 1.1, "bounce": 0.8, "pages": 1.6}
        }
        
        source_mult = source_multipliers.get(traffic_source, {"duration": 1.0, "bounce": 1.0, "pages": 1.0})
        
        # Determine category from page path
        category = ContentCategory.COMPANY_INFO  # default
        for cat, pages in self._content_pages.items():
            if any(page["path"] in p["path"] for p in pages):
                category = cat
                break
        
        cat_mult = category_multipliers.get(category, {"duration": 1.0, "bounce": 1.0, "pages": 1.0})
        
        # Calculate final metrics
        duration = int(base_duration * source_mult["duration"] * cat_mult["duration"] * 
                      customer['engagement_score'] * random.uniform(0.5, 2.0))
        
        bounce_rate = min(base_bounce_rate * source_mult["bounce"] * cat_mult["bounce"] * 
                         (1 / customer['engagement_score']), 1.0)
        
        pageviews = max(1, int(base_pageviews * source_mult["pages"] * cat_mult["pages"] * 
                              customer['engagement_score'] * random.uniform(0.7, 1.8)))
        
        return duration, bounce_rate, pageviews
    
    def _determine_conversion_metrics(self, customer: Dict, page: Dict, 
                                    session_duration: int, pageviews: int) -> Tuple[int, float, float]:
        """Calculate goal completions and revenue based on session quality"""
        goal_completions = 0
        goal_value = 0.0
        ecommerce_revenue = 0.0
        
        # High-value pages more likely to convert
        page_value = page.get("goal_value", 5)
        
        # Conversion probability based on multiple factors
        conversion_prob = (customer['conversion_probability'] * 
                          (session_duration / 300) *  # 5+ minutes increases probability
                          (pageviews / 3) *            # 3+ pages increases probability
                          (page_value / 20))           # Higher value pages
        
        conversion_prob = min(conversion_prob, 0.25)  # Cap at 25%
        
        if random.random() < conversion_prob:
            goal_completions = 1
            goal_value = page_value * random.uniform(0.8, 1.2)
            
            # Account signup generates revenue
            if "signup" in page["path"] or "pricing" in page["path"]:
                if customer['segment'].name.startswith("B2B"):
                    ecommerce_revenue = random.uniform(50, 200)  # Monthly B2B value
                else:
                    ecommerce_revenue = random.uniform(10, 50)   # Monthly B2C value
        
        return goal_completions, goal_value, ecommerce_revenue
    
    def _generate_sessions_for_campaign(self, campaign: CampaignConfig) -> List[GoogleAnalyticsRecord]:
        """Generate GA sessions for a specific content campaign"""
        records = []
        campaign_days = (campaign.end_date - campaign.start_date).days + 1
        
        # Daily session volume based on campaign intensity
        daily_sessions = int(2000 * campaign.organic_multiplier * campaign.seasonal_boost)
        
        # Select customers likely to engage with this content
        eligible_customers = [c for c in self.customer_pool 
                            if c['segment'] in campaign.target_segments]
        
        for day_offset in range(campaign_days):
            current_date = campaign.start_date + timedelta(days=day_offset)
            
            # Daily session variation (weekends lower)
            if current_date.weekday() >= 5:  # Weekend
                daily_multiplier = 0.7
            else:  # Weekday
                daily_multiplier = 1.0
            
            daily_session_count = int(daily_sessions * daily_multiplier)
            
            # Generate sessions for this day
            for _ in range(daily_session_count):
                customer = random.choice(eligible_customers)
                
                # Determine traffic source
                traffic_source = self._weighted_choice(list(self.traffic_source_distribution.keys()),
                                                     list(self.traffic_source_distribution.values()))
                
                # Select content page based on campaign focus
                content_category = random.choice(campaign.content_focus)
                available_pages = self._content_pages[content_category]
                selected_page = random.choice(available_pages)
                
                # Generate session timestamp
                hour = random.choices(range(24), weights=[0.2]*6 + [1.5]*12 + [0.8]*6)[0]
                session_time = current_date.replace(
                    hour=hour,
                    minute=random.randint(0, 59),
                    second=random.randint(0, 59)
                )
                
                # Calculate session metrics
                duration, bounce_rate, pageviews = self._calculate_session_metrics(
                    customer, selected_page, traffic_source)
                
                # Determine if new or returning visitor
                is_new = random.random() > customer['returning_visitor_probability']
                user_type = UserType.NEW_VISITOR if is_new else UserType.RETURNING_VISITOR
                
                # Generate source/medium details
                if traffic_source == TrafficSource.ORGANIC_SEARCH:
                    source = "google"
                    medium = "organic"
                    keywords = self._generate_search_keywords(content_category, customer['segment'])
                    keyword = random.choice(keywords)
                elif traffic_source == TrafficSource.DIRECT:
                    source = "(direct)"
                    medium = "(none)"
                    keyword = None
                elif traffic_source == TrafficSource.REFERRAL:
                    source = random.choice(["nu.nl", "tweakers.net", "linkedin.com", "facebook.com"])
                    medium = "referral"
                    keyword = None
                else:
                    source = traffic_source.value
                    medium = "organic" if traffic_source == TrafficSource.SOCIAL else traffic_source.value
                    keyword = None
                
                # UTM campaign tracking
                utm_campaign = self._generate_utm_campaign(content_category, current_date)
                
                # Calculate conversions
                goal_completions, goal_value, ecommerce_revenue = self._determine_conversion_metrics(
                    customer, selected_page, duration, pageviews)
                
                # Lead scoring for B2B
                lead_score = None
                if customer['segment'].name.startswith("B2B") and goal_completions > 0:
                    lead_score = customer['engagement_score'] * (goal_value / 10) * random.uniform(0.7, 1.3)
                
                # Exit page (same as landing for single-page sessions)
                exit_page = selected_page["path"] if pageviews == 1 else random.choice(available_pages)["path"]
                
                # Create GA record
                record = GoogleAnalyticsRecord(
                    date=current_date.strftime("%Y%m%d"),
                    session_id=f"session_{str(uuid.uuid4())[:8]}",
                    client_id=customer['client_id'],
                    user_id=customer['user_id'],
                    sessions=1,
                    users=1 if is_new else 0,
                    new_users=1 if is_new else 0,
                    pageviews=pageviews,
                    session_duration=duration,
                    bounce_rate=bounce_rate,
                    source=source,
                    medium=medium,
                    campaign=utm_campaign,
                    keyword=keyword,
                    landing_page=selected_page["path"],
                    exit_page=exit_page,
                    page_path=selected_page["path"],
                    page_title=selected_page["title"],
                    content_category=content_category.value,
                    device_category=customer['device_preference'].value,
                    browser=customer['browser'],
                    operating_system=customer['operating_system'],
                    country="Netherlands",
                    region="Netherlands",
                    city=customer['city'],
                    user_type=user_type.value,
                    customer_segment=customer['segment'].value,
                    engagement_score=customer['engagement_score'],
                    lead_score=lead_score,
                    goal_completions=goal_completions,
                    goal_value=goal_value,
                    ecommerce_revenue=ecommerce_revenue,
                    attribution_touchpoint_id=f"content_{str(uuid.uuid4())[:8]}",
                    cross_channel_customer_id=customer['customer_id'],
                    session_timestamp=session_time.isoformat() + "Z"
                )
                
                records.append(record)
        
        return records
    
    def generate_filtered_data(self, request: DataRequest) -> List[Dict]:
        """Generate filtered GA data based on API request parameters"""
        campaigns = self.get_campaign_configs()
        
        # Apply date filters
        if request.start_date:
            start_filter = datetime.fromisoformat(request.start_date.replace('Z', ''))
            campaigns = [c for c in campaigns if c.end_date >= start_filter]
        
        if request.end_date:
            end_filter = datetime.fromisoformat(request.end_date.replace('Z', ''))
            campaigns = [c for c in campaigns if c.start_date <= end_filter]
        
        all_records = []
        
        for campaign in campaigns:
            campaign_records = self._generate_sessions_for_campaign(campaign)
            
            # Apply filters
            if request.traffic_sources:
                campaign_records = [r for r in campaign_records if r.medium in request.traffic_sources]
            
            if request.device_categories:
                campaign_records = [r for r in campaign_records if r.device_category in request.device_categories]
            
            if request.content_categories:
                campaign_records = [r for r in campaign_records if r.content_category in request.content_categories]
            
            if request.customer_segments:
                campaign_records = [r for r in campaign_records if r.customer_segment in request.customer_segments]
            
            all_records.extend(campaign_records)
            
            # Respect max_records limit
            if len(all_records) >= request.max_records:
                all_records = all_records[:request.max_records]
                break
        
        return [asdict(record) for record in all_records]

# Initialize the generator instance
generator = ContentWebsiteSEOGenerator()

# Create FastAPI app
app = FastAPI(
    title="Content/Website/SEO Synthetic Data API",
    description="Google Analytics-style Dutch market content and SEO data generator",
    version="1.0.0"
)

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "content-website-seo-generator", "version": "1.0.0"}

@app.get("/campaigns")
async def get_campaigns():
    """Get list of all available content campaigns"""
    campaigns = generator.get_campaign_configs()
    
    campaign_list = []
    for campaign in campaigns:
        campaign_list.append({
            "name": campaign.name,
            "start_date": campaign.start_date.isoformat(),
            "end_date": campaign.end_date.isoformat(),
            "content_focus": [cf.value for cf in campaign.content_focus],
            "target_segments": [seg.value for seg in campaign.target_segments],
            "organic_multiplier": campaign.organic_multiplier,
            "seasonal_boost": campaign.seasonal_boost
        })
    
    return {
        "total_campaigns": len(campaign_list),
        "campaigns": campaign_list
    }

@app.post("/ga4/data")
async def get_ga4_data(request: DataRequest):
    """Get Google Analytics 4 style data (primary endpoint for N8N)"""
    try:
        data = generator.generate_filtered_data(request)
        
        # Calculate summary metrics
        total_sessions = len(data)
        total_users = len(set(r['client_id'] for r in data))
        total_pageviews = sum(r['pageviews'] for r in data)
        total_goal_completions = sum(r['goal_completions'] for r in data)
        total_revenue = sum(r['ecommerce_revenue'] for r in data)
        
        return {
            "summary_metrics": {
                "sessions": total_sessions,
                "users": total_users,
                "pageviews": total_pageviews,
                "goal_completions": total_goal_completions,
                "revenue": round(total_revenue, 2),
                "conversion_rate": round(total_goal_completions / total_sessions * 100, 2) if total_sessions > 0 else 0
            },
            "filters_applied": {
                "start_date": request.start_date,
                "end_date": request.end_date,
                "metrics": request.metrics,
                "dimensions": request.dimensions,
                "traffic_sources": request.traffic_sources,
                "device_categories": request.device_categories,
                "content_categories": request.content_categories,
                "customer_segments": request.customer_segments,
                "max_records": request.max_records
            },
            "data": data,
            "metadata": {
                "generated_at": datetime.now().isoformat(),
                "gdpr_compliant": True,
                "market": "Netherlands",
                "channel": "Content/Website/SEO",
                "data_source": "Google Analytics 4 Compatible"
            }
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating GA data: {str(e)}")

@app.get("/data")
async def get_recent_data(
    days: int = Query(30, description="Number of recent days"),
    max_records: int = Query(1000, description="Maximum records to return"),
    traffic_sources: Optional[List[str]] = Query(None, description="Traffic sources to include"),
    content_categories: Optional[List[str]] = Query(None, description="Content categories to include"),
    device_categories: Optional[List[str]] = Query(None, description="Device categories to include")
):
    """Get recent content/website data (convenient endpoint for N8N)"""
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    request = DataRequest(
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
        traffic_sources=traffic_sources,
        content_categories=content_categories,
        device_categories=device_categories,
        max_records=max_records
    )
    
    return await get_ga4_data(request)

@app.get("/content-performance")
async def get_content_performance(
    days: int = Query(30, description="Number of days to analyze"),
    limit: int = Query(20, description="Number of top pages to return")
):
    """Get top-performing content pages"""
    try:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        request = DataRequest(
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            max_records=50000
        )
        
        data = generator.generate_filtered_data(request)
        
        # Aggregate by page
        page_performance = {}
        for record in data:
            page = record['page_path']
            if page not in page_performance:
                page_performance[page] = {
                    "page_path": page,
                    "page_title": record['page_title'],
                    "content_category": record['content_category'],
                    "sessions": 0,
                    "pageviews": 0,
                    "goal_completions": 0,
                    "revenue": 0.0,
                    "avg_session_duration": 0,
                    "bounce_rate": 0
                }
            
            perf = page_performance[page]
            perf["sessions"] += 1
            perf["pageviews"] += record['pageviews']
            perf["goal_completions"] += record['goal_completions']
            perf["revenue"] += record['ecommerce_revenue']
            perf["avg_session_duration"] += record['session_duration']
            perf["bounce_rate"] += record['bounce_rate']
        
        # Calculate averages and sort
        for page, perf in page_performance.items():
            if perf["sessions"] > 0:
                perf["avg_session_duration"] = int(perf["avg_session_duration"] / perf["sessions"])
                perf["bounce_rate"] = round(perf["bounce_rate"] / perf["sessions"], 3)
                perf["conversion_rate"] = round(perf["goal_completions"] / perf["sessions"] * 100, 2)
        
        # Sort by sessions and take top pages
        top_pages = sorted(page_performance.values(), key=lambda x: x["sessions"], reverse=True)[:limit]
        
        return {
            "date_range": {
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat()
            },
            "top_pages": top_pages
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error analyzing content performance: {str(e)}")

@app.get("/seo-keywords")
async def get_seo_keywords(days: int = Query(30, description="Number of days to analyze")):
    """Get top organic search keywords"""
    try:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        request = DataRequest(
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            traffic_sources=["organic"],
            max_records=10000
        )
        
        data = generator.generate_filtered_data(request)
        
        # Aggregate by keyword
        keyword_performance = {}
        for record in data:
            if record['medium'] == 'organic' and record['keyword']:
                keyword = record['keyword']
                if keyword not in keyword_performance:
                    keyword_performance[keyword] = {
                        "keyword": keyword,
                        "sessions": 0,
                        "goal_completions": 0,
                        "revenue": 0.0
                    }
                
                perf = keyword_performance[keyword]
                perf["sessions"] += 1
                perf["goal_completions"] += record['goal_completions']
                perf["revenue"] += record['ecommerce_revenue']
        
        # Calculate conversion rates and sort
        for keyword, perf in keyword_performance.items():
            if perf["sessions"] > 0:
                perf["conversion_rate"] = round(perf["goal_completions"] / perf["sessions"] * 100, 2)
        
        top_keywords = sorted(keyword_performance.values(), key=lambda x: x["sessions"], reverse=True)[:25]
        
        return {
            "date_range": {
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat()
            },
            "top_keywords": top_keywords
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error analyzing SEO keywords: {str(e)}")

@app.get("/attribution-touchpoints")
async def get_attribution_touchpoints(limit: int = Query(100, description="Number of touchpoints to return")):
    """Get content touchpoints for cross-channel attribution"""
    try:
        # Generate recent data for attribution
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)
        
        request = DataRequest(
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            max_records=limit
        )
        
        data = generator.generate_filtered_data(request)
        
        touchpoints = []
        for record in data:
            touchpoints.append({
                "touchpoint_id": record['attribution_touchpoint_id'],
                "customer_id": record['cross_channel_customer_id'],
                "client_id": record['client_id'],
                "user_id": record['user_id'],
                "session_timestamp": record['session_timestamp'],
                "source": record['source'],
                "medium": record['medium'],
                "page_path": record['page_path'],
                "content_category": record['content_category'],
                "goal_completions": record['goal_completions'],
                "revenue": record['ecommerce_revenue']
            })
        
        return {
            "total_touchpoints": len(touchpoints),
            "touchpoints": touchpoints,
            "note": "Content touchpoints provide organic attribution with GA client_id matching"
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating attribution touchpoints: {str(e)}")

@app.get("/ga-dimensions")
async def get_available_dimensions():
    """Get available Google Analytics dimensions and metrics"""
    return {
        "dimensions": {
            "traffic_sources": ["organic", "direct", "referral", "social", "email", "cpc"],
            "device_categories": ["desktop", "mobile", "tablet"],
            "content_categories": [cat.value for cat in ContentCategory],
            "customer_segments": [seg.value for seg in CustomerSegment],
            "user_types": ["New Visitor", "Returning Visitor"],
            "browsers": ["Chrome", "Safari", "Firefox", "Edge", "Opera"],
            "countries": ["Netherlands"],
            "cities": list(generator.dutch_cities.keys())
        },
        "metrics": [
            "sessions", "users", "new_users", "pageviews", "session_duration",
            "bounce_rate", "goal_completions", "goal_value", "ecommerce_revenue"
        ],
        "ga4_compatible": True,
        "attribution_fields": [
            "attribution_touchpoint_id", "cross_channel_customer_id", "client_id", "user_id"
        ]
    }

if __name__ == "__main__":
    print("Starting Content/Website/SEO Synthetic Data API...")
    print("API Documentation: http://localhost:8005/docs")
    print("Health Check: http://localhost:8005/health")
    uvicorn.run(app, host="0.0.0.0", port=8005)