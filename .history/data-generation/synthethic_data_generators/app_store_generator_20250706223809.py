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

class Platform(Enum):
    GOOGLE_PLAY = "google_play"
    APPLE_APP_STORE = "apple_app_store"

class EventType(Enum):
    INSTALL = "install"
    FIRST_OPEN = "first_open"
    SESSION_START = "session_start"
    REGISTRATION = "registration"
    ACCOUNT_CREATED = "account_created"
    FIRST_TRANSACTION = "first_transaction"
    IN_APP_PURCHASE = "in_app_purchase"

class InstallSource(Enum):
    ORGANIC_SEARCH = "organic_search"
    GOOGLE_ADS = "google_ads"
    FACEBOOK_ADS = "facebook_ads"
    WEBSITE_REFERRAL = "website_referral"
    EMAIL_CAMPAIGN = "email_campaign"
    SOCIAL_MEDIA = "social_media"
    DIRECT_LINK = "direct_link"
    CROSS_PROMOTION = "cross_promotion"

class DeviceType(Enum):
    PHONE = "phone"
    TABLET = "tablet"

class CustomerSegment(Enum):
    B2C_WORKING_AGE = "B2C_WORKING_AGE"
    B2C_STUDENTS = "B2C_STUDENTS"
    B2C_NON_WORKING = "B2C_NON_WORKING"
    B2B_SMALL = "B2B_SMALL"
    B2B_MEDIUM = "B2B_MEDIUM"
    B2B_LARGE = "B2B_LARGE"

@dataclass
class GooglePlayRecord:
    """Google Play Console API-style record"""
    # App Identity
    package_name: str
    app_version_code: int
    app_version_name: str
    
    # Install/Event Data
    event_type: str
    event_timestamp: str
    install_timestamp: Optional[str]
    
    # Attribution Data
    acquisition_channel: str
    acquisition_source: str
    acquisition_medium: str
    campaign_name: Optional[str]
    gclid: Optional[str]  # Google Ads attribution
    utm_source: Optional[str]
    utm_medium: Optional[str]
    utm_campaign: Optional[str]
    
    # Device Information
    device_brand: str
    device_model: str
    device_type: str
    android_version: str
    gaid: Optional[str]  # Google Advertising ID
    
    # Geographic Data
    country_code: str
    region: str
    city: str
    language: str
    
    # User Behavior
    session_duration: Optional[int]  # seconds
    screens_per_session: Optional[int]
    retention_day_1: bool
    retention_day_7: bool
    retention_day_30: bool
    
    # Monetization
    revenue_micros: int  # Revenue in micros
    currency_code: str
    
    # Custom Attribution
    customer_id: str
    segment: str
    attribution_touchpoint_id: str

@dataclass
class AppleAppStoreRecord:
    """Apple App Store Connect API-style record"""
    # App Identity
    app_id: str
    bundle_id: str
    app_version: str
    
    # Install/Event Data
    event_type: str
    event_timestamp: str
    install_timestamp: Optional[str]
    
    # Attribution Data (Limited due to iOS privacy)
    acquisition_channel: str
    acquisition_source: str
    campaign_id: Optional[str]
    campaign_name: Optional[str]
    referrer_url: Optional[str]
    
    # Device Information
    device_type: str
    device_model: str
    ios_version: str
    idfa: Optional[str]  # Identifier for Advertisers (privacy limited)
    idfv: str  # Identifier for Vendor
    
    # Geographic Data
    country_code: str
    region: str
    language: str
    
    # User Behavior
    session_duration: Optional[int]
    app_launches: int
    retention_day_1: bool
    retention_day_7: bool
    retention_day_30: bool
    
    # Monetization
    revenue_usd: float
    proceeds_usd: float  # After Apple's cut
    
    # Custom Attribution
    customer_id: str
    segment: str
    attribution_touchpoint_id: str

@dataclass
class CampaignConfig:
    """App campaign configuration"""
    name: str
    start_date: datetime
    end_date: datetime
    platforms: List[Platform]
    target_segments: List[CustomerSegment]
    primary_sources: List[InstallSource]
    budget_android: int  # euros
    budget_ios: int  # euros
    conversion_multiplier: float

class DataRequest(BaseModel):
    """API request model for app store data generation"""
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    platforms: Optional[List[str]] = None
    event_types: Optional[List[str]] = None
    acquisition_sources: Optional[List[str]] = None
    customer_segments: Optional[List[str]] = None
    countries: Optional[List[str]] = None
    max_records: Optional[int] = 10000

class AppStoreGenerator:
    """
    App Store synthetic data API service for Dutch market
    Generates Google Play Console and Apple App Store Connect data
    """
    
    def __init__(self):
        self.total_customers = 200000
        self.app_penetration = 0.62  # 62% of customers download mobile app
        
        # Platform distribution for Netherlands
        self.platform_distribution = {
            Platform.GOOGLE_PLAY: 0.65,  # Android dominance in NL
            Platform.APPLE_APP_STORE: 0.35
        }
        
        # Install source distribution
        self.install_source_distribution = {
            InstallSource.ORGANIC_SEARCH: 0.35,      # App store search
            InstallSource.WEBSITE_REFERRAL: 0.20,    # From bunq.com
            InstallSource.GOOGLE_ADS: 0.15,          # Paid app campaigns
            InstallSource.FACEBOOK_ADS: 0.12,        # Social media ads
            InstallSource.EMAIL_CAMPAIGN: 0.08,      # Email links
            InstallSource.SOCIAL_MEDIA: 0.06,        # Organic social
            InstallSource.DIRECT_LINK: 0.03,         # Direct app links
            InstallSource.CROSS_PROMOTION: 0.01      # Other apps
        }
        
        # Android device distribution (Netherlands market)
        self.android_devices = {
            "Samsung": {"models": ["Galaxy S24", "Galaxy S23", "Galaxy A54", "Galaxy A34"], "weight": 0.35},
            "Google": {"models": ["Pixel 8", "Pixel 7", "Pixel 6a", "Pixel 8 Pro"], "weight": 0.15},
            "OnePlus": {"models": ["OnePlus 12", "OnePlus 11", "OnePlus Nord"], "weight": 0.12},
            "Xiaomi": {"models": ["Redmi Note 13", "Mi 13", "Redmi 12"], "weight": 0.10},
            "Huawei": {"models": ["P60", "Mate 50", "Nova 11"], "weight": 0.08},
            "Oppo": {"models": ["Find X6", "Reno 10", "A98"], "weight": 0.06},
            "Motorola": {"models": ["Edge 40", "Moto G84", "Edge 30"], "weight": 0.05},
            "Other": {"models": ["Android Device"], "weight": 0.09}
        }
        
        # iOS device distribution
        self.ios_devices = {
            "iPhone": {
                "models": ["iPhone 15 Pro", "iPhone 15", "iPhone 14", "iPhone 13", "iPhone 12", "iPhone SE"],
                "weight": 0.85
            },
            "iPad": {
                "models": ["iPad Pro", "iPad Air", "iPad", "iPad mini"],
                "weight": 0.15
            }
        }
        
        # Android version distribution
        self.android_versions = {
            "14": 0.15,  # Latest
            "13": 0.35,  # Most common
            "12": 0.25,
            "11": 0.15,
            "10": 0.08,
            "9": 0.02
        }
        
        # iOS version distribution
        self.ios_versions = {
            "17.4": 0.25,
            "17.3": 0.20,
            "17.2": 0.15,
            "17.1": 0.12,
            "16.7": 0.15,
            "16.6": 0.08,
            "15.8": 0.05
        }
        
        # Customer segment distribution for app users
        self.segment_distribution = {
            CustomerSegment.B2C_WORKING_AGE: 0.45,
            CustomerSegment.B2C_STUDENTS: 0.30,  # Higher app adoption
            CustomerSegment.B2C_NON_WORKING: 0.15,
            CustomerSegment.B2B_SMALL: 0.08,     # Some business app usage
            CustomerSegment.B2B_MEDIUM: 0.02,
            CustomerSegment.B2B_LARGE: 0.00     # Enterprise uses web
        }
        
        self.customer_pool = self._generate_customer_pool()
        self._campaign_configs = None
        
    def _generate_customer_pool(self) -> List[Dict]:
        """Generate realistic customer pool with mobile app attributes"""
        customers = []
        active_customers = int(self.total_customers * self.app_penetration)
        
        for i in range(active_customers):
            segment = self._weighted_choice(list(self.segment_distribution.keys()), 
                                          list(self.segment_distribution.values()))
            
            platform = self._weighted_choice(list(self.platform_distribution.keys()),
                                            list(self.platform_distribution.values()))
            
            # Generate device information based on platform
            if platform == Platform.GOOGLE_PLAY:
                brand = self._weighted_choice(list(self.android_devices.keys()),
                                            [d["weight"] for d in self.android_devices.values()])
                device_model = random.choice(self.android_devices[brand]["models"])
                android_version = self._weighted_choice(list(self.android_versions.keys()),
                                                      list(self.android_versions.values()))
                
                # GAID availability (70% due to privacy changes)
                gaid = f"gaid_{str(uuid.uuid4())}" if random.random() < 0.7 else None
                
                device_info = {
                    "brand": brand,
                    "model": device_model,
                    "android_version": android_version,
                    "gaid": gaid
                }
            else:  # Apple App Store
                device_category = self._weighted_choice(["iPhone", "iPad"], [0.85, 0.15])
                device_model = random.choice(self.ios_devices[device_category]["models"])
                ios_version = self._weighted_choice(list(self.ios_versions.keys()),
                                                  list(self.ios_versions.values()))
                
                # IDFA availability (30% due to iOS 14.5+ privacy)
                idfa = f"idfa_{str(uuid.uuid4())}" if random.random() < 0.3 else None
                idfv = f"idfv_{str(uuid.uuid4())}"
                
                device_info = {
                    "model": device_model,
                    "ios_version": ios_version,
                    "idfa": idfa,
                    "idfv": idfv
                }
            
            customer = {
                'customer_id': f"cust_{str(uuid.uuid4())[:8]}",
                'segment': segment,
                'platform': platform,
                'device_info': device_info,
                'device_type': DeviceType.PHONE if random.random() < 0.92 else DeviceType.TABLET,
                'location': {
                    "country": "Netherlands",
                    "region": "Netherlands",
                    "city": random.choice(["Amsterdam", "Rotterdam", "The Hague", "Utrecht", "Eindhoven"])
                },
                'language': random.choice(["nl", "en"]),
                'app_engagement': random.uniform(0.3, 0.95),
                'conversion_probability': random.uniform(0.15, 0.85),  # Higher than web
                'retention_probability': {
                    "day_1": random.uniform(0.7, 0.95),
                    "day_7": random.uniform(0.4, 0.8),
                    "day_30": random.uniform(0.2, 0.6)
                },
                'session_frequency': random.uniform(2, 15),  # Sessions per week
                'monetization_probability': random.uniform(0.02, 0.25)
            }
            customers.append(customer)
            
        return customers
    
    def _weighted_choice(self, choices: List, weights: List) -> Any:
        """Select random choice based on weights"""
        return random.choices(choices, weights=weights)[0]
    
    def get_campaign_configs(self) -> List[CampaignConfig]:
        """Define app store campaigns based on Dutch market calendar"""
        if self._campaign_configs is not None:
            return self._campaign_configs
            
        campaigns = []
        
        # 2024 Campaigns
        campaigns.extend([
            # January 2024 - New Year App Downloads
            CampaignConfig(
                name="App_Store_New_Year_Banking_Resolution",
                start_date=datetime(2024, 1, 2),
                end_date=datetime(2024, 1, 31),
                platforms=[Platform.GOOGLE_PLAY, Platform.APPLE_APP_STORE],
                target_segments=[CustomerSegment.B2C_WORKING_AGE, CustomerSegment.B2C_STUDENTS],
                primary_sources=[InstallSource.ORGANIC_SEARCH, InstallSource.WEBSITE_REFERRAL],
                budget_android=45000,
                budget_ios=35000,
                conversion_multiplier=0.9
            ),
            
            # February-March 2024 - Spring Banking Apps
            CampaignConfig(
                name="App_Store_Spring_Mobile_Banking",
                start_date=datetime(2024, 2, 20),
                end_date=datetime(2024, 3, 31),
                platforms=[Platform.GOOGLE_PLAY, Platform.APPLE_APP_STORE],
                target_segments=[CustomerSegment.B2C_WORKING_AGE, CustomerSegment.B2C_STUDENTS],
                primary_sources=[InstallSource.GOOGLE_ADS, InstallSource.FACEBOOK_ADS, InstallSource.ORGANIC_SEARCH],
                budget_android=75000,
                budget_ios=55000,
                conversion_multiplier=1.3
            ),
            
            # April 2024 - King's Day Mobile Campaign
            CampaignConfig(
                name="App_Store_Kings_Day_Mobile_Freedom",
                start_date=datetime(2024, 4, 20),
                end_date=datetime(2024, 4, 30),
                platforms=[Platform.GOOGLE_PLAY, Platform.APPLE_APP_STORE],
                target_segments=[CustomerSegment.B2C_WORKING_AGE, CustomerSegment.B2C_STUDENTS, CustomerSegment.B2C_NON_WORKING],
                primary_sources=[InstallSource.SOCIAL_MEDIA, InstallSource.FACEBOOK_ADS, InstallSource.ORGANIC_SEARCH],
                budget_android=40000,
                budget_ios=30000,
                conversion_multiplier=1.2
            ),
            
            # May-June 2024 - Early Summer Mobile
            CampaignConfig(
                name="App_Store_Summer_Mobile_Banking",
                start_date=datetime(2024, 5, 10),
                end_date=datetime(2024, 6, 30),
                platforms=[Platform.GOOGLE_PLAY, Platform.APPLE_APP_STORE],
                target_segments=[CustomerSegment.B2C_WORKING_AGE, CustomerSegment.B2C_STUDENTS],
                primary_sources=[InstallSource.ORGANIC_SEARCH, InstallSource.WEBSITE_REFERRAL, InstallSource.EMAIL_CAMPAIGN],
                budget_android=60000,
                budget_ios=45000,
                conversion_multiplier=1.1
            ),
            
            # June-July 2024 - Festival Season Apps
            CampaignConfig(
                name="App_Store_Festival_Banking_Convenience",
                start_date=datetime(2024, 6, 15),
                end_date=datetime(2024, 7, 15),
                platforms=[Platform.GOOGLE_PLAY, Platform.APPLE_APP_STORE],
                target_segments=[CustomerSegment.B2C_STUDENTS, CustomerSegment.B2C_WORKING_AGE],
                primary_sources=[InstallSource.SOCIAL_MEDIA, InstallSource.ORGANIC_SEARCH],
                budget_android=35000,
                budget_ios=25000,
                conversion_multiplier=0.8
            ),
            
            # September 2024 - Back to School Apps
            CampaignConfig(
                name="App_Store_Student_Banking_App",
                start_date=datetime(2024, 9, 1),
                end_date=datetime(2024, 9, 30),
                platforms=[Platform.GOOGLE_PLAY, Platform.APPLE_APP_STORE],
                target_segments=[CustomerSegment.B2C_STUDENTS, CustomerSegment.B2C_WORKING_AGE],
                primary_sources=[InstallSource.GOOGLE_ADS, InstallSource.ORGANIC_SEARCH, InstallSource.WEBSITE_REFERRAL],
                budget_android=55000,
                budget_ios=40000,
                conversion_multiplier=1.1
            ),
            
            # October-November 2024 - Autumn App Push
            CampaignConfig(
                name="App_Store_Autumn_Mobile_Convenience",
                start_date=datetime(2024, 10, 1),
                end_date=datetime(2024, 11, 30),
                platforms=[Platform.GOOGLE_PLAY, Platform.APPLE_APP_STORE],
                target_segments=[CustomerSegment.B2C_WORKING_AGE, CustomerSegment.B2C_STUDENTS],
                primary_sources=[InstallSource.EMAIL_CAMPAIGN, InstallSource.GOOGLE_ADS, InstallSource.ORGANIC_SEARCH],
                budget_android=70000,
                budget_ios=50000,
                conversion_multiplier=1.0
            ),
            
            # November-December 2024 - Holiday Mobile Banking
            CampaignConfig(
                name="App_Store_Holiday_Mobile_Banking",
                start_date=datetime(2024, 11, 15),
                end_date=datetime(2024, 12, 31),
                platforms=[Platform.GOOGLE_PLAY, Platform.APPLE_APP_STORE],
                target_segments=[CustomerSegment.B2C_WORKING_AGE, CustomerSegment.B2C_NON_WORKING],
                primary_sources=[InstallSource.FACEBOOK_ADS, InstallSource.EMAIL_CAMPAIGN, InstallSource.ORGANIC_SEARCH],
                budget_android=65000,
                budget_ios=45000,
                conversion_multiplier=1.15
            )
        ])
        
        # 2025 Campaigns (January - June)
        campaigns.extend([
            # January 2025
            CampaignConfig(
                name="App_Store_New_Year_Banking_Resolution",
                start_date=datetime(2025, 1, 2),
                end_date=datetime(2025, 1, 31),
                platforms=[Platform.GOOGLE_PLAY, Platform.APPLE_APP_STORE],
                target_segments=[CustomerSegment.B2C_WORKING_AGE, CustomerSegment.B2C_STUDENTS],
                primary_sources=[InstallSource.ORGANIC_SEARCH, InstallSource.WEBSITE_REFERRAL],
                budget_android=50000,
                budget_ios=38000,
                conversion_multiplier=0.95
            ),
            
            # February-March 2025
            CampaignConfig(
                name="App_Store_Spring_Mobile_Banking",
                start_date=datetime(2025, 2, 20),
                end_date=datetime(2025, 3, 31),
                platforms=[Platform.GOOGLE_PLAY, Platform.APPLE_APP_STORE],
                target_segments=[CustomerSegment.B2C_WORKING_AGE, CustomerSegment.B2C_STUDENTS],
                primary_sources=[InstallSource.GOOGLE_ADS, InstallSource.FACEBOOK_ADS, InstallSource.ORGANIC_SEARCH],
                budget_android=80000,
                budget_ios=60000,
                conversion_multiplier=1.35
            ),
            
            # April 2025
            CampaignConfig(
                name="App_Store_Kings_Day_Mobile_Freedom",
                start_date=datetime(2025, 4, 20),
                end_date=datetime(2025, 4, 30),
                platforms=[Platform.GOOGLE_PLAY, Platform.APPLE_APP_STORE],
                target_segments=[CustomerSegment.B2C_WORKING_AGE, CustomerSegment.B2C_STUDENTS, CustomerSegment.B2C_NON_WORKING],
                primary_sources=[InstallSource.SOCIAL_MEDIA, InstallSource.FACEBOOK_ADS, InstallSource.ORGANIC_SEARCH],
                budget_android=42000,
                budget_ios=32000,
                conversion_multiplier=1.25
            ),
            
            # May-June 2025
            CampaignConfig(
                name="App_Store_Summer_Mobile_Banking",
                start_date=datetime(2025, 5, 10),
                end_date=datetime(2025, 6, 30),
                platforms=[Platform.GOOGLE_PLAY, Platform.APPLE_APP_STORE],
                target_segments=[CustomerSegment.B2C_WORKING_AGE, CustomerSegment.B2C_STUDENTS],
                primary_sources=[InstallSource.ORGANIC_SEARCH, InstallSource.WEBSITE_REFERRAL, InstallSource.EMAIL_CAMPAIGN],
                budget_android=65000,
                budget_ios=48000,
                conversion_multiplier=1.15
            )
        ])
        
        self._campaign_configs = campaigns
        return campaigns
    
    def _generate_app_store_keywords(self, segment: CustomerSegment, platform: Platform) -> List[str]:
        """Generate realistic app store search keywords"""
        base_keywords = {
            "dutch": ["bunq", "mobiel bankieren", "banking app", "nederlande bank", "internet bankieren"],
            "english": ["bunq", "mobile banking", "banking app", "digital bank", "online banking"],
            "business": ["business banking", "zakelijk bankieren", "bedrijf bank", "business finance"]
        }
        
        if segment.name.startswith("B2B"):
            return base_keywords["business"] + base_keywords["dutch"][:3]
        
        # Mix Dutch and English based on platform and demographics
        if platform == Platform.APPLE_APP_STORE:
            return base_keywords["english"] + base_keywords["dutch"][:3]  # iOS users more international
        else:
            return base_keywords["dutch"] + base_keywords["english"][:2]
    
    def _calculate_retention_rates(self, customer: Dict, install_source: InstallSource) -> Dict[str, bool]:
        """Calculate realistic app retention rates"""
        base_retention = customer['retention_probability']
        
        # Install source impact on retention
        source_multipliers = {
            InstallSource.ORGANIC_SEARCH: 1.2,      # High intent users
            InstallSource.WEBSITE_REFERRAL: 1.4,    # Highest quality
            InstallSource.EMAIL_CAMPAIGN: 1.3,      # Engaged users
            InstallSource.GOOGLE_ADS: 1.0,          # Average
            InstallSource.FACEBOOK_ADS: 0.9,        # Lower intent
            InstallSource.SOCIAL_MEDIA: 0.8,        # Casual users
            InstallSource.DIRECT_LINK: 1.1,
            InstallSource.CROSS_PROMOTION: 0.7      # Lowest quality
        }
        
        multiplier = source_multipliers.get(install_source, 1.0)
        
        return {
            "day_1": random.random() < (base_retention["day_1"] * multiplier),
            "day_7": random.random() < (base_retention["day_7"] * multiplier),
            "day_30": random.random() < (base_retention["day_30"] * multiplier)
        }
    
    def _generate_attribution_data(self, install_source: InstallSource, 
                                 customer: Dict) -> Dict[str, Optional[str]]:
        """Generate attribution data based on install source"""
        attribution = {
            "gclid": None,
            "utm_source": None,
            "utm_medium": None,
            "utm_campaign": None,
            "referrer_url": None,
            "campaign_name": None
        }
        
        if install_source == InstallSource.GOOGLE_ADS:
            attribution.update({
                "gclid": f"Gj0CAQiA{random.randint(100000, 999999)}",
                "utm_source": "google",
                "utm_medium": "cpc",
                "utm_campaign": "app_install_campaign",
                "campaign_name": "Google_Ads_App_Install"
            })
        elif install_source == InstallSource.FACEBOOK_ADS:
            attribution.update({
                "utm_source": "facebook",
                "utm_medium": "social",
                "utm_campaign": "app_install_social",
                "campaign_name": "Facebook_App_Install"
            })
        elif install_source == InstallSource.EMAIL_CAMPAIGN:
            attribution.update({
                "utm_source": "email",
                "utm_medium": "email",
                "utm_campaign": "app_download_email",
                "campaign_name": "Email_App_Download"
            })
        elif install_source == InstallSource.WEBSITE_REFERRAL:
            attribution.update({
                "referrer_url": "https://bunq.com/app",
                "utm_source": "website",
                "utm_medium": "referral",
                "utm_campaign": "website_app_download"
            })
        
        return attribution
    
    def _generate_events_for_campaign(self, campaign: CampaignConfig) -> List[Dict]:
        """Generate app store events for a specific campaign"""
        records = []
        campaign_days = (campaign.end_date - campaign.start_date).days + 1
        
        # Calculate daily install volume
        total_budget = sum([campaign.budget_android, campaign.budget_ios])
        avg_cost_per_install = 3.50  # euros
        total_installs = int((total_budget / avg_cost_per_install) * campaign.conversion_multiplier)
        daily_installs = total_installs // campaign_days
        
        # Select customers for this campaign
        eligible_customers = [c for c in self.customer_pool 
                            if c['segment'] in campaign.target_segments and
                            c['platform'] in campaign.platforms]
        
        for day_offset in range(campaign_days):
            current_date = campaign.start_date + timedelta(days=day_offset)
            
            # Daily variation (weekends higher for consumer apps)
            if current_date.weekday() >= 5:  # Weekend
                daily_multiplier = 1.3
            else:  # Weekday
                daily_multiplier = 1.0
            
            daily_install_count = int(daily_installs * daily_multiplier)
            
            # Generate installs for this day
            daily_customers = random.sample(eligible_customers, 
                                          min(daily_install_count, len(eligible_customers)))
            
            for customer in daily_customers:
                # Determine install source
                install_source = self._weighted_choice(campaign.primary_sources, 
                                                     [1.0] * len(campaign.primary_sources))
                
                # Generate install timestamp
                hour = random.choices(range(24), weights=[0.3]*6 + [1.2]*12 + [1.5]*6)[0]
                install_time = current_date.replace(
                    hour=hour,
                    minute=random.randint(0, 59),
                    second=random.randint(0, 59)
                )
                
                # Generate attribution data
                attribution = self._generate_attribution_data(install_source, customer)
                
                # Calculate retention
                retention = self._calculate_retention_rates(customer, install_source)
                
                # Generate revenue (for premium features)
                revenue_micros = 0
                revenue_usd = 0.0
                if random.random() < customer['monetization_probability']:
                    monthly_value = random.uniform(5, 25)  # euros
                    revenue_micros = int(monthly_value * 1000000)
                    revenue_usd = monthly_value * 1.1  # EUR to USD
                
                # Platform-specific record generation
                if customer['platform'] == Platform.GOOGLE_PLAY:
                    # Generate Google Play Console record
                    record = GooglePlayRecord(
                        package_name="com.bunq.android",
                        app_version_code=random.randint(200, 250),
                        app_version_name=f"4.{random.randint(20, 35)}.{random.randint(0, 9)}",
                        event_type=EventType.INSTALL.value,
                        event_timestamp=install_time.isoformat() + "Z",
                        install_timestamp=install_time.isoformat() + "Z",
                        acquisition_channel=install_source.value,
                        acquisition_source=attribution.get("utm_source", "organic"),
                        acquisition_medium=attribution.get("utm_medium", "app_store"),
                        campaign_name=attribution.get("campaign_name"),
                        gclid=attribution.get("gclid"),
                        utm_source=attribution.get("utm_source"),
                        utm_medium=attribution.get("utm_medium"),
                        utm_campaign=attribution.get("utm_campaign"),
                        device_brand=customer['device_info']['brand'],
                        device_model=customer['device_info']['model'],
                        device_type=customer['device_type'].value,
                        android_version=customer['device_info']['android_version'],
                        gaid=customer['device_info']['gaid'],
                        country_code="NL",
                        region=customer['location']['region'],
                        city=customer['location']['city'],
                        language=customer['language'],
                        session_duration=random.randint(120, 600),
                        screens_per_session=random.randint(3, 12),
                        retention_day_1=retention["day_1"],
                        retention_day_7=retention["day_7"],
                        retention_day_30=retention["day_30"],
                        revenue_micros=revenue_micros,
                        currency_code="EUR",
                        customer_id=customer['customer_id'],
                        segment=customer['segment'].value,
                        attribution_touchpoint_id=f"app_android_{str(uuid.uuid4())[:8]}"
                    )
                    
                else:  # Apple App Store
                    record = AppleAppStoreRecord(
                        app_id="1021178240",  # Bunq iOS app ID
                        bundle_id="com.bunq.bunq",
                        app_version=f"4.{random.randint(20, 35)}.{random.randint(0, 9)}",
                        event_type=EventType.INSTALL.value,
                        event_timestamp=install_time.isoformat() + "Z",
                        install_timestamp=install_time.isoformat() + "Z",
                        acquisition_channel=install_source.value,
                        acquisition_source=attribution.get("utm_source", "app_store"),
                        campaign_id=f"camp_{random.randint(100000, 999999)}" if attribution.get("campaign_name") else None,
                        campaign_name=attribution.get("campaign_name"),
                        referrer_url=attribution.get("referrer_url"),
                        device_type=customer['device_type'].value,
                        device_model=customer['device_info']['model'],
                        ios_version=customer['device_info']['ios_version'],
                        idfa=customer['device_info']['idfa'],
                        idfv=customer['device_info']['idfv'],
                        country_code="NL",
                        region=customer['location']['region'],
                        language=customer['language'],
                        session_duration=random.randint(120, 600),
                        app_launches=random.randint(1, 5),
                        retention_day_1=retention["day_1"],
                        retention_day_7=retention["day_7"],
                        retention_day_30=retention["day_30"],
                        revenue_usd=revenue_usd,
                        proceeds_usd=revenue_usd * 0.7,  # After Apple's 30% cut
                        customer_id=customer['customer_id'],
                        segment=customer['segment'].value,
                        attribution_touchpoint_id=f"app_ios_{str(uuid.uuid4())[:8]}"
                    )
                
                records.append(asdict(record))
        
        return records
    
    def generate_filtered_data(self, request: DataRequest) -> List[Dict]:
        """Generate filtered app store data based on API request parameters"""
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
            campaign_records = self._generate_events_for_campaign(campaign)
            
            # Apply filters
            if request.platforms:
                # Filter by platform (check if Google Play or Apple fields exist)
                platform_filtered = []
                for record in campaign_records:
                    if ("google_play" in request.platforms and "package_name" in record) or \
                       ("apple_app_store" in request.platforms and "bundle_id" in record):
                        platform_filtered.append(record)
                campaign_records = platform_filtered
            
            if request.event_types:
                campaign_records = [r for r in campaign_records if r['event_type'] in request.event_types]
            
            if request.acquisition_sources:
                campaign_records = [r for r in campaign_records if r['acquisition_source'] in request.acquisition_sources]
            
            if request.customer_segments:
                campaign_records = [r for r in campaign_records if r['segment'] in request.customer_segments]
            
            if request.countries:
                campaign_records = [r for r in campaign_records if r.get('country_code', 'NL') in request.countries]
            
            all_records.extend(campaign_records)
            
            # Respect max_records limit
            if len(all_records) >= request.max_records:
                all_records = all_records[:request.max_records]
                break
        
        return all_records

# Initialize the generator instance
generator = AppStoreGenerator()

# Create FastAPI app
app = FastAPI(
    title="App Store Synthetic Data API",
    description="Google Play Console and Apple App Store Connect data generator for mobile attribution",
    version="1.0.0"
)

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "app-store-generator", "version": "1.0.0"}

@app.get("/campaigns")
async def get_campaigns():
    """Get list of all available app store campaigns"""
    campaigns = generator.get_campaign_configs()
    
    campaign_list = []
    for campaign in campaigns:
        campaign_list.append({
            "name": campaign.name,
            "start_date": campaign.start_date.isoformat(),
            "end_date": campaign.end_date.isoformat(),
            "platforms": [p.value for p in campaign.platforms],
            "target_segments": [seg.value for seg in campaign.target_segments],
            "primary_sources": [src.value for src in campaign.primary_sources],
            "budget_android": campaign.budget_android,
            "budget_ios": campaign.budget_ios,
            "conversion_multiplier": campaign.conversion_multiplier
        })
    
    return {
        "total_campaigns": len(campaign_list),
        "campaigns": campaign_list
    }

@app.post("/data")
async def get_app_store_data(request: DataRequest):
    """Get app store data from both Google Play and Apple App Store"""
    try:
        data = generator.generate_filtered_data(request)
        
        # Calculate summary metrics
        total_installs = len([r for r in data if r['event_type'] == 'install'])
        android_installs = len([r for r in data if 'package_name' in r])
        ios_installs = len([r for r in data if 'bundle_id' in r])
        total_revenue = sum(r.get('revenue_micros', r.get('revenue_usd', 0)) for r in data)
        
        # Retention analysis
        day_1_retained = len([r for r in data if r.get('retention_day_1', False)])
        day_7_retained = len([r for r in data if r.get('retention_day_7', False)])
        day_30_retained = len([r for r in data if r.get('retention_day_30', False)])
        
        return {
            "summary_metrics": {
                "total_installs": total_installs,
                "android_installs": android_installs,
                "ios_installs": ios_installs,
                "total_revenue": round(total_revenue, 2),
                "day_1_retention_rate": round(day_1_retained / total_installs * 100, 2) if total_installs > 0 else 0,
                "day_7_retention_rate": round(day_7_retained / total_installs * 100, 2) if total_installs > 0 else 0,
                "day_30_retention_rate": round(day_30_retained / total_installs * 100, 2) if total_installs > 0 else 0
            },
            "filters_applied": {
                "start_date": request.start_date,
                "end_date": request.end_date,
                "platforms": request.platforms,
                "event_types": request.event_types,
                "acquisition_sources": request.acquisition_sources,
                "customer_segments": request.customer_segments,
                "countries": request.countries,
                "max_records": request.max_records
            },
            "data": data,
            "metadata": {
                "generated_at": datetime.now().isoformat(),
                "gdpr_compliant": True,
                "market": "Netherlands",
                "channels": ["Google Play Console", "Apple App Store Connect"],
                "privacy_note": "IDFA/GAID availability reflects iOS 14.5+ and Android privacy changes"
            }
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating app store data: {str(e)}")

@app.get("/data")
async def get_recent_data(
    days: int = Query(30, description="Number of recent days"),
    max_records: int = Query(1000, description="Maximum records to return"),
    platforms: Optional[List[str]] = Query(None, description="Platforms to include"),
    acquisition_sources: Optional[List[str]] = Query(None, description="Acquisition sources to include")
):
    """Get recent app store data (convenient endpoint for N8N)"""
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    request = DataRequest(
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
        platforms=platforms,
        acquisition_sources=acquisition_sources,
        max_records=max_records
    )
    
    return await get_app_store_data(request)

@app.get("/google-play")
async def get_google_play_data(
    days: int = Query(30, description="Number of recent days"),
    max_records: int = Query(1000, description="Maximum records to return")
):
    """Get Google Play Console specific data"""
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    request = DataRequest(
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
        platforms=["google_play"],
        max_records=max_records
    )
    
    return await get_app_store_data(request)

@app.get("/apple-app-store")
async def get_apple_app_store_data(
    days: int = Query(30, description="Number of recent days"),
    max_records: int = Query(1000, description="Maximum records to return")
):
    """Get Apple App Store Connect specific data"""
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    request = DataRequest(
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
        platforms=["apple_app_store"],
        max_records=max_records
    )
    
    return await get_app_store_data(request)

@app.get("/install-attribution")
async def get_install_attribution(limit: int = Query(100, description="Number of attribution records to return")):
    """Get app install attribution data for cross-channel matching"""
    try:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)
        
        request = DataRequest(
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            event_types=["install"],
            max_records=limit
        )
        
        data = generator.generate_filtered_data(request)
        
        attribution_data = []
        for record in data:
            attribution_data.append({
                "attribution_touchpoint_id": record['attribution_touchpoint_id'],
                "customer_id": record['customer_id'],
                "platform": "google_play" if 'package_name' in record else "apple_app_store",
                "install_timestamp": record['install_timestamp'],
                "acquisition_source": record['acquisition_source'],
                "acquisition_channel": record['acquisition_channel'],
                "gclid": record.get('gclid'),
                "utm_campaign": record.get('utm_campaign'),
                "campaign_name": record.get('campaign_name'),
                "device_info": {
                    "type": record.get('device_type'),
                    "model": record.get('device_model'),
                    "os_version": record.get('android_version') or record.get('ios_version')
                }
            })
        
        return {
            "total_attributions": len(attribution_data),
            "attribution_data": attribution_data,
            "note": "App install attribution provides definitive conversion events for mobile attribution"
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating install attribution: {str(e)}")

@app.get("/platform-insights")
async def get_platform_insights():
    """Get mobile platform insights for the Dutch market"""
    return {
        "market_overview": {
            "country": "Netherlands",
            "android_market_share": 65,
            "ios_market_share": 35,
            "tablet_usage": 8,
            "mobile_banking_adoption": 78
        },
        "device_insights": {
            "top_android_brands": ["Samsung", "Google", "OnePlus", "Xiaomi"],
            "top_ios_devices": ["iPhone 15", "iPhone 14", "iPhone 13"],
            "avg_android_version": "13.0",
            "avg_ios_version": "17.2"
        },
        "attribution_challenges": {
            "ios_idfa_availability": "30% (post iOS 14.5)",
            "android_gaid_availability": "70% (privacy changes)",
            "primary_attribution_method": "First-party identifiers + probabilistic matching"
        },
        "acquisition_insights": {
            "top_sources": ["organic_search", "website_referral", "google_ads"],
            "highest_ltv": "website_referral",
            "best_retention": "organic_search",
            "lowest_cost": "organic_search"
        }
    }

if __name__ == "__main__":
    print("Starting App Store Synthetic Data API...")
    print("API Documentation: http://localhost:8006/docs")
    print("Health Check: http://localhost:8006/health")
    uvicorn.run(app, host="0.0.0.0", port=8006)