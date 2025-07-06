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
    LINKEDIN_ORGANIC = "linkedin_organic"
    INSTAGRAM_ORGANIC = "instagram_organic"
    FACEBOOK_ORGANIC = "facebook_organic"
    TIKTOK_ORGANIC = "tiktok_organic"
    X_ORGANIC = "x_organic"  # Twitter/X
    YOUTUBE_ORGANIC = "youtube_organic"

class EngagementType(Enum):
    VIEW = "view"
    LIKE = "like"
    SHARE = "share"
    COMMENT = "comment"
    CLICK = "click"
    FOLLOW = "follow"
    SAVE = "save"
    VIDEO_VIEW = "video_view"

class ContentType(Enum):
    POST = "post"
    STORY = "story"
    VIDEO = "video"
    REEL = "reel"
    CAROUSEL = "carousel"
    LIVE = "live"
    ARTICLE = "article"

class DeviceType(Enum):
    MOBILE = "mobile"
    DESKTOP = "desktop"
    TABLET = "tablet"

class CustomerSegment(Enum):
    B2C_WORKING_AGE = "B2C_WORKING_AGE"
    B2C_STUDENTS = "B2C_STUDENTS"
    B2C_NON_WORKING = "B2C_NON_WORKING"
    B2B_SMALL = "B2B_SMALL"
    B2B_MEDIUM = "B2B_MEDIUM"
    B2B_LARGE = "B2B_LARGE"

@dataclass
class OrganicSocialRecord:
    """Organic social media engagement record with limited tracking"""
    # Platform Identity
    platform: str
    post_id: str
    content_type: str
    
    # Engagement Data
    engagement_type: str
    engagement_timestamp: str
    
    # Limited Device Info (what platforms typically share)
    device_type: str
    
    # Geographic Data (aggregated/limited)
    location: str
    country: str
    
    # Content Information
    hashtags: List[str]
    mentions: List[str]
    content_theme: str
    
    # Traffic Data (when click-through occurs)
    referrer_url: Optional[str]
    destination_url: Optional[str]
    utm_source: Optional[str]
    utm_medium: Optional[str]
    utm_campaign: Optional[str]
    
    # Limited User Identification (realistic privacy constraints)
    user_id_hash: Optional[str]  # Hashed/anonymous identifier
    session_id: Optional[str]    # Only for click-throughs
    
    # Engagement Metrics
    engagement_score: float
    virality_factor: float
    
    # Attribution Fields (limited availability)
    follower_status: str  # follower, non_follower, unknown
    customer_segment_estimate: Optional[str]  # Estimated, not definitive
    cross_channel_probability: float  # Likelihood of same customer in other channels
    
    # Tracking Limitations
    tracking_confidence: str  # low, medium, high
    attribution_touchpoint_id: str
    weak_customer_signal: Optional[str]  # Very weak cross-channel signal

@dataclass
class ContentConfig:
    """Organic content campaign configuration"""
    name: str
    start_date: datetime
    end_date: datetime
    platforms: List[Platform]
    content_themes: List[str]
    target_segments: List[CustomerSegment]
    engagement_multiplier: float
    cultural_boost: float
    viral_probability: float

class DataRequest(BaseModel):
    """API request model for organic social data generation"""
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    platforms: Optional[List[str]] = None
    engagement_types: Optional[List[str]] = None
    content_types: Optional[List[str]] = None
    customer_segments: Optional[List[str]] = None
    tracking_confidence: Optional[List[str]] = None
    max_records: Optional[int] = 10000

class OrganicSocialGenerator:
    """
    Organic Social Media synthetic data API service for Dutch market
    Generates realistic limited-tracking organic social data (Jan 2024 - June 2025)
    """
    
    def __init__(self):
        self.total_customers = 200000
        self.organic_social_reach = 0.45  # 45% of customers see organic content
        
        # Platform distribution for Netherlands
        self.platform_distribution = {
            Platform.INSTAGRAM_ORGANIC: 0.28,
            Platform.LINKEDIN_ORGANIC: 0.25,  # High professional usage
            Platform.FACEBOOK_ORGANIC: 0.20,
            Platform.TIKTOK_ORGANIC: 0.15,
            Platform.X_ORGANIC: 0.08,
            Platform.YOUTUBE_ORGANIC: 0.04
        }
        
        # Engagement type distribution by platform
        self.engagement_distributions = {
            Platform.INSTAGRAM_ORGANIC: {
                EngagementType.VIEW: 0.45,
                EngagementType.LIKE: 0.25,
                EngagementType.SAVE: 0.12,
                EngagementType.SHARE: 0.08,
                EngagementType.COMMENT: 0.06,
                EngagementType.CLICK: 0.04
            },
            Platform.LINKEDIN_ORGANIC: {
                EngagementType.VIEW: 0.40,
                EngagementType.LIKE: 0.30,
                EngagementType.CLICK: 0.15,
                EngagementType.SHARE: 0.10,
                EngagementType.COMMENT: 0.05
            },
            Platform.TIKTOK_ORGANIC: {
                EngagementType.VIDEO_VIEW: 0.50,
                EngagementType.LIKE: 0.25,
                EngagementType.SHARE: 0.15,
                EngagementType.FOLLOW: 0.06,
                EngagementType.COMMENT: 0.04
            },
            Platform.FACEBOOK_ORGANIC: {
                EngagementType.VIEW: 0.35,
                EngagementType.LIKE: 0.28,
                EngagementType.SHARE: 0.20,
                EngagementType.CLICK: 0.10,
                EngagementType.COMMENT: 0.07
            }
        }
        
        # Device distribution (heavily mobile for social)
        self.device_distribution = {
            DeviceType.MOBILE: 0.88,
            DeviceType.DESKTOP: 0.09,
            DeviceType.TABLET: 0.03
        }
        
        # Customer segment distribution (broader social reach)
        self.segment_distribution = {
            CustomerSegment.B2C_STUDENTS: 0.35,      # High social media usage
            CustomerSegment.B2C_WORKING_AGE: 0.40,
            CustomerSegment.B2C_NON_WORKING: 0.15,
            CustomerSegment.B2B_SMALL: 0.07,         # Some LinkedIn presence
            CustomerSegment.B2B_MEDIUM: 0.03,
            CustomerSegment.B2B_LARGE: 0.00
        }
        
        # Dutch locations for organic reach
        self.dutch_locations = [
            "Amsterdam", "Rotterdam", "The Hague", "Utrecht", "Eindhoven",
            "Tilburg", "Groningen", "Almere", "Breda", "Nijmegen"
        ]
        
        self.customer_pool = self._generate_customer_pool()
        self._content_configs = None
        
    def _generate_customer_pool(self) -> List[Dict]:
        """Generate realistic customer pool with organic social attributes"""
        customers = []
        active_customers = int(self.total_customers * self.organic_social_reach)
        
        for i in range(active_customers):
            segment = self._weighted_choice(list(self.segment_distribution.keys()), 
                                          list(self.segment_distribution.values()))
            
            # Platform preferences based on demographics
            platform_preferences = self._calculate_platform_preferences(segment)
            
            customer = {
                'customer_id': f"cust_{str(uuid.uuid4())[:8]}",
                'segment': segment,
                'platform_preferences': platform_preferences,
                'device_preference': self._weighted_choice(list(self.device_distribution.keys()),
                                                         list(self.device_distribution.values())),
                'location': random.choice(self.dutch_locations),
                'engagement_propensity': random.uniform(0.1, 0.8),
                'viral_amplification': random.uniform(0.05, 0.4),
                'follower_likelihood': random.uniform(0.2, 0.9),
                'cross_channel_signal_strength': random.uniform(0.1, 0.6),  # Weak signals
                'content_affinity': {
                    'financial_education': random.uniform(0.2, 0.8),
                    'banking_features': random.uniform(0.1, 0.6),
                    'cultural_content': random.uniform(0.3, 0.9),
                    'brand_storytelling': random.uniform(0.2, 0.7)
                }
            }
            customers.append(customer)
            
        return customers
    
    def _weighted_choice(self, choices: List, weights: List) -> Any:
        """Select random choice based on weights"""
        return random.choices(choices, weights=weights)[0]
    
    def _calculate_platform_preferences(self, segment: CustomerSegment) -> Dict[Platform, float]:
        """Calculate platform usage likelihood based on customer segment"""
        base_preferences = {
            Platform.INSTAGRAM_ORGANIC: 0.5,
            Platform.LINKEDIN_ORGANIC: 0.3,
            Platform.FACEBOOK_ORGANIC: 0.4,
            Platform.TIKTOK_ORGANIC: 0.3,
            Platform.X_ORGANIC: 0.2,
            Platform.YOUTUBE_ORGANIC: 0.4
        }
        
        # Adjust based on segment
        if segment == CustomerSegment.B2C_STUDENTS:
            adjustments = {
                Platform.TIKTOK_ORGANIC: 0.8,
                Platform.INSTAGRAM_ORGANIC: 0.9,
                Platform.LINKEDIN_ORGANIC: 0.1,
                Platform.FACEBOOK_ORGANIC: 0.2
            }
        elif segment == CustomerSegment.B2C_WORKING_AGE:
            adjustments = {
                Platform.LINKEDIN_ORGANIC: 0.7,
                Platform.INSTAGRAM_ORGANIC: 0.6,
                Platform.FACEBOOK_ORGANIC: 0.5,
                Platform.TIKTOK_ORGANIC: 0.3
            }
        elif segment.name.startswith("B2B"):
            adjustments = {
                Platform.LINKEDIN_ORGANIC: 0.9,
                Platform.X_ORGANIC: 0.4,
                Platform.YOUTUBE_ORGANIC: 0.3,
                Platform.INSTAGRAM_ORGANIC: 0.2,
                Platform.TIKTOK_ORGANIC: 0.1,
                Platform.FACEBOOK_ORGANIC: 0.2
            }
        else:  # B2C_NON_WORKING
            adjustments = {
                Platform.FACEBOOK_ORGANIC: 0.7,
                Platform.YOUTUBE_ORGANIC: 0.6,
                Platform.INSTAGRAM_ORGANIC: 0.4,
                Platform.LINKEDIN_ORGANIC: 0.1
            }
        
        # Apply adjustments
        adjusted_preferences = {}
        for platform, base_pref in base_preferences.items():
            adjustment = adjustments.get(platform, 1.0)
            adjusted_preferences[platform] = min(base_pref * adjustment, 1.0)
        
        return adjusted_preferences
    
    def get_content_configs(self) -> List[ContentConfig]:
        """Define organic social content campaigns based on Dutch market calendar"""
        if self._content_configs is not None:
            return self._content_configs
            
        campaigns = []
        
        # 2024 Content Campaigns
        campaigns.extend([
            # February 2024 - Carnival Organic Content
            ContentConfig(
                name="Organic_Social_Carnival_Banking_Culture",
                start_date=datetime(2024, 2, 10),
                end_date=datetime(2024, 2, 25),
                platforms=[Platform.INSTAGRAM_ORGANIC, Platform.TIKTOK_ORGANIC, Platform.FACEBOOK_ORGANIC],
                content_themes=["carnival_celebration", "mobile_payments", "cultural_banking"],
                target_segments=[CustomerSegment.B2C_STUDENTS, CustomerSegment.B2C_NON_WORKING],
                engagement_multiplier=1.4,
                cultural_boost=1.8,
                viral_probability=0.15
            ),
            
            # March 2024 - Spring Financial Education
            ContentConfig(
                name="Organic_Social_Spring_Financial_Education",
                start_date=datetime(2024, 3, 1),
                end_date=datetime(2024, 3, 31),
                platforms=[Platform.LINKEDIN_ORGANIC, Platform.INSTAGRAM_ORGANIC, Platform.YOUTUBE_ORGANIC],
                content_themes=["financial_literacy", "spring_savings", "banking_tips"],
                target_segments=[CustomerSegment.B2C_WORKING_AGE, CustomerSegment.B2C_STUDENTS],
                engagement_multiplier=1.2,
                cultural_boost=1.0,
                viral_probability=0.08
            ),
            
            # April 2024 - King's Day Orange Content
            ContentConfig(
                name="Organic_Social_Kings_Day_Orange_Banking",
                start_date=datetime(2024, 4, 20),
                end_date=datetime(2024, 4, 30),
                platforms=[Platform.INSTAGRAM_ORGANIC, Platform.TIKTOK_ORGANIC, Platform.FACEBOOK_ORGANIC, Platform.X_ORGANIC],
                content_themes=["kings_day", "orange_branding", "dutch_pride", "mobile_banking"],
                target_segments=[CustomerSegment.B2C_WORKING_AGE, CustomerSegment.B2C_STUDENTS, CustomerSegment.B2C_NON_WORKING],
                engagement_multiplier=2.0,
                cultural_boost=2.5,
                viral_probability=0.25
            ),
            
            # May-June 2024 - Festival Season Content
            ContentConfig(
                name="Organic_Social_Festival_Banking_Lifestyle",
                start_date=datetime(2024, 5, 15),
                end_date=datetime(2024, 7, 15),
                platforms=[Platform.INSTAGRAM_ORGANIC, Platform.TIKTOK_ORGANIC, Platform.YOUTUBE_ORGANIC],
                content_themes=["festival_season", "contactless_payments", "summer_banking", "lifestyle"],
                target_segments=[CustomerSegment.B2C_STUDENTS, CustomerSegment.B2C_WORKING_AGE],
                engagement_multiplier=1.3,
                cultural_boost=1.2,
                viral_probability=0.12
            ),
            
            # July-August 2024 - Summer Lifestyle (Lower Activity)
            ContentConfig(
                name="Organic_Social_Summer_Banking_Lifestyle",
                start_date=datetime(2024, 7, 1),
                end_date=datetime(2024, 8, 31),
                platforms=[Platform.INSTAGRAM_ORGANIC, Platform.YOUTUBE_ORGANIC],
                content_themes=["summer_travel", "vacation_banking", "lifestyle_content"],
                target_segments=[CustomerSegment.B2C_NON_WORKING, CustomerSegment.B2C_STUDENTS],
                engagement_multiplier=0.8,
                cultural_boost=0.7,
                viral_probability=0.06
            ),
            
            # September 2024 - Back to School Social
            ContentConfig(
                name="Organic_Social_Student_Banking_Education",
                start_date=datetime(2024, 9, 1),
                end_date=datetime(2024, 9, 30),
                platforms=[Platform.TIKTOK_ORGANIC, Platform.INSTAGRAM_ORGANIC, Platform.YOUTUBE_ORGANIC],
                content_themes=["student_banking", "financial_independence", "back_to_school"],
                target_segments=[CustomerSegment.B2C_STUDENTS, CustomerSegment.B2C_WORKING_AGE],
                engagement_multiplier=1.4,
                cultural_boost=1.1,
                viral_probability=0.10
            ),
            
            # October-November 2024 - Professional Content
            ContentConfig(
                name="Organic_Social_Professional_Banking_Growth",
                start_date=datetime(2024, 10, 1),
                end_date=datetime(2024, 11, 30),
                platforms=[Platform.LINKEDIN_ORGANIC, Platform.X_ORGANIC, Platform.YOUTUBE_ORGANIC],
                content_themes=["professional_growth", "business_banking", "financial_planning"],
                target_segments=[CustomerSegment.B2C_WORKING_AGE, CustomerSegment.B2B_SMALL],
                engagement_multiplier=1.1,
                cultural_boost=0.9,
                viral_probability=0.05
            ),
            
            # December 2024 - Holiday Content
            ContentConfig(
                name="Organic_Social_Holiday_Banking_Family",
                start_date=datetime(2024, 12, 1),
                end_date=datetime(2024, 12, 31),
                platforms=[Platform.INSTAGRAM_ORGANIC, Platform.FACEBOOK_ORGANIC, Platform.YOUTUBE_ORGANIC],
                content_themes=["holiday_budgeting", "family_banking", "year_end_savings"],
                target_segments=[CustomerSegment.B2C_WORKING_AGE, CustomerSegment.B2C_NON_WORKING],
                engagement_multiplier=1.2,
                cultural_boost=1.3,
                viral_probability=0.07
            )
        ])
        
        # 2025 Content Campaigns (January - June)
        campaigns.extend([
            # February 2025 - Carnival
            ContentConfig(
                name="Organic_Social_Carnival_Banking_Culture",
                start_date=datetime(2025, 2, 15),
                end_date=datetime(2025, 2, 28),
                platforms=[Platform.INSTAGRAM_ORGANIC, Platform.TIKTOK_ORGANIC, Platform.FACEBOOK_ORGANIC],
                content_themes=["carnival_celebration", "mobile_payments", "cultural_banking"],
                target_segments=[CustomerSegment.B2C_STUDENTS, CustomerSegment.B2C_NON_WORKING],
                engagement_multiplier=1.5,
                cultural_boost=1.9,
                viral_probability=0.18
            ),
            
            # March 2025 - Spring Content
            ContentConfig(
                name="Organic_Social_Spring_Financial_Education",
                start_date=datetime(2025, 3, 1),
                end_date=datetime(2025, 3, 31),
                platforms=[Platform.LINKEDIN_ORGANIC, Platform.INSTAGRAM_ORGANIC, Platform.YOUTUBE_ORGANIC],
                content_themes=["financial_literacy", "spring_savings", "banking_tips"],
                target_segments=[CustomerSegment.B2C_WORKING_AGE, CustomerSegment.B2C_STUDENTS],
                engagement_multiplier=1.25,
                cultural_boost=1.05,
                viral_probability=0.09
            ),
            
            # April 2025 - King's Day
            ContentConfig(
                name="Organic_Social_Kings_Day_Orange_Banking",
                start_date=datetime(2025, 4, 20),
                end_date=datetime(2025, 4, 30),
                platforms=[Platform.INSTAGRAM_ORGANIC, Platform.TIKTOK_ORGANIC, Platform.FACEBOOK_ORGANIC, Platform.X_ORGANIC],
                content_themes=["kings_day", "orange_branding", "dutch_pride", "mobile_banking"],
                target_segments=[CustomerSegment.B2C_WORKING_AGE, CustomerSegment.B2C_STUDENTS, CustomerSegment.B2C_NON_WORKING],
                engagement_multiplier=2.1,
                cultural_boost=2.6,
                viral_probability=0.28
            ),
            
            # May-June 2025 - Festival Season
            ContentConfig(
                name="Organic_Social_Festival_Banking_Lifestyle",
                start_date=datetime(2025, 5, 15),
                end_date=datetime(2025, 6, 30),
                platforms=[Platform.INSTAGRAM_ORGANIC, Platform.TIKTOK_ORGANIC, Platform.YOUTUBE_ORGANIC],
                content_themes=["festival_season", "contactless_payments", "summer_banking", "lifestyle"],
                target_segments=[CustomerSegment.B2C_STUDENTS, CustomerSegment.B2C_WORKING_AGE],
                engagement_multiplier=1.35,
                cultural_boost=1.25,
                viral_probability=0.14
            )
        ])
        
        self._content_configs = campaigns
        return campaigns
    
    def _generate_content_hashtags(self, content_theme: str, platform: Platform) -> List[str]:
        """Generate realistic hashtags based on content theme and platform"""
        base_hashtags = {
            "carnival_celebration": ["#bunq", "#carnaval", "#mobielbanken", "#nederland", "#vrijheid"],
            "kings_day": ["#bunq", "#koningsdag", "#oranje", "#nederland", "#banking", "#mobiel"],
            "festival_season": ["#bunq", "#festival", "#contactloos", "#mobielbanken", "#zomer"],
            "financial_literacy": ["#bunq", "#financieelgezond", "#sparen", "#bankieren", "#tips"],
            "student_banking": ["#bunq", "#student", "#bankieren", "#financieelonafhankelijk"],
            "professional_growth": ["#bunq", "#zakelijkbankieren", "#groei", "#financieel"],
            "summer_travel": ["#bunq", "#reizen", "#mobielbanken", "#zomer", "#vakantie"]
        }
        
        platform_specific = {
            Platform.LINKEDIN_ORGANIC: ["#fintech", "#banking", "#netherlands", "#business"],
            Platform.TIKTOK_ORGANIC: ["#fyp", "#netherlands", "#bankinglife", "#fintech"],
            Platform.INSTAGRAM_ORGANIC: ["#bunqlife", "#netherlands", "#banking", "#lifestyle"],
            Platform.FACEBOOK_ORGANIC: ["#bunq", "#netherlands", "#mobielbanken"]
        }
        
        theme_tags = base_hashtags.get(content_theme, ["#bunq", "#banking", "#netherlands"])
        platform_tags = platform_specific.get(platform, [])
        
        # Combine and limit to realistic number
        all_tags = theme_tags + platform_tags
        return random.sample(all_tags, min(len(all_tags), random.randint(3, 8)))
    
    def _generate_utm_tracking(self, platform: Platform, content_theme: str) -> Dict[str, Optional[str]]:
        """Generate UTM tracking for click-through traffic (limited organic tracking)"""
        # Only 15% of organic social generates trackable traffic
        if random.random() > 0.15:
            return {"utm_source": None, "utm_medium": None, "utm_campaign": None}
        
        utm_data = {
            "utm_source": platform.value.replace("_organic", ""),
            "utm_medium": "social",
            "utm_campaign": f"organic_{content_theme}_{datetime.now().strftime('%Y%m')}"
        }
        
        return utm_data
    
    def _determine_tracking_confidence(self, engagement_type: EngagementType, 
                                     platform: Platform, has_click: bool) -> str:
        """Determine tracking confidence level based on engagement and platform"""
        if engagement_type == EngagementType.CLICK and has_click:
            return "high"  # Click-through provides good tracking
        elif platform == Platform.LINKEDIN_ORGANIC and engagement_type in [EngagementType.SHARE, EngagementType.COMMENT]:
            return "medium"  # LinkedIn provides better business tracking
        elif engagement_type in [EngagementType.VIEW, EngagementType.LIKE]:
            return "low"  # Basic engagement, limited tracking
        else:
            return "low"  # Default low confidence
    
    def _generate_engagements_for_campaign(self, campaign: ContentConfig) -> List[OrganicSocialRecord]:
        """Generate organic social engagements for a specific content campaign"""
        records = []
        campaign_days = (campaign.end_date - campaign.start_date).days + 1
        
        # Daily engagement volume (much lower than paid, harder to track)
        base_daily_engagements = 800
        daily_engagements = int(base_daily_engagements * campaign.engagement_multiplier * campaign.cultural_boost)
        
        # Select customers likely to engage with this content
        eligible_customers = []
        for customer in self.customer_pool:
            if customer['segment'] in campaign.target_segments:
                for platform in campaign.platforms:
                    platform_likelihood = customer['platform_preferences'].get(platform, 0.3)
                    if random.random() < platform_likelihood:
                        eligible_customers.append((customer, platform))
        
        for day_offset in range(campaign_days):
            current_date = campaign.start_date + timedelta(days=day_offset)
            
            # Daily variation (social media peaks in evenings/weekends)
            if current_date.weekday() >= 5:  # Weekend
                daily_multiplier = 1.4
            else:  # Weekday
                daily_multiplier = 1.0
            
            daily_engagement_count = int(daily_engagements * daily_multiplier)
            
            # Generate engagements for this day
            for _ in range(daily_engagement_count):
                if not eligible_customers:
                    continue
                    
                customer, platform = random.choice(eligible_customers)
                
                # Determine content theme for this engagement
                content_theme = random.choice(campaign.content_themes)
                
                # Select engagement type based on platform
                engagement_dist = self.engagement_distributions.get(platform, {
                    EngagementType.VIEW: 0.5,
                    EngagementType.LIKE: 0.3,
                    EngagementType.CLICK: 0.2
                })
                
                engagement_type = self._weighted_choice(list(engagement_dist.keys()),
                                                      list(engagement_dist.values()))
                
                # Generate engagement timestamp (social peak hours: 7-10 PM)
                hour = random.choices(range(24), weights=[0.3]*7 + [1]*12 + [2.5]*5)[0]
                engagement_time = current_date.replace(
                    hour=hour,
                    minute=random.randint(0, 59),
                    second=random.randint(0, 59)
                )
                
                # Determine if this generates trackable click-through
                has_click_through = (engagement_type == EngagementType.CLICK and 
                                   random.random() < 0.25)  # 25% of clicks are trackable
                
                # Generate tracking data
                utm_data = self._generate_utm_tracking(platform, content_theme)
                tracking_confidence = self._determine_tracking_confidence(engagement_type, platform, has_click_through)
                
                # Generate weak customer signals (very limited cross-channel correlation)
                weak_customer_signal = None
                if random.random() < customer['cross_channel_signal_strength']:
                    weak_customer_signal = f"weak_signal_{str(uuid.uuid4())[:8]}"
                
                # Determine follower status (limited visibility)
                follower_status = "unknown"
                if random.random() < 0.3:  # Only 30% visibility into follower status
                    follower_status = "follower" if random.random() < customer['follower_likelihood'] else "non_follower"
                
                # Calculate engagement score and virality
                engagement_score = (customer['engagement_propensity'] * 
                                  campaign.engagement_multiplier * 
                                  random.uniform(0.7, 1.3))
                
                virality_factor = customer['viral_amplification'] * campaign.viral_probability
                
                # Generate destination URL for click-throughs
                destination_url = None
                referrer_url = None
                session_id = None
                
                if has_click_through:
                    destination_url = f"https://bunq.com/{random.choice(['features', 'signup', 'about', 'blog'])}?utm_source={utm_data['utm_source']}"
                    referrer_url = f"https://{platform.value.replace('_organic', '')}.com"
                    session_id = f"social_session_{str(uuid.uuid4())[:8]}"
                
                # Create the record
                record = OrganicSocialRecord(
                    platform=platform.value,
                    post_id=f"{platform.value}_post_{random.randint(100000, 999999)}",
                    content_type=random.choice([ContentType.POST, ContentType.VIDEO, ContentType.STORY]).value,
                    engagement_type=engagement_type.value,
                    engagement_timestamp=engagement_time.isoformat() + "Z",
                    device_type=customer['device_preference'].value,
                    location=f"{customer['location']}, Netherlands",
                    country="Netherlands",
                    hashtags=self._generate_content_hashtags(content_theme, platform),
                    mentions=["@bunq"] if random.random() < 0.4 else [],
                    content_theme=content_theme,
                    referrer_url=referrer_url,
                    destination_url=destination_url,
                    utm_source=utm_data.get("utm_source"),
                    utm_medium=utm_data.get("utm_medium"),
                    utm_campaign=utm_data.get("utm_campaign"),
                    user_id_hash=f"social_user_{str(uuid.uuid4())[:12]}" if random.random() < 0.2 else None,
                    session_id=session_id,
                    engagement_score=engagement_score,
                    virality_factor=virality_factor,
                    follower_status=follower_status,
                    customer_segment_estimate=customer['segment'].value if random.random() < 0.3 else None,
                    cross_channel_probability=customer['cross_channel_signal_strength'],
                    tracking_confidence=tracking_confidence,
                    attribution_touchpoint_id=f"organic_social_{str(uuid.uuid4())[:8]}",
                    weak_customer_signal=weak_customer_signal
                )
                
                records.append(record)
        
        return records
    
    def generate_filtered_data(self, request: DataRequest) -> List[Dict]:
        """Generate filtered organic social data based on API request parameters"""
        campaigns = self.get_content_configs()
        
        # Apply date filters
        if request.start_date:
            start_filter = datetime.fromisoformat(request.start_date.replace('Z', ''))
            campaigns = [c for c in campaigns if c.end_date >= start_filter]
        
        if request.end_date:
            end_filter = datetime.fromisoformat(request.end_date.replace('Z', ''))
            campaigns = [c for c in campaigns if c.start_date <= end_filter]
        
        all_records = []
        
        for campaign in campaigns:
            campaign_records = self._generate_engagements_for_campaign(campaign)
            
            # Apply filters
            if request.platforms:
                campaign_records = [r for r in campaign_records if r.platform in request.platforms]
            
            if request.engagement_types:
                campaign_records = [r for r in campaign_records if r.engagement_type in request.engagement_types]
            
            if request.content_types:
                campaign_records = [r for r in campaign_records if r.content_type in request.content_types]
            
            if request.customer_segments:
                campaign_records = [r for r in campaign_records 
                                  if r.customer_segment_estimate and r.customer_segment_estimate in request.customer_segments]
            
            if request.tracking_confidence:
                campaign_records = [r for r in campaign_records if r.tracking_confidence in request.tracking_confidence]
            
            all_records.extend(campaign_records)
            
            # Respect max_records limit
            if len(all_records) >= request.max_records:
                all_records = all_records[:request.max_records]
                break
        
        return [asdict(record) for record in all_records]

# Initialize the generator instance
generator = OrganicSocialGenerator()

# Create FastAPI app
app = FastAPI(
    title="Organic Social Media Synthetic Data API",
    description="Limited-tracking organic social media data generator for brand awareness attribution",
    version="1.0.0"
)

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "organic-social-generator", "version": "1.0.0"}

@app.get("/campaigns")
async def get_campaigns():
    """Get list of all available organic social campaigns"""
    campaigns = generator.get_content_configs()
    
    campaign_list = []
    for campaign in campaigns:
        campaign_list.append({
            "name": campaign.name,
            "start_date": campaign.start_date.isoformat(),
            "end_date": campaign.end_date.isoformat(),
            "platforms": [p.value for p in campaign.platforms],
            "content_themes": campaign.content_themes,
            "target_segments": [seg.value for seg in campaign.target_segments],
            "engagement_multiplier": campaign.engagement_multiplier,
            "cultural_boost": campaign.cultural_boost,
            "viral_probability": campaign.viral_probability
        })
    
    return {
        "total_campaigns": len(campaign_list),
        "campaigns": campaign_list
    }

@app.post("/data")
async def get_organic_social_data(request: DataRequest):
    """Get organic social media engagement data"""
    try:
        data = generator.generate_filtered_data(request)
        
        # Calculate summary metrics
        total_engagements = len(data)
        platforms = list(set(r['platform'] for r in data))
        trackable_clicks = len([r for r in data if r['engagement_type'] == 'click' and r['destination_url']])
        high_confidence = len([r for r in data if r['tracking_confidence'] == 'high'])
        viral_content = len([r for r in data if r['virality_factor'] > 0.1])
        
        return {
            "summary_metrics": {
                "total_engagements": total_engagements,
                "platforms_active": len(platforms),
                "trackable_clicks": trackable_clicks,
                "high_confidence_tracking": high_confidence,
                "viral_content_pieces": viral_content,
                "tracking_rate": round(trackable_clicks / total_engagements * 100, 2) if total_engagements > 0 else 0
            },
            "filters_applied": {
                "start_date": request.start_date,
                "end_date": request.end_date,
                "platforms": request.platforms,
                "engagement_types": request.engagement_types,
                "content_types": request.content_types,
                "customer_segments": request.customer_segments,
                "tracking_confidence": request.tracking_confidence,
                "max_records": request.max_records
            },
            "data": data,
            "metadata": {
                "generated_at": datetime.now().isoformat(),
                "gdpr_compliant": True,
                "market": "Netherlands",
                "channel": "Organic Social Media",
                "tracking_limitations": "Organic social has inherently limited attribution tracking",
                "attribution_value": "Brand awareness and weak customer signals"
            }
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating organic social data: {str(e)}")

@app.get("/data")
async def get_recent_data(
    days: int = Query(30, description="Number of recent days"),
    max_records: int = Query(1000, description="Maximum records to return"),
    platforms: Optional[List[str]] = Query(None, description="Platforms to include"),
    engagement_types: Optional[List[str]] = Query(None, description="Engagement types to include"),
    tracking_confidence: Optional[List[str]] = Query(None, description="Tracking confidence levels")
):
    """Get recent organic social data (convenient endpoint for N8N)"""
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    request = DataRequest(
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
        platforms=platforms,
        engagement_types=engagement_types,
        tracking_confidence=tracking_confidence,
        max_records=max_records
    )
    
    return await get_organic_social_data(request)

@app.get("/trackable-traffic")
async def get_trackable_traffic(
    days: int = Query(30, description="Number of recent days"),
    limit: int = Query(500, description="Maximum trackable events to return")
):
    """Get only trackable organic social traffic (clicks with UTM data)"""
    try:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        request = DataRequest(
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            engagement_types=["click"],
            tracking_confidence=["high", "medium"],
            max_records=limit * 3  # Get more to filter down
        )
        
        data = generator.generate_filtered_data(request)
        
        # Filter to only trackable traffic
        trackable_traffic = [r for r in data if r['destination_url'] and r['utm_source']][:limit]
        
        return {
            "total_trackable_events": len(trackable_traffic),
            "attribution_note": "Organic social provides limited but valuable brand awareness attribution",
            "trackable_traffic": trackable_traffic
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating trackable traffic: {str(e)}")

@app.get("/platform-insights")
async def get_platform_insights():
    """Get organic social platform insights for Dutch market"""
    return {
        "platform_characteristics": {
            "instagram_organic": {
                "primary_audience": "B2C Students + Working Age",
                "content_types": ["Stories", "Reels", "Posts"],
                "tracking_capability": "Limited - clicks only",
                "engagement_patterns": "Visual content, lifestyle focus"
            },
            "linkedin_organic": {
                "primary_audience": "B2B + Professional B2C",
                "content_types": ["Articles", "Posts", "Videos"],
                "tracking_capability": "Medium - business context helps",
                "engagement_patterns": "Professional content, thought leadership"
            },
            "tiktok_organic": {
                "primary_audience": "B2C Students (dominant)",
                "content_types": ["Short Videos", "Challenges"],
                "tracking_capability": "Very Limited",
                "engagement_patterns": "Viral potential, entertainment focus"
            },
            "facebook_organic": {
                "primary_audience": "B2C Non-working + Older demographics",
                "content_types": ["Posts", "Videos", "Stories"],
                "tracking_capability": "Limited - declining organic reach",
                "engagement_patterns": "Community engagement, sharing focus"
            }
        },
        "attribution_challenges": {
            "limited_tracking": "Organic social provides minimal attribution data",
            "privacy_constraints": "Platform privacy policies limit user identification",
            "weak_signals": "Cross-channel correlation relies on probabilistic matching",
            "brand_value": "Primary value is brand awareness and cultural engagement"
        },
        "dutch_cultural_opportunities": {
            "kings_day": "Massive engagement opportunity with orange-themed content",
            "carnival": "Regional focus (South Netherlands) with high viral potential",
            "festival_season": "Summer festivals provide lifestyle marketing opportunities"
        }
    }

@app.get("/content-themes")
async def get_available_content_themes():
    """Get available content themes and platform combinations"""
    return {
        "content_themes": [
            "carnival_celebration", "kings_day", "festival_season", 
            "financial_literacy", "student_banking", "professional_growth",
            "summer_travel", "holiday_budgeting", "mobile_payments"
        ],
        "engagement_types": [
            "view", "like", "share", "comment", "click", "follow", "save", "video_view"
        ],
        "platforms": [
            "instagram_organic", "linkedin_organic", "facebook_organic", 
            "tiktok_organic", "x_organic", "youtube_organic"
        ],
        "tracking_confidence_levels": ["low", "medium", "high"],
        "content_types": ["post", "story", "video", "reel", "carousel", "live", "article"]
    }

@app.get("/weak-attribution-signals")
async def get_weak_attribution_signals(limit: int = Query(50, description="Number of weak signals to return")):
    """Get weak attribution signals for probabilistic cross-channel matching"""
    try:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=14)
        
        request = DataRequest(
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            max_records=limit * 2
        )
        
        data = generator.generate_filtered_data(request)
        
        # Filter to records with weak customer signals
        weak_signals = [r for r in data if r['weak_customer_signal']][:limit]
        
        attribution_signals = []
        for record in weak_signals:
            attribution_signals.append({
                "attribution_touchpoint_id": record['attribution_touchpoint_id'],
                "weak_customer_signal": record['weak_customer_signal'],
                "platform": record['platform'],
                "engagement_timestamp": record['engagement_timestamp'],
                "cross_channel_probability": record['cross_channel_probability'],
                "tracking_confidence": record['tracking_confidence'],
                "content_theme": record['content_theme'],
                "location": record['location']
            })
        
        return {
            "total_weak_signals": len(attribution_signals),
            "attribution_signals": attribution_signals,
            "usage_note": "These weak signals can be used for probabilistic cross-channel customer matching"
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating weak attribution signals: {str(e)}")

if __name__ == "__main__":
    print("Starting Organic Social Media Synthetic Data API...")
    print("API Documentation: http://localhost:8007/docs")
    print("Health Check: http://localhost:8007/health")
    uvicorn.run(app, host="0.0.0.0", port=8007)