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

class DeviceType(Enum):
    DESKTOP = "DESKTOP"
    MOBILE = "MOBILE"
    TABLET = "TABLET"

class EmailClient(Enum):
    GMAIL = "Gmail"
    OUTLOOK = "Outlook"
    APPLE_MAIL = "Apple Mail"
    YAHOO = "Yahoo"
    OTHER = "Other"

class EmailType(Enum):
    WELCOME_SERIES = "welcome_series"
    NURTURING = "nurturing"
    PROMOTIONAL = "promotional"
    RETENTION = "retention"
    NEWSLETTER = "newsletter"
    TRANSACTIONAL = "transactional"

class CustomerSegment(Enum):
    B2C_WORKING_AGE = "B2C_WORKING_AGE"
    B2C_STUDENTS = "B2C_STUDENTS"
    B2C_NON_WORKING = "B2C_NON_WORKING"
    B2B_SMALL = "B2B_SMALL"
    B2B_MEDIUM = "B2B_MEDIUM"
    B2B_LARGE = "B2B_LARGE"

class SubscriptionStatus(Enum):
    SUBSCRIBED = "subscribed"
    UNSUBSCRIBED = "unsubscribed"
    PENDING = "pending"
    BOUNCED = "bounced"

@dataclass
class EmailMarketingRecord:
    """Email Marketing touchpoint record structure"""
    email_id: str
    campaign_id: str
    campaign_name: str
    email_address: str  # PRIMARY IDENTIFIER for cross-channel matching
    send_timestamp: str
    open_timestamp: Optional[str]
    click_timestamp: Optional[str]
    device_type: str
    location: str
    email_client: str
    link_clicked: Optional[str]
    user_id: str  # Internal customer ID
    list_id: str
    engagement_score: float
    subscription_status: str
    email_type: str
    subject_line: str
    customer_id: str
    segment: str

@dataclass
class CampaignConfig:
    """Email campaign configuration structure"""
    name: str
    start_date: datetime
    end_date: datetime
    stages: List[str]
    stage_weights: Dict[str, float]
    target_segments: List[CustomerSegment]
    email_type: EmailType
    send_frequency_days: int  # How often emails are sent
    seasonality_multiplier: float
    cross_channel_correlation: float  # Likelihood of triggering other channels

class DataRequest(BaseModel):
    """API request model for data generation"""
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    campaign_names: Optional[List[str]] = None
    customer_segments: Optional[List[str]] = None
    email_types: Optional[List[str]] = None
    subscription_status: Optional[List[str]] = None
    engagement_score_min: Optional[float] = None
    max_records: Optional[int] = 10000

class EmailMarketingGenerator:
    """
    Email Marketing synthetic data API service for Dutch market
    Generates realistic data (Jan 2024 - June 2025) with strong customer identifiers
    """
    
    def __init__(self):
        self.base_open_rate = 0.25  # 25% open rate
        self.base_click_rate = 0.035  # 3.5% click rate (of delivered)
        self.total_customers = 200000
        self.email_penetration = 0.85  # High email list penetration
        
        # Dutch locations for realistic targeting
        self.dutch_locations = [
            "Amsterdam", "Rotterdam", "The Hague", "Utrecht", "Eindhoven",
            "Tilburg", "Groningen", "Almere", "Breda", "Nijmegen"
        ]
        
        # Device distribution for email (higher desktop usage than social)
        self.device_distribution = {
            DeviceType.MOBILE: 0.58,
            DeviceType.DESKTOP: 0.35,
            DeviceType.TABLET: 0.07
        }
        
        # Email client distribution for Netherlands
        self.email_client_distribution = {
            EmailClient.GMAIL: 0.45,
            EmailClient.OUTLOOK: 0.25,
            EmailClient.APPLE_MAIL: 0.15,
            EmailClient.YAHOO: 0.08,
            EmailClient.OTHER: 0.07
        }
        
        # Customer segment distribution (broader email reach)
        self.segment_distribution = {
            CustomerSegment.B2C_WORKING_AGE: 0.42,
            CustomerSegment.B2C_STUDENTS: 0.22,
            CustomerSegment.B2C_NON_WORKING: 0.18,
            CustomerSegment.B2B_SMALL: 0.12,
            CustomerSegment.B2B_MEDIUM: 0.05,
            CustomerSegment.B2B_LARGE: 0.01
        }
        
        # Subscription status distribution
        self.subscription_distribution = {
            SubscriptionStatus.SUBSCRIBED: 0.82,
            SubscriptionStatus.UNSUBSCRIBED: 0.12,
            SubscriptionStatus.PENDING: 0.04,
            SubscriptionStatus.BOUNCED: 0.02
        }
        
        self.customer_pool = self._generate_customer_pool()
        self._campaign_configs = None
        
    def _generate_customer_pool(self) -> List[Dict]:
        """Generate realistic customer pool with email-specific attributes"""
        customers = []
        active_customers = int(self.total_customers * self.email_penetration)
        
        # Domain distributions for realistic email generation
        b2c_domains = ["gmail.com", "hotmail.com", "outlook.com", "yahoo.com", "icloud.com", "live.nl", "ziggo.nl"]
        b2b_domains = ["company.nl", "bedrijf.com", "business.nl", "corp.com", "bv.nl", "group.com"]
        
        for i in range(active_customers):
            segment = self._weighted_choice(list(self.segment_distribution.keys()), 
                                          list(self.segment_distribution.values()))
            
            subscription_status = self._weighted_choice(list(self.subscription_distribution.keys()),
                                                      list(self.subscription_distribution.values()))
            
            # Generate realistic email address based on segment
            if segment.name.startswith("B2B"):
                domain = random.choice(b2b_domains)
                username = f"{random.choice(['info', 'admin', 'finance', 'manager', 'director'])}{random.randint(1, 999)}"
            else:
                domain = random.choice(b2c_domains)
                username = f"user{random.randint(1000, 9999)}"
            
            email_address = f"{username}@{domain}"
            
            customer = {
                'customer_id': f"cust_{str(uuid.uuid4())[:8]}",
                'user_id': f"usr_{random.randint(100000000, 999999999)}",
                'email_address': email_address,
                'segment': segment,
                'subscription_status': subscription_status,
                'preferred_device': self._weighted_choice(list(self.device_distribution.keys()),
                                                       list(self.device_distribution.values())),
                'email_client': self._weighted_choice(list(self.email_client_distribution.keys()),
                                                    list(self.email_client_distribution.values())),
                'location': random.choice(self.dutch_locations),
                'engagement_score': random.uniform(0.2, 1.0),
                'seasonal_sensitivity': random.uniform(0.5, 1.5),
                'cross_channel_probability': random.uniform(0.35, 0.65),  # Email to other channels
                'registration_date': datetime.now() - timedelta(days=random.randint(1, 365))
            }
            customers.append(customer)
            
        return customers
    
    def _weighted_choice(self, choices: List, weights: List) -> Any:
        """Select random choice based on weights"""
        return random.choices(choices, weights=weights)[0]
    
    def get_campaign_configs(self) -> List[CampaignConfig]:
        """Define all Email Marketing campaigns based on Dutch market calendar"""
        if self._campaign_configs is not None:
            return self._campaign_configs
            
        campaigns = []
        
        # 2024 Campaigns
        campaigns.extend([
            # January 2024 - Welcome & Nurturing
            CampaignConfig(
                name="Email_Marketing_New_Year_Financial_Resolutions",
                start_date=datetime(2024, 1, 2),
                end_date=datetime(2024, 1, 31),
                stages=["Interest", "Consideration"],
                stage_weights={"Interest": 0.6, "Consideration": 0.4},
                target_segments=[CustomerSegment.B2C_WORKING_AGE, CustomerSegment.B2B_SMALL],
                email_type=EmailType.NURTURING,
                send_frequency_days=7,
                seasonality_multiplier=0.9,
                cross_channel_correlation=0.6
            ),
            
            CampaignConfig(
                name="Email_Marketing_Student_Exam_Banking_Support",
                start_date=datetime(2024, 1, 15),
                end_date=datetime(2024, 2, 5),
                stages=["Interest"],
                stage_weights={"Interest": 1.0},
                target_segments=[CustomerSegment.B2C_STUDENTS],
                email_type=EmailType.PROMOTIONAL,
                send_frequency_days=5,
                seasonality_multiplier=0.8,
                cross_channel_correlation=0.4
            ),
            
            # February 2024 - Spring Preparation
            CampaignConfig(
                name="Email_Marketing_Spring_Preparation_Banking",
                start_date=datetime(2024, 2, 20),
                end_date=datetime(2024, 3, 15),
                stages=["Interest", "Consideration"],
                stage_weights={"Interest": 0.5, "Consideration": 0.5},
                target_segments=[CustomerSegment.B2C_WORKING_AGE],
                email_type=EmailType.NURTURING,
                send_frequency_days=10,
                seasonality_multiplier=1.0,
                cross_channel_correlation=0.7
            ),
            
            # March 2024 - Peak Season
            CampaignConfig(
                name="Email_Marketing_Spring_Financial_Fresh_Start",
                start_date=datetime(2024, 3, 1),
                end_date=datetime(2024, 3, 31),
                stages=["Interest", "Consideration", "Conversion"],
                stage_weights={"Interest": 0.4, "Consideration": 0.4, "Conversion": 0.2},
                target_segments=[CustomerSegment.B2C_WORKING_AGE, CustomerSegment.B2C_STUDENTS],
                email_type=EmailType.NURTURING,
                send_frequency_days=7,
                seasonality_multiplier=1.3,
                cross_channel_correlation=0.8
            ),
            
            # April 2024 - King's Day
            CampaignConfig(
                name="Email_Marketing_Kings_Day_Banking_Freedom",
                start_date=datetime(2024, 4, 20),
                end_date=datetime(2024, 4, 30),
                stages=["Interest", "Conversion"],
                stage_weights={"Interest": 0.6, "Conversion": 0.4},
                target_segments=[CustomerSegment.B2C_WORKING_AGE, CustomerSegment.B2C_STUDENTS],
                email_type=EmailType.PROMOTIONAL,
                send_frequency_days=3,
                seasonality_multiplier=1.2,
                cross_channel_correlation=0.5
            ),
            
            # May 2024 - Early Summer
            CampaignConfig(
                name="Email_Marketing_Early_Summer_Mobile_Banking",
                start_date=datetime(2024, 5, 10),
                end_date=datetime(2024, 6, 10),
                stages=["Interest", "Consideration", "Conversion"],
                stage_weights={"Interest": 0.4, "Consideration": 0.35, "Conversion": 0.25},
                target_segments=[CustomerSegment.B2C_WORKING_AGE, CustomerSegment.B2C_STUDENTS],
                email_type=EmailType.NURTURING,
                send_frequency_days=14,
                seasonality_multiplier=1.1,
                cross_channel_correlation=0.6
            ),
            
            # June 2024 - Festival Season
            CampaignConfig(
                name="Email_Marketing_Festival_Season_Banking",
                start_date=datetime(2024, 6, 1),
                end_date=datetime(2024, 7, 15),
                stages=["Interest", "Conversion"],
                stage_weights={"Interest": 0.7, "Conversion": 0.3},
                target_segments=[CustomerSegment.B2C_STUDENTS, CustomerSegment.B2C_NON_WORKING],
                email_type=EmailType.PROMOTIONAL,
                send_frequency_days=7,
                seasonality_multiplier=0.9,
                cross_channel_correlation=0.4
            ),
            
            # July-August 2024 - Summer Retention
            CampaignConfig(
                name="Email_Marketing_Summer_Travel_Banking",
                start_date=datetime(2024, 7, 1),
                end_date=datetime(2024, 8, 31),
                stages=["Retention", "Interest"],
                stage_weights={"Retention": 0.7, "Interest": 0.3},
                target_segments=[CustomerSegment.B2C_NON_WORKING, CustomerSegment.B2C_STUDENTS],
                email_type=EmailType.RETENTION,
                send_frequency_days=21,
                seasonality_multiplier=0.6,
                cross_channel_correlation=0.3
            ),
            
            CampaignConfig(
                name="Email_Marketing_Working_Holiday_Banking",
                start_date=datetime(2024, 7, 15),
                end_date=datetime(2024, 8, 15),
                stages=["Retention"],
                stage_weights={"Retention": 1.0},
                target_segments=[CustomerSegment.B2C_WORKING_AGE],
                email_type=EmailType.RETENTION,
                send_frequency_days=30,
                seasonality_multiplier=0.5,
                cross_channel_correlation=0.2
            ),
            
            # September 2024 - Back to School
            CampaignConfig(
                name="Email_Marketing_Autumn_Financial_Planning",
                start_date=datetime(2024, 9, 15),
                end_date=datetime(2024, 10, 15),
                stages=["Interest", "Consideration", "Conversion"],
                stage_weights={"Interest": 0.4, "Consideration": 0.35, "Conversion": 0.25},
                target_segments=[CustomerSegment.B2C_WORKING_AGE, CustomerSegment.B2B_SMALL],
                email_type=EmailType.NURTURING,
                send_frequency_days=10,
                seasonality_multiplier=1.0,
                cross_channel_correlation=0.7
            ),
            
            # October 2024 - Autumn Challenge
            CampaignConfig(
                name="Email_Marketing_Autumn_Savings_Challenge",
                start_date=datetime(2024, 10, 1),
                end_date=datetime(2024, 10, 31),
                stages=["Interest", "Consideration", "Conversion"],
                stage_weights={"Interest": 0.4, "Consideration": 0.35, "Conversion": 0.25},
                target_segments=[CustomerSegment.B2C_WORKING_AGE, CustomerSegment.B2C_STUDENTS],
                email_type=EmailType.PROMOTIONAL,
                send_frequency_days=7,
                seasonality_multiplier=1.0,
                cross_channel_correlation=0.6
            ),
            
            # November 2024 - Holiday Season
            CampaignConfig(
                name="Email_Marketing_Holiday_Spending_Smart",
                start_date=datetime(2024, 11, 1),
                end_date=datetime(2024, 12, 20),
                stages=["Interest", "Consideration", "Conversion"],
                stage_weights={"Interest": 0.3, "Consideration": 0.4, "Conversion": 0.3},
                target_segments=[CustomerSegment.B2C_WORKING_AGE, CustomerSegment.B2C_NON_WORKING],
                email_type=EmailType.NURTURING,
                send_frequency_days=14,
                seasonality_multiplier=1.2,
                cross_channel_correlation=0.8
            ),
            
            CampaignConfig(
                name="Email_Marketing_Black_Friday_Banking_Deals",
                start_date=datetime(2024, 11, 20),
                end_date=datetime(2024, 11, 30),
                stages=["Interest", "Conversion"],
                stage_weights={"Interest": 0.3, "Conversion": 0.7},
                target_segments=[CustomerSegment.B2C_WORKING_AGE, CustomerSegment.B2C_STUDENTS],
                email_type=EmailType.PROMOTIONAL,
                send_frequency_days=2,
                seasonality_multiplier=1.5,
                cross_channel_correlation=0.9
            ),
            
            # December 2024 - Year End
            CampaignConfig(
                name="Email_Marketing_Year_End_Financial_Reflection",
                start_date=datetime(2024, 12, 1),
                end_date=datetime(2024, 12, 20),
                stages=["Interest", "Consideration"],
                stage_weights={"Interest": 0.6, "Consideration": 0.4},
                target_segments=[CustomerSegment.B2C_WORKING_AGE, CustomerSegment.B2B_SMALL],
                email_type=EmailType.NEWSLETTER,
                send_frequency_days=7,
                seasonality_multiplier=0.8,
                cross_channel_correlation=0.5
            )
        ])
        
        # 2025 Campaigns (January - June)
        campaigns.extend([
            # January 2025
            CampaignConfig(
                name="Email_Marketing_New_Year_Financial_Resolutions",
                start_date=datetime(2025, 1, 2),
                end_date=datetime(2025, 1, 31),
                stages=["Interest", "Consideration"],
                stage_weights={"Interest": 0.6, "Consideration": 0.4},
                target_segments=[CustomerSegment.B2C_WORKING_AGE, CustomerSegment.B2B_SMALL],
                email_type=EmailType.NURTURING,
                send_frequency_days=7,
                seasonality_multiplier=0.95,
                cross_channel_correlation=0.65
            ),
            
            # February 2025
            CampaignConfig(
                name="Email_Marketing_Spring_Preparation_Banking",
                start_date=datetime(2025, 2, 20),
                end_date=datetime(2025, 3, 15),
                stages=["Interest", "Consideration"],
                stage_weights={"Interest": 0.5, "Consideration": 0.5},
                target_segments=[CustomerSegment.B2C_WORKING_AGE],
                email_type=EmailType.NURTURING,
                send_frequency_days=10,
                seasonality_multiplier=1.05,
                cross_channel_correlation=0.75
            ),
            
            # March 2025 - Peak Season
            CampaignConfig(
                name="Email_Marketing_Spring_Financial_Fresh_Start",
                start_date=datetime(2025, 3, 1),
                end_date=datetime(2025, 3, 31),
                stages=["Interest", "Consideration", "Conversion"],
                stage_weights={"Interest": 0.4, "Consideration": 0.4, "Conversion": 0.2},
                target_segments=[CustomerSegment.B2C_WORKING_AGE, CustomerSegment.B2C_STUDENTS],
                email_type=EmailType.NURTURING,
                send_frequency_days=7,
                seasonality_multiplier=1.35,
                cross_channel_correlation=0.85
            ),
            
            # April 2025
            CampaignConfig(
                name="Email_Marketing_Kings_Day_Banking_Freedom",
                start_date=datetime(2025, 4, 20),
                end_date=datetime(2025, 4, 30),
                stages=["Interest", "Conversion"],
                stage_weights={"Interest": 0.6, "Conversion": 0.4},
                target_segments=[CustomerSegment.B2C_WORKING_AGE, CustomerSegment.B2C_STUDENTS],
                email_type=EmailType.PROMOTIONAL,
                send_frequency_days=3,
                seasonality_multiplier=1.25,
                cross_channel_correlation=0.55
            ),
            
            # May 2025
            CampaignConfig(
                name="Email_Marketing_Early_Summer_Mobile_Banking",
                start_date=datetime(2025, 5, 10),
                end_date=datetime(2025, 6, 10),
                stages=["Interest", "Consideration", "Conversion"],
                stage_weights={"Interest": 0.4, "Consideration": 0.35, "Conversion": 0.25},
                target_segments=[CustomerSegment.B2C_WORKING_AGE, CustomerSegment.B2C_STUDENTS],
                email_type=EmailType.NURTURING,
                send_frequency_days=14,
                seasonality_multiplier=1.15,
                cross_channel_correlation=0.65
            ),
            
            # June 2025 - Festival Season
            CampaignConfig(
                name="Email_Marketing_Festival_Season_Banking",
                start_date=datetime(2025, 6, 1),
                end_date=datetime(2025, 6, 30),
                stages=["Interest", "Conversion"],
                stage_weights={"Interest": 0.7, "Conversion": 0.3},
                target_segments=[CustomerSegment.B2C_STUDENTS, CustomerSegment.B2C_NON_WORKING],
                email_type=EmailType.PROMOTIONAL,
                send_frequency_days=7,
                seasonality_multiplier=0.95,
                cross_channel_correlation=0.45
            )
        ])
        
        self._campaign_configs = campaigns
        return campaigns
    
    def _generate_subject_line(self, campaign_name: str, stage: str, email_type: EmailType) -> str:
        """Generate realistic email subject lines based on campaign and stage"""
        campaign_themes = {
            "New_Year_Financial_Resolutions": [
                "🎯 New Year, New Financial Goals",
                "Start 2024 with Smart Banking",
                "Your Financial Resolution Starts Here"
            ],
            "Spring_Financial_Fresh_Start": [
                "🌱 Spring into Better Banking",
                "Fresh Start, Fresh Banking",
                "Transform Your Financial Spring"
            ],
            "Kings_Day_Banking_Freedom": [
                "🧡 King's Day Banking Freedom!",
                "Celebrate Freedom with Bunq",
                "Orange You Ready for Better Banking?"
            ],
            "Holiday_Spending_Smart": [
                "🎄 Smart Holiday Spending Tips",
                "Your Holiday Budget Helper",
                "Make the Holidays Stress-Free"
            ],
            "Black_Friday_Banking_Deals": [
                "🔥 Black Friday Banking Deals!",
                "Limited Time: Banking Offers",
                "Don't Miss Out - Banking Deals!"
            ]
        }
        
        # Extract theme from campaign name
        theme_key = None
        for theme in campaign_themes.keys():
            if theme in campaign_name:
                theme_key = theme
                break
        
        if theme_key and campaign_themes[theme_key]:
            return random.choice(campaign_themes[theme_key])
        
        # Fallback generic subjects based on type and stage
        fallback_subjects = {
            EmailType.WELCOME_SERIES: ["Welcome to Better Banking", "Your Journey Starts Here"],
            EmailType.NURTURING: ["Banking Tips Inside", "Improve Your Financial Health"],
            EmailType.PROMOTIONAL: ["Special Offer Inside", "Limited Time Banking Deal"],
            EmailType.RETENTION: ["We Miss You", "Come Back to Better Banking"],
            EmailType.NEWSLETTER: ["Your Monthly Banking Update", "What's New at Bunq"]
        }
        
        return random.choice(fallback_subjects.get(email_type, ["Banking Update"]))
    
    def _generate_link_clicked(self, campaign_name: str, stage: str) -> Optional[str]:
        """Generate realistic clicked links based on campaign and stage"""
        base_url = "https://bunq.com"
        
        stage_links = {
            "Interest": [
                f"{base_url}/features?utm_source=email&utm_campaign=interest",
                f"{base_url}/blog/banking-tips?utm_source=email",
                f"{base_url}/about?utm_source=email"
            ],
            "Consideration": [
                f"{base_url}/pricing?utm_source=email&utm_campaign=consideration",
                f"{base_url}/compare?utm_source=email",
                f"{base_url}/testimonials?utm_source=email"
            ],
            "Conversion": [
                f"{base_url}/signup?utm_source=email&utm_campaign=conversion",
                f"{base_url}/get-started?utm_source=email",
                f"{base_url}/register?utm_source=email"
            ],
            "Retention": [
                f"{base_url}/login?utm_source=email&utm_campaign=retention",
                f"{base_url}/dashboard?utm_source=email",
                f"{base_url}/settings?utm_source=email"
            ]
        }
        
        return random.choice(stage_links.get(stage, [f"{base_url}?utm_source=email"]))
    
    def _calculate_performance_metrics(self, campaign: CampaignConfig, 
                                     customer: Dict, stage: str) -> Tuple[float, float]:
        """Calculate realistic open and click rates based on campaign, customer, and stage"""
        base_open_rate = self.base_open_rate
        base_click_rate = self.base_click_rate
        
        # Stage impact on performance
        stage_multipliers = {
            "Interest": {"open": 1.0, "click": 1.0},
            "Consideration": {"open": 1.2, "click": 1.4},
            "Conversion": {"open": 1.4, "click": 2.0},
            "Retention": {"open": 0.8, "click": 0.6}
        }
        
        # Customer segment impact
        segment_multipliers = {
            CustomerSegment.B2C_WORKING_AGE: {"open": 1.0, "click": 1.0},
            CustomerSegment.B2C_STUDENTS: {"open": 1.3, "click": 1.5},
            CustomerSegment.B2C_NON_WORKING: {"open": 1.1, "click": 0.9},
            CustomerSegment.B2B_SMALL: {"open": 0.9, "click": 1.1},
            CustomerSegment.B2B_MEDIUM: {"open": 0.8, "click": 1.2},
            CustomerSegment.B2B_LARGE: {"open": 0.7, "click": 1.3}
        }
        
        # Email type impact
        email_type_multipliers = {
            EmailType.WELCOME_SERIES: {"open": 1.8, "click": 2.2},
            EmailType.NURTURING: {"open": 1.1, "click": 1.3},
            EmailType.PROMOTIONAL: {"open": 0.9, "click": 1.8},
            EmailType.RETENTION: {"open": 0.7, "click": 0.8},
            EmailType.NEWSLETTER: {"open": 1.0, "click": 0.9},
            EmailType.TRANSACTIONAL: {"open": 2.5, "click": 3.0}
        }
        
        stage_mult = stage_multipliers.get(stage, {"open": 1.0, "click": 1.0})
        segment_mult = segment_multipliers[customer['segment']]
        email_type_mult = email_type_multipliers[campaign.email_type]
        
        # Calculate final metrics
        final_open_rate = (base_open_rate * stage_mult["open"] * segment_mult["open"] * 
                          email_type_mult["open"] * campaign.seasonality_multiplier * 
                          customer['engagement_score'])
        
        final_click_rate = (base_click_rate * stage_mult["click"] * segment_mult["click"] * 
                           email_type_mult["click"] * campaign.seasonality_multiplier * 
                           customer['engagement_score'])
        
        return min(final_open_rate, 1.0), min(final_click_rate, 1.0)
    
    def _generate_touchpoints_for_campaign(self, campaign: CampaignConfig) -> List[EmailMarketingRecord]:
        """Generate all touchpoints for a specific campaign"""
        records = []
        campaign_days = (campaign.end_date - campaign.start_date).days + 1
        
        # Determine customer participation (email has high reach)
        total_campaign_customers = int(len(self.customer_pool) * 0.25 * campaign.seasonality_multiplier)
        participating_customers = random.sample(self.customer_pool, min(total_campaign_customers, len(self.customer_pool)))
        
        # Filter by subscription status (only subscribed customers get emails)
        participating_customers = [c for c in participating_customers if c['subscription_status'] == SubscriptionStatus.SUBSCRIBED]
        
        for stage, weight in campaign.stage_weights.items():
            stage_customers = random.sample(participating_customers, 
                                          int(len(participating_customers) * weight))
            
            for customer in stage_customers:
                # Skip if customer segment not in target
                if customer['segment'] not in campaign.target_segments:
                    continue
                    
                # Calculate number of emails based on send frequency
                email_count = max(1, campaign_days // campaign.send_frequency_days)
                
                for email_num in range(email_count):
                    # Calculate send date based on frequency
                    days_offset = email_num * campaign.send_frequency_days
                    if days_offset >= campaign_days:
                        break
                        
                    send_date = campaign.start_date + timedelta(days=days_offset)
                    
                    # Add realistic send time (business hours for B2B, mixed for B2C)
                    if customer['segment'].name.startswith("B2B"):
                        hour = random.choices(range(8, 18), weights=[1]*10)[0]
                    else:
                        hour = random.choices(range(24), weights=[0.5]*6 + [1.5]*12 + [1]*6)[0]
                    
                    send_timestamp = send_date.replace(
                        hour=hour,
                        minute=random.randint(0, 59),
                        second=random.randint(0, 59)
                    )
                    
                    # Calculate performance metrics
                    open_rate, click_rate = self._calculate_performance_metrics(campaign, customer, stage)
                    
                    # Determine if email was opened and clicked
                    is_opened = random.random() < open_rate
                    is_clicked = is_opened and random.random() < (click_rate / open_rate) if open_rate > 0 else False
                    
                    open_timestamp = None
                    click_timestamp = None
                    link_clicked = None
                    
                    if is_opened:
                        # Open typically happens within 24 hours
                        open_delay = random.randint(1, 1440)  # 1 minute to 24 hours
                        open_timestamp = (send_timestamp + timedelta(minutes=open_delay)).isoformat() + "Z"
                        
                        if is_clicked:
                            # Click typically happens within 1 hour of open
                            click_delay = random.randint(1, 60)  # 1 to 60 minutes after open
                            click_timestamp = (datetime.fromisoformat(open_timestamp.replace('Z', '')) + 
                                             timedelta(minutes=click_delay)).isoformat() + "Z"
                            link_clicked = self._generate_link_clicked(campaign.name, stage)
                    
                    # Generate email ID and list ID
                    email_id = f"em_{random.randint(100000000, 999999999)}"
                    list_id = f"list_{campaign.email_type.value}_{customer['segment'].value.lower()}"
                    
                    # Create the record
                    record = EmailMarketingRecord(
                        email_id=email_id,
                        campaign_id=f"camp_{hash(campaign.name) % 100000000}",
                        campaign_name=f"{campaign.name}_{stage}",
                        email_address=customer['email_address'],
                        send_timestamp=send_timestamp.isoformat() + "Z",
                        open_timestamp=open_timestamp,
                        click_timestamp=click_timestamp,
                        device_type=customer['preferred_device'].value,
                        location=f"{customer['location']}, Netherlands",
                        email_client=customer['email_client'].value,
                        link_clicked=link_clicked,
                        user_id=customer['user_id'],
                        list_id=list_id,
                        engagement_score=customer['engagement_score'],
                        subscription_status=customer['subscription_status'].value,
                        email_type=campaign.email_type.value,
                        subject_line=self._generate_subject_line(campaign.name, stage, campaign.email_type),
                        customer_id=customer['customer_id'],
                        segment=customer['segment'].value
                    )
                    
                    records.append(record)
        
        return records
    
    def generate_filtered_data(self, request: DataRequest) -> List[Dict]:
        """Generate filtered data based on API request parameters"""
        campaigns = self.get_campaign_configs()
        
        # Apply filters
        if request.start_date:
            start_filter = datetime.fromisoformat(request.start_date.replace('Z', ''))
            campaigns = [c for c in campaigns if c.end_date >= start_filter]
        
        if request.end_date:
            end_filter = datetime.fromisoformat(request.end_date.replace('Z', ''))
            campaigns = [c for c in campaigns if c.start_date <= end_filter]
        
        if request.campaign_names:
            campaigns = [c for c in campaigns if c.name in request.campaign_names]
        
        if request.email_types:
            campaigns = [c for c in campaigns if c.email_type.value in request.email_types]
        
        all_records = []
        
        for campaign in campaigns:
            campaign_records = self._generate_touchpoints_for_campaign(campaign)
            
            # Apply filters
            if request.customer_segments:
                campaign_records = [r for r in campaign_records if r.segment in request.customer_segments]
            
            if request.subscription_status:
                campaign_records = [r for r in campaign_records if r.subscription_status in request.subscription_status]
            
            if request.engagement_score_min is not None:
                campaign_records = [r for r in campaign_records if r.engagement_score >= request.engagement_score_min]
            
            all_records.extend(campaign_records)
            
            # Respect max_records limit
            if len(all_records) >= request.max_records:
                all_records = all_records[:request.max_records]
                break
        
        return [asdict(record) for record in all_records]

# Initialize the generator instance
generator = EmailMarketingGenerator()

# Create FastAPI app
app = FastAPI(
    title="Email Marketing Synthetic Data API",
    description="Dutch market Email Marketing synthetic data generator for omnichannel attribution",
    version="1.0.0"
)

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "email-marketing-generator", "version": "1.0.0"}

@app.get("/campaigns")
async def get_campaigns():
    """Get list of all available campaigns"""
    campaigns = generator.get_campaign_configs()
    
    campaign_list = []
    for campaign in campaigns:
        campaign_list.append({
            "name": campaign.name,
            "start_date": campaign.start_date.isoformat(),
            "end_date": campaign.end_date.isoformat(),
            "stages": campaign.stages,
            "target_segments": [seg.value for seg in campaign.target_segments],
            "email_type": campaign.email_type.value,
            "send_frequency_days": campaign.send_frequency_days,
            "cross_channel_correlation": campaign.cross_channel_correlation
        })
    
    return {
        "total_campaigns": len(campaign_list),
        "campaigns": campaign_list
    }

@app.get("/campaigns/{campaign_name}")
async def get_campaign_data(campaign_name: str, max_records: int = Query(1000, description="Maximum records to return")):
    """Get data for a specific campaign"""
    try:
        request = DataRequest(campaign_names=[campaign_name], max_records=max_records)
        data = generator.generate_filtered_data(request)
        
        if not data:
            raise HTTPException(status_code=404, detail=f"Campaign '{campaign_name}' not found or no data available")
        
        return {
            "campaign_name": campaign_name,
            "total_records": len(data),
            "data": data,
            "metadata": {
                "generated_at": datetime.now().isoformat(),
                "gdpr_compliant": True,
                "market": "Netherlands",
                "channel": "Email Marketing"
            }
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating data: {str(e)}")

@app.post("/data")
async def get_filtered_data(request: DataRequest):
    """Get filtered touchpoint data based on request parameters"""
    try:
        data = generator.generate_filtered_data(request)
        
        return {
            "total_records": len(data),
            "filters_applied": {
                "start_date": request.start_date,
                "end_date": request.end_date,
                "campaign_names": request.campaign_names,
                "customer_segments": request.customer_segments,
                "email_types": request.email_types,
                "subscription_status": request.subscription_status,
                "engagement_score_min": request.engagement_score_min,
                "max_records": request.max_records
            },
            "data": data,
            "metadata": {
                "generated_at": datetime.now().isoformat(),
                "gdpr_compliant": True,
                "market": "Netherlands",
                "channel": "Email Marketing"
            }
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating data: {str(e)}")

@app.get("/data")
async def get_recent_data(
    days: int = Query(30, description="Number of recent days"),
    max_records: int = Query(1000, description="Maximum records to return"),
    segments: Optional[List[str]] = Query(None, description="Customer segments to include"),
    email_types: Optional[List[str]] = Query(None, description="Email types to include"),
    engagement_min: Optional[float] = Query(None, description="Minimum engagement score")
):
    """Get recent touchpoint data (convenient endpoint for N8N)"""
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    request = DataRequest(
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
        customer_segments=segments,
        email_types=email_types,
        engagement_score_min=engagement_min,
        max_records=max_records
    )
    
    return await get_filtered_data(request)

@app.get("/email-types")
async def get_available_email_types():
    """Get list of available email types"""
    return {
        "email_types": [email_type.value for email_type in EmailType],
        "email_type_descriptions": {
            "welcome_series": "Welcome emails for new customers",
            "nurturing": "Educational and relationship-building emails",
            "promotional": "Offers and promotional campaigns",
            "retention": "Re-engagement campaigns for inactive customers",
            "newsletter": "Regular updates and news",
            "transactional": "Account and transaction notifications"
        }
    }

@app.get("/customer-identifiers")
async def get_customer_identifiers(limit: int = Query(100, description="Number of customer identifiers to return")):
    """Get customer identifiers for cross-channel matching (useful for testing)"""
    customer_sample = random.sample(generator.customer_pool, min(limit, len(generator.customer_pool)))
    
    identifiers = []
    for customer in customer_sample:
        identifiers.append({
            "customer_id": customer['customer_id'],
            "email_address": customer['email_address'],
            "user_id": customer['user_id'],
            "segment": customer['segment'].value,
            "cross_channel_probability": customer['cross_channel_probability']
        })
    
    return {
        "total_identifiers": len(identifiers),
        "identifiers": identifiers,
        "note": "These identifiers can be used for cross-channel attribution testing"
    }

if __name__ == "__main__":
    print("Starting Email Marketing Synthetic Data API...")
    print("API Documentation: http://localhost:8002/docs")
    print("Health Check: http://localhost:8002/health")
    uvicorn.run(app, host="0.0.0.0", port=8002)