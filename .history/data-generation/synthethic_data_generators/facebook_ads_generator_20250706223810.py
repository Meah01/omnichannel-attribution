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

class Placement(Enum):
    FACEBOOK_FEED = "facebook_feed"
    FACEBOOK_STORIES = "facebook_stories"
    FACEBOOK_RIGHT_COLUMN = "facebook_right_column"
    INSTAGRAM_FEED = "instagram_feed"
    INSTAGRAM_STORIES = "instagram_stories"
    MESSENGER = "messenger"
    AUDIENCE_NETWORK = "audience_network"

class AgeRange(Enum):
    AGE_18_24 = "18-24"
    AGE_25_34 = "25-34"
    AGE_35_44 = "35-44"
    AGE_45_54 = "45-54"
    AGE_55_64 = "55-64"
    AGE_65_PLUS = "65+"

class Gender(Enum):
    ALL = "ALL"
    MALE = "MALE"
    FEMALE = "FEMALE"

class CustomerSegment(Enum):
    B2C_WORKING_AGE = "B2C_WORKING_AGE"
    B2C_STUDENTS = "B2C_STUDENTS"
    B2C_NON_WORKING = "B2C_NON_WORKING"
    B2B_SMALL = "B2B_SMALL"
    B2B_MEDIUM = "B2B_MEDIUM"
    B2B_LARGE = "B2B_LARGE"

@dataclass
class FacebookAdsRecord:
    """Facebook Ads touchpoint record structure"""
    fbclid: str
    campaign_id: str
    campaign_name: str
    adset_id: str
    adset_name: str
    ad_id: str
    ad_name: str
    click_timestamp: str
    impression_timestamp: str
    device_type: str
    placement: str
    age_range: str
    gender: str
    location: str
    cost_micros: int
    impressions: int
    clicks: int
    estimated_audience_overlap: float
    customer_id: str
    segment: str

@dataclass
class CampaignConfig:
    """Campaign configuration structure"""
    name: str
    start_date: datetime
    end_date: datetime
    stages: List[str]
    stage_weights: Dict[str, float]
    target_segments: List[CustomerSegment]
    budget_euros: int
    seasonality_multiplier: float
    primary_placement: Placement
    target_age_ranges: List[AgeRange]

class DataRequest(BaseModel):
    """API request model for data generation"""
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    campaign_names: Optional[List[str]] = None
    customer_segments: Optional[List[str]] = None
    placements: Optional[List[str]] = None
    age_ranges: Optional[List[str]] = None
    max_records: Optional[int] = 10000

class FacebookAdsGenerator:
    """
    Facebook Ads synthetic data API service for Dutch market
    Generates realistic data (Jan 2024 - June 2025) with Facebook-specific targeting
    """
    
    def __init__(self):
        self.base_ctr = 0.018  # 1.8% CTR (lower than Google)
        self.base_cpc_euros = 1.85  # Lower CPC than Google
        self.total_customers = 200000
        self.facebook_ads_penetration = 0.73  # Higher penetration for social
        
        # Dutch locations for realistic targeting
        self.dutch_locations = [
            "Amsterdam", "Rotterdam", "The Hague", "Utrecht", "Eindhoven",
            "Tilburg", "Groningen", "Almere", "Breda", "Nijmegen"
        ]
        
        # Device distribution for Facebook (higher mobile usage)
        self.device_distribution = {
            DeviceType.MOBILE: 0.85,
            DeviceType.DESKTOP: 0.12,
            DeviceType.TABLET: 0.03
        }
        
        # Placement distribution
        self.placement_distribution = {
            Placement.FACEBOOK_FEED: 0.40,
            Placement.INSTAGRAM_FEED: 0.25,
            Placement.FACEBOOK_STORIES: 0.15,
            Placement.INSTAGRAM_STORIES: 0.12,
            Placement.MESSENGER: 0.05,
            Placement.AUDIENCE_NETWORK: 0.03
        }
        
        # Age distribution for Dutch market
        self.age_distribution = {
            AgeRange.AGE_18_24: 0.22,  # Students + young workers
            AgeRange.AGE_25_34: 0.28,  # Primary working age
            AgeRange.AGE_35_44: 0.24,  # Peak earning years
            AgeRange.AGE_45_54: 0.16,  # Established professionals
            AgeRange.AGE_55_64: 0.08,  # Pre-retirement
            AgeRange.AGE_65_PLUS: 0.02  # Seniors
        }
        
        # Customer segment distribution
        self.segment_distribution = {
            CustomerSegment.B2C_WORKING_AGE: 0.48,  # Higher for social
            CustomerSegment.B2C_STUDENTS: 0.25,     # Higher student engagement
            CustomerSegment.B2C_NON_WORKING: 0.17,
            CustomerSegment.B2B_SMALL: 0.07,        # Lower B2B presence
            CustomerSegment.B2B_MEDIUM: 0.02,
            CustomerSegment.B2B_LARGE: 0.01
        }
        
        self.customer_pool = self._generate_customer_pool()
        self._campaign_configs = None
        
    def _generate_customer_pool(self) -> List[Dict]:
        """Generate realistic customer pool with Facebook-specific demographics"""
        customers = []
        active_customers = int(self.total_customers * self.facebook_ads_penetration)
        
        for i in range(active_customers):
            segment = self._weighted_choice(list(self.segment_distribution.keys()), 
                                          list(self.segment_distribution.values()))
            
            age_range = self._weighted_choice(list(self.age_distribution.keys()),
                                            list(self.age_distribution.values()))
            
            # Gender distribution (slightly skewed female for banking/fintech)
            gender = random.choices([Gender.FEMALE, Gender.MALE], weights=[0.52, 0.48])[0]
            
            customer = {
                'customer_id': f"cust_{str(uuid.uuid4())[:8]}",
                'segment': segment,
                'preferred_device': self._weighted_choice(list(self.device_distribution.keys()),
                                                       list(self.device_distribution.values())),
                'preferred_placement': self._weighted_choice(list(self.placement_distribution.keys()),
                                                           list(self.placement_distribution.values())),
                'age_range': age_range,
                'gender': gender,
                'location': random.choice(self.dutch_locations),
                'engagement_score': random.uniform(0.4, 1.0),  # Higher baseline engagement
                'seasonal_sensitivity': random.uniform(0.5, 1.5),
                'cross_channel_probability': random.uniform(0.60, 0.85)  # Likelihood of Google Ads overlap
            }
            customers.append(customer)
            
        return customers
    
    def _weighted_choice(self, choices: List, weights: List) -> Any:
        """Select random choice based on weights"""
        return random.choices(choices, weights=weights)[0]
    
    def get_campaign_configs(self) -> List[CampaignConfig]:
        """Define all Facebook Ads campaigns based on Dutch market calendar"""
        if self._campaign_configs is not None:
            return self._campaign_configs
            
        campaigns = []
        
        # 2024 Campaigns
        campaigns.extend([
            # February 2024 - Carnival Campaign
            CampaignConfig(
                name="Facebook_Ads_Carnival_Payment_Freedom",
                start_date=datetime(2024, 2, 10),
                end_date=datetime(2024, 2, 25),
                stages=["Awareness", "Interest"],
                stage_weights={"Awareness": 0.6, "Interest": 0.4},
                target_segments=[CustomerSegment.B2C_STUDENTS, CustomerSegment.B2C_NON_WORKING],
                budget_euros=85000,
                seasonality_multiplier=1.1,
                primary_placement=Placement.INSTAGRAM_STORIES,
                target_age_ranges=[AgeRange.AGE_18_24, AgeRange.AGE_25_34]
            ),
            
            CampaignConfig(
                name="Facebook_Ads_Spring_Preparation_Banking",
                start_date=datetime(2024, 2, 20),
                end_date=datetime(2024, 3, 15),
                stages=["Awareness", "Interest", "Consideration"],
                stage_weights={"Awareness": 0.4, "Interest": 0.35, "Consideration": 0.25},
                target_segments=[CustomerSegment.B2C_WORKING_AGE, CustomerSegment.B2C_STUDENTS],
                budget_euros=140000,
                seasonality_multiplier=0.95,
                primary_placement=Placement.FACEBOOK_FEED,
                target_age_ranges=[AgeRange.AGE_25_34, AgeRange.AGE_35_44]
            ),
            
            # March 2024 - Peak Season
            CampaignConfig(
                name="Facebook_Ads_Spring_Financial_Fresh_Start",
                start_date=datetime(2024, 3, 1),
                end_date=datetime(2024, 3, 31),
                stages=["Awareness", "Interest", "Consideration", "Conversion"],
                stage_weights={"Awareness": 0.35, "Interest": 0.30, "Consideration": 0.25, "Conversion": 0.10},
                target_segments=[CustomerSegment.B2C_WORKING_AGE, CustomerSegment.B2C_STUDENTS],
                budget_euros=220000,
                seasonality_multiplier=1.35,
                primary_placement=Placement.FACEBOOK_FEED,
                target_age_ranges=[AgeRange.AGE_18_24, AgeRange.AGE_25_34, AgeRange.AGE_35_44]
            ),
            
            # April 2024 - King's Day Focus
            CampaignConfig(
                name="Facebook_Ads_Kings_Day_Banking_Freedom",
                start_date=datetime(2024, 4, 15),
                end_date=datetime(2024, 5, 5),
                stages=["Awareness", "Conversion"],
                stage_weights={"Awareness": 0.7, "Conversion": 0.3},
                target_segments=[CustomerSegment.B2C_WORKING_AGE, CustomerSegment.B2C_STUDENTS, CustomerSegment.B2C_NON_WORKING],
                budget_euros=160000,
                seasonality_multiplier=1.25,
                primary_placement=Placement.INSTAGRAM_STORIES,
                target_age_ranges=[AgeRange.AGE_18_24, AgeRange.AGE_25_34, AgeRange.AGE_35_44]
            ),
            
            # May 2024 - Early Summer
            CampaignConfig(
                name="Facebook_Ads_Early_Summer_Mobile_Banking",
                start_date=datetime(2024, 5, 10),
                end_date=datetime(2024, 6, 10),
                stages=["Interest", "Consideration", "Conversion"],
                stage_weights={"Interest": 0.4, "Consideration": 0.35, "Conversion": 0.25},
                target_segments=[CustomerSegment.B2C_WORKING_AGE, CustomerSegment.B2C_STUDENTS],
                budget_euros=130000,
                seasonality_multiplier=1.15,
                primary_placement=Placement.INSTAGRAM_FEED,
                target_age_ranges=[AgeRange.AGE_18_24, AgeRange.AGE_25_34]
            ),
            
            # July-August 2024 - Summer Low (Limited campaigns)
            CampaignConfig(
                name="Facebook_Ads_Summer_Travel_Banking",
                start_date=datetime(2024, 7, 1),
                end_date=datetime(2024, 8, 31),
                stages=["Interest", "Conversion"],
                stage_weights={"Interest": 0.6, "Conversion": 0.4},
                target_segments=[CustomerSegment.B2C_STUDENTS, CustomerSegment.B2C_NON_WORKING],
                budget_euros=70000,
                seasonality_multiplier=0.6,
                primary_placement=Placement.INSTAGRAM_STORIES,
                target_age_ranges=[AgeRange.AGE_18_24, AgeRange.AGE_55_64]
            ),
            
            # October 2024 - Autumn Campaign
            CampaignConfig(
                name="Facebook_Ads_Autumn_Savings_Challenge",
                start_date=datetime(2024, 10, 1),
                end_date=datetime(2024, 10, 31),
                stages=["Interest", "Consideration", "Conversion"],
                stage_weights={"Interest": 0.4, "Consideration": 0.35, "Conversion": 0.25},
                target_segments=[CustomerSegment.B2C_WORKING_AGE, CustomerSegment.B2C_STUDENTS],
                budget_euros=150000,
                seasonality_multiplier=1.0,
                primary_placement=Placement.FACEBOOK_FEED,
                target_age_ranges=[AgeRange.AGE_25_34, AgeRange.AGE_35_44]
            ),
            
            # November-December 2024 - Holiday Season
            CampaignConfig(
                name="Facebook_Ads_Holiday_Spending_Smart",
                start_date=datetime(2024, 11, 1),
                end_date=datetime(2024, 12, 20),
                stages=["Awareness", "Interest", "Consideration", "Conversion"],
                stage_weights={"Awareness": 0.3, "Interest": 0.3, "Consideration": 0.25, "Conversion": 0.15},
                target_segments=[CustomerSegment.B2C_WORKING_AGE, CustomerSegment.B2C_NON_WORKING],
                budget_euros=180000,
                seasonality_multiplier=1.2,
                primary_placement=Placement.FACEBOOK_FEED,
                target_age_ranges=[AgeRange.AGE_25_34, AgeRange.AGE_35_44, AgeRange.AGE_45_54]
            ),
            
            CampaignConfig(
                name="Facebook_Ads_Black_Friday_Banking_Deals",
                start_date=datetime(2024, 11, 20),
                end_date=datetime(2024, 11, 30),
                stages=["Interest", "Conversion"],
                stage_weights={"Interest": 0.4, "Conversion": 0.6},
                target_segments=[CustomerSegment.B2C_WORKING_AGE, CustomerSegment.B2C_STUDENTS],
                budget_euros=95000,
                seasonality_multiplier=1.3,
                primary_placement=Placement.INSTAGRAM_STORIES,
                target_age_ranges=[AgeRange.AGE_18_24, AgeRange.AGE_25_34, AgeRange.AGE_35_44]
            )
        ])
        
        # 2025 Campaigns (January - June)
        campaigns.extend([
            # February 2025 - Carnival
            CampaignConfig(
                name="Facebook_Ads_Carnival_Payment_Freedom",
                start_date=datetime(2025, 2, 10),
                end_date=datetime(2025, 2, 25),
                stages=["Awareness", "Interest"],
                stage_weights={"Awareness": 0.6, "Interest": 0.4},
                target_segments=[CustomerSegment.B2C_STUDENTS, CustomerSegment.B2C_NON_WORKING],
                budget_euros=90000,
                seasonality_multiplier=1.15,
                primary_placement=Placement.INSTAGRAM_STORIES,
                target_age_ranges=[AgeRange.AGE_18_24, AgeRange.AGE_25_34]
            ),
            
            CampaignConfig(
                name="Facebook_Ads_Spring_Preparation_Banking",
                start_date=datetime(2025, 2, 20),
                end_date=datetime(2025, 3, 15),
                stages=["Awareness", "Interest", "Consideration"],
                stage_weights={"Awareness": 0.4, "Interest": 0.35, "Consideration": 0.25},
                target_segments=[CustomerSegment.B2C_WORKING_AGE, CustomerSegment.B2C_STUDENTS],
                budget_euros=145000,
                seasonality_multiplier=1.0,
                primary_placement=Placement.FACEBOOK_FEED,
                target_age_ranges=[AgeRange.AGE_25_34, AgeRange.AGE_35_44]
            ),
            
            # March 2025 - Peak Season
            CampaignConfig(
                name="Facebook_Ads_Spring_Financial_Fresh_Start",
                start_date=datetime(2025, 3, 1),
                end_date=datetime(2025, 3, 31),
                stages=["Awareness", "Interest", "Consideration", "Conversion"],
                stage_weights={"Awareness": 0.35, "Interest": 0.30, "Consideration": 0.25, "Conversion": 0.10},
                target_segments=[CustomerSegment.B2C_WORKING_AGE, CustomerSegment.B2C_STUDENTS],
                budget_euros=235000,
                seasonality_multiplier=1.4,
                primary_placement=Placement.FACEBOOK_FEED,
                target_age_ranges=[AgeRange.AGE_18_24, AgeRange.AGE_25_34, AgeRange.AGE_35_44]
            ),
            
            # April 2025 - King's Day
            CampaignConfig(
                name="Facebook_Ads_Kings_Day_Banking_Freedom",
                start_date=datetime(2025, 4, 15),
                end_date=datetime(2025, 5, 5),
                stages=["Awareness", "Conversion"],
                stage_weights={"Awareness": 0.7, "Conversion": 0.3},
                target_segments=[CustomerSegment.B2C_WORKING_AGE, CustomerSegment.B2C_STUDENTS, CustomerSegment.B2C_NON_WORKING],
                budget_euros=170000,
                seasonality_multiplier=1.3,
                primary_placement=Placement.INSTAGRAM_STORIES,
                target_age_ranges=[AgeRange.AGE_18_24, AgeRange.AGE_25_34, AgeRange.AGE_35_44]
            ),
            
            # May 2025
            CampaignConfig(
                name="Facebook_Ads_Early_Summer_Mobile_Banking",
                start_date=datetime(2025, 5, 10),
                end_date=datetime(2025, 6, 10),
                stages=["Interest", "Consideration", "Conversion"],
                stage_weights={"Interest": 0.4, "Consideration": 0.35, "Conversion": 0.25},
                target_segments=[CustomerSegment.B2C_WORKING_AGE, CustomerSegment.B2C_STUDENTS],
                budget_euros=140000,
                seasonality_multiplier=1.2,
                primary_placement=Placement.INSTAGRAM_FEED,
                target_age_ranges=[AgeRange.AGE_18_24, AgeRange.AGE_25_34]
            ),
            
            # June 2025 - Festival Season
            CampaignConfig(
                name="Facebook_Ads_Festival_Season_Banking",
                start_date=datetime(2025, 6, 1),
                end_date=datetime(2025, 6, 30),
                stages=["Awareness", "Interest", "Conversion"],
                stage_weights={"Awareness": 0.4, "Interest": 0.35, "Conversion": 0.25},
                target_segments=[CustomerSegment.B2C_STUDENTS, CustomerSegment.B2C_NON_WORKING],
                budget_euros=110000,
                seasonality_multiplier=0.95,
                primary_placement=Placement.INSTAGRAM_STORIES,
                target_age_ranges=[AgeRange.AGE_18_24, AgeRange.AGE_25_34]
            )
        ])
        
        self._campaign_configs = campaigns
        return campaigns
    
    def _generate_ad_creative_name(self, stage: str, segment: CustomerSegment, placement: Placement) -> str:
        """Generate realistic ad creative names based on stage and targeting"""
        creative_types = {
            "Awareness": ["Video", "Carousel", "Image"],
            "Interest": ["Carousel", "Collection", "Video"],
            "Consideration": ["Lead_Form", "Carousel", "Video"],
            "Conversion": ["Lead_Form", "Dynamic", "Carousel"]
        }
        
        creative_themes = {
            CustomerSegment.B2C_STUDENTS: ["Student_Life", "Mobile_First", "Easy_Banking"],
            CustomerSegment.B2C_WORKING_AGE: ["Professional", "Time_Saving", "Growth"],
            CustomerSegment.B2C_NON_WORKING: ["Simple", "Security", "Family"],
            CustomerSegment.B2B_SMALL: ["Business_Growth", "Efficiency", "Professional"],
            CustomerSegment.B2B_MEDIUM: ["Enterprise", "Scale", "Partnership"],
            CustomerSegment.B2B_LARGE: ["Corporate", "Advanced", "Solutions"]
        }
        
        placement_suffix = {
            Placement.FACEBOOK_FEED: "Feed",
            Placement.INSTAGRAM_FEED: "IG_Feed",
            Placement.FACEBOOK_STORIES: "Stories",
            Placement.INSTAGRAM_STORIES: "IG_Stories",
            Placement.MESSENGER: "Messenger",
            Placement.AUDIENCE_NETWORK: "AN"
        }
        
        creative_type = random.choice(creative_types[stage])
        theme = random.choice(creative_themes[segment])
        placement_tag = placement_suffix[placement]
        
        return f"{creative_type}_{theme}_{placement_tag}"
    
    def _calculate_performance_metrics(self, campaign: CampaignConfig, 
                                     customer: Dict, stage: str, placement: Placement) -> Tuple[float, int]:
        """Calculate realistic CTR and CPC based on campaign, customer, stage, and placement"""
        base_ctr = self.base_ctr
        base_cpc = self.base_cpc_euros
        
        # Stage impact on performance
        stage_multipliers = {
            "Awareness": {"ctr": 0.9, "cpc": 0.8},
            "Interest": {"ctr": 1.0, "cpc": 1.0},
            "Consideration": {"ctr": 1.3, "cpc": 1.2},
            "Conversion": {"ctr": 1.8, "cpc": 1.4}
        }
        
        # Customer segment impact
        segment_multipliers = {
            CustomerSegment.B2C_WORKING_AGE: {"ctr": 1.0, "cpc": 1.0},
            CustomerSegment.B2C_STUDENTS: {"ctr": 1.4, "cpc": 0.6},
            CustomerSegment.B2C_NON_WORKING: {"ctr": 1.1, "cpc": 0.7},
            CustomerSegment.B2B_SMALL: {"ctr": 0.7, "cpc": 1.8},
            CustomerSegment.B2B_MEDIUM: {"ctr": 0.5, "cpc": 2.5},
            CustomerSegment.B2B_LARGE: {"ctr": 0.4, "cpc": 3.2}
        }
        
        # Device impact
        device_multipliers = {
            DeviceType.MOBILE: {"ctr": 1.3, "cpc": 0.85},
            DeviceType.DESKTOP: {"ctr": 1.0, "cpc": 1.0},
            DeviceType.TABLET: {"ctr": 0.9, "cpc": 1.1}
        }
        
        # Placement impact
        placement_multipliers = {
            Placement.FACEBOOK_FEED: {"ctr": 1.0, "cpc": 1.0},
            Placement.INSTAGRAM_FEED: {"ctr": 1.2, "cpc": 1.1},
            Placement.FACEBOOK_STORIES: {"ctr": 1.4, "cpc": 0.9},
            Placement.INSTAGRAM_STORIES: {"ctr": 1.6, "cpc": 0.85},
            Placement.MESSENGER: {"ctr": 0.8, "cpc": 1.3},
            Placement.AUDIENCE_NETWORK: {"ctr": 0.6, "cpc": 0.7}
        }
        
        # Age range impact
        age_multipliers = {
            AgeRange.AGE_18_24: {"ctr": 1.5, "cpc": 0.7},
            AgeRange.AGE_25_34: {"ctr": 1.2, "cpc": 1.0},
            AgeRange.AGE_35_44: {"ctr": 1.0, "cpc": 1.1},
            AgeRange.AGE_45_54: {"ctr": 0.8, "cpc": 1.2},
            AgeRange.AGE_55_64: {"ctr": 0.6, "cpc": 1.4},
            AgeRange.AGE_65_PLUS: {"ctr": 0.4, "cpc": 1.6}
        }
        
        stage_mult = stage_multipliers[stage]
        segment_mult = segment_multipliers[customer['segment']]
        device_mult = device_multipliers[customer['preferred_device']]
        placement_mult = placement_multipliers[placement]
        age_mult = age_multipliers[customer['age_range']]
        
        # Calculate final metrics
        final_ctr = (base_ctr * stage_mult["ctr"] * segment_mult["ctr"] * 
                    device_mult["ctr"] * placement_mult["ctr"] * age_mult["ctr"] *
                    campaign.seasonality_multiplier * customer['engagement_score'])
        
        final_cpc = (base_cpc * stage_mult["cpc"] * segment_mult["cpc"] * 
                    device_mult["cpc"] * placement_mult["cpc"] * age_mult["cpc"] *
                    campaign.seasonality_multiplier)
        
        return final_ctr, int(final_cpc * 1000000)  # Convert to micros
    
    def _generate_touchpoints_for_campaign(self, campaign: CampaignConfig) -> List[FacebookAdsRecord]:
        """Generate all touchpoints for a specific campaign"""
        records = []
        campaign_days = (campaign.end_date - campaign.start_date).days + 1
        daily_budget = campaign.budget_euros / campaign_days
        
        # Determine customer participation (higher social engagement)
        total_campaign_customers = int(len(self.customer_pool) * 0.18 * campaign.seasonality_multiplier)
        participating_customers = random.sample(self.customer_pool, min(total_campaign_customers, len(self.customer_pool)))
        
        for stage, weight in campaign.stage_weights.items():
            stage_budget = daily_budget * weight
            stage_customers = random.sample(participating_customers, 
                                          int(len(participating_customers) * weight))
            
            for customer in stage_customers:
                # Skip if customer segment not in target
                if customer['segment'] not in campaign.target_segments:
                    continue
                
                # Skip if age range not in target
                if customer['age_range'] not in campaign.target_age_ranges:
                    continue
                    
                # Generate touchpoints for this customer-stage combination
                touchpoint_count = random.randint(1, 7)  # Higher frequency for social
                
                for _ in range(touchpoint_count):
                    # Random date within campaign period
                    days_offset = random.randint(0, campaign_days - 1)
                    click_date = campaign.start_date + timedelta(days=days_offset)
                    
                    # Add realistic time (evening hours higher for social)
                    if customer['segment'].name.startswith("B2B"):
                        hour = random.choices(range(24), weights=[0.3]*8 + [1.5]*10 + [0.8]*6)[0]
                    else:
                        hour = random.choices(range(24), weights=[0.5]*6 + [1]*12 + [2.5]*6)[0]
                    
                    click_timestamp = click_date.replace(
                        hour=hour,
                        minute=random.randint(0, 59),
                        second=random.randint(0, 59)
                    )
                    
                    impression_timestamp = click_timestamp - timedelta(seconds=random.randint(1, 15))
                    
                    # Determine placement (prefer customer's favorite)
                    if random.random() < 0.7:
                        placement = customer['preferred_placement']
                    else:
                        placement = campaign.primary_placement
                    
                    # Calculate performance metrics
                    ctr, cpc_micros = self._calculate_performance_metrics(campaign, customer, stage, placement)
                    
                    # Generate impressions based on CTR
                    clicks = 1  # This record represents a click
                    impressions = max(1, int(clicks / max(ctr, 0.001)))
                    
                    # Create the record
                    record = FacebookAdsRecord(
                        fbclid=f"IwAR3X8k9m2N{random.randint(100000, 999999)}",
                        campaign_id=f"camp_{hash(campaign.name) % 100000000}",
                        campaign_name=f"{campaign.name}_{stage}",
                        adset_id=f"adset_{random.randint(10000000, 99999999)}",
                        adset_name=f"{stage}_{customer['age_range'].value}_{customer['gender'].value}",
                        ad_id=f"ad_{random.randint(100000000, 999999999)}",
                        ad_name=self._generate_ad_creative_name(stage, customer['segment'], placement),
                        click_timestamp=click_timestamp.isoformat() + "Z",
                        impression_timestamp=impression_timestamp.isoformat() + "Z",
                        device_type=customer['preferred_device'].value,
                        placement=placement.value,
                        age_range=customer['age_range'].value,
                        gender=customer['gender'].value,
                        location=f"{customer['location']}, Netherlands",
                        cost_micros=cpc_micros,
                        impressions=impressions,
                        clicks=clicks,
                        estimated_audience_overlap=customer['cross_channel_probability'],
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
        
        all_records = []
        
        for campaign in campaigns:
            campaign_records = self._generate_touchpoints_for_campaign(campaign)
            
            # Apply filters
            if request.customer_segments:
                campaign_records = [r for r in campaign_records if r.segment in request.customer_segments]
            
            if request.placements:
                campaign_records = [r for r in campaign_records if r.placement in request.placements]
            
            if request.age_ranges:
                campaign_records = [r for r in campaign_records if r.age_range in request.age_ranges]
            
            all_records.extend(campaign_records)
            
            # Respect max_records limit
            if len(all_records) >= request.max_records:
                all_records = all_records[:request.max_records]
                break
        
        return [asdict(record) for record in all_records]

# Initialize the generator instance
generator = FacebookAdsGenerator()

# Create FastAPI app
app = FastAPI(
    title="Facebook Ads Synthetic Data API",
    description="Dutch market Facebook Ads synthetic data generator for omnichannel attribution",
    version="1.0.0"
)

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "facebook-ads-generator", "version": "1.0.0"}

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
            "budget_euros": campaign.budget_euros,
            "target_segments": [seg.value for seg in campaign.target_segments],
            "primary_placement": campaign.primary_placement.value,
            "target_age_ranges": [age.value for age in campaign.target_age_ranges]
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
                "channel": "Facebook Ads"
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
                "placements": request.placements,
                "age_ranges": request.age_ranges,
                "max_records": request.max_records
            },
            "data": data,
            "metadata": {
                "generated_at": datetime.now().isoformat(),
                "gdpr_compliant": True,
                "market": "Netherlands",
                "channel": "Facebook Ads"
            }
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating data: {str(e)}")

@app.get("/data")
async def get_recent_data(
    days: int = Query(30, description="Number of recent days"),
    max_records: int = Query(1000, description="Maximum records to return"),
    segments: Optional[List[str]] = Query(None, description="Customer segments to include"),
    placements: Optional[List[str]] = Query(None, description="Placements to include"),
    age_ranges: Optional[List[str]] = Query(None, description="Age ranges to include")
):
    """Get recent touchpoint data (convenient endpoint for N8N)"""
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    request = DataRequest(
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
        customer_segments=segments,
        placements=placements,
        age_ranges=age_ranges,
        max_records=max_records
    )
    
    return await get_filtered_data(request)

@app.get("/placements")
async def get_available_placements():
    """Get list of available Facebook ad placements"""
    return {
        "placements": [placement.value for placement in Placement],
        "placement_descriptions": {
            "facebook_feed": "Facebook News Feed",
            "facebook_stories": "Facebook Stories",
            "facebook_right_column": "Facebook Right Column",
            "instagram_feed": "Instagram Feed",
            "instagram_stories": "Instagram Stories",
            "messenger": "Facebook Messenger",
            "audience_network": "Meta Audience Network"
        }
    }

@app.get("/demographics")
async def get_available_demographics():
    """Get available demographic targeting options"""
    return {
        "age_ranges": [age.value for age in AgeRange],
        "genders": [gender.value for gender in Gender],
        "customer_segments": [segment.value for segment in CustomerSegment]
    }

if __name__ == "__main__":
    print("Starting Facebook Ads Synthetic Data API...")
    print("API Documentation: http://localhost:8001/docs")
    print("Health Check: http://localhost:8001/health")
    uvicorn.run(app, host="0.0.0.0", port=8001)