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

class CustomerSegment(Enum):
    B2C_WORKING_AGE = "B2C_WORKING_AGE"
    B2C_STUDENTS = "B2C_STUDENTS"
    B2C_NON_WORKING = "B2C_NON_WORKING"
    B2B_SMALL = "B2B_SMALL"
    B2B_MEDIUM = "B2B_MEDIUM"
    B2B_LARGE = "B2B_LARGE"

@dataclass
class GoogleAdsRecord:
    """Google Ads touchpoint record structure"""
    gclid: str
    campaign_id: str
    campaign_name: str
    ad_group_id: str
    ad_group_name: str
    keyword: str
    match_type: str
    ad_id: str
    click_timestamp: str
    impression_timestamp: str
    device_type: str
    location: str
    cost_micros: int
    impressions: int
    clicks: int
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

class DataRequest(BaseModel):
    """API request model for data generation"""
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    campaign_names: Optional[List[str]] = None
    customer_segments: Optional[List[str]] = None
    max_records: Optional[int] = 10000

class GoogleAdsGenerator:
    """
    Google Ads synthetic data API service for Dutch market
    Generates realistic data (Jan 2024 - June 2025) via API endpoints
    """
    
    def __init__(self):
        self.base_ctr = 0.021  # 2.1% CTR
        self.base_cpc_euros = 2.34
        self.total_customers = 200000
        self.google_ads_penetration = 0.65
        
        # Dutch locations for realistic targeting
        self.dutch_locations = [
            "Amsterdam", "Rotterdam", "The Hague", "Utrecht", "Eindhoven",
            "Tilburg", "Groningen", "Almere", "Breda", "Nijmegen"
        ]
        
        # Device distribution realistic for Netherlands
        self.device_distribution = {
            DeviceType.MOBILE: 0.67,
            DeviceType.DESKTOP: 0.28,
            DeviceType.TABLET: 0.05
        }
        
        # Customer segment distribution
        self.segment_distribution = {
            CustomerSegment.B2C_WORKING_AGE: 0.45,
            CustomerSegment.B2C_STUDENTS: 0.20,
            CustomerSegment.B2C_NON_WORKING: 0.15,
            CustomerSegment.B2B_SMALL: 0.12,
            CustomerSegment.B2B_MEDIUM: 0.06,
            CustomerSegment.B2B_LARGE: 0.02
        }
        
        self.customer_pool = self._generate_customer_pool()
        self._campaign_configs = None
        
    def _generate_customer_pool(self) -> List[Dict]:
        """Generate realistic customer pool with segments and behaviors"""
        customers = []
        active_customers = int(self.total_customers * self.google_ads_penetration)
        
        for i in range(active_customers):
            segment = self._weighted_choice(list(self.segment_distribution.keys()), 
                                          list(self.segment_distribution.values()))
            
            customer = {
                'customer_id': f"cust_{str(uuid.uuid4())[:8]}",
                'segment': segment,
                'preferred_device': self._weighted_choice(list(self.device_distribution.keys()),
                                                       list(self.device_distribution.values())),
                'location': random.choice(self.dutch_locations),
                'engagement_score': random.uniform(0.3, 1.0),
                'seasonal_sensitivity': random.uniform(0.5, 1.5)
            }
            customers.append(customer)
            
        return customers
    
    def _weighted_choice(self, choices: List, weights: List) -> Any:
        """Select random choice based on weights"""
        return random.choices(choices, weights=weights)[0]
    
    def get_campaign_configs(self) -> List[CampaignConfig]:
        """Define all Google Ads campaigns based on Dutch market calendar"""
        if self._campaign_configs is not None:
            return self._campaign_configs
            
        campaigns = []
        
        # 2024 Campaigns
        campaigns.extend([
            # January 2024
            CampaignConfig(
                name="Google_Ads_New_Year_Financial_Resolutions",
                start_date=datetime(2024, 1, 2),
                end_date=datetime(2024, 1, 31),
                stages=["Interest", "Consideration"],
                stage_weights={"Interest": 0.5, "Consideration": 0.5},
                target_segments=[CustomerSegment.B2C_WORKING_AGE, CustomerSegment.B2B_SMALL],
                budget_euros=75000,
                seasonality_multiplier=0.8
            ),
            
            # February 2024
            CampaignConfig(
                name="Google_Ads_Spring_Preparation_Banking",
                start_date=datetime(2024, 2, 20),
                end_date=datetime(2024, 3, 15),
                stages=["Awareness", "Interest", "Consideration"],
                stage_weights={"Awareness": 0.4, "Interest": 0.35, "Consideration": 0.25},
                target_segments=[CustomerSegment.B2C_WORKING_AGE, CustomerSegment.B2B_MEDIUM],
                budget_euros=120000,
                seasonality_multiplier=0.9
            ),
            
            # March 2024 - Peak Season
            CampaignConfig(
                name="Google_Ads_Spring_Financial_Fresh_Start",
                start_date=datetime(2024, 3, 1),
                end_date=datetime(2024, 3, 31),
                stages=["Awareness", "Interest", "Consideration", "Conversion"],
                stage_weights={"Awareness": 0.35, "Interest": 0.30, "Consideration": 0.25, "Conversion": 0.10},
                target_segments=[CustomerSegment.B2C_WORKING_AGE, CustomerSegment.B2C_STUDENTS, CustomerSegment.B2B_SMALL, CustomerSegment.B2B_MEDIUM],
                budget_euros=200000,
                seasonality_multiplier=1.3
            ),
            
            # April 2024 - Peak Season + King's Day
            CampaignConfig(
                name="Google_Ads_Spring_Business_Growth",
                start_date=datetime(2024, 4, 1),
                end_date=datetime(2024, 4, 30),
                stages=["Interest", "Consideration", "Conversion"],
                stage_weights={"Interest": 0.4, "Consideration": 0.4, "Conversion": 0.2},
                target_segments=[CustomerSegment.B2C_WORKING_AGE, CustomerSegment.B2B_SMALL, CustomerSegment.B2B_MEDIUM],
                budget_euros=180000,
                seasonality_multiplier=1.25
            ),
            
            # May 2024 - Sustained Activity
            CampaignConfig(
                name="Google_Ads_Early_Summer_Mobile_Banking",
                start_date=datetime(2024, 5, 10),
                end_date=datetime(2024, 6, 10),
                stages=["Interest", "Consideration", "Conversion"],
                stage_weights={"Interest": 0.4, "Consideration": 0.35, "Conversion": 0.25},
                target_segments=[CustomerSegment.B2C_WORKING_AGE, CustomerSegment.B2C_STUDENTS],
                budget_euros=140000,
                seasonality_multiplier=1.1
            ),
            
            # July-August 2024 - Summer Low
            CampaignConfig(
                name="Google_Ads_Summer_Travel_Banking",
                start_date=datetime(2024, 7, 1),
                end_date=datetime(2024, 8, 31),
                stages=["Interest", "Conversion"],
                stage_weights={"Interest": 0.6, "Conversion": 0.4},
                target_segments=[CustomerSegment.B2C_NON_WORKING, CustomerSegment.B2C_STUDENTS],
                budget_euros=60000,
                seasonality_multiplier=0.5
            ),
            
            # September 2024 - Back to School
            CampaignConfig(
                name="Google_Ads_Back_To_School_Student_Banking",
                start_date=datetime(2024, 9, 1),
                end_date=datetime(2024, 9, 30),
                stages=["Awareness", "Interest", "Conversion"],
                stage_weights={"Awareness": 0.4, "Interest": 0.35, "Conversion": 0.25},
                target_segments=[CustomerSegment.B2C_STUDENTS, CustomerSegment.B2C_WORKING_AGE],
                budget_euros=110000,
                seasonality_multiplier=0.9
            ),
            
            CampaignConfig(
                name="Google_Ads_Autumn_Financial_Planning",
                start_date=datetime(2024, 9, 15),
                end_date=datetime(2024, 10, 15),
                stages=["Interest", "Consideration", "Conversion"],
                stage_weights={"Interest": 0.4, "Consideration": 0.35, "Conversion": 0.25},
                target_segments=[CustomerSegment.B2C_WORKING_AGE, CustomerSegment.B2B_SMALL],
                budget_euros=130000,
                seasonality_multiplier=1.0
            ),
            
            # November-December 2024 - Holiday Season
            CampaignConfig(
                name="Google_Ads_Holiday_Spending_Smart",
                start_date=datetime(2024, 11, 1),
                end_date=datetime(2024, 12, 20),
                stages=["Awareness", "Interest", "Consideration", "Conversion"],
                stage_weights={"Awareness": 0.3, "Interest": 0.3, "Consideration": 0.25, "Conversion": 0.15},
                target_segments=[CustomerSegment.B2C_WORKING_AGE, CustomerSegment.B2C_NON_WORKING],
                budget_euros=160000,
                seasonality_multiplier=1.15
            )
        ])
        
        # 2025 Campaigns (January - June)
        campaigns.extend([
            # January 2025 - New Year Resolutions
            CampaignConfig(
                name="Google_Ads_New_Year_Financial_Resolutions",
                start_date=datetime(2025, 1, 2),
                end_date=datetime(2025, 1, 31),
                stages=["Interest", "Consideration"],
                stage_weights={"Interest": 0.5, "Consideration": 0.5},
                target_segments=[CustomerSegment.B2C_WORKING_AGE, CustomerSegment.B2B_SMALL],
                budget_euros=80000,
                seasonality_multiplier=0.85
            ),
            
            # February 2025
            CampaignConfig(
                name="Google_Ads_Spring_Preparation_Banking",
                start_date=datetime(2025, 2, 20),
                end_date=datetime(2025, 3, 15),
                stages=["Awareness", "Interest", "Consideration"],
                stage_weights={"Awareness": 0.4, "Interest": 0.35, "Consideration": 0.25},
                target_segments=[CustomerSegment.B2C_WORKING_AGE, CustomerSegment.B2B_MEDIUM],
                budget_euros=125000,
                seasonality_multiplier=0.95
            ),
            
            # March 2025 - Peak Season
            CampaignConfig(
                name="Google_Ads_Spring_Financial_Fresh_Start",
                start_date=datetime(2025, 3, 1),
                end_date=datetime(2025, 3, 31),
                stages=["Awareness", "Interest", "Consideration", "Conversion"],
                stage_weights={"Awareness": 0.35, "Interest": 0.30, "Consideration": 0.25, "Conversion": 0.10},
                target_segments=[CustomerSegment.B2C_WORKING_AGE, CustomerSegment.B2C_STUDENTS, CustomerSegment.B2B_SMALL, CustomerSegment.B2B_MEDIUM],
                budget_euros=210000,
                seasonality_multiplier=1.35
            ),
            
            # April 2025
            CampaignConfig(
                name="Google_Ads_Spring_Business_Growth",
                start_date=datetime(2025, 4, 1),
                end_date=datetime(2025, 4, 30),
                stages=["Interest", "Consideration", "Conversion"],
                stage_weights={"Interest": 0.4, "Consideration": 0.4, "Conversion": 0.2},
                target_segments=[CustomerSegment.B2C_WORKING_AGE, CustomerSegment.B2B_SMALL, CustomerSegment.B2B_MEDIUM],
                budget_euros=185000,
                seasonality_multiplier=1.3
            ),
            
            # May 2025
            CampaignConfig(
                name="Google_Ads_Early_Summer_Mobile_Banking",
                start_date=datetime(2025, 5, 10),
                end_date=datetime(2025, 6, 10),
                stages=["Interest", "Consideration", "Conversion"],
                stage_weights={"Interest": 0.4, "Consideration": 0.35, "Conversion": 0.25},
                target_segments=[CustomerSegment.B2C_WORKING_AGE, CustomerSegment.B2C_STUDENTS],
                budget_euros=145000,
                seasonality_multiplier=1.15
            ),
            
            # June 2025 - Summer Transition
            CampaignConfig(
                name="Google_Ads_Festival_Season_Banking",
                start_date=datetime(2025, 6, 1),
                end_date=datetime(2025, 6, 30),
                stages=["Awareness", "Interest", "Conversion"],
                stage_weights={"Awareness": 0.4, "Interest": 0.35, "Conversion": 0.25},
                target_segments=[CustomerSegment.B2C_STUDENTS, CustomerSegment.B2C_NON_WORKING],
                budget_euros=100000,
                seasonality_multiplier=0.9
            )
        ])
        
        self._campaign_configs = campaigns
        return campaigns
    
    def _generate_keywords_for_stage(self, stage: str, segment: CustomerSegment) -> List[str]:
        """Generate realistic keywords based on campaign stage and customer segment"""
        base_keywords = {
            "Awareness": [
                "online banking", "digital banking", "mobile banking", "banking app",
                "banking netherlands", "fintech", "modern banking", "dutch bank"
            ],
            "Interest": [
                "best online bank", "banking features", "mobile payment", "digital wallet",
                "banking comparison", "bank account benefits", "free banking", "banking services"
            ],
            "Consideration": [
                "bunq banking", "open bank account", "switch bank", "business banking",
                "banking reviews", "bank account features", "digital banking platform"
            ],
            "Conversion": [
                "open bunq account", "sign up banking", "create bank account",
                "start banking", "join bunq", "banking registration"
            ]
        }
        
        # Adjust keywords based on segment
        if segment.name.startswith("B2B"):
            business_modifiers = ["business", "company", "corporate", "enterprise"]
            stage_keywords = base_keywords[stage]
            return [f"{modifier} {keyword}" for modifier in business_modifiers 
                   for keyword in stage_keywords[:4]]
        
        return base_keywords[stage]
    
    def _calculate_performance_metrics(self, campaign: CampaignConfig, 
                                     customer: Dict, stage: str) -> Tuple[float, int]:
        """Calculate realistic CTR and CPC based on campaign, customer, and stage"""
        base_ctr = self.base_ctr
        base_cpc = self.base_cpc_euros
        
        # Stage impact on performance
        stage_multipliers = {
            "Awareness": {"ctr": 0.8, "cpc": 0.9},
            "Interest": {"ctr": 1.0, "cpc": 1.0},
            "Consideration": {"ctr": 1.2, "cpc": 1.1},
            "Conversion": {"ctr": 1.5, "cpc": 1.3}
        }
        
        # Customer segment impact
        segment_multipliers = {
            CustomerSegment.B2C_WORKING_AGE: {"ctr": 1.0, "cpc": 1.0},
            CustomerSegment.B2C_STUDENTS: {"ctr": 1.3, "cpc": 0.7},
            CustomerSegment.B2C_NON_WORKING: {"ctr": 0.9, "cpc": 0.8},
            CustomerSegment.B2B_SMALL: {"ctr": 0.8, "cpc": 1.5},
            CustomerSegment.B2B_MEDIUM: {"ctr": 0.7, "cpc": 2.0},
            CustomerSegment.B2B_LARGE: {"ctr": 0.6, "cpc": 3.0}
        }
        
        # Device impact
        device_multipliers = {
            DeviceType.MOBILE: {"ctr": 1.2, "cpc": 0.9},
            DeviceType.DESKTOP: {"ctr": 1.0, "cpc": 1.0},
            DeviceType.TABLET: {"ctr": 0.8, "cpc": 1.1}
        }
        
        stage_mult = stage_multipliers[stage]
        segment_mult = segment_multipliers[customer['segment']]
        device_mult = device_multipliers[customer['preferred_device']]
        
        # Calculate final metrics
        final_ctr = (base_ctr * stage_mult["ctr"] * segment_mult["ctr"] * 
                    device_mult["ctr"] * campaign.seasonality_multiplier * 
                    customer['engagement_score'])
        
        final_cpc = (base_cpc * stage_mult["cpc"] * segment_mult["cpc"] * 
                    device_mult["cpc"] * campaign.seasonality_multiplier)
        
        return final_ctr, int(final_cpc * 1000000)  # Convert to micros
    
    def _generate_touchpoints_for_campaign(self, campaign: CampaignConfig) -> List[GoogleAdsRecord]:
        """Generate all touchpoints for a specific campaign"""
        records = []
        campaign_days = (campaign.end_date - campaign.start_date).days + 1
        daily_budget = campaign.budget_euros / campaign_days
        
        # Determine customer participation (realistic funnel)
        total_campaign_customers = int(len(self.customer_pool) * 0.15 * campaign.seasonality_multiplier)
        participating_customers = random.sample(self.customer_pool, min(total_campaign_customers, len(self.customer_pool)))
        
        for stage, weight in campaign.stage_weights.items():
            stage_budget = daily_budget * weight
            stage_customers = random.sample(participating_customers, 
                                          int(len(participating_customers) * weight))
            
            for customer in stage_customers:
                # Skip if customer segment not in target
                if customer['segment'] not in campaign.target_segments:
                    continue
                    
                # Generate touchpoints for this customer-stage combination
                touchpoint_count = random.randint(1, 5)  # Realistic touchpoint frequency
                
                for _ in range(touchpoint_count):
                    # Random date within campaign period
                    days_offset = random.randint(0, campaign_days - 1)
                    click_date = campaign.start_date + timedelta(days=days_offset)
                    
                    # Add realistic time (working hours higher probability)
                    if customer['segment'].name.startswith("B2B"):
                        hour = random.choices(range(24), weights=[0.5]*8 + [2]*10 + [0.5]*6)[0]
                    else:
                        hour = random.choices(range(24), weights=[0.8]*6 + [1.5]*12 + [2]*6)[0]
                    
                    click_timestamp = click_date.replace(
                        hour=hour,
                        minute=random.randint(0, 59),
                        second=random.randint(0, 59)
                    )
                    
                    impression_timestamp = click_timestamp - timedelta(seconds=random.randint(1, 10))
                    
                    # Calculate performance metrics
                    ctr, cpc_micros = self._calculate_performance_metrics(campaign, customer, stage)
                    
                    # Generate impressions based on CTR
                    clicks = 1  # This record represents a click
                    impressions = max(1, int(clicks / max(ctr, 0.001)))  # Avoid division by zero
                    
                    # Generate keywords for this stage
                    keywords = self._generate_keywords_for_stage(stage, customer['segment'])
                    selected_keyword = random.choice(keywords)
                    
                    # Create the record
                    record = GoogleAdsRecord(
                        gclid=f"Gj0CAQiA{random.randint(100000, 999999)}",
                        campaign_id=f"camp_{hash(campaign.name) % 100000000}",
                        campaign_name=f"{campaign.name}_{stage}",
                        ad_group_id=f"adg_{random.randint(10000000, 99999999)}",
                        ad_group_name=f"{stage}_{customer['segment'].name.split('_')[0]}_Keywords",
                        keyword=selected_keyword,
                        match_type=random.choice(["EXACT", "PHRASE", "BROAD"]),
                        ad_id=f"ad_{random.randint(100000000, 999999999)}",
                        click_timestamp=click_timestamp.isoformat() + "Z",
                        impression_timestamp=impression_timestamp.isoformat() + "Z",
                        device_type=customer['preferred_device'].value,
                        location=f"{customer['location']}, Netherlands",
                        cost_micros=cpc_micros,
                        impressions=impressions,
                        clicks=clicks,
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
            
            # Apply segment filter
            if request.customer_segments:
                campaign_records = [r for r in campaign_records if r.segment in request.customer_segments]
            
            all_records.extend(campaign_records)
            
            # Respect max_records limit
            if len(all_records) >= request.max_records:
                all_records = all_records[:request.max_records]
                break
        
        return [asdict(record) for record in all_records]

# Initialize the generator instance
generator = GoogleAdsGenerator()

# Create FastAPI app
app = FastAPI(
    title="Google Ads Synthetic Data API",
    description="Dutch market Google Ads synthetic data generator for omnichannel attribution",
    version="1.0.0"
)

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "google-ads-generator", "version": "1.0.0"}

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
            "target_segments": [seg.value for seg in campaign.target_segments]
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
                "channel": "Google Ads"
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
                "max_records": request.max_records
            },
            "data": data,
            "metadata": {
                "generated_at": datetime.now().isoformat(),
                "gdpr_compliant": True,
                "market": "Netherlands",
                "channel": "Google Ads"
            }
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating data: {str(e)}")

@app.get("/data")
async def get_recent_data(
    days: int = Query(30, description="Number of recent days"),
    max_records: int = Query(1000, description="Maximum records to return"),
    segments: Optional[List[str]] = Query(None, description="Customer segments to include")
):
    """Get recent touchpoint data (convenient endpoint for N8N)"""
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    request = DataRequest(
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
        customer_segments=segments,
        max_records=max_records
    )
    
    return await get_filtered_data(request)

if __name__ == "__main__":
    print("Starting Google Ads Synthetic Data API...")
    print("API Documentation: http://localhost:8000/docs")
    print("Health Check: http://localhost:8000/health")
    uvicorn.run(app, host="0.0.0.0", port=8000)