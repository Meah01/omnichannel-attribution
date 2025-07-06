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

class JobFunction(Enum):
    FINANCE = "Finance"
    OPERATIONS = "Operations"
    MANAGEMENT = "Management"
    MARKETING = "Marketing"
    SALES = "Sales"
    IT = "Information Technology"
    HR = "Human Resources"
    CONSULTING = "Consulting"
    BUSINESS_DEVELOPMENT = "Business Development"
    STRATEGY = "Strategy"

class SeniorityLevel(Enum):
    ENTRY = "Entry"
    SENIOR = "Senior"
    MANAGER = "Manager"
    DIRECTOR = "Director"
    VP = "VP"
    C_LEVEL = "C-Level"

class CompanySize(Enum):
    STARTUP = "1-10"
    SMALL = "11-50"
    MEDIUM_SMALL = "51-200"
    MEDIUM = "201-500"
    LARGE = "501-1000"
    ENTERPRISE = "1001+"

class Industry(Enum):
    FINANCIAL_SERVICES = "Financial Services"
    TECHNOLOGY = "Technology"
    CONSULTING = "Consulting"
    HEALTHCARE = "Healthcare"
    MANUFACTURING = "Manufacturing"
    RETAIL = "Retail"
    REAL_ESTATE = "Real Estate"
    EDUCATION = "Education"
    GOVERNMENT = "Government"
    NON_PROFIT = "Non-profit"

class CustomerSegment(Enum):
    B2C_WORKING_AGE = "B2C_WORKING_AGE"
    B2C_STUDENTS = "B2C_STUDENTS"
    B2C_NON_WORKING = "B2C_NON_WORKING"
    B2B_SMALL = "B2B_SMALL"
    B2B_MEDIUM = "B2B_MEDIUM"
    B2B_LARGE = "B2B_LARGE"

@dataclass
class LinkedInAdsRecord:
    """LinkedIn Ads touchpoint record structure"""
    linkedin_click_id: str
    campaign_id: str
    campaign_name: str
    creative_id: str
    creative_name: str
    click_timestamp: str
    impression_timestamp: str
    device_type: str
    job_function: str
    seniority_level: str
    company_size: str
    industry: str
    location: str
    cost_micros: int
    impressions: int
    clicks: int
    member_id_hash: str  # Hashed LinkedIn member ID
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
    target_job_functions: List[JobFunction]
    target_seniority_levels: List[SeniorityLevel]
    target_company_sizes: List[CompanySize]
    target_industries: List[Industry]

class DataRequest(BaseModel):
    """API request model for data generation"""
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    campaign_names: Optional[List[str]] = None
    customer_segments: Optional[List[str]] = None
    job_functions: Optional[List[str]] = None
    seniority_levels: Optional[List[str]] = None
    company_sizes: Optional[List[str]] = None
    industries: Optional[List[str]] = None
    max_records: Optional[int] = 10000

class LinkedInAdsGenerator:
    """
    LinkedIn Ads synthetic data API service for Dutch market
    Generates realistic B2B-focused data (Jan 2024 - June 2025) with professional targeting
    """
    
    def __init__(self):
        self.base_ctr = 0.012  # 1.2% CTR (lower than Google/Facebook)
        self.base_cpc_euros = 4.25  # Higher CPC for B2B targeting
        self.total_customers = 200000
        self.linkedin_penetration = 0.45  # Lower penetration, more selective B2B audience
        
        # Dutch locations for realistic targeting
        self.dutch_locations = [
            "Amsterdam", "Rotterdam", "The Hague", "Utrecht", "Eindhoven",
            "Tilburg", "Groningen", "Almere", "Breda", "Nijmegen"
        ]
        
        # Device distribution for LinkedIn (higher desktop usage for professional)
        self.device_distribution = {
            DeviceType.DESKTOP: 0.75,  # High desktop usage for business
            DeviceType.MOBILE: 0.22,
            DeviceType.TABLET: 0.03
        }
        
        # Job function distribution for Dutch market
        self.job_function_distribution = {
            JobFunction.FINANCE: 0.20,
            JobFunction.MANAGEMENT: 0.18,
            JobFunction.OPERATIONS: 0.15,
            JobFunction.SALES: 0.12,
            JobFunction.MARKETING: 0.10,
            JobFunction.IT: 0.08,
            JobFunction.BUSINESS_DEVELOPMENT: 0.07,
            JobFunction.STRATEGY: 0.05,
            JobFunction.HR: 0.03,
            JobFunction.CONSULTING: 0.02
        }
        
        # Seniority level distribution
        self.seniority_distribution = {
            SeniorityLevel.MANAGER: 0.35,
            SeniorityLevel.SENIOR: 0.25,
            SeniorityLevel.DIRECTOR: 0.20,
            SeniorityLevel.ENTRY: 0.10,
            SeniorityLevel.VP: 0.07,
            SeniorityLevel.C_LEVEL: 0.03
        }
        
        # Company size distribution for Netherlands
        self.company_size_distribution = {
            CompanySize.SMALL: 0.40,  # Many small businesses in NL
            CompanySize.MEDIUM_SMALL: 0.25,
            CompanySize.MEDIUM: 0.15,
            CompanySize.STARTUP: 0.10,
            CompanySize.LARGE: 0.07,
            CompanySize.ENTERPRISE: 0.03
        }
        
        # Industry distribution
        self.industry_distribution = {
            Industry.FINANCIAL_SERVICES: 0.25,  # High for banking target
            Industry.TECHNOLOGY: 0.20,
            Industry.CONSULTING: 0.15,
            Industry.MANUFACTURING: 0.12,
            Industry.HEALTHCARE: 0.08,
            Industry.RETAIL: 0.07,
            Industry.REAL_ESTATE: 0.05,
            Industry.EDUCATION: 0.04,
            Industry.GOVERNMENT: 0.03,
            Industry.NON_PROFIT: 0.01
        }
        
        # Customer segment distribution (heavily B2B focused)
        self.segment_distribution = {
            CustomerSegment.B2B_SMALL: 0.55,    # Primary LinkedIn audience
            CustomerSegment.B2B_MEDIUM: 0.25,
            CustomerSegment.B2B_LARGE: 0.15,
            CustomerSegment.B2C_WORKING_AGE: 0.04,  # Very limited B2C
            CustomerSegment.B2C_STUDENTS: 0.01,
            CustomerSegment.B2C_NON_WORKING: 0.00
        }
        
        self.customer_pool = self._generate_customer_pool()
        self._campaign_configs = None
        
    def _generate_customer_pool(self) -> List[Dict]:
        """Generate realistic customer pool with LinkedIn professional attributes"""
        customers = []
        active_customers = int(self.total_customers * self.linkedin_penetration)
        
        for i in range(active_customers):
            segment = self._weighted_choice(list(self.segment_distribution.keys()), 
                                          list(self.segment_distribution.values()))
            
            job_function = self._weighted_choice(list(self.job_function_distribution.keys()),
                                               list(self.job_function_distribution.values()))
            
            seniority_level = self._weighted_choice(list(self.seniority_distribution.keys()),
                                                  list(self.seniority_distribution.values()))
            
            company_size = self._weighted_choice(list(self.company_size_distribution.keys()),
                                               list(self.company_size_distribution.values()))
            
            industry = self._weighted_choice(list(self.industry_distribution.keys()),
                                           list(self.industry_distribution.values()))
            
            # Adjust attributes based on segment
            if segment == CustomerSegment.B2B_LARGE:
                # Large companies more likely to have senior roles
                if random.random() < 0.6:
                    seniority_level = random.choice([SeniorityLevel.DIRECTOR, SeniorityLevel.VP, SeniorityLevel.C_LEVEL])
                company_size = random.choice([CompanySize.LARGE, CompanySize.ENTERPRISE])
            
            customer = {
                'customer_id': f"cust_{str(uuid.uuid4())[:8]}",
                'member_id_hash': f"li_mem_{str(uuid.uuid4())[:12]}",
                'segment': segment,
                'job_function': job_function,
                'seniority_level': seniority_level,
                'company_size': company_size,
                'industry': industry,
                'preferred_device': self._weighted_choice(list(self.device_distribution.keys()),
                                                       list(self.device_distribution.values())),
                'location': random.choice(self.dutch_locations),
                'engagement_score': random.uniform(0.3, 0.9),  # Professional engagement
                'seasonal_sensitivity': random.uniform(0.3, 1.8),  # High B2B seasonality
                'decision_making_power': self._calculate_decision_power(seniority_level, company_size),
                'cross_channel_probability': random.uniform(0.25, 0.55)  # Lower cross-channel than social
            }
            customers.append(customer)
            
        return customers
    
    def _weighted_choice(self, choices: List, weights: List) -> Any:
        """Select random choice based on weights"""
        return random.choices(choices, weights=weights)[0]
    
    def _calculate_decision_power(self, seniority: SeniorityLevel, company_size: CompanySize) -> float:
        """Calculate decision-making power based on seniority and company size"""
        seniority_scores = {
            SeniorityLevel.ENTRY: 0.1,
            SeniorityLevel.SENIOR: 0.3,
            SeniorityLevel.MANAGER: 0.6,
            SeniorityLevel.DIRECTOR: 0.8,
            SeniorityLevel.VP: 0.9,
            SeniorityLevel.C_LEVEL: 1.0
        }
        
        company_size_multipliers = {
            CompanySize.STARTUP: 1.2,      # More individual decision power
            CompanySize.SMALL: 1.1,
            CompanySize.MEDIUM_SMALL: 1.0,
            CompanySize.MEDIUM: 0.9,
            CompanySize.LARGE: 0.8,
            CompanySize.ENTERPRISE: 0.7     # More complex decision processes
        }
        
        base_score = seniority_scores[seniority]
        multiplier = company_size_multipliers[company_size]
        
        return min(base_score * multiplier, 1.0)
    
    def get_campaign_configs(self) -> List[CampaignConfig]:
        """Define all LinkedIn Ads campaigns based on Dutch market calendar"""
        if self._campaign_configs is not None:
            return self._campaign_configs
            
        campaigns = []
        
        # 2024 Campaigns
        campaigns.extend([
            # January 2024 - B2B Resume Activity
            CampaignConfig(
                name="LinkedIn_Ads_Q1_Business_Planning_Banking",
                start_date=datetime(2024, 1, 7),
                end_date=datetime(2024, 1, 28),
                stages=["Interest", "Consideration"],
                stage_weights={"Interest": 0.6, "Consideration": 0.4},
                target_segments=[CustomerSegment.B2B_SMALL, CustomerSegment.B2B_MEDIUM],
                budget_euros=120000,
                seasonality_multiplier=0.8,
                target_job_functions=[JobFunction.FINANCE, JobFunction.MANAGEMENT, JobFunction.OPERATIONS],
                target_seniority_levels=[SeniorityLevel.MANAGER, SeniorityLevel.DIRECTOR],
                target_company_sizes=[CompanySize.SMALL, CompanySize.MEDIUM_SMALL, CompanySize.MEDIUM],
                target_industries=[Industry.FINANCIAL_SERVICES, Industry.TECHNOLOGY, Industry.CONSULTING]
            ),
            
            # February-March 2024 - FinTech Conference Season
            CampaignConfig(
                name="LinkedIn_Ads_FinTech_Conference_Leadership",
                start_date=datetime(2024, 2, 15),
                end_date=datetime(2024, 3, 10),
                stages=["Awareness", "Interest"],
                stage_weights={"Awareness": 0.6, "Interest": 0.4},
                target_segments=[CustomerSegment.B2B_MEDIUM, CustomerSegment.B2B_LARGE],
                budget_euros=180000,
                seasonality_multiplier=1.2,
                target_job_functions=[JobFunction.FINANCE, JobFunction.STRATEGY, JobFunction.MANAGEMENT],
                target_seniority_levels=[SeniorityLevel.DIRECTOR, SeniorityLevel.VP, SeniorityLevel.C_LEVEL],
                target_company_sizes=[CompanySize.MEDIUM, CompanySize.LARGE, CompanySize.ENTERPRISE],
                target_industries=[Industry.FINANCIAL_SERVICES, Industry.TECHNOLOGY]
            ),
            
            # March 2024 - Peak B2B Season
            CampaignConfig(
                name="LinkedIn_Ads_Q1_Growth_Banking_Solutions",
                start_date=datetime(2024, 3, 1),
                end_date=datetime(2024, 3, 31),
                stages=["Interest", "Consideration", "Conversion"],
                stage_weights={"Interest": 0.4, "Consideration": 0.4, "Conversion": 0.2},
                target_segments=[CustomerSegment.B2B_SMALL, CustomerSegment.B2B_MEDIUM, CustomerSegment.B2B_LARGE],
                budget_euros=250000,
                seasonality_multiplier=1.4,
                target_job_functions=[JobFunction.FINANCE, JobFunction.MANAGEMENT, JobFunction.OPERATIONS, JobFunction.BUSINESS_DEVELOPMENT],
                target_seniority_levels=[SeniorityLevel.MANAGER, SeniorityLevel.DIRECTOR, SeniorityLevel.VP],
                target_company_sizes=[CompanySize.SMALL, CompanySize.MEDIUM_SMALL, CompanySize.MEDIUM, CompanySize.LARGE],
                target_industries=[Industry.FINANCIAL_SERVICES, Industry.TECHNOLOGY, Industry.CONSULTING, Industry.MANUFACTURING]
            ),
            
            # April-May 2024 - Innovation Partnership Drive
            CampaignConfig(
                name="LinkedIn_Ads_Innovation_Partnership_Drive",
                start_date=datetime(2024, 4, 1),
                end_date=datetime(2024, 5, 15),
                stages=["Awareness", "Interest", "Consideration"],
                stage_weights={"Awareness": 0.4, "Interest": 0.35, "Consideration": 0.25},
                target_segments=[CustomerSegment.B2B_MEDIUM, CustomerSegment.B2B_LARGE],
                budget_euros=300000,
                seasonality_multiplier=1.3,
                target_job_functions=[JobFunction.STRATEGY, JobFunction.BUSINESS_DEVELOPMENT, JobFunction.MANAGEMENT],
                target_seniority_levels=[SeniorityLevel.DIRECTOR, SeniorityLevel.VP, SeniorityLevel.C_LEVEL],
                target_company_sizes=[CompanySize.MEDIUM, CompanySize.LARGE, CompanySize.ENTERPRISE],
                target_industries=[Industry.FINANCIAL_SERVICES, Industry.TECHNOLOGY, Industry.CONSULTING]
            ),
            
            # May-June 2024 - Mid-Year Business Review
            CampaignConfig(
                name="LinkedIn_Ads_Mid_Year_Business_Review",
                start_date=datetime(2024, 5, 15),
                end_date=datetime(2024, 6, 5),
                stages=["Interest", "Consideration"],
                stage_weights={"Interest": 0.6, "Consideration": 0.4},
                target_segments=[CustomerSegment.B2B_SMALL, CustomerSegment.B2B_MEDIUM],
                budget_euros=160000,
                seasonality_multiplier=1.1,
                target_job_functions=[JobFunction.FINANCE, JobFunction.OPERATIONS, JobFunction.MANAGEMENT],
                target_seniority_levels=[SeniorityLevel.MANAGER, SeniorityLevel.DIRECTOR],
                target_company_sizes=[CompanySize.SMALL, CompanySize.MEDIUM_SMALL, CompanySize.MEDIUM],
                target_industries=[Industry.FINANCIAL_SERVICES, Industry.MANUFACTURING, Industry.RETAIL]
            ),
            
            # July-August 2024 - Summer Business Continuity (Limited)
            CampaignConfig(
                name="LinkedIn_Ads_Summer_Business_Continuity",
                start_date=datetime(2024, 7, 1),
                end_date=datetime(2024, 8, 31),
                stages=["Retention"],
                stage_weights={"Retention": 1.0},
                target_segments=[CustomerSegment.B2B_SMALL, CustomerSegment.B2B_MEDIUM],
                budget_euros=50000,
                seasonality_multiplier=0.3,
                target_job_functions=[JobFunction.FINANCE, JobFunction.OPERATIONS],
                target_seniority_levels=[SeniorityLevel.MANAGER, SeniorityLevel.DIRECTOR],
                target_company_sizes=[CompanySize.SMALL, CompanySize.MEDIUM_SMALL],
                target_industries=[Industry.FINANCIAL_SERVICES]
            ),
            
            # September-October 2024 - Q4 Business Preparation
            CampaignConfig(
                name="LinkedIn_Ads_Q4_Business_Preparation",
                start_date=datetime(2024, 9, 15),
                end_date=datetime(2024, 10, 31),
                stages=["Awareness", "Interest", "Consideration"],
                stage_weights={"Awareness": 0.4, "Interest": 0.35, "Consideration": 0.25},
                target_segments=[CustomerSegment.B2B_SMALL, CustomerSegment.B2B_MEDIUM],
                budget_euros=200000,
                seasonality_multiplier=1.0,
                target_job_functions=[JobFunction.FINANCE, JobFunction.MANAGEMENT, JobFunction.STRATEGY],
                target_seniority_levels=[SeniorityLevel.MANAGER, SeniorityLevel.DIRECTOR, SeniorityLevel.VP],
                target_company_sizes=[CompanySize.SMALL, CompanySize.MEDIUM_SMALL, CompanySize.MEDIUM],
                target_industries=[Industry.FINANCIAL_SERVICES, Industry.TECHNOLOGY, Industry.CONSULTING]
            ),
            
            # October-November 2024 - Year End Business Banking
            CampaignConfig(
                name="LinkedIn_Ads_Year_End_Business_Banking",
                start_date=datetime(2024, 10, 15),
                end_date=datetime(2024, 11, 30),
                stages=["Interest", "Consideration", "Conversion"],
                stage_weights={"Interest": 0.4, "Consideration": 0.4, "Conversion": 0.2},
                target_segments=[CustomerSegment.B2B_SMALL, CustomerSegment.B2B_MEDIUM, CustomerSegment.B2B_LARGE],
                budget_euros=220000,
                seasonality_multiplier=1.2,
                target_job_functions=[JobFunction.FINANCE, JobFunction.MANAGEMENT, JobFunction.OPERATIONS],
                target_seniority_levels=[SeniorityLevel.MANAGER, SeniorityLevel.DIRECTOR, SeniorityLevel.VP],
                target_company_sizes=[CompanySize.SMALL, CompanySize.MEDIUM_SMALL, CompanySize.MEDIUM, CompanySize.LARGE],
                target_industries=[Industry.FINANCIAL_SERVICES, Industry.TECHNOLOGY, Industry.MANUFACTURING]
            ),
            
            # November 2024 - Last Push Business Solutions
            CampaignConfig(
                name="LinkedIn_Ads_Last_Push_Business_Solutions",
                start_date=datetime(2024, 11, 1),
                end_date=datetime(2024, 11, 30),
                stages=["Consideration", "Conversion"],
                stage_weights={"Consideration": 0.6, "Conversion": 0.4},
                target_segments=[CustomerSegment.B2B_MEDIUM, CustomerSegment.B2B_LARGE],
                budget_euros=180000,
                seasonality_multiplier=1.15,
                target_job_functions=[JobFunction.FINANCE, JobFunction.MANAGEMENT],
                target_seniority_levels=[SeniorityLevel.DIRECTOR, SeniorityLevel.VP, SeniorityLevel.C_LEVEL],
                target_company_sizes=[CompanySize.MEDIUM, CompanySize.LARGE, CompanySize.ENTERPRISE],
                target_industries=[Industry.FINANCIAL_SERVICES, Industry.TECHNOLOGY]
            )
        ])
        
        # 2025 Campaigns (January - June)
        campaigns.extend([
            # January 2025
            CampaignConfig(
                name="LinkedIn_Ads_Q1_Business_Planning_Banking",
                start_date=datetime(2025, 1, 7),
                end_date=datetime(2025, 1, 28),
                stages=["Interest", "Consideration"],
                stage_weights={"Interest": 0.6, "Consideration": 0.4},
                target_segments=[CustomerSegment.B2B_SMALL, CustomerSegment.B2B_MEDIUM],
                budget_euros=130000,
                seasonality_multiplier=0.85,
                target_job_functions=[JobFunction.FINANCE, JobFunction.MANAGEMENT, JobFunction.OPERATIONS],
                target_seniority_levels=[SeniorityLevel.MANAGER, SeniorityLevel.DIRECTOR],
                target_company_sizes=[CompanySize.SMALL, CompanySize.MEDIUM_SMALL, CompanySize.MEDIUM],
                target_industries=[Industry.FINANCIAL_SERVICES, Industry.TECHNOLOGY, Industry.CONSULTING]
            ),
            
            # February-March 2025
            CampaignConfig(
                name="LinkedIn_Ads_FinTech_Conference_Leadership",
                start_date=datetime(2025, 2, 15),
                end_date=datetime(2025, 3, 10),
                stages=["Awareness", "Interest"],
                stage_weights={"Awareness": 0.6, "Interest": 0.4},
                target_segments=[CustomerSegment.B2B_MEDIUM, CustomerSegment.B2B_LARGE],
                budget_euros=190000,
                seasonality_multiplier=1.25,
                target_job_functions=[JobFunction.FINANCE, JobFunction.STRATEGY, JobFunction.MANAGEMENT],
                target_seniority_levels=[SeniorityLevel.DIRECTOR, SeniorityLevel.VP, SeniorityLevel.C_LEVEL],
                target_company_sizes=[CompanySize.MEDIUM, CompanySize.LARGE, CompanySize.ENTERPRISE],
                target_industries=[Industry.FINANCIAL_SERVICES, Industry.TECHNOLOGY]
            ),
            
            # March 2025 - Peak Season
            CampaignConfig(
                name="LinkedIn_Ads_Q1_Growth_Banking_Solutions",
                start_date=datetime(2025, 3, 1),
                end_date=datetime(2025, 3, 31),
                stages=["Interest", "Consideration", "Conversion"],
                stage_weights={"Interest": 0.4, "Consideration": 0.4, "Conversion": 0.2},
                target_segments=[CustomerSegment.B2B_SMALL, CustomerSegment.B2B_MEDIUM, CustomerSegment.B2B_LARGE],
                budget_euros=270000,
                seasonality_multiplier=1.45,
                target_job_functions=[JobFunction.FINANCE, JobFunction.MANAGEMENT, JobFunction.OPERATIONS, JobFunction.BUSINESS_DEVELOPMENT],
                target_seniority_levels=[SeniorityLevel.MANAGER, SeniorityLevel.DIRECTOR, SeniorityLevel.VP],
                target_company_sizes=[CompanySize.SMALL, CompanySize.MEDIUM_SMALL, CompanySize.MEDIUM, CompanySize.LARGE],
                target_industries=[Industry.FINANCIAL_SERVICES, Industry.TECHNOLOGY, Industry.CONSULTING, Industry.MANUFACTURING]
            ),
            
            # April-May 2025
            CampaignConfig(
                name="LinkedIn_Ads_Innovation_Partnership_Drive",
                start_date=datetime(2025, 4, 1),
                end_date=datetime(2025, 5, 15),
                stages=["Awareness", "Interest", "Consideration"],
                stage_weights={"Awareness": 0.4, "Interest": 0.35, "Consideration": 0.25},
                target_segments=[CustomerSegment.B2B_MEDIUM, CustomerSegment.B2B_LARGE],
                budget_euros=320000,
                seasonality_multiplier=1.35,
                target_job_functions=[JobFunction.STRATEGY, JobFunction.BUSINESS_DEVELOPMENT, JobFunction.MANAGEMENT],
                target_seniority_levels=[SeniorityLevel.DIRECTOR, SeniorityLevel.VP, SeniorityLevel.C_LEVEL],
                target_company_sizes=[CompanySize.MEDIUM, CompanySize.LARGE, CompanySize.ENTERPRISE],
                target_industries=[Industry.FINANCIAL_SERVICES, Industry.TECHNOLOGY, Industry.CONSULTING]
            ),
            
            # May-June 2025
            CampaignConfig(
                name="LinkedIn_Ads_Mid_Year_Business_Review",
                start_date=datetime(2025, 5, 15),
                end_date=datetime(2025, 6, 5),
                stages=["Interest", "Consideration"],
                stage_weights={"Interest": 0.6, "Consideration": 0.4},
                target_segments=[CustomerSegment.B2B_SMALL, CustomerSegment.B2B_MEDIUM],
                budget_euros=170000,
                seasonality_multiplier=1.15,
                target_job_functions=[JobFunction.FINANCE, JobFunction.OPERATIONS, JobFunction.MANAGEMENT],
                target_seniority_levels=[SeniorityLevel.MANAGER, SeniorityLevel.DIRECTOR],
                target_company_sizes=[CompanySize.SMALL, CompanySize.MEDIUM_SMALL, CompanySize.MEDIUM],
                target_industries=[Industry.FINANCIAL_SERVICES, Industry.MANUFACTURING, Industry.RETAIL]
            )
        ])
        
        self._campaign_configs = campaigns
        return campaigns
    
    def _generate_creative_name(self, stage: str, job_function: JobFunction, 
                              seniority_level: SeniorityLevel) -> str:
        """Generate realistic LinkedIn ad creative names based on targeting"""
        creative_types = {
            "Awareness": ["Thought_Leadership", "Industry_Insights", "Company_Story"],
            "Interest": ["Product_Demo", "Case_Study", "Webinar_Invitation"],
            "Consideration": ["ROI_Calculator", "Free_Trial", "Consultation_Offer"],
            "Conversion": ["Demo_Request", "Contact_Sales", "Start_Trial"],
            "Retention": ["Success_Story", "Feature_Update", "Account_Review"]
        }
        
        job_function_themes = {
            JobFunction.FINANCE: "CFO_Banking_Solutions",
            JobFunction.MANAGEMENT: "Executive_Leadership",
            JobFunction.OPERATIONS: "Operational_Efficiency",
            JobFunction.SALES: "Revenue_Growth",
            JobFunction.MARKETING: "Growth_Marketing",
            JobFunction.IT: "Tech_Innovation",
            JobFunction.BUSINESS_DEVELOPMENT: "Partnership_Growth",
            JobFunction.STRATEGY: "Strategic_Planning",
            JobFunction.HR: "People_Operations",
            JobFunction.CONSULTING: "Advisory_Services"
        }
        
        seniority_prefix = {
            SeniorityLevel.ENTRY: "Junior",
            SeniorityLevel.SENIOR: "Senior",
            SeniorityLevel.MANAGER: "Manager",
            SeniorityLevel.DIRECTOR: "Director",
            SeniorityLevel.VP: "VP",
            SeniorityLevel.C_LEVEL: "Executive"
        }
        
        creative_type = random.choice(creative_types[stage])
        job_theme = job_function_themes[job_function]
        seniority_tag = seniority_prefix[seniority_level]
        
        return f"{creative_type}_{job_theme}_{seniority_tag}"
    
    def _calculate_performance_metrics(self, campaign: CampaignConfig, customer: Dict, 
                                     stage: str) -> Tuple[float, int]:
        """Calculate realistic CTR and CPC based on professional targeting"""
        base_ctr = self.base_ctr
        base_cpc = self.base_cpc_euros
        
        # Stage impact on performance
        stage_multipliers = {
            "Awareness": {"ctr": 0.8, "cpc": 0.9},
            "Interest": {"ctr": 1.0, "cpc": 1.0},
            "Consideration": {"ctr": 1.4, "cpc": 1.3},
            "Conversion": {"ctr": 2.0, "cpc": 1.6},
            "Retention": {"ctr": 0.6, "cpc": 0.8}
        }
        
        # Seniority level impact (higher seniority = higher engagement but higher cost)
        seniority_multipliers = {
            SeniorityLevel.ENTRY: {"ctr": 1.2, "cpc": 0.7},
            SeniorityLevel.SENIOR: {"ctr": 1.1, "cpc": 0.8},
            SeniorityLevel.MANAGER: {"ctr": 1.0, "cpc": 1.0},
            SeniorityLevel.DIRECTOR: {"ctr": 0.9, "cpc": 1.4},
            SeniorityLevel.VP: {"ctr": 0.8, "cpc": 1.8},
            SeniorityLevel.C_LEVEL: {"ctr": 0.7, "cpc": 2.5}
        }
        
        # Company size impact
        company_size_multipliers = {
            CompanySize.STARTUP: {"ctr": 1.3, "cpc": 0.8},
            CompanySize.SMALL: {"ctr": 1.2, "cpc": 0.9},
            CompanySize.MEDIUM_SMALL: {"ctr": 1.0, "cpc": 1.0},
            CompanySize.MEDIUM: {"ctr": 0.9, "cpc": 1.2},
            CompanySize.LARGE: {"ctr": 0.8, "cpc": 1.5},
            CompanySize.ENTERPRISE: {"ctr": 0.7, "cpc": 2.0}
        }
        
        # Industry relevance (Financial Services highest engagement for banking)
        industry_multipliers = {
            Industry.FINANCIAL_SERVICES: {"ctr": 1.5, "cpc": 1.0},
            Industry.TECHNOLOGY: {"ctr": 1.2, "cpc": 1.1},
            Industry.CONSULTING: {"ctr": 1.1, "cpc": 1.2},
            Industry.MANUFACTURING: {"ctr": 0.9, "cpc": 0.9},
            Industry.HEALTHCARE: {"ctr": 0.8, "cpc": 1.0},
            Industry.RETAIL: {"ctr": 0.7, "cpc": 0.8},
            Industry.REAL_ESTATE: {"ctr": 0.8, "cpc": 0.9},
            Industry.EDUCATION: {"ctr": 0.6, "cpc": 0.7},
            Industry.GOVERNMENT: {"ctr": 0.5, "cpc": 0.6},
            Industry.NON_PROFIT: {"ctr": 0.4, "cpc": 0.5}
        }
        
        # Decision-making power impact
        decision_power_multiplier = 0.7 + (customer['decision_making_power'] * 0.6)
        
        stage_mult = stage_multipliers[stage]
        seniority_mult = seniority_multipliers[customer['seniority_level']]
        company_mult = company_size_multipliers[customer['company_size']]
        industry_mult = industry_multipliers[customer['industry']]
        
        # Calculate final metrics
        final_ctr = (base_ctr * stage_mult["ctr"] * seniority_mult["ctr"] * 
                    company_mult["ctr"] * industry_mult["ctr"] * 
                    decision_power_multiplier * campaign.seasonality_multiplier * 
                    customer['engagement_score'])
        
        final_cpc = (base_cpc * stage_mult["cpc"] * seniority_mult["cpc"] * 
                    company_mult["cpc"] * industry_mult["cpc"] * 
                    campaign.seasonality_multiplier)
        
        return final_ctr, int(final_cpc * 1000000)  # Convert to micros
    
    def _generate_touchpoints_for_campaign(self, campaign: CampaignConfig) -> List[LinkedInAdsRecord]:
        """Generate all touchpoints for a specific campaign"""
        records = []
        campaign_days = (campaign.end_date - campaign.start_date).days + 1
        daily_budget = campaign.budget_euros / campaign_days
        
        # Determine customer participation (lower than social, higher value)
        total_campaign_customers = int(len(self.customer_pool) * 0.12 * campaign.seasonality_multiplier)
        participating_customers = random.sample(self.customer_pool, min(total_campaign_customers, len(self.customer_pool)))
        
        for stage, weight in campaign.stage_weights.items():
            stage_budget = daily_budget * weight
            stage_customers = random.sample(participating_customers, 
                                          int(len(participating_customers) * weight))
            
            for customer in stage_customers:
                # Skip if customer segment not in target
                if customer['segment'] not in campaign.target_segments:
                    continue
                
                # Skip if professional attributes not in target
                if (customer['job_function'] not in campaign.target_job_functions or
                    customer['seniority_level'] not in campaign.target_seniority_levels or
                    customer['company_size'] not in campaign.target_company_sizes or
                    customer['industry'] not in campaign.target_industries):
                    continue
                    
                # Generate touchpoints for this customer-stage combination
                touchpoint_count = random.randint(1, 3)  # Lower frequency for B2B
                
                for _ in range(touchpoint_count):
                    # Random date within campaign period
                    days_offset = random.randint(0, campaign_days - 1)
                    click_date = campaign.start_date + timedelta(days=days_offset)
                    
                    # Business hours timing (9 AM - 6 PM weekdays strongly preferred)
                    if click_date.weekday() < 5:  # Monday-Friday
                        hour = random.choices(range(24), weights=[0.1]*9 + [3]*9 + [0.2]*6)[0]
                    else:  # Weekend (very low activity)
                        if random.random() > 0.1:  # 90% chance to skip weekend
                            continue
                        hour = random.choices(range(24), weights=[0.05]*24)[0]
                    
                    click_timestamp = click_date.replace(
                        hour=hour,
                        minute=random.randint(0, 59),
                        second=random.randint(0, 59)
                    )
                    
                    impression_timestamp = click_timestamp - timedelta(seconds=random.randint(1, 20))
                    
                    # Calculate performance metrics
                    ctr, cpc_micros = self._calculate_performance_metrics(campaign, customer, stage)
                    
                    # Generate impressions based on CTR
                    clicks = 1  # This record represents a click
                    impressions = max(1, int(clicks / max(ctr, 0.001)))
                    
                    # Create the record
                    record = LinkedInAdsRecord(
                        linkedin_click_id=f"LI_CID_{random.randint(100000, 999999)}",
                        campaign_id=f"camp_{hash(campaign.name) % 100000000}",
                        campaign_name=f"{campaign.name}_{stage}",
                        creative_id=f"creative_{random.randint(10000000, 99999999)}",
                        creative_name=self._generate_creative_name(stage, customer['job_function'], 
                                                                customer['seniority_level']),
                        click_timestamp=click_timestamp.isoformat() + "Z",
                        impression_timestamp=impression_timestamp.isoformat() + "Z",
                        device_type=customer['preferred_device'].value,
                        job_function=customer['job_function'].value,
                        seniority_level=customer['seniority_level'].value,
                        company_size=customer['company_size'].value,
                        industry=customer['industry'].value,
                        location=f"{customer['location']}, Netherlands",
                        cost_micros=cpc_micros,
                        impressions=impressions,
                        clicks=clicks,
                        member_id_hash=customer['member_id_hash'],
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
            
            if request.job_functions:
                campaign_records = [r for r in campaign_records if r.job_function in request.job_functions]
            
            if request.seniority_levels:
                campaign_records = [r for r in campaign_records if r.seniority_level in request.seniority_levels]
            
            if request.company_sizes:
                campaign_records = [r for r in campaign_records if r.company_size in request.company_sizes]
            
            if request.industries:
                campaign_records = [r for r in campaign_records if r.industry in request.industries]
            
            all_records.extend(campaign_records)
            
            # Respect max_records limit
            if len(all_records) >= request.max_records:
                all_records = all_records[:request.max_records]
                break
        
        return [asdict(record) for record in all_records]

# Initialize the generator instance
generator = LinkedInAdsGenerator()

# Create FastAPI app
app = FastAPI(
    title="LinkedIn Ads Synthetic Data API",
    description="Dutch market LinkedIn Ads synthetic data generator for B2B omnichannel attribution",
    version="1.0.0"
)

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "linkedin-ads-generator", "version": "1.0.0"}

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
            "target_job_functions": [jf.value for jf in campaign.target_job_functions],
            "target_seniority_levels": [sl.value for sl in campaign.target_seniority_levels],
            "target_company_sizes": [cs.value for cs in campaign.target_company_sizes],
            "target_industries": [ind.value for ind in campaign.target_industries]
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
                "channel": "LinkedIn Ads"
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
                "job_functions": request.job_functions,
                "seniority_levels": request.seniority_levels,
                "company_sizes": request.company_sizes,
                "industries": request.industries,
                "max_records": request.max_records
            },
            "data": data,
            "metadata": {
                "generated_at": datetime.now().isoformat(),
                "gdpr_compliant": True,
                "market": "Netherlands",
                "channel": "LinkedIn Ads"
            }
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating data: {str(e)}")

@app.get("/data")
async def get_recent_data(
    days: int = Query(30, description="Number of recent days"),
    max_records: int = Query(1000, description="Maximum records to return"),
    segments: Optional[List[str]] = Query(None, description="Customer segments to include"),
    job_functions: Optional[List[str]] = Query(None, description="Job functions to include"),
    seniority_levels: Optional[List[str]] = Query(None, description="Seniority levels to include"),
    company_sizes: Optional[List[str]] = Query(None, description="Company sizes to include"),
    industries: Optional[List[str]] = Query(None, description="Industries to include")
):
    """Get recent touchpoint data (convenient endpoint for N8N)"""
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    request = DataRequest(
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
        customer_segments=segments,
        job_functions=job_functions,
        seniority_levels=seniority_levels,
        company_sizes=company_sizes,
        industries=industries,
        max_records=max_records
    )
    
    return await get_filtered_data(request)

@app.get("/targeting-options")
async def get_targeting_options():
    """Get all available professional targeting options"""
    return {
        "job_functions": [jf.value for jf in JobFunction],
        "seniority_levels": [sl.value for sl in SeniorityLevel],
        "company_sizes": [cs.value for cs in CompanySize],
        "industries": [ind.value for ind in Industry],
        "customer_segments": [seg.value for seg in CustomerSegment]
    }

@app.get("/professional-insights")
async def get_professional_insights():
    """Get insights about professional targeting performance"""
    return {
        "high_value_targeting": {
            "job_functions": ["Finance", "Management", "Strategy"],
            "seniority_levels": ["Director", "VP", "C-Level"],
            "industries": ["Financial Services", "Technology", "Consulting"],
            "company_sizes": ["201-500", "501-1000", "1001+"]
        },
        "engagement_patterns": {
            "peak_hours": "9 AM - 6 PM CET",
            "peak_days": "Tuesday - Thursday",
            "low_activity": "Weekends, July-August",
            "high_activity": "March-May, September-November"
        },
        "cost_factors": {
            "highest_cpc": "C-Level + Enterprise + Financial Services",
            "lowest_cpc": "Entry Level + Startup + Non-profit",
            "sweet_spot": "Manager Level + Small-Medium Companies + Technology"
        }
    }

if __name__ == "__main__":
    print("Starting LinkedIn Ads Synthetic Data API...")
    print("API Documentation: http://localhost:8003/docs")
    print("Health Check: http://localhost:8003/health")
    uvicorn.run(app, host="0.0.0.0", port=8003)