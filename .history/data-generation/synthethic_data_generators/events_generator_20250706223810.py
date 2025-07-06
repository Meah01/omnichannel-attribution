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

class EventType(Enum):
    INDUSTRY_CONFERENCE = "industry_conference"
    NETWORKING_EVENT = "networking_event"
    WEBINAR = "webinar"
    TRADE_SHOW = "trade_show"
    CULTURAL_EVENT = "cultural_event"
    FESTIVAL = "festival"
    WORKSHOP = "workshop"
    MEETUP = "meetup"

class InteractionType(Enum):
    REGISTRATION = "registration"
    ATTENDANCE = "attendance"
    BOOTH_SCAN = "booth_scan"
    SESSION_ATTENDANCE = "session_attendance"
    NETWORKING_SCAN = "networking_scan"
    LEAD_COLLECTION = "lead_collection"
    DEMO_REQUEST = "demo_request"

class LeadQuality(Enum):
    COLD = "cold"
    WARM = "warm"
    HOT = "hot"
    QUALIFIED = "qualified"

class CustomerSegment(Enum):
    B2C_WORKING_AGE = "B2C_WORKING_AGE"
    B2C_STUDENTS = "B2C_STUDENTS"
    B2C_NON_WORKING = "B2C_NON_WORKING"
    B2B_SMALL = "B2B_SMALL"
    B2B_MEDIUM = "B2B_MEDIUM"
    B2B_LARGE = "B2B_LARGE"

@dataclass
class EventsRecord:
    """Events touchpoint record structure"""
    event_id: str
    event_name: str
    event_type: str
    interaction_type: str
    registration_timestamp: Optional[str]
    attendance_timestamp: Optional[str]
    badge_scan_timestamp: Optional[str]
    email_address: str  # PRIMARY IDENTIFIER
    company_name: Optional[str]
    job_title: Optional[str]
    phone_number: Optional[str]
    location: str
    booth_interactions: List[str]
    session_attendance: List[str]
    lead_quality_score: float
    follow_up_consent: bool
    event_cost_per_lead: int  # Cost in euros
    networking_connections: int
    customer_id: str
    segment: str

@dataclass
class EventConfig:
    """Event configuration structure"""
    name: str
    event_type: EventType
    start_date: datetime
    end_date: datetime
    location: str
    stages: List[str]
    stage_weights: Dict[str, float]
    target_segments: List[CustomerSegment]
    expected_attendance: int
    cost_per_attendee: int  # euros
    lead_quality_multiplier: float
    cultural_significance: float  # 0-1 scale
    b2b_focus: float  # 0-1 scale (0 = pure B2C, 1 = pure B2B)

class DataRequest(BaseModel):
    """API request model for data generation"""
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    event_names: Optional[List[str]] = None
    customer_segments: Optional[List[str]] = None
    event_types: Optional[List[str]] = None
    interaction_types: Optional[List[str]] = None
    lead_quality_min: Optional[float] = None
    locations: Optional[List[str]] = None
    max_records: Optional[int] = 10000

class EventsGenerator:
    """
    Events synthetic data API service for Dutch market
    Generates realistic B2B and cultural event data (Jan 2024 - June 2025)
    """
    
    def __init__(self):
        self.total_customers = 200000
        self.events_penetration = 0.35  # 35% of customers attend events
        
        # Dutch event locations
        self.event_locations = {
            "Amsterdam": 0.35,  # Major business hub
            "Rotterdam": 0.20,  # Port city, logistics events
            "The Hague": 0.15,  # Government, international events
            "Utrecht": 0.12,    # Central location, conferences
            "Eindhoven": 0.08,  # Technology hub
            "Tilburg": 0.05,    # Regional events
            "Groningen": 0.03,  # Northern Netherlands
            "Breda": 0.02      # Southern events, Carnival
        }
        
        # Job titles for B2B events
        self.b2b_job_titles = [
            "CEO", "CFO", "CTO", "Managing Director", "Finance Director",
            "Operations Manager", "Business Development Manager", "Sales Director",
            "Marketing Manager", "IT Manager", "Strategy Consultant", "Partner",
            "Vice President", "Senior Manager", "Team Lead", "Project Manager",
            "Financial Controller", "Business Analyst", "Product Manager"
        ]
        
        # Company names for Netherlands
        self.company_names = [
            "TechCorp BV", "InnovatieGroup", "Nederlandse Solutions", "AmsterdamTech",
            "Rotterdam Logistics BV", "Utrecht Consulting", "Fintech Netherlands",
            "Digital Innovations BV", "Business Partners NL", "Growth Company",
            "Sustainable Solutions", "Smart Technology BV", "Future Finance",
            "Efficient Operations", "Strategic Ventures", "Dutch Enterprises"
        ]
        
        # Customer segment distribution for events
        self.segment_distribution = {
            CustomerSegment.B2B_SMALL: 0.45,     # High B2B event attendance
            CustomerSegment.B2B_MEDIUM: 0.25,
            CustomerSegment.B2B_LARGE: 0.15,
            CustomerSegment.B2C_WORKING_AGE: 0.10,  # Some cultural events
            CustomerSegment.B2C_STUDENTS: 0.04,     # Festival attendance
            CustomerSegment.B2C_NON_WORKING: 0.01   # Limited participation
        }
        
        self.customer_pool = self._generate_customer_pool()
        self._event_configs = None
        
    def _generate_customer_pool(self) -> List[Dict]:
        """Generate realistic customer pool with event-specific attributes"""
        customers = []
        active_customers = int(self.total_customers * self.events_penetration)
        
        for i in range(active_customers):
            segment = self._weighted_choice(list(self.segment_distribution.keys()), 
                                          list(self.segment_distribution.values()))
            
            # Generate realistic email and identifiers
            if segment.name.startswith("B2B"):
                email_domain = random.choice(["company.nl", "business.com", "corp.nl", "bv.nl"])
                first_name = random.choice(["Jan", "Emma", "Luca", "Sophie", "Daan", "Eva", "Noah", "Olivia"])
                last_name = random.choice(["de Jong", "Jansen", "van der Berg", "Bakker", "Visser", "Smit"])
                email_address = f"{first_name.lower()}.{last_name.lower().replace(' ', '')}@{email_domain}"
                company_name = random.choice(self.company_names)
                job_title = random.choice(self.b2b_job_titles)
                phone_number = f"+316{random.randint(10000000, 99999999)}"
            else:
                email_domain = random.choice(["gmail.com", "hotmail.com", "outlook.com", "icloud.com"])
                username = f"user{random.randint(1000, 9999)}"
                email_address = f"{username}@{email_domain}"
                company_name = None
                job_title = None
                phone_number = f"+316{random.randint(10000000, 99999999)}" if random.random() < 0.7 else None
            
            customer = {
                'customer_id': f"cust_{str(uuid.uuid4())[:8]}",
                'email_address': email_address,
                'company_name': company_name,
                'job_title': job_title,
                'phone_number': phone_number,
                'segment': segment,
                'location': self._weighted_choice(list(self.event_locations.keys()),
                                                list(self.event_locations.values())),
                'networking_score': random.uniform(0.2, 1.0),
                'event_engagement': random.uniform(0.3, 0.95),
                'lead_conversion_probability': random.uniform(0.1, 0.8),
                'cultural_interest': random.uniform(0.1, 0.9),
                'professional_interest': random.uniform(0.3, 1.0) if segment.name.startswith("B2B") else random.uniform(0.1, 0.5)
            }
            customers.append(customer)
            
        return customers
    
    def _weighted_choice(self, choices: List, weights: List) -> Any:
        """Select random choice based on weights"""
        return random.choices(choices, weights=weights)[0]
    
    def get_event_configs(self) -> List[EventConfig]:
        """Define all Events based on Dutch market calendar"""
        if self._event_configs is not None:
            return self._event_configs
            
        events = []
        
        # 2024 Events
        events.extend([
            # February 2024 - Carnival (Southern Netherlands)
            EventConfig(
                name="Events_Carnival_Payment_Freedom_Breda",
                event_type=EventType.CULTURAL_EVENT,
                start_date=datetime(2024, 2, 10),
                end_date=datetime(2024, 2, 13),
                location="Breda",
                stages=["Awareness", "Interest"],
                stage_weights={"Awareness": 0.8, "Interest": 0.2},
                target_segments=[CustomerSegment.B2C_STUDENTS, CustomerSegment.B2C_NON_WORKING, CustomerSegment.B2C_WORKING_AGE],
                expected_attendance=2500,
                cost_per_attendee=15,
                lead_quality_multiplier=0.6,
                cultural_significance=0.9,
                b2b_focus=0.1
            ),
            
            # February-March 2024 - FinTech Conference Season
            EventConfig(
                name="Events_FinTech_Amsterdam_2024",
                event_type=EventType.INDUSTRY_CONFERENCE,
                start_date=datetime(2024, 2, 28),
                end_date=datetime(2024, 3, 1),
                location="Amsterdam",
                stages=["Awareness", "Interest", "Consideration"],
                stage_weights={"Awareness": 0.4, "Interest": 0.4, "Consideration": 0.2},
                target_segments=[CustomerSegment.B2B_MEDIUM, CustomerSegment.B2B_LARGE],
                expected_attendance=1200,
                cost_per_attendee=450,
                lead_quality_multiplier=1.8,
                cultural_significance=0.2,
                b2b_focus=0.95
            ),
            
            EventConfig(
                name="Events_Digital_Banking_Innovation_Rotterdam",
                event_type=EventType.INDUSTRY_CONFERENCE,
                start_date=datetime(2024, 3, 15),
                end_date=datetime(2024, 3, 16),
                location="Rotterdam",
                stages=["Interest", "Consideration", "Conversion"],
                stage_weights={"Interest": 0.3, "Consideration": 0.5, "Conversion": 0.2},
                target_segments=[CustomerSegment.B2B_SMALL, CustomerSegment.B2B_MEDIUM],
                expected_attendance=800,
                cost_per_attendee=380,
                lead_quality_multiplier=1.6,
                cultural_significance=0.1,
                b2b_focus=0.9
            ),
            
            # April 2024 - King's Day
            EventConfig(
                name="Events_Kings_Day_Banking_Freedom_Amsterdam",
                event_type=EventType.CULTURAL_EVENT,
                start_date=datetime(2024, 4, 27),
                end_date=datetime(2024, 4, 27),
                location="Amsterdam",
                stages=["Awareness", "Interest", "Conversion"],
                stage_weights={"Awareness": 0.6, "Interest": 0.25, "Conversion": 0.15},
                target_segments=[CustomerSegment.B2C_WORKING_AGE, CustomerSegment.B2C_STUDENTS, CustomerSegment.B2C_NON_WORKING],
                expected_attendance=15000,
                cost_per_attendee=8,
                lead_quality_multiplier=0.8,
                cultural_significance=1.0,
                b2b_focus=0.05
            ),
            
            # April-May 2024 - Business Networking Events
            EventConfig(
                name="Events_Netherlands_Business_Summit_Utrecht",
                event_type=EventType.NETWORKING_EVENT,
                start_date=datetime(2024, 4, 18),
                end_date=datetime(2024, 4, 19),
                location="Utrecht",
                stages=["Interest", "Consideration", "Conversion"],
                stage_weights={"Interest": 0.35, "Consideration": 0.4, "Conversion": 0.25},
                target_segments=[CustomerSegment.B2B_SMALL, CustomerSegment.B2B_MEDIUM, CustomerSegment.B2B_LARGE],
                expected_attendance=600,
                cost_per_attendee=320,
                lead_quality_multiplier=1.7,
                cultural_significance=0.1,
                b2b_focus=0.95
            ),
            
            EventConfig(
                name="Events_SME_Finance_Forum_The_Hague",
                event_type=EventType.WORKSHOP,
                start_date=datetime(2024, 5, 8),
                end_date=datetime(2024, 5, 9),
                location="The Hague",
                stages=["Consideration", "Conversion"],
                stage_weights={"Consideration": 0.6, "Conversion": 0.4},
                target_segments=[CustomerSegment.B2B_SMALL, CustomerSegment.B2B_MEDIUM],
                expected_attendance=350,
                cost_per_attendee=280,
                lead_quality_multiplier=1.9,
                cultural_significance=0.1,
                b2b_focus=0.98
            ),
            
            # June 2024 - Festival Season
            EventConfig(
                name="Events_Lowlands_Festival_Banking_Biddinghuizen",
                event_type=EventType.FESTIVAL,
                start_date=datetime(2024, 6, 14),
                end_date=datetime(2024, 6, 16),
                location="Utrecht",
                stages=["Awareness", "Interest"],
                stage_weights={"Awareness": 0.7, "Interest": 0.3},
                target_segments=[CustomerSegment.B2C_STUDENTS, CustomerSegment.B2C_WORKING_AGE],
                expected_attendance=8500,
                cost_per_attendee=25,
                lead_quality_multiplier=0.4,
                cultural_significance=0.8,
                b2b_focus=0.0
            ),
            
            EventConfig(
                name="Events_Pinkpop_Mobile_Banking_Landgraaf",
                event_type=EventType.FESTIVAL,
                start_date=datetime(2024, 6, 21),
                end_date=datetime(2024, 6, 23),
                location="Eindhoven",
                stages=["Awareness", "Conversion"],
                stage_weights={"Awareness": 0.8, "Conversion": 0.2},
                target_segments=[CustomerSegment.B2C_STUDENTS, CustomerSegment.B2C_WORKING_AGE],
                expected_attendance=12000,
                cost_per_attendee=30,
                lead_quality_multiplier=0.3,
                cultural_significance=0.9,
                b2b_focus=0.0
            ),
            
            # September 2024 - Business Resume
            EventConfig(
                name="Events_Business_Banking_Symposium_Amsterdam",
                event_type=EventType.INDUSTRY_CONFERENCE,
                start_date=datetime(2024, 9, 19),
                end_date=datetime(2024, 9, 20),
                location="Amsterdam",
                stages=["Interest", "Consideration", "Conversion"],
                stage_weights={"Interest": 0.3, "Consideration": 0.45, "Conversion": 0.25},
                target_segments=[CustomerSegment.B2B_SMALL, CustomerSegment.B2B_MEDIUM, CustomerSegment.B2B_LARGE],
                expected_attendance=950,
                cost_per_attendee=420,
                lead_quality_multiplier=1.8,
                cultural_significance=0.1,
                b2b_focus=0.95
            ),
            
            # October 2024 - Autumn Events
            EventConfig(
                name="Events_Dutch_Startup_Week_Amsterdam",
                event_type=EventType.NETWORKING_EVENT,
                start_date=datetime(2024, 10, 14),
                end_date=datetime(2024, 10, 18),
                location="Amsterdam",
                stages=["Awareness", "Interest", "Consideration"],
                stage_weights={"Awareness": 0.4, "Interest": 0.4, "Consideration": 0.2},
                target_segments=[CustomerSegment.B2B_SMALL, CustomerSegment.B2B_MEDIUM],
                expected_attendance=1500,
                cost_per_attendee=180,
                lead_quality_multiplier=1.4,
                cultural_significance=0.3,
                b2b_focus=0.8
            ),
            
            # November 2024 - Year-End Events
            EventConfig(
                name="Events_Finance_Leadership_Conference_Rotterdam",
                event_type=EventType.INDUSTRY_CONFERENCE,
                start_date=datetime(2024, 11, 7),
                end_date=datetime(2024, 11, 8),
                location="Rotterdam",
                stages=["Consideration", "Conversion"],
                stage_weights={"Consideration": 0.5, "Conversion": 0.5},
                target_segments=[CustomerSegment.B2B_MEDIUM, CustomerSegment.B2B_LARGE],
                expected_attendance=400,
                cost_per_attendee=520,
                lead_quality_multiplier=2.1,
                cultural_significance=0.1,
                b2b_focus=0.98
            )
        ])
        
        # 2025 Events (January - June)
        events.extend([
            # February 2025 - Carnival
            EventConfig(
                name="Events_Carnival_Payment_Freedom_Breda",
                event_type=EventType.CULTURAL_EVENT,
                start_date=datetime(2025, 2, 15),
                end_date=datetime(2025, 2, 18),
                location="Breda",
                stages=["Awareness", "Interest"],
                stage_weights={"Awareness": 0.8, "Interest": 0.2},
                target_segments=[CustomerSegment.B2C_STUDENTS, CustomerSegment.B2C_NON_WORKING, CustomerSegment.B2C_WORKING_AGE],
                expected_attendance=2800,
                cost_per_attendee=18,
                lead_quality_multiplier=0.65,
                cultural_significance=0.9,
                b2b_focus=0.1
            ),
            
            # March 2025 - FinTech Conference
            EventConfig(
                name="Events_FinTech_Amsterdam_2025",
                event_type=EventType.INDUSTRY_CONFERENCE,
                start_date=datetime(2025, 3, 6),
                end_date=datetime(2025, 3, 7),
                location="Amsterdam",
                stages=["Awareness", "Interest", "Consideration"],
                stage_weights={"Awareness": 0.4, "Interest": 0.4, "Consideration": 0.2},
                target_segments=[CustomerSegment.B2B_MEDIUM, CustomerSegment.B2B_LARGE],
                expected_attendance=1350,
                cost_per_attendee=480,
                lead_quality_multiplier=1.9,
                cultural_significance=0.2,
                b2b_focus=0.95
            ),
            
            EventConfig(
                name="Events_Digital_Banking_Innovation_Rotterdam",
                event_type=EventType.INDUSTRY_CONFERENCE,
                start_date=datetime(2025, 3, 20),
                end_date=datetime(2025, 3, 21),
                location="Rotterdam",
                stages=["Interest", "Consideration", "Conversion"],
                stage_weights={"Interest": 0.3, "Consideration": 0.5, "Conversion": 0.2},
                target_segments=[CustomerSegment.B2B_SMALL, CustomerSegment.B2B_MEDIUM],
                expected_attendance=850,
                cost_per_attendee=400,
                lead_quality_multiplier=1.7,
                cultural_significance=0.1,
                b2b_focus=0.9
            ),
            
            # April 2025 - King's Day
            EventConfig(
                name="Events_Kings_Day_Banking_Freedom_Amsterdam",
                event_type=EventType.CULTURAL_EVENT,
                start_date=datetime(2025, 4, 27),
                end_date=datetime(2025, 4, 27),
                location="Amsterdam",
                stages=["Awareness", "Interest", "Conversion"],
                stage_weights={"Awareness": 0.6, "Interest": 0.25, "Conversion": 0.15},
                target_segments=[CustomerSegment.B2C_WORKING_AGE, CustomerSegment.B2C_STUDENTS, CustomerSegment.B2C_NON_WORKING],
                expected_attendance=16500,
                cost_per_attendee=10,
                lead_quality_multiplier=0.85,
                cultural_significance=1.0,
                b2b_focus=0.05
            ),
            
            # May 2025 - Business Events
            EventConfig(
                name="Events_Netherlands_Business_Summit_Utrecht",
                event_type=EventType.NETWORKING_EVENT,
                start_date=datetime(2025, 5, 15),
                end_date=datetime(2025, 5, 16),
                location="Utrecht",
                stages=["Interest", "Consideration", "Conversion"],
                stage_weights={"Interest": 0.35, "Consideration": 0.4, "Conversion": 0.25},
                target_segments=[CustomerSegment.B2B_SMALL, CustomerSegment.B2B_MEDIUM, CustomerSegment.B2B_LARGE],
                expected_attendance=680,
                cost_per_attendee=340,
                lead_quality_multiplier=1.8,
                cultural_significance=0.1,
                b2b_focus=0.95
            ),
            
            # June 2025 - Festival Season
            EventConfig(
                name="Events_Lowlands_Festival_Banking_Biddinghuizen",
                event_type=EventType.FESTIVAL,
                start_date=datetime(2025, 6, 13),
                end_date=datetime(2025, 6, 15),
                location="Utrecht",
                stages=["Awareness", "Interest"],
                stage_weights={"Awareness": 0.7, "Interest": 0.3},
                target_segments=[CustomerSegment.B2C_STUDENTS, CustomerSegment.B2C_WORKING_AGE],
                expected_attendance=9200,
                cost_per_attendee=28,
                lead_quality_multiplier=0.45,
                cultural_significance=0.8,
                b2b_focus=0.0
            )
        ])
        
        self._event_configs = events
        return events
    
    def _calculate_lead_quality(self, event: EventConfig, customer: Dict, stage: str) -> float:
        """Calculate lead quality score based on event, customer, and interaction"""
        base_quality = 0.5
        
        # Event type impact
        event_type_multipliers = {
            EventType.INDUSTRY_CONFERENCE: 1.8,
            EventType.WORKSHOP: 1.9,
            EventType.NETWORKING_EVENT: 1.6,
            EventType.TRADE_SHOW: 1.4,
            EventType.WEBINAR: 1.2,
            EventType.MEETUP: 1.1,
            EventType.CULTURAL_EVENT: 0.6,
            EventType.FESTIVAL: 0.3
        }
        
        # Stage impact
        stage_multipliers = {
            "Awareness": 0.7,
            "Interest": 1.0,
            "Consideration": 1.5,
            "Conversion": 2.0,
            "Retention": 1.3
        }
        
        # Customer segment impact
        segment_multipliers = {
            CustomerSegment.B2B_LARGE: 2.0,
            CustomerSegment.B2B_MEDIUM: 1.7,
            CustomerSegment.B2B_SMALL: 1.4,
            CustomerSegment.B2C_WORKING_AGE: 0.8,
            CustomerSegment.B2C_STUDENTS: 0.4,
            CustomerSegment.B2C_NON_WORKING: 0.3
        }
        
        event_mult = event_type_multipliers[event.event_type]
        stage_mult = stage_multipliers[stage]
        segment_mult = segment_multipliers[customer['segment']]
        
        # Calculate final quality
        final_quality = (base_quality * event_mult * stage_mult * segment_mult * 
                        event.lead_quality_multiplier * customer['professional_interest'])
        
        return min(final_quality, 1.0)
    
    def _generate_booth_interactions(self, event: EventConfig, customer: Dict) -> List[str]:
        """Generate realistic booth interactions based on event and customer"""
        if event.b2b_focus < 0.5:  # Cultural/consumer events
            return []
        
        interactions = []
        interaction_count = random.randint(0, 4)
        
        booth_types = [
            "bunq_main_booth", "bunq_demo_station", "bunq_consultation_booth",
            "partner_fintech_booth", "innovation_showcase", "product_demo_area"
        ]
        
        for _ in range(interaction_count):
            interactions.append(f"{random.choice(booth_types)}_scan_{random.randint(1, 99)}")
        
        return interactions
    
    def _generate_session_attendance(self, event: EventConfig, customer: Dict) -> List[str]:
        """Generate realistic session attendance based on customer interests"""
        if event.event_type in [EventType.CULTURAL_EVENT, EventType.FESTIVAL]:
            return []
        
        sessions = []
        if event.b2b_focus > 0.7:  # Business events
            business_sessions = [
                "mobile_banking_future", "fintech_regulations", "digital_transformation",
                "banking_innovation_panel", "cybersecurity_banking", "api_banking_workshop",
                "payment_solutions_demo", "business_banking_trends", "compliance_updates"
            ]
            
            # Attend 1-3 sessions based on professional interest
            session_count = random.randint(1, min(3, int(customer['professional_interest'] * 4)))
            sessions = random.sample(business_sessions, min(session_count, len(business_sessions)))
        
        return sessions
    
    def _generate_touchpoints_for_event(self, event: EventConfig) -> List[EventsRecord]:
        """Generate all touchpoints for a specific event"""
        records = []
        
        # Calculate actual attendance based on expected + randomness
        actual_attendance = int(event.expected_attendance * random.uniform(0.8, 1.2))
        
        # Select customers likely to attend this event
        eligible_customers = [c for c in self.customer_pool 
                            if c['segment'] in event.target_segments and
                            c['location'] == event.location or random.random() < 0.3]  # 30% travel
        
        if len(eligible_customers) < actual_attendance:
            # If not enough local customers, sample from all
            eligible_customers = [c for c in self.customer_pool if c['segment'] in event.target_segments]
        
        attending_customers = random.sample(eligible_customers, 
                                          min(actual_attendance, len(eligible_customers)))
        
        for stage, weight in event.stage_weights.items():
            stage_customers = random.sample(attending_customers, 
                                          int(len(attending_customers) * weight))
            
            for customer in stage_customers:
                # Registration timestamp (typically days/weeks before event)
                if event.event_type in [EventType.INDUSTRY_CONFERENCE, EventType.WORKSHOP, EventType.NETWORKING_EVENT]:
                    reg_days_before = random.randint(7, 45)
                elif event.event_type == EventType.WEBINAR:
                    reg_days_before = random.randint(1, 14)
                else:  # Cultural events, festivals
                    reg_days_before = random.randint(0, 7)
                
                registration_timestamp = (event.start_date - timedelta(days=reg_days_before))
                
                # Attendance timestamp (during event)
                event_duration_hours = (event.end_date - event.start_date).total_seconds() / 3600
                attendance_offset_hours = random.uniform(0, event_duration_hours)
                attendance_timestamp = event.start_date + timedelta(hours=attendance_offset_hours)
                
                # Badge scan timestamp (for B2B events)
                badge_scan_timestamp = None
                if event.b2b_focus > 0.5 and random.random() < 0.85:
                    badge_scan_offset = random.uniform(0.5, 8)  # Within 8 hours of attendance
                    badge_scan_timestamp = (attendance_timestamp + 
                                          timedelta(hours=badge_scan_offset)).isoformat() + "Z"
                
                # Calculate lead quality
                lead_quality = self._calculate_lead_quality(event, customer, stage)
                
                # Generate interactions
                booth_interactions = self._generate_booth_interactions(event, customer)
                session_attendance = self._generate_session_attendance(event, customer)
                
                # Follow-up consent (higher for B2B)
                follow_up_consent = random.random() < (0.85 if event.b2b_focus > 0.5 else 0.45)
                
                # Networking connections (0-10 for B2B, 0-3 for B2C)
                max_connections = 10 if event.b2b_focus > 0.5 else 3
                networking_connections = random.randint(0, int(max_connections * customer['networking_score']))
                
                # Create the record
                record = EventsRecord(
                    event_id=f"event_{hash(event.name) % 100000000}",
                    event_name=f"{event.name}_{stage}",
                    event_type=event.event_type.value,
                    interaction_type=InteractionType.ATTENDANCE.value,
                    registration_timestamp=registration_timestamp.isoformat() + "Z",
                    attendance_timestamp=attendance_timestamp.isoformat() + "Z",
                    badge_scan_timestamp=badge_scan_timestamp,
                    email_address=customer['email_address'],
                    company_name=customer['company_name'],
                    job_title=customer['job_title'],
                    phone_number=customer['phone_number'],
                    location=f"{event.location}, Netherlands",
                    booth_interactions=booth_interactions,
                    session_attendance=session_attendance,
                    lead_quality_score=lead_quality,
                    follow_up_consent=follow_up_consent,
                    event_cost_per_lead=event.cost_per_attendee,
                    networking_connections=networking_connections,
                    customer_id=customer['customer_id'],
                    segment=customer['segment'].value
                )
                
                records.append(record)
        
        return records
    
    def generate_filtered_data(self, request: DataRequest) -> List[Dict]:
        """Generate filtered data based on API request parameters"""
        events = self.get_event_configs()
        
        # Apply filters
        if request.start_date:
            start_filter = datetime.fromisoformat(request.start_date.replace('Z', ''))
            events = [e for e in events if e.end_date >= start_filter]
        
        if request.end_date:
            end_filter = datetime.fromisoformat(request.end_date.replace('Z', ''))
            events = [e for e in events if e.start_date <= end_filter]
        
        if request.event_names:
            events = [e for e in events if e.name in request.event_names]
        
        if request.event_types:
            events = [e for e in events if e.event_type.value in request.event_types]
        
        if request.locations:
            events = [e for e in events if e.location in request.locations]
        
        all_records = []
        
        for event in events:
            event_records = self._generate_touchpoints_for_event(event)
            
            # Apply filters
            if request.customer_segments:
                event_records = [r for r in event_records if r.segment in request.customer_segments]
            
            if request.interaction_types:
                event_records = [r for r in event_records if r.interaction_type in request.interaction_types]
            
            if request.lead_quality_min is not None:
                event_records = [r for r in event_records if r.lead_quality_score >= request.lead_quality_min]
            
            all_records.extend(event_records)
            
            # Respect max_records limit
            if len(all_records) >= request.max_records:
                all_records = all_records[:request.max_records]
                break
        
        return [asdict(record) for record in all_records]

# Initialize the generator instance
generator = EventsGenerator()

# Create FastAPI app
app = FastAPI(
    title="Events Synthetic Data API",
    description="Dutch market Events synthetic data generator for omnichannel attribution",
    version="1.0.0"
)

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "events-generator", "version": "1.0.0"}

@app.get("/events")
async def get_events():
    """Get list of all available events"""
    events = generator.get_event_configs()
    
    event_list = []
    for event in events:
        event_list.append({
            "name": event.name,
            "event_type": event.event_type.value,
            "start_date": event.start_date.isoformat(),
            "end_date": event.end_date.isoformat(),
            "location": event.location,
            "stages": event.stages,
            "target_segments": [seg.value for seg in event.target_segments],
            "expected_attendance": event.expected_attendance,
            "cost_per_attendee": event.cost_per_attendee,
            "cultural_significance": event.cultural_significance,
            "b2b_focus": event.b2b_focus
        })
    
    return {
        "total_events": len(event_list),
        "events": event_list
    }

@app.get("/events/{event_name}")
async def get_event_data(event_name: str, max_records: int = Query(1000, description="Maximum records to return")):
    """Get data for a specific event"""
    try:
        request = DataRequest(event_names=[event_name], max_records=max_records)
        data = generator.generate_filtered_data(request)
        
        if not data:
            raise HTTPException(status_code=404, detail=f"Event '{event_name}' not found or no data available")
        
        return {
            "event_name": event_name,
            "total_records": len(data),
            "data": data,
            "metadata": {
                "generated_at": datetime.now().isoformat(),
                "gdpr_compliant": True,
                "market": "Netherlands",
                "channel": "Events"
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
                "event_names": request.event_names,
                "customer_segments": request.customer_segments,
                "event_types": request.event_types,
                "interaction_types": request.interaction_types,
                "lead_quality_min": request.lead_quality_min,
                "locations": request.locations,
                "max_records": request.max_records
            },
            "data": data,
            "metadata": {
                "generated_at": datetime.now().isoformat(),
                "gdpr_compliant": True,
                "market": "Netherlands",
                "channel": "Events"
            }
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating data: {str(e)}")

@app.get("/data")
async def get_recent_data(
    days: int = Query(30, description="Number of recent days"),
    max_records: int = Query(1000, description="Maximum records to return"),
    segments: Optional[List[str]] = Query(None, description="Customer segments to include"),
    event_types: Optional[List[str]] = Query(None, description="Event types to include"),
    locations: Optional[List[str]] = Query(None, description="Locations to include"),
    lead_quality_min: Optional[float] = Query(None, description="Minimum lead quality score")
):
    """Get recent event data (convenient endpoint for N8N)"""
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    request = DataRequest(
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
        customer_segments=segments,
        event_types=event_types,
        locations=locations,
        lead_quality_min=lead_quality_min,
        max_records=max_records
    )
    
    return await get_filtered_data(request)

@app.get("/event-types")
async def get_available_event_types():
    """Get list of available event types"""
    return {
        "event_types": [event_type.value for event_type in EventType],
        "event_type_descriptions": {
            "industry_conference": "Professional B2B conferences with high-value leads",
            "networking_event": "Business networking with relationship building",
            "webinar": "Online educational sessions",
            "trade_show": "Industry exhibitions and product demonstrations",
            "cultural_event": "Dutch cultural celebrations (King's Day, Carnival)",
            "festival": "Music and entertainment festivals for brand awareness",
            "workshop": "Hands-on educational sessions",
            "meetup": "Informal professional gatherings"
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
            "phone_number": customer['phone_number'],
            "company_name": customer['company_name'],
            "segment": customer['segment'].value,
            "professional_interest": customer['professional_interest']
        })
    
    return {
        "total_identifiers": len(identifiers),
        "identifiers": identifiers,
        "note": "Events provide the strongest customer identifiers for B2B attribution"
    }

@app.get("/cultural-calendar")
async def get_cultural_calendar():
    """Get Dutch cultural events calendar"""
    return {
        "major_cultural_events": {
            "kings_day": {
                "date": "April 27",
                "significance": "National holiday celebrating the Dutch King",
                "color_theme": "Orange",
                "marketing_opportunity": "High brand visibility, national unity messaging"
            },
            "carnival": {
                "dates": "February/March (varies by year)",
                "locations": "Primarily Southern Netherlands (Limburg, Noord-Brabant)",
                "significance": "Catholic celebration before Lent",
                "marketing_opportunity": "Local activation, celebration themes"
            },
            "liberation_day": {
                "date": "May 5",
                "significance": "End of WWII occupation",
                "marketing_opportunity": "Freedom themes, historical respect"
            }
        },
        "seasonal_patterns": {
            "high_activity": "February-May, September-November",
            "low_activity": "July-August (vacation season)",
            "b2b_pause": "December 20 - January 7",
            "conference_season": "February-May peak"
        }
    }

if __name__ == "__main__":
    print("Starting Events Synthetic Data API...")
    print("API Documentation: http://localhost:8004/docs")
    print("Health Check: http://localhost:8004/health")
    uvicorn.run(app, host="0.0.0.0", port=8004)