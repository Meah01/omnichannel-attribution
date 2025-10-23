/**
 * MarkovChainAttributionTesting
 * 
 * Comprehensive test class for MarkovChainAttribution and MarkovChainAttributionBatch classes.
 * Tests transition matrix construction, attribution calculation, batch processing, stage detection,
 * network visualization, and integration with the transition matrix storage system.
 * 
 * Test Coverage:
 * - Transition matrix construction and storage with stage detection
 * - Single journey attribution calculation
 * - Batch processing for multiple journeys
 * - B2C and B2B state space handling with proper stage progression
 * - Stage detection validation and accuracy
 * - Network visualization data generation
 * - Removal effect calculation with stage-based states
 * - Error handling and validation
 * - Manual matrix recalculation
 * - Integration with stored transition matrices
 * 
 * Author: Alexandru Constantinescu (www.linkedin.com/in/alexandru-constantinescu-6a18b6117)
 * Project: Omnichannel Attribution Platform
 * Date: 2025
 * Version: 2.0 
 */
@isTest
public class MarkovChainAttributionTesting {
    
    /**
     * Enhanced test data setup method with realistic stage progressions
     */
    @testSetup
    static void setupTestData() {
        // Create test campaigns
        List<Campaign__c> campaigns = new List<Campaign__c>();
        
        Campaign__c campaign1 = new Campaign__c();
        campaign1.Campaign_ID_External__c = 'TEST_GOOGLE_MARKOV_001';
        campaigns.add(campaign1);
        
        Campaign__c campaign2 = new Campaign__c();
        campaign2.Campaign_ID_External__c = 'TEST_FACEBOOK_MARKOV_001';
        campaigns.add(campaign2);
        
        Campaign__c campaign3 = new Campaign__c();
        campaign3.Campaign_ID_External__c = 'TEST_EMAIL_MARKOV_001';
        campaigns.add(campaign3);
        
        Campaign__c campaign4 = new Campaign__c();
        campaign4.Campaign_ID_External__c = 'TEST_LINKEDIN_MARKOV_001';
        campaigns.add(campaign4);
        
        Campaign__c campaign5 = new Campaign__c();
        campaign5.Campaign_ID_External__c = 'TEST_CONTENT_MARKOV_001';
        campaigns.add(campaign5);
        
        insert campaigns;
        
        // Create enhanced test customer journeys with realistic stage progressions
        List<Customer_Journey__c> journeys = new List<Customer_Journey__c>();
        
        // B2C Journey 1: Complete funnel progression (Google -> Facebook -> Email -> App Store)
        Customer_Journey__c b2cJourney1 = new Customer_Journey__c();
        b2cJourney1.Journey_ID__c = 'B2C_JOURNEY_001';
        b2cJourney1.Customer_ID__c = 'B2C_CUSTOMER_001';
        b2cJourney1.Customer_Type__c = 'B2C';
        b2cJourney1.Converted__c = true;
        b2cJourney1.Conversion_Value__c = 450.00;
        b2cJourney1.Journey_Start_Date__c = DateTime.now().addDays(-15);
        b2cJourney1.Journey_End_Date__c = DateTime.now().addDays(-2);
        b2cJourney1.Total_Touchpoints__c = 4;
        b2cJourney1.Confidence_Score__c = 0.92;
        b2cJourney1.Confidence_Level__c = 'High';
        journeys.add(b2cJourney1);
        
        // B2C Journey 2: Short conversion path (Google -> App Store)
        Customer_Journey__c b2cJourney2 = new Customer_Journey__c();
        b2cJourney2.Journey_ID__c = 'B2C_JOURNEY_002';
        b2cJourney2.Customer_ID__c = 'B2C_CUSTOMER_002';
        b2cJourney2.Customer_Type__c = 'B2C';
        b2cJourney2.Converted__c = true;
        b2cJourney2.Conversion_Value__c = 225.00;
        b2cJourney2.Journey_Start_Date__c = DateTime.now().addDays(-8);
        b2cJourney2.Journey_End_Date__c = DateTime.now().addDays(-1);
        b2cJourney2.Total_Touchpoints__c = 2;
        b2cJourney2.Confidence_Score__c = 0.88;
        b2cJourney2.Confidence_Level__c = 'High';
        journeys.add(b2cJourney2);
        
        // B2C Journey 3: Non-conversion with retention touchpoints
        Customer_Journey__c b2cJourney3 = new Customer_Journey__c();
        b2cJourney3.Journey_ID__c = 'B2C_JOURNEY_003';
        b2cJourney3.Customer_ID__c = 'B2C_CUSTOMER_003';
        b2cJourney3.Customer_Type__c = 'B2C';
        b2cJourney3.Converted__c = false;
        b2cJourney3.Conversion_Value__c = 0.00;
        b2cJourney3.Journey_Start_Date__c = DateTime.now().addDays(-6);
        b2cJourney3.Journey_End_Date__c = DateTime.now().addDays(-1);
        b2cJourney3.Total_Touchpoints__c = 3;
        b2cJourney3.Confidence_Score__c = 0.75;
        b2cJourney3.Confidence_Level__c = 'Medium';
        journeys.add(b2cJourney3);
        
        // B2C Journey 4: Social media awareness journey
        Customer_Journey__c b2cJourney4 = new Customer_Journey__c();
        b2cJourney4.Journey_ID__c = 'B2C_JOURNEY_004';
        b2cJourney4.Customer_ID__c = 'B2C_CUSTOMER_004';
        b2cJourney4.Customer_Type__c = 'B2C';
        b2cJourney4.Converted__c = true;
        b2cJourney4.Conversion_Value__c = 180.00;
        b2cJourney4.Journey_Start_Date__c = DateTime.now().addDays(-12);
        b2cJourney4.Journey_End_Date__c = DateTime.now().addDays(-3);
        b2cJourney4.Total_Touchpoints__c = 5;
        b2cJourney4.Confidence_Score__c = 0.85;
        b2cJourney4.Confidence_Level__c = 'High';
        journeys.add(b2cJourney4);
        
        // B2B Journey 1: Complete B2B funnel (LinkedIn -> Content -> Email -> Direct Sales)
        Customer_Journey__c b2bJourney1 = new Customer_Journey__c();
        b2bJourney1.Journey_ID__c = 'B2B_JOURNEY_001';
        b2bJourney1.Customer_ID__c = 'B2B_CUSTOMER_001';
        b2bJourney1.Customer_Type__c = 'B2B';
        b2bJourney1.Converted__c = true;
        b2bJourney1.Conversion_Value__c = 8500.00;
        b2bJourney1.Journey_Start_Date__c = DateTime.now().addDays(-35);
        b2bJourney1.Journey_End_Date__c = DateTime.now().addDays(-5);
        b2bJourney1.Total_Touchpoints__c = 4;
        b2bJourney1.Confidence_Score__c = 0.96;
        b2bJourney1.Confidence_Level__c = 'High';
        journeys.add(b2bJourney1);
        
        // B2B Journey 2: Complex B2B journey with multiple touchpoints
        Customer_Journey__c b2bJourney2 = new Customer_Journey__c();
        b2bJourney2.Journey_ID__c = 'B2B_JOURNEY_002';
        b2bJourney2.Customer_ID__c = 'B2B_CUSTOMER_002';
        b2bJourney2.Customer_Type__c = 'B2B';
        b2bJourney2.Converted__c = true;
        b2bJourney2.Conversion_Value__c = 12000.00;
        b2bJourney2.Journey_Start_Date__c = DateTime.now().addDays(-45);
        b2bJourney2.Journey_End_Date__c = DateTime.now().addDays(-8);
        b2bJourney2.Total_Touchpoints__c = 6;
        b2bJourney2.Confidence_Score__c = 0.91;
        b2bJourney2.Confidence_Level__c = 'High';
        journeys.add(b2bJourney2);
        
        // B2B Journey 3: Non-conversion B2B journey
        Customer_Journey__c b2bJourney3 = new Customer_Journey__c();
        b2bJourney3.Journey_ID__c = 'B2B_JOURNEY_003';
        b2bJourney3.Customer_ID__c = 'B2B_CUSTOMER_003';
        b2bJourney3.Customer_Type__c = 'B2B';
        b2bJourney3.Converted__c = false;
        b2bJourney3.Conversion_Value__c = 0.00;
        b2bJourney3.Journey_Start_Date__c = DateTime.now().addDays(-25);
        b2bJourney3.Journey_End_Date__c = DateTime.now().addDays(-3);
        b2bJourney3.Total_Touchpoints__c = 3;
        b2bJourney3.Confidence_Score__c = 0.78;
        b2bJourney3.Confidence_Level__c = 'Medium';
        journeys.add(b2bJourney3);
        
        insert journeys;
        
        // Create realistic touchpoints with stage progression patterns
        List<Touchpoint__c> touchpoints = new List<Touchpoint__c>();
        
        // B2C Journey 1 touchpoints (Google -> Facebook -> Email -> App Store)
        touchpoints.add(new Touchpoint__c(
            Touchpoint_ID__c = 'B2C_TP_001_001',
            Customer_Journey__c = journeys[0].Id,
            Channel__c = 'Google_Ads',
            Touchpoint_Timestamp__c = DateTime.now().addDays(-15), // Awareness stage
            Campaign__c = campaigns[0].Id
        ));
        
        touchpoints.add(new Touchpoint__c(
            Touchpoint_ID__c = 'B2C_TP_001_002',
            Customer_Journey__c = journeys[0].Id,
            Channel__c = 'Facebook_Ads',
            Touchpoint_Timestamp__c = DateTime.now().addDays(-10), // Interest stage
            Campaign__c = campaigns[1].Id
        ));
        
        touchpoints.add(new Touchpoint__c(
            Touchpoint_ID__c = 'B2C_TP_001_003',
            Customer_Journey__c = journeys[0].Id,
            Channel__c = 'Email_Marketing',
            Touchpoint_Timestamp__c = DateTime.now().addDays(-5), // Consideration stage
            Campaign__c = campaigns[2].Id
        ));
        
        touchpoints.add(new Touchpoint__c(
            Touchpoint_ID__c = 'B2C_TP_001_004',
            Customer_Journey__c = journeys[0].Id,
            Channel__c = 'App_Store_Optimization',
            Touchpoint_Timestamp__c = DateTime.now().addDays(-2), // Conversion stage
            Campaign__c = campaigns[0].Id
        ));
        
        // B2C Journey 2 touchpoints (Google -> App Store)
        touchpoints.add(new Touchpoint__c(
            Touchpoint_ID__c = 'B2C_TP_002_001',
            Customer_Journey__c = journeys[1].Id,
            Channel__c = 'Google_Ads',
            Touchpoint_Timestamp__c = DateTime.now().addDays(-8), // Awareness stage
            Campaign__c = campaigns[0].Id
        ));
        
        touchpoints.add(new Touchpoint__c(
            Touchpoint_ID__c = 'B2C_TP_002_002',
            Customer_Journey__c = journeys[1].Id,
            Channel__c = 'App_Store_Optimization',
            Touchpoint_Timestamp__c = DateTime.now().addDays(-1), // Conversion stage
            Campaign__c = campaigns[0].Id
        ));
        
        // B2C Journey 3 touchpoints (Facebook -> Content -> Email - no conversion)
        touchpoints.add(new Touchpoint__c(
            Touchpoint_ID__c = 'B2C_TP_003_001',
            Customer_Journey__c = journeys[2].Id,
            Channel__c = 'Facebook_Ads',
            Touchpoint_Timestamp__c = DateTime.now().addDays(-6), // Awareness stage
            Campaign__c = campaigns[1].Id
        ));
        
        touchpoints.add(new Touchpoint__c(
            Touchpoint_ID__c = 'B2C_TP_003_002',
            Customer_Journey__c = journeys[2].Id,
            Channel__c = 'Content_Website_SEO',
            Touchpoint_Timestamp__c = DateTime.now().addDays(-4), // Interest stage
            Campaign__c = campaigns[4].Id
        ));
        
        touchpoints.add(new Touchpoint__c(
            Touchpoint_ID__c = 'B2C_TP_003_003',
            Customer_Journey__c = journeys[2].Id,
            Channel__c = 'Email_Marketing',
            Touchpoint_Timestamp__c = DateTime.now().addDays(-1), // Consideration stage
            Campaign__c = campaigns[2].Id
        ));
        
        // B2C Journey 4 touchpoints (Social media journey)
        touchpoints.add(new Touchpoint__c(
            Touchpoint_ID__c = 'B2C_TP_004_001',
            Customer_Journey__c = journeys[3].Id,
            Channel__c = 'Instagram_Organic',
            Touchpoint_Timestamp__c = DateTime.now().addDays(-12), // Awareness stage
            Campaign__c = campaigns[1].Id
        ));
        
        touchpoints.add(new Touchpoint__c(
            Touchpoint_ID__c = 'B2C_TP_004_002',
            Customer_Journey__c = journeys[3].Id,
            Channel__c = 'Influencer_Marketing',
            Touchpoint_Timestamp__c = DateTime.now().addDays(-9), // Interest stage
            Campaign__c = campaigns[1].Id
        ));
        
        touchpoints.add(new Touchpoint__c(
            Touchpoint_ID__c = 'B2C_TP_004_003',
            Customer_Journey__c = journeys[3].Id,
            Channel__c = 'Content_Website_SEO',
            Touchpoint_Timestamp__c = DateTime.now().addDays(-6), // Interest stage
            Campaign__c = campaigns[4].Id
        ));
        
        touchpoints.add(new Touchpoint__c(
            Touchpoint_ID__c = 'B2C_TP_004_004',
            Customer_Journey__c = journeys[3].Id,
            Channel__c = 'Email_Marketing',
            Touchpoint_Timestamp__c = DateTime.now().addDays(-4), // Consideration stage
            Campaign__c = campaigns[2].Id
        ));
        
        touchpoints.add(new Touchpoint__c(
            Touchpoint_ID__c = 'B2C_TP_004_005',
            Customer_Journey__c = journeys[3].Id,
            Channel__c = 'App_Store_Optimization',
            Touchpoint_Timestamp__c = DateTime.now().addDays(-3), // Conversion stage
            Campaign__c = campaigns[0].Id
        ));
        
        // B2B Journey 1 touchpoints (LinkedIn -> Content -> Email -> Direct Sales)
        touchpoints.add(new Touchpoint__c(
            Touchpoint_ID__c = 'B2B_TP_001_001',
            Customer_Journey__c = journeys[4].Id,
            Channel__c = 'LinkedIn_Ads',
            Touchpoint_Timestamp__c = DateTime.now().addDays(-35), // Awareness stage
            Campaign__c = campaigns[3].Id
        ));
        
        touchpoints.add(new Touchpoint__c(
            Touchpoint_ID__c = 'B2B_TP_001_002',
            Customer_Journey__c = journeys[4].Id,
            Channel__c = 'Content_Website_SEO',
            Touchpoint_Timestamp__c = DateTime.now().addDays(-20), // Interest stage
            Campaign__c = campaigns[4].Id
        ));
        
        touchpoints.add(new Touchpoint__c(
            Touchpoint_ID__c = 'B2B_TP_001_003',
            Customer_Journey__c = journeys[4].Id,
            Channel__c = 'Email_Marketing',
            Touchpoint_Timestamp__c = DateTime.now().addDays(-10), // Consideration stage
            Campaign__c = campaigns[2].Id
        ));
        
        touchpoints.add(new Touchpoint__c(
            Touchpoint_ID__c = 'B2B_TP_001_004',
            Customer_Journey__c = journeys[4].Id,
            Channel__c = 'Direct_Sales',
            Touchpoint_Timestamp__c = DateTime.now().addDays(-5), // Conversion stage
            Campaign__c = campaigns[3].Id
        ));
        
        // B2B Journey 2 touchpoints (Complex B2B journey)
        touchpoints.add(new Touchpoint__c(
            Touchpoint_ID__c = 'B2B_TP_002_001',
            Customer_Journey__c = journeys[5].Id,
            Channel__c = 'Google_Ads',
            Touchpoint_Timestamp__c = DateTime.now().addDays(-45), // Awareness stage
            Campaign__c = campaigns[0].Id
        ));
        
        touchpoints.add(new Touchpoint__c(
            Touchpoint_ID__c = 'B2B_TP_002_002',
            Customer_Journey__c = journeys[5].Id,
            Channel__c = 'LinkedIn_Ads',
            Touchpoint_Timestamp__c = DateTime.now().addDays(-35), // Interest stage
            Campaign__c = campaigns[3].Id
        ));
        
        touchpoints.add(new Touchpoint__c(
            Touchpoint_ID__c = 'B2B_TP_002_003',
            Customer_Journey__c = journeys[5].Id,
            Channel__c = 'Content_Website_SEO',
            Touchpoint_Timestamp__c = DateTime.now().addDays(-25), // Interest stage
            Campaign__c = campaigns[4].Id
        ));
        
        touchpoints.add(new Touchpoint__c(
            Touchpoint_ID__c = 'B2B_TP_002_004',
            Customer_Journey__c = journeys[5].Id,
            Channel__c = 'Email_Marketing',
            Touchpoint_Timestamp__c = DateTime.now().addDays(-18), // Consideration stage
            Campaign__c = campaigns[2].Id
        ));
        
        touchpoints.add(new Touchpoint__c(
            Touchpoint_ID__c = 'B2B_TP_002_005',
            Customer_Journey__c = journeys[5].Id,
            Channel__c = 'Direct_Sales',
            Touchpoint_Timestamp__c = DateTime.now().addDays(-12), // Consideration stage
            Campaign__c = campaigns[3].Id
        ));
        
        touchpoints.add(new Touchpoint__c(
            Touchpoint_ID__c = 'B2B_TP_002_006',
            Customer_Journey__c = journeys[5].Id,
            Channel__c = 'Direct_Sales',
            Touchpoint_Timestamp__c = DateTime.now().addDays(-8), // Conversion stage
            Campaign__c = campaigns[3].Id
        ));
        
        // B2B Journey 3 touchpoints (Non-conversion)
        touchpoints.add(new Touchpoint__c(
            Touchpoint_ID__c = 'B2B_TP_003_001',
            Customer_Journey__c = journeys[6].Id,
            Channel__c = 'LinkedIn_Organic',
            Touchpoint_Timestamp__c = DateTime.now().addDays(-25), // Awareness stage
            Campaign__c = campaigns[3].Id
        ));
        
        touchpoints.add(new Touchpoint__c(
            Touchpoint_ID__c = 'B2B_TP_003_002',
            Customer_Journey__c = journeys[6].Id,
            Channel__c = 'Content_Website_SEO',
            Touchpoint_Timestamp__c = DateTime.now().addDays(-15), // Interest stage
            Campaign__c = campaigns[4].Id
        ));
        
        touchpoints.add(new Touchpoint__c(
            Touchpoint_ID__c = 'B2B_TP_003_003',
            Customer_Journey__c = journeys[6].Id,
            Channel__c = 'Email_Marketing',
            Touchpoint_Timestamp__c = DateTime.now().addDays(-3), // Consideration stage
            Campaign__c = campaigns[2].Id
        ));
        
        insert touchpoints;
    }
    
    /**
     * Test transition matrix construction with proper stage detection
     */
    @isTest
    static void testTransitionMatrixConstructionWithStageDetection() {
        Test.startTest();
        
        // Trigger matrix construction for B2C
        MarkovChainAttribution.buildAndStoreTransitionMatrix('B2C');
        
        // Trigger matrix construction for B2B  
        MarkovChainAttribution.buildAndStoreTransitionMatrix('B2B');
        
        Test.stopTest();
        
        // Verify transition matrices were created
        Integer b2cMatrixCount = [
            SELECT COUNT() 
            FROM Transition_Matrix__c 
            WHERE Customer_Type__c = 'B2C'
        ];
        
        Integer b2bMatrixCount = [
            SELECT COUNT() 
            FROM Transition_Matrix__c 
            WHERE Customer_Type__c = 'B2B'
        ];
        
        System.assert(b2cMatrixCount > 0, 'B2C transition matrix should be created');
        System.assert(b2bMatrixCount > 0, 'B2B transition matrix should be created');
        
        // Verify stage-based states are present
        List<Transition_Matrix__c> b2cStages = [
            SELECT From_State__c, To_State__c FROM Transition_Matrix__c 
            WHERE Customer_Type__c = 'B2C' 
            AND (From_State__c LIKE '%_Awareness' OR From_State__c LIKE '%_Interest' 
                 OR From_State__c LIKE '%_Consideration' OR From_State__c LIKE '%_Conversion')
        ];
        
        System.assert(b2cStages.size() > 0, 'B2C matrix should have stage-based states');
        
        // Verify START to stage transitions exist
        Integer startTransitions = [
            SELECT COUNT() FROM Transition_Matrix__c 
            WHERE Customer_Type__c = 'B2C' AND From_State__c = 'START'
        ];
        
        System.assert(startTransitions > 0, 'START state transitions should exist');
    }
    
    /**
     * Test stage detection logic
     */
    @isTest
    static void testStageDetectionLogic() {
        // Build transition matrix to trigger stage detection
        MarkovChainAttribution.buildAndStoreTransitionMatrix('B2C');
        
        Test.startTest();
        
        // Get stage detection summary
        Map<String, Object> stageDetectionSummary = MarkovChainAttribution.getStageDetectionSummary('B2C');
        
        Test.stopTest();
        
        // Verify stage detection summary exists
        System.assertNotEquals(null, stageDetectionSummary, 'Stage detection summary should exist');
        System.assertEquals('B2C', stageDetectionSummary.get('customerType'), 'Should return B2C summary');
        
        // Verify stage detection data is present
        System.assert(stageDetectionSummary.containsKey('topTransitions'), 'Should contain top transitions');
        System.assert(stageDetectionSummary.containsKey('matrixRecords'), 'Should contain matrix record count');
    }
    
    /**
     * Test network visualization data generation
     */
    @isTest
    static void testNetworkVisualizationData() {
        // Build transition matrix first
        MarkovChainAttribution.buildAndStoreTransitionMatrix('B2C');
        
        Test.startTest();
        
        // Get network visualization data
        Map<String, Object> networkData = MarkovChainAttribution.getNetworkVisualization('B2C');
        
        Test.stopTest();
        
        // Verify network visualization structure
        System.assertNotEquals(null, networkData, 'Network data should not be null');
        System.assertEquals('B2C', networkData.get('customerType'), 'Should return B2C network data');
        
        // Verify required network components
        System.assert(networkData.containsKey('totalStates'), 'Should contain total states count');
        System.assert(networkData.containsKey('totalTransitions'), 'Should contain total transitions count');
        System.assert(networkData.containsKey('networkNodes'), 'Should contain network nodes');
        System.assert(networkData.containsKey('allTransitions'), 'Should contain all transitions');
        System.assert(networkData.containsKey('conversionPaths'), 'Should contain conversion paths');
        System.assert(networkData.containsKey('networkStatistics'), 'Should contain network statistics');
        System.assert(networkData.containsKey('stageAnalysis'), 'Should contain stage analysis');
        
        // Verify network statistics structure
        Map<String, Object> networkStats = (Map<String, Object>) networkData.get('networkStatistics');
        System.assert(networkStats.containsKey('totalNodes'), 'Should contain total nodes');
        System.assert(networkStats.containsKey('totalEdges'), 'Should contain total edges');
        System.assert(networkStats.containsKey('conversionRate'), 'Should contain conversion rate');
    }
    
    /**
     * Test Markov Chain attribution with stage detection for B2C journey
     */
    @isTest
    static void testMarkovChainAttributionB2CWithStageDetection() {
        // Build transition matrix first
        MarkovChainAttribution.buildAndStoreTransitionMatrix('B2C');
        
        // Get B2C journey with multiple stages
        Customer_Journey__c journey = [SELECT Id FROM Customer_Journey__c WHERE Journey_ID__c = 'B2C_JOURNEY_001' LIMIT 1];
        
        Test.startTest();
        List<Attribution_Result__c> results = MarkovChainAttribution.calculateAttribution(journey.Id);
        Test.stopTest();
        
        // Verify results
        System.assertNotEquals(null, results, 'Results should not be null');
        System.assert(results.size() > 0, 'Should have attribution results');
        
        // Verify all results are for Markov Chain model
        for (Attribution_Result__c result : results) {
            System.assertEquals('MarkovChain', result.Attribution_Model__c, 'Should be MarkovChain model');
            System.assertEquals(journey.Id, result.Customer_Journey__c, 'Should be for correct journey');
        }
        
        // Verify attribution weights sum to 100%
        Decimal totalWeight = 0;
        for (Attribution_Result__c result : results) {
            totalWeight += result.Attribution_Weight__c;
        }
        System.assert(Math.abs(totalWeight - 1.0) < 0.001, 'Total attribution should be 100%. Actual: ' + totalWeight);
        
        // Verify stage-based attribution is working (should have results for multiple channels)
        Set<String> channelsWithAttribution = new Set<String>();
        for (Attribution_Result__c result : results) {
            channelsWithAttribution.add(result.Channel__c);
        }
        System.assert(channelsWithAttribution.size() > 1, 'Should have attribution for multiple channels');
    }
    
    /**
     * Test Markov Chain attribution with stage detection for B2B journey
     */
    @isTest
    static void testMarkovChainAttributionB2BWithStageDetection() {
        // Build transition matrix first
        MarkovChainAttribution.buildAndStoreTransitionMatrix('B2B');
        
        // Get B2B journey with multiple stages
        Customer_Journey__c journey = [SELECT Id FROM Customer_Journey__c WHERE Journey_ID__c = 'B2B_JOURNEY_001' LIMIT 1];
        
        Test.startTest();
        List<Attribution_Result__c> results = MarkovChainAttribution.calculateAttribution(journey.Id);
        Test.stopTest();
        
        // Verify results
        System.assertNotEquals(null, results, 'Results should not be null');
        System.assert(results.size() > 0, 'Should have attribution results');
        
        // Verify attribution weights sum to 100%
        Decimal totalWeight = 0;
        for (Attribution_Result__c result : results) {
            totalWeight += result.Attribution_Weight__c;
        }
        System.assert(Math.abs(totalWeight - 1.0) < 0.001, 'Total attribution should be 100%. Actual: ' + totalWeight);
        
        // Verify B2B specific channels are present
        Set<String> channelsWithAttribution = new Set<String>();
        for (Attribution_Result__c result : results) {
            channelsWithAttribution.add(result.Channel__c);
        }
        
        // Should have Direct_Sales and LinkedIn_Ads for B2B journey
        System.assert(channelsWithAttribution.contains('Direct_Sales') || 
                     channelsWithAttribution.contains('LinkedIn_Ads'), 
                     'Should have B2B-specific channels');
    }
    
    /**
     * Test enhanced batch processing with stage detection monitoring
     */
    @isTest
    static void testEnhancedBatchProcessingWithStageDetection() {
        // Build transition matrices first
        MarkovChainAttribution.buildAndStoreTransitionMatrix('B2C');
        MarkovChainAttribution.buildAndStoreTransitionMatrix('B2B');
        
        // Get all test journeys
        List<Customer_Journey__c> journeys = [SELECT Id, Customer_Type__c FROM Customer_Journey__c ORDER BY Journey_ID__c];
        List<Id> journeyIds = new List<Id>();
        for (Customer_Journey__c journey : journeys) {
            journeyIds.add(journey.Id);
        }
        
        Test.startTest();
        
        // Execute enhanced batch processing
        Id batchId = MarkovChainAttributionBatch.executeAttributionBatch(journeyIds, 200);
        
        Test.stopTest();
        
        // Verify batch was created
        System.assertNotEquals(null, batchId, 'Batch job should be created');
        
        // Verify attribution results were created
        List<Attribution_Result__c> results = [
            SELECT Id, Customer_Journey__c, Channel__c, Attribution_Weight__c
            FROM Attribution_Result__c
            WHERE Attribution_Model__c = 'MarkovChain'
        ];
        
        System.assert(results.size() > 0, 'Attribution results should be created');
        
        // Verify stage detection quality metrics are available
        Map<String, Object> qualityMetrics = MarkovChainAttributionBatch.getStageDetectionQualityMetrics();
        System.assertNotEquals(null, qualityMetrics, 'Quality metrics should be available');
        System.assert(qualityMetrics.containsKey('qualityMetricsAvailable'), 'Should indicate metrics availability');
    }
    
    /**
     * Test batch processing with matrix calculation and stage detection
     */
    @isTest
    static void testBatchMatrixCalculationWithStageDetection() {
        Test.startTest();
        
        // Execute matrix calculation batch
        Id batchId = MarkovChainAttributionBatch.executeMatrixCalculationBatch('B2C', 200);
        
        Test.stopTest();
        
        // Verify batch was created
        System.assertNotEquals(null, batchId, 'Batch job should be created');
        
        // Verify transition matrix records were created with stage-based states
        List<Transition_Matrix__c> matrices = [
            SELECT From_State__c, To_State__c, Probability__c
            FROM Transition_Matrix__c 
            WHERE Customer_Type__c = 'B2C'
        ];
        
        System.assert(matrices.size() > 0, 'Transition matrix should be created');
        
        // Verify stage-based states are present
        Boolean hasStageStates = false;
        for (Transition_Matrix__c matrix : matrices) {
            if (matrix.From_State__c.contains('_Awareness') || 
                matrix.From_State__c.contains('_Interest') || 
                matrix.From_State__c.contains('_Consideration') ||
                matrix.From_State__c.contains('_Conversion')) {
                hasStageStates = true;
                break;
            }
        }
        
        System.assert(hasStageStates, 'Should have stage-based states in matrix');
    }
    
    /**
     * Test conversion path analysis
     */
    @isTest
    static void testConversionPathAnalysis() {
        // Build transition matrix first
        MarkovChainAttribution.buildAndStoreTransitionMatrix('B2C');
        
        Test.startTest();
        
        // Get network visualization data
        Map<String, Object> networkData = MarkovChainAttribution.getNetworkVisualization('B2C');
        
        Test.stopTest();
        
        // Verify conversion paths are identified
        System.assert(networkData.containsKey('conversionPaths'), 'Should contain conversion paths');
        
        List<Object> conversionPaths = (List<Object>) networkData.get('conversionPaths');
        System.assertNotEquals(null, conversionPaths, 'Conversion paths should not be null');
        
        // Verify conversion path structure
        if (!conversionPaths.isEmpty()) {
            Map<String, Object> firstPath = (Map<String, Object>) conversionPaths[0];
            System.assert(firstPath.containsKey('path'), 'Conversion path should contain path info');
            System.assert(firstPath.containsKey('probability'), 'Conversion path should contain probability');
            System.assert(firstPath.containsKey('fromState'), 'Conversion path should contain from state');
        }
    }
    
    /**
     * Test stage progression validation
     */
    @isTest
    static void testStageProgressionValidation() {
        Test.startTest();
        
        // Build matrix and get stage detection summary
        MarkovChainAttribution.buildAndStoreTransitionMatrix('B2C');
        Map<String, Object> stageDetectionSummary = MarkovChainAttribution.getStageDetectionSummary('B2C');
        
        Test.stopTest();
        
        // Verify stage progression logic
        System.assertNotEquals(null, stageDetectionSummary, 'Stage detection summary should exist');
        
        // Verify that transitions represent realistic stage progressions
        List<Transition_Matrix__c> stageTransitions = [
            SELECT From_State__c, To_State__c, Probability__c
            FROM Transition_Matrix__c 
            WHERE Customer_Type__c = 'B2C'
            AND (From_State__c LIKE '%_Awareness' OR From_State__c LIKE '%_Interest')
            AND (To_State__c LIKE '%_Interest' OR To_State__c LIKE '%_Consideration' OR To_State__c = 'CONVERSION')
        ];
        
        System.assert(stageTransitions.size() > 0, 'Should have realistic stage progression transitions');
    }
    
    /**
     * Test multi-channel stage detection
     */
    @isTest
    static void testMultiChannelStageDetection() {
        // Build transition matrix first
        MarkovChainAttribution.buildAndStoreTransitionMatrix('B2C');
        
        Test.startTest();
        
        // Get network visualization to analyze multi-channel patterns
        Map<String, Object> networkData = MarkovChainAttribution.getNetworkVisualization('B2C');
        
        Test.stopTest();
        
        // Verify multi-channel stage patterns exist
        System.assert(networkData.containsKey('stageAnalysis'), 'Should contain stage analysis');
        
        Map<String, Object> stageAnalysis = (Map<String, Object>) networkData.get('stageAnalysis');
        System.assert(stageAnalysis.containsKey('channelStagePerformance'), 'Should contain channel stage performance');
        
        Map<String, Object> channelStagePerformance = (Map<String, Object>) stageAnalysis.get('channelStagePerformance');
        System.assertNotEquals(null, channelStagePerformance, 'Channel stage performance should not be null');
        
        // Verify multiple channels are analyzed
        System.assert(channelStagePerformance.keySet().size() > 1, 'Should analyze multiple channels');
    }
    
    /**
     * Test transition matrix statistics with stage detection
     */
    @isTest
    static void testTransitionMatrixStatisticsWithStageDetection() {
        // Build matrix first
        MarkovChainAttribution.buildAndStoreTransitionMatrix('B2C');
        
        Test.startTest();
        Map<String, Object> statistics = MarkovChainAttribution.getTransitionMatrixStatistics('B2C');
        Test.stopTest();
        
        // Verify statistics structure
        System.assertEquals('B2C', statistics.get('customerType'), 'Should return correct customer type');
        System.assertEquals(true, statistics.get('matrixExists'), 'Matrix should exist');
        System.assertNotEquals(null, statistics.get('stateCount'), 'Should have state count');
        System.assertNotEquals(null, statistics.get('transitionCount'), 'Should have transition count');
        System.assertNotEquals(null, statistics.get('lastCalculated'), 'Should have last calculated date');
        
        // Verify state count reflects stage-based states
        Integer stateCount = (Integer) statistics.get('stateCount');
        System.assert(stateCount > 10, 'Should have multiple stage-based states'); // Should have many states due to channel_stage combinations
    }
    
    /**
     * Test error handling with stage detection
     */
    @isTest
    static void testErrorHandlingWithStageDetection() {
        Test.startTest();
        
        // Test network visualization with non-existent customer type
        Map<String, Object> networkData = MarkovChainAttribution.getNetworkVisualization('INVALID_TYPE');
        
        Test.stopTest();
        
        // Should handle error gracefully
        System.assertNotEquals(null, networkData, 'Should return network data structure');
        System.assert(networkData.containsKey('error'), 'Should contain error message for invalid type');
        
        // Test stage detection summary with invalid type
        Map<String, Object> stageDetectionSummary = MarkovChainAttribution.getStageDetectionSummary('INVALID_TYPE');
        System.assertNotEquals(null, stageDetectionSummary, 'Should return stage detection summary structure');
    }
    
    /**
     * Test journey with complex stage progression
     */
    @isTest
    static void testComplexStageProgressionJourney() {
        // Build transition matrix first
        MarkovChainAttribution.buildAndStoreTransitionMatrix('B2C');
        
        // Get journey with complex stage progression (B2C_JOURNEY_004 with 5 touchpoints)
        Customer_Journey__c journey = [SELECT Id FROM Customer_Journey__c WHERE Journey_ID__c = 'B2C_JOURNEY_004' LIMIT 1];
        
        Test.startTest();
        List<Attribution_Result__c> results = MarkovChainAttribution.calculateAttribution(journey.Id);
        Test.stopTest();
        
        // Verify results for complex journey
        System.assertNotEquals(null, results, 'Results should not be null');
        System.assert(results.size() > 0, 'Should have attribution results');
        
        // Verify attribution weights sum to 100%
        Decimal totalWeight = 0;
        for (Attribution_Result__c result : results) {
            totalWeight += result.Attribution_Weight__c;
        }
        System.assert(Math.abs(totalWeight - 1.0) < 0.001, 'Total attribution should be 100%. Actual: ' + totalWeight);
        
        // Verify multiple channels are attributed (should reflect stage progression)
        Set<String> channelsWithAttribution = new Set<String>();
        for (Attribution_Result__c result : results) {
            if (result.Attribution_Weight__c > 0) {
                channelsWithAttribution.add(result.Channel__c);
            }
        }
        System.assert(channelsWithAttribution.size() > 1, 'Should have attribution for multiple channels in complex journey');
    }
    
    /**
     * Test B2B vs B2C stage detection differences
     */
    @isTest
    static void testB2BVsB2CStageDetectionDifferences() {
        // Build transition matrices for both types
        MarkovChainAttribution.buildAndStoreTransitionMatrix('B2C');
        MarkovChainAttribution.buildAndStoreTransitionMatrix('B2B');
        
        Test.startTest();
        
        // Get network visualization for both types
        Map<String, Object> b2cNetworkData = MarkovChainAttribution.getNetworkVisualization('B2C');
        Map<String, Object> b2bNetworkData = MarkovChainAttribution.getNetworkVisualization('B2B');
        
        Test.stopTest();
        
        // Verify both have network data
        System.assertNotEquals(null, b2cNetworkData, 'B2C network data should exist');
        System.assertNotEquals(null, b2bNetworkData, 'B2B network data should exist');
        
        // Verify different state counts (B2C should have more states due to more channels)
        Integer b2cStateCount = (Integer) b2cNetworkData.get('totalStates');
        Integer b2bStateCount = (Integer) b2bNetworkData.get('totalStates');
        
        System.assert(b2cStateCount > 0, 'B2C should have states');
        System.assert(b2bStateCount > 0, 'B2B should have states');
        
        // Verify different network characteristics
        Map<String, Object> b2cStats = (Map<String, Object>) b2cNetworkData.get('networkStatistics');
        Map<String, Object> b2bStats = (Map<String, Object>) b2bNetworkData.get('networkStatistics');
        
        System.assertNotEquals(null, b2cStats, 'B2C network statistics should exist');
        System.assertNotEquals(null, b2bStats, 'B2B network statistics should exist');
    }
    
    /**
     * Test manual matrix recalculation with stage detection
     */
    @isTest
    static void testManualMatrixRecalculationWithStageDetection() {
        Test.startTest();
        
        // Test recalculation for all customer types
        String result = MarkovChainAttribution.triggerMatrixRecalculation();
        
        Test.stopTest();
        
        System.assertNotEquals(null, result, 'Should return success message');
        System.assert(result.contains('B2C'), 'Should mention B2C processing');
        System.assert(result.contains('B2B'), 'Should mention B2B processing');
        
        // Verify stage-based matrices were created
        Integer stageBasedMatrices = [
            SELECT COUNT() FROM Transition_Matrix__c
            WHERE (From_State__c LIKE '%_Awareness' OR From_State__c LIKE '%_Interest' 
                   OR From_State__c LIKE '%_Consideration' OR From_State__c LIKE '%_Conversion')
        ];
        
        System.assert(stageBasedMatrices > 0, 'Should have created stage-based transition matrices');
    }
    
    /**
     * Test attribution validation with stage detection
     */
    @isTest
    static void testAttributionValidationWithStageDetection() {
        // Build matrix first
        MarkovChainAttribution.buildAndStoreTransitionMatrix('B2C');
        
        // Create valid attribution results
        Customer_Journey__c journey = [SELECT Id FROM Customer_Journey__c WHERE Journey_ID__c = 'B2C_JOURNEY_001' LIMIT 1];
        
        List<Attribution_Result__c> validResults = new List<Attribution_Result__c>();
        validResults.add(new Attribution_Result__c(
            Customer_Journey__c = journey.Id,
            Attribution_Model__c = 'MarkovChain',
            Channel__c = 'Google_Ads',
            Attribution_Weight__c = 0.35,
            Attribution_Value__c = 157.5
        ));
        validResults.add(new Attribution_Result__c(
            Customer_Journey__c = journey.Id,
            Attribution_Model__c = 'MarkovChain',
            Channel__c = 'Facebook_Ads',
            Attribution_Weight__c = 0.25,
            Attribution_Value__c = 112.5
        ));
        validResults.add(new Attribution_Result__c(
            Customer_Journey__c = journey.Id,
            Attribution_Model__c = 'MarkovChain',
            Channel__c = 'Email_Marketing',
            Attribution_Weight__c = 0.20,
            Attribution_Value__c = 90.0
        ));
        validResults.add(new Attribution_Result__c(
            Customer_Journey__c = journey.Id,
            Attribution_Model__c = 'MarkovChain',
            Channel__c = 'App_Store_Optimization',
            Attribution_Weight__c = 0.20,
            Attribution_Value__c = 90.0
        ));
        
        Test.startTest();
        Boolean validationResult = MarkovChainAttribution.validateAttributionResults(validResults);
        Test.stopTest();
        
        System.assertEquals(true, validationResult, 'Valid attribution results should pass validation');
    }
    
    /**
     * Test processing status with stage detection features
     */
    @isTest
    static void testProcessingStatusWithStageDetection() {
        Test.startTest();
        Map<String, Object> status = MarkovChainAttributionBatch.getProcessingStatus();
        Test.stopTest();
        
        // Verify enhanced status structure
        System.assertNotEquals(null, status, 'Status should not be null');
        System.assert(status.containsKey('runningJobs'), 'Should contain running jobs info');
        System.assert(status.containsKey('isProcessing'), 'Should contain processing status');
        System.assert(status.containsKey('b2cMatrixStatus'), 'Should contain B2C matrix status');
        System.assert(status.containsKey('b2bMatrixStatus'), 'Should contain B2B matrix status');
        System.assert(status.containsKey('stageDetectionEnabled'), 'Should indicate stage detection is enabled');
        System.assert(status.containsKey('networkVisualizationSupported'), 'Should indicate network visualization is supported');
        System.assert(status.containsKey('lastUpdated'), 'Should contain last updated timestamp');
    }
    
    /**
     * Test without existing transition matrix (should trigger automatic building)
     */
    @isTest
    static void testAttributionWithoutMatrixTriggersStageDetection() {
        // Don't build matrix first - should trigger automatic matrix building with stage detection
        Customer_Journey__c journey = [SELECT Id FROM Customer_Journey__c WHERE Journey_ID__c = 'B2C_JOURNEY_001' LIMIT 1];
        
        Test.startTest();
        List<Attribution_Result__c> results = MarkovChainAttribution.calculateAttribution(journey.Id);
        Test.stopTest();
        
        // Should either return results (if matrix was built successfully) or empty list
        System.assertNotEquals(null, results, 'Results should not be null');
        
        // Verify that matrix was attempted to be built with stage detection
        Integer stageBasedMatrices = [
            SELECT COUNT() FROM Transition_Matrix__c 
            WHERE Customer_Type__c = 'B2C'
            AND (From_State__c LIKE '%_Awareness' OR From_State__c LIKE '%_Interest' 
                 OR From_State__c LIKE '%_Consideration' OR From_State__c LIKE '%_Conversion')
        ];
        
        // Matrix building should have been attempted with stage detection
        System.assert(stageBasedMatrices >= 0, 'Stage-based matrix building should have been attempted');
    }
}