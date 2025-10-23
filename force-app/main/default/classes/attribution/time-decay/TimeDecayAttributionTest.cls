/**
 * TimeDecayAttributionTest
 * 
 * Comprehensive test class for TimeDecayAttribution and TimeDecayAttributionBatch classes.
 * Provides test coverage for all methods and edge cases to ensure reliable time decay
 * attribution calculation functionality.
 * 
 * Test Coverage:
 * - Single journey attribution calculation with time decay
 * - Batch processing for multiple journeys
 * - Time decay weight calculation and normalization
 * - Error handling and validation
 * - Edge cases and boundary conditions
 * - Different decay factor scenarios
 * - Reference date calculation logic
 * 
 * Author: Alexandru Constantinescu (www.linkedin.com/in/alexandru-constantinescu-6a18b6117)
 * Project: Omnichannel Attribution Platform
 * Date: 2025
 * Version: 1.3
 */
@isTest
public class TimeDecayAttributionTest {
    
    /**
     * Test data setup method
     */
    @testSetup
    static void setupTestData() {
        // Create test campaigns
        List<Campaign__c> campaigns = new List<Campaign__c>();
        
        Campaign__c campaign1 = new Campaign__c();
        campaign1.Campaign_ID_External__c = 'TEST_GOOGLE_001';
        campaigns.add(campaign1);
        
        Campaign__c campaign2 = new Campaign__c();
        campaign2.Campaign_ID_External__c = 'TEST_FACEBOOK_001';
        campaigns.add(campaign2);
        
        Campaign__c campaign3 = new Campaign__c();
        campaign3.Campaign_ID_External__c = 'TEST_EMAIL_001';
        campaigns.add(campaign3);
        
        insert campaigns;
        
        // Create test customer journeys
        List<Customer_Journey__c> journeys = new List<Customer_Journey__c>();
        
        // Journey 1: B2C journey with conversion - 7 days duration
        Customer_Journey__c journey1 = new Customer_Journey__c();
        journey1.Journey_ID__c = 'JOURNEY_001';
        journey1.Customer_ID__c = 'CUSTOMER_001';
        journey1.Customer_Type__c = 'B2C';
        journey1.Converted__c = true;
        journey1.Conversion_Value__c = 100.00;
        journey1.Journey_Start_Date__c = DateTime.now().addDays(-7);
        journey1.Journey_End_Date__c = DateTime.now().addDays(-1);
        journey1.Total_Touchpoints__c = 3;
        journey1.Confidence_Score__c = 0.85;
        journey1.Confidence_Level__c = 'High';
        journeys.add(journey1);
        
        // Journey 2: B2B journey with conversion - 14 days duration
        Customer_Journey__c journey2 = new Customer_Journey__c();
        journey2.Journey_ID__c = 'JOURNEY_002';
        journey2.Customer_ID__c = 'CUSTOMER_002';
        journey2.Customer_Type__c = 'B2B';
        journey2.Converted__c = true;
        journey2.Conversion_Value__c = 500.00;
        journey2.Journey_Start_Date__c = DateTime.now().addDays(-14);
        journey2.Journey_End_Date__c = DateTime.now().addDays(-2);
        journey2.Total_Touchpoints__c = 4;
        journey2.Confidence_Score__c = 0.90;
        journey2.Confidence_Level__c = 'High';
        journeys.add(journey2);
        
        // Journey 3: Non-converting journey with single touchpoint
        Customer_Journey__c journey3 = new Customer_Journey__c();
        journey3.Journey_ID__c = 'JOURNEY_003';
        journey3.Customer_ID__c = 'CUSTOMER_003';
        journey3.Customer_Type__c = 'B2C';
        journey3.Converted__c = false;
        journey3.Conversion_Value__c = 0.00;
        journey3.Journey_Start_Date__c = DateTime.now().addDays(-3);
        journey3.Journey_End_Date__c = DateTime.now().addDays(-1);
        journey3.Total_Touchpoints__c = 1;
        journey3.Confidence_Score__c = 0.75;
        journey3.Confidence_Level__c = 'Medium';
        journeys.add(journey3);
        
        // Journey 4: B2C journey with different time gaps for testing
        Customer_Journey__c journey4 = new Customer_Journey__c();
        journey4.Journey_ID__c = 'JOURNEY_004';
        journey4.Customer_ID__c = 'CUSTOMER_004';
        journey4.Customer_Type__c = 'B2C';
        journey4.Converted__c = true;
        journey4.Conversion_Value__c = 200.00;
        journey4.Journey_Start_Date__c = DateTime.now().addDays(-5);
        journey4.Journey_End_Date__c = DateTime.now().addDays(-1);
        journey4.Total_Touchpoints__c = 2;
        journey4.Confidence_Score__c = 0.80;
        journey4.Confidence_Level__c = 'High';
        journeys.add(journey4);
        
        insert journeys;
        
        // Create touchpoints for Journey 1 (Google -> Facebook -> Email with time decay)
        List<Touchpoint__c> touchpoints1 = new List<Touchpoint__c>();
        
        Touchpoint__c tp1 = new Touchpoint__c();
        tp1.Touchpoint_ID__c = 'TP_001_001';
        tp1.Customer_Journey__c = journeys[0].Id;
        tp1.Channel__c = 'Google_Ads';
        tp1.Touchpoint_Timestamp__c = DateTime.now().addDays(-7); // 6 days before end
        tp1.Campaign__c = campaigns[0].Id;
        touchpoints1.add(tp1);
        
        Touchpoint__c tp2 = new Touchpoint__c();
        tp2.Touchpoint_ID__c = 'TP_001_002';
        tp2.Customer_Journey__c = journeys[0].Id;
        tp2.Channel__c = 'Facebook_Ads';
        tp2.Touchpoint_Timestamp__c = DateTime.now().addDays(-4); // 3 days before end
        tp2.Campaign__c = campaigns[1].Id;
        touchpoints1.add(tp2);
        
        Touchpoint__c tp3 = new Touchpoint__c();
        tp3.Touchpoint_ID__c = 'TP_001_003';
        tp3.Customer_Journey__c = journeys[0].Id;
        tp3.Channel__c = 'Email_Marketing';
        tp3.Touchpoint_Timestamp__c = DateTime.now().addDays(-2); // 1 day before end
        tp3.Campaign__c = campaigns[2].Id;
        touchpoints1.add(tp3);
        
        // Create touchpoints for Journey 2 (B2B with longer timeframe)
        List<Touchpoint__c> touchpoints2 = new List<Touchpoint__c>();
        
        Touchpoint__c tp4 = new Touchpoint__c();
        tp4.Touchpoint_ID__c = 'TP_002_001';
        tp4.Customer_Journey__c = journeys[1].Id;
        tp4.Channel__c = 'LinkedIn_Ads';
        tp4.Touchpoint_Timestamp__c = DateTime.now().addDays(-14); // 12 days before end
        tp4.Campaign__c = campaigns[0].Id;
        touchpoints2.add(tp4);
        
        Touchpoint__c tp5 = new Touchpoint__c();
        tp5.Touchpoint_ID__c = 'TP_002_002';
        tp5.Customer_Journey__c = journeys[1].Id;
        tp5.Channel__c = 'Email_Marketing';
        tp5.Touchpoint_Timestamp__c = DateTime.now().addDays(-10); // 8 days before end
        tp5.Campaign__c = campaigns[2].Id;
        touchpoints2.add(tp5);
        
        Touchpoint__c tp6 = new Touchpoint__c();
        tp6.Touchpoint_ID__c = 'TP_002_003';
        tp6.Customer_Journey__c = journeys[1].Id;
        tp6.Channel__c = 'Content_Website_SEO';
        tp6.Touchpoint_Timestamp__c = DateTime.now().addDays(-6); // 4 days before end
        tp6.Campaign__c = campaigns[1].Id;
        touchpoints2.add(tp6);
        
        Touchpoint__c tp7 = new Touchpoint__c();
        tp7.Touchpoint_ID__c = 'TP_002_004';
        tp7.Customer_Journey__c = journeys[1].Id;
        tp7.Channel__c = 'Google_Ads';
        tp7.Touchpoint_Timestamp__c = DateTime.now().addDays(-3); // 1 day before end
        tp7.Campaign__c = campaigns[0].Id;
        touchpoints2.add(tp7);
        
        // Create touchpoints for Journey 3 (Single touchpoint)
        List<Touchpoint__c> touchpoints3 = new List<Touchpoint__c>();
        
        Touchpoint__c tp8 = new Touchpoint__c();
        tp8.Touchpoint_ID__c = 'TP_003_001';
        tp8.Customer_Journey__c = journeys[2].Id;
        tp8.Channel__c = 'Google_Ads';
        tp8.Touchpoint_Timestamp__c = DateTime.now().addDays(-2);
        tp8.Campaign__c = campaigns[0].Id;
        touchpoints3.add(tp8);
        
        // Create touchpoints for Journey 4 (Different time gaps for decay testing)
        List<Touchpoint__c> touchpoints4 = new List<Touchpoint__c>();
        
        Touchpoint__c tp9 = new Touchpoint__c();
        tp9.Touchpoint_ID__c = 'TP_004_001';
        tp9.Customer_Journey__c = journeys[3].Id;
        tp9.Channel__c = 'Facebook_Ads';
        tp9.Touchpoint_Timestamp__c = DateTime.now().addDays(-5);
        tp9.Campaign__c = campaigns[1].Id;
        touchpoints4.add(tp9);
        
        Touchpoint__c tp10 = new Touchpoint__c();
        tp10.Touchpoint_ID__c = 'TP_004_002';
        tp10.Customer_Journey__c = journeys[3].Id;
        tp10.Channel__c = 'Email_Marketing';
        tp10.Touchpoint_Timestamp__c = DateTime.now().addDays(-2);
        tp10.Campaign__c = campaigns[2].Id;
        touchpoints4.add(tp10);
        
        List<Touchpoint__c> allTouchpoints = new List<Touchpoint__c>();
        allTouchpoints.addAll(touchpoints1);
        allTouchpoints.addAll(touchpoints2);
        allTouchpoints.addAll(touchpoints3);
        allTouchpoints.addAll(touchpoints4);
        
        insert allTouchpoints;
    }
    
    /**
     * Test single journey attribution calculation with time decay
     */
    @isTest
    static void testSingleJourneyTimeDecayAttribution() {
        // Get test journey
        Customer_Journey__c journey = [SELECT Id FROM Customer_Journey__c WHERE Journey_ID__c = 'JOURNEY_001' LIMIT 1];
        
        Test.startTest();
        List<Attribution_Result__c> results = TimeDecayAttribution.calculateAttribution(journey.Id);
        Test.stopTest();
        
        // Verify results
        System.assertNotEquals(null, results, 'Results should not be null');
        System.assertEquals(3, results.size(), 'Should have results for 3 channels');
        
        // Verify time decay attribution (recent touchpoints should have higher weight)
        Map<String, Attribution_Result__c> resultsByChannel = new Map<String, Attribution_Result__c>();
        for (Attribution_Result__c result : results) {
            resultsByChannel.put(result.Channel__c, result);
        }
        
        // Email Marketing (most recent) should have highest weight
        Decimal emailWeight = resultsByChannel.get('Email_Marketing').Attribution_Weight__c;
        Decimal facebookWeight = resultsByChannel.get('Facebook_Ads').Attribution_Weight__c;
        Decimal googleWeight = resultsByChannel.get('Google_Ads').Attribution_Weight__c;
        
        System.assert(emailWeight > facebookWeight, 'Email Marketing should have higher weight than Facebook Ads');
        System.assert(facebookWeight > googleWeight, 'Facebook Ads should have higher weight than Google Ads');
        
        // Verify weights sum to 100%
        Decimal totalWeight = emailWeight + facebookWeight + googleWeight;
        System.assert(Math.abs(totalWeight - 1.0) < 0.001, 'Total weights should sum to 100%');
        
        // Verify attribution values
        Decimal totalValue = 0;
        for (Attribution_Result__c result : results) {
            totalValue += result.Attribution_Value__c;
        }
        System.assert(Math.abs(totalValue - 100.0) < 0.001, 'Total attribution value should equal conversion value');
    }
    
    /**
     * Test B2B journey with gentler decay factor
     */
    @isTest
    static void testB2BJourneyDecayFactor() {
        // Get B2B test journey
        Customer_Journey__c journey = [SELECT Id FROM Customer_Journey__c WHERE Journey_ID__c = 'JOURNEY_002' LIMIT 1];
        
        Test.startTest();
        List<Attribution_Result__c> results = TimeDecayAttribution.calculateAttribution(journey.Id);
        Test.stopTest();
        
        // Verify results
        System.assertNotEquals(null, results, 'Results should not be null');
        System.assertEquals(4, results.size(), 'Should have results for 4 channels');
        
        // Verify weights sum to 100%
        Decimal totalWeight = 0;
        for (Attribution_Result__c result : results) {
            totalWeight += result.Attribution_Weight__c;
        }
        System.assert(Math.abs(totalWeight - 1.0) < 0.001, 'Total weights should sum to 100%');
        
        // Verify attribution values sum to conversion value
        Decimal totalValue = 0;
        for (Attribution_Result__c result : results) {
            totalValue += result.Attribution_Value__c;
        }
        System.assert(Math.abs(totalValue - 500.0) < 0.001, 'Total attribution value should equal conversion value');
    }
    
    /**
     * Test batch processing for multiple journeys
     */
    @isTest
    static void testBatchAttribution() {
        // Get test journeys
        List<Customer_Journey__c> journeys = [SELECT Id FROM Customer_Journey__c ORDER BY Journey_ID__c];
        List<Id> journeyIds = new List<Id>();
        for (Customer_Journey__c journey : journeys) {
            journeyIds.add(journey.Id);
        }
        
        Test.startTest();
        List<Attribution_Result__c> results = TimeDecayAttribution.calculateAttributionBatch(journeyIds);
        Test.stopTest();
        
        // Verify results - should have results for all journeys
        System.assertNotEquals(null, results, 'Results should not be null');
        System.assert(results.size() > 0, 'Should have results for all journeys');
        
        // Verify attribution totals per journey
        Map<Id, List<Attribution_Result__c>> resultsByJourney = new Map<Id, List<Attribution_Result__c>>();
        for (Attribution_Result__c result : results) {
            if (!resultsByJourney.containsKey(result.Customer_Journey__c)) {
                resultsByJourney.put(result.Customer_Journey__c, new List<Attribution_Result__c>());
            }
            resultsByJourney.get(result.Customer_Journey__c).add(result);
        }
        
        // Each journey should have weights that sum to 100%
        for (Id journeyId : resultsByJourney.keySet()) {
            Decimal totalAttribution = 0;
            
            for (Attribution_Result__c result : resultsByJourney.get(journeyId)) {
                totalAttribution += result.Attribution_Weight__c;
            }
            
            System.assert(Math.abs(totalAttribution - 1.0) < 0.001, 
                         'Total attribution should be 100% for journey ' + journeyId);
        }
    }
    
    /**
     * Test time decay calculation with different time gaps
     */
    @isTest
    static void testTimeDecayCalculationWithDifferentGaps() {
        // Get journey with different time gaps
        Customer_Journey__c journey = [SELECT Id FROM Customer_Journey__c WHERE Journey_ID__c = 'JOURNEY_004' LIMIT 1];
        
        Test.startTest();
        List<Attribution_Result__c> results = TimeDecayAttribution.calculateAttribution(journey.Id);
        Test.stopTest();
        
        // Verify results
        System.assertNotEquals(null, results, 'Results should not be null');
        System.assertEquals(2, results.size(), 'Should have results for 2 channels');
        
        // Verify weights sum to 100%
        Decimal totalWeight = 0;
        for (Attribution_Result__c result : results) {
            totalWeight += result.Attribution_Weight__c;
        }
        System.assert(Math.abs(totalWeight - 1.0) < 0.001, 'Total weights should sum to 100%');
        
        // Email Marketing should have higher weight as it's more recent
        Map<String, Attribution_Result__c> resultsByChannel = new Map<String, Attribution_Result__c>();
        for (Attribution_Result__c result : results) {
            resultsByChannel.put(result.Channel__c, result);
        }
        
        Decimal emailWeight = resultsByChannel.get('Email_Marketing').Attribution_Weight__c;
        Decimal facebookWeight = resultsByChannel.get('Facebook_Ads').Attribution_Weight__c;
        
        System.assert(emailWeight > facebookWeight, 'Email Marketing should have higher weight than Facebook Ads due to time decay');
    }
    
    /**
     * Test attribution validation
     */
    @isTest
    static void testAttributionValidation() {
        // Create valid attribution results
        Customer_Journey__c journey = [SELECT Id FROM Customer_Journey__c WHERE Journey_ID__c = 'JOURNEY_001' LIMIT 1];
        
        List<Attribution_Result__c> validResults = new List<Attribution_Result__c>();
        validResults.add(new Attribution_Result__c(
            Customer_Journey__c = journey.Id,
            Attribution_Model__c = 'TimeDecay',
            Channel__c = 'Email_Marketing',
            Attribution_Weight__c = 0.5,
            Attribution_Value__c = 50.0
        ));
        validResults.add(new Attribution_Result__c(
            Customer_Journey__c = journey.Id,
            Attribution_Model__c = 'TimeDecay',
            Channel__c = 'Facebook_Ads',
            Attribution_Weight__c = 0.3,
            Attribution_Value__c = 30.0
        ));
        validResults.add(new Attribution_Result__c(
            Customer_Journey__c = journey.Id,
            Attribution_Model__c = 'TimeDecay',
            Channel__c = 'Google_Ads',
            Attribution_Weight__c = 0.2,
            Attribution_Value__c = 20.0
        ));
        
        Test.startTest();
        Boolean validationResult = TimeDecayAttribution.validateAttributionResults(validResults);
        Test.stopTest();
        
        System.assertEquals(true, validationResult, 'Valid attribution results should pass validation');
        
        // Test invalid attribution results (total != 100%)
        List<Attribution_Result__c> invalidResults = new List<Attribution_Result__c>();
        invalidResults.add(new Attribution_Result__c(
            Customer_Journey__c = journey.Id,
            Attribution_Model__c = 'TimeDecay',
            Channel__c = 'Email_Marketing',
            Attribution_Weight__c = 0.5,
            Attribution_Value__c = 50.0
        ));
        invalidResults.add(new Attribution_Result__c(
            Customer_Journey__c = journey.Id,
            Attribution_Model__c = 'TimeDecay',
            Channel__c = 'Google_Ads',
            Attribution_Weight__c = 0.3,
            Attribution_Value__c = 30.0
        ));
        
        Boolean invalidValidationResult = TimeDecayAttribution.validateAttributionResults(invalidResults);
        System.assertEquals(false, invalidValidationResult, 'Invalid attribution results should fail validation');
    }
    
    /**
     * Test trigger attribution calculation
     */
    @isTest
    static void testTriggerAttributionCalculation() {
        // Get test journey
        Customer_Journey__c journey = [SELECT Id FROM Customer_Journey__c WHERE Journey_ID__c = 'JOURNEY_001' LIMIT 1];
        List<Id> journeyIds = new List<Id>{journey.Id};
        
        Test.startTest();
        TimeDecayAttribution.triggerAttributionCalculation(journeyIds);
        Test.stopTest();
        
        // Verify attribution results were created
        List<Attribution_Result__c> results = [
            SELECT Id, Customer_Journey__c, Channel__c, Attribution_Weight__c, Attribution_Value__c
            FROM Attribution_Result__c
            WHERE Customer_Journey__c = :journey.Id
            AND Attribution_Model__c = 'TimeDecay'
        ];
        
        System.assertEquals(3, results.size(), 'Should have created attribution results for all channels');
        
        // Verify weights sum to 100%
        Decimal totalWeight = 0;
        for (Attribution_Result__c result : results) {
            totalWeight += result.Attribution_Weight__c;
        }
        System.assert(Math.abs(totalWeight - 1.0) < 0.001, 'Total weights should sum to 100%');
    }
    
    /**
     * Test batch processing class
     */
    @isTest
    static void testBatchProcessing() {
        // Get test journeys
        List<Customer_Journey__c> journeys = [SELECT Id FROM Customer_Journey__c];
        List<Id> journeyIds = new List<Id>();
        for (Customer_Journey__c journey : journeys) {
            journeyIds.add(journey.Id);
        }
        
        Test.startTest();
        Id batchId = TimeDecayAttributionBatch.executeBatch(journeyIds, 200);
        Test.stopTest();
        
        // Verify batch was created
        System.assertNotEquals(null, batchId, 'Batch job should be created');
        
        // Verify attribution results were created (in real scenario, would need to check after batch completes)
        List<Attribution_Result__c> results = [
            SELECT Id, Customer_Journey__c, Channel__c, Attribution_Weight__c
            FROM Attribution_Result__c
            WHERE Attribution_Model__c = 'TimeDecay'
        ];
        
        System.assert(results.size() > 0, 'Attribution results should be created');
    }
    
    /**
     * Test error handling
     */
    @isTest
    static void testErrorHandling() {
        // Test with null journey ID
        Test.startTest();
        try {
            TimeDecayAttribution.calculateAttribution(null);
            System.assert(false, 'Should have thrown exception for null journey ID');
        } catch (IllegalArgumentException e) {
            System.assertEquals('Journey ID cannot be null', e.getMessage());
        }
        
        // Test with empty journey list
        List<Attribution_Result__c> results = TimeDecayAttribution.calculateAttributionBatch(new List<Id>());
        System.assertEquals(0, results.size(), 'Should return empty list for empty input');
        
        Test.stopTest();
    }
    
    /**
     * Test journey with no touchpoints
     */
    @isTest
    static void testJourneyWithNoTouchpoints() {
        // Create journey without touchpoints
        Customer_Journey__c emptyJourney = new Customer_Journey__c();
        emptyJourney.Journey_ID__c = 'JOURNEY_EMPTY';
        emptyJourney.Customer_ID__c = 'CUSTOMER_EMPTY';
        emptyJourney.Customer_Type__c = 'B2C';
        emptyJourney.Converted__c = false;
        emptyJourney.Conversion_Value__c = 0.00;
        emptyJourney.Journey_Start_Date__c = DateTime.now().addDays(-1);
        emptyJourney.Journey_End_Date__c = DateTime.now();
        emptyJourney.Total_Touchpoints__c = 0;
        emptyJourney.Confidence_Score__c = 0.60;
        emptyJourney.Confidence_Level__c = 'Medium';
        
        insert emptyJourney;
        
        Test.startTest();
        List<Attribution_Result__c> results = TimeDecayAttribution.calculateAttribution(emptyJourney.Id);
        Test.stopTest();
        
        System.assertEquals(0, results.size(), 'Should return empty results for journey with no touchpoints');
    }
    
    /**
     * Test single touchpoint journey
     */
    @isTest
    static void testSingleTouchpointJourney() {
        // Get the journey with single touchpoint
        Customer_Journey__c journey = [SELECT Id FROM Customer_Journey__c WHERE Journey_ID__c = 'JOURNEY_003' LIMIT 1];
        
        Test.startTest();
        List<Attribution_Result__c> results = TimeDecayAttribution.calculateAttribution(journey.Id);
        Test.stopTest();
        
        // Verify single touchpoint gets 100% attribution
        System.assertEquals(1, results.size(), 'Should have result for single channel');
        System.assert(Math.abs(results[0].Attribution_Weight__c - 1.0) < 0.001, 'Single touchpoint should get 100% attribution');
        System.assertEquals('Google_Ads', results[0].Channel__c, 'Should be Google Ads channel');
    }
    
    /**
     * Test channel statistics functionality
     */
    @isTest
    static void testChannelStatistics() {
        // Get test journey and calculate attribution
        Customer_Journey__c journey = [SELECT Id FROM Customer_Journey__c WHERE Journey_ID__c = 'JOURNEY_001' LIMIT 1];
        List<Id> journeyIds = new List<Id>{journey.Id};
        
        // First calculate attribution to create results
        TimeDecayAttribution.triggerAttributionCalculation(journeyIds);
        
        Test.startTest();
        Map<String, Object> statistics = TimeDecayAttribution.getChannelStatistics(journeyIds);
        Test.stopTest();
        
        // Verify statistics were calculated
        System.assertNotEquals(null, statistics, 'Statistics should not be null');
        System.assert(statistics.size() > 0, 'Statistics should contain data');
        
        // Verify channel statistics exist
        System.assert(statistics.containsKey('Email_Marketing_total_attributions'), 
                     'Should have Email Marketing attribution count');
        System.assert(statistics.containsKey('Facebook_Ads_total_attributions'), 
                     'Should have Facebook Ads attribution count');
        System.assert(statistics.containsKey('Google_Ads_total_attributions'), 
                     'Should have Google Ads attribution count');
    }
    
    /**
     * Test batch with large dataset simulation
     */
    @isTest
    static void testBatchLargeDataset() {
        // Create additional journeys to simulate larger dataset
        List<Customer_Journey__c> additionalJourneys = new List<Customer_Journey__c>();
        for (Integer i = 0; i < 50; i++) {
            Customer_Journey__c journey = new Customer_Journey__c();
            journey.Journey_ID__c = 'JOURNEY_BULK_' + i;
            journey.Customer_ID__c = 'CUSTOMER_BULK_' + i;
            journey.Customer_Type__c = 'B2C';
            journey.Converted__c = true;
            journey.Conversion_Value__c = 75.00;
            journey.Journey_Start_Date__c = DateTime.now().addDays(-5);
            journey.Journey_End_Date__c = DateTime.now().addDays(-1);
            journey.Total_Touchpoints__c = 2;
            journey.Confidence_Score__c = 0.80;
            journey.Confidence_Level__c = 'High';
            additionalJourneys.add(journey);
        }
        insert additionalJourneys;
        
        // Create touchpoints for additional journeys
        List<Touchpoint__c> additionalTouchpoints = new List<Touchpoint__c>();
        for (Integer i = 0; i < additionalJourneys.size(); i++) {
            // First touchpoint
            Touchpoint__c touchpoint1 = new Touchpoint__c();
            touchpoint1.Touchpoint_ID__c = 'TP_BULK_' + i + '_1';
            touchpoint1.Customer_Journey__c = additionalJourneys[i].Id;
            touchpoint1.Channel__c = 'Google_Ads';
            touchpoint1.Touchpoint_Timestamp__c = DateTime.now().addDays(-4);
            additionalTouchpoints.add(touchpoint1);
            
            // Second touchpoint
            Touchpoint__c touchpoint2 = new Touchpoint__c();
            touchpoint2.Touchpoint_ID__c = 'TP_BULK_' + i + '_2';
            touchpoint2.Customer_Journey__c = additionalJourneys[i].Id;
            touchpoint2.Channel__c = 'Email_Marketing';
            touchpoint2.Touchpoint_Timestamp__c = DateTime.now().addDays(-2);
            additionalTouchpoints.add(touchpoint2);
        }
        insert additionalTouchpoints;
        
        Test.startTest();
        
        // Test batch class creation
        List<Id> journeyIds = new List<Id>();
        for (Customer_Journey__c journey : additionalJourneys) {
            journeyIds.add(journey.Id);
        }
        
        TimeDecayAttributionBatch batch = new TimeDecayAttributionBatch(journeyIds);
        System.assertNotEquals(null, batch, 'Batch should be instantiated');
        
        // Test synchronous processing for large datasets
        List<Attribution_Result__c> results = TimeDecayAttribution.calculateAttributionBatch(journeyIds);
        System.assert(results.size() > 0, 'Should generate attribution results for large dataset');
        
        Test.stopTest();
        
        // Each journey should have 2 channels, so 100 total results
        System.assertEquals(100, results.size(), 'Should have attribution results for all 50 journeys (2 channels each)');
        
        // Verify each journey's weights sum to 100%
        Map<Id, List<Attribution_Result__c>> resultsByJourney = new Map<Id, List<Attribution_Result__c>>();
        for (Attribution_Result__c result : results) {
            if (!resultsByJourney.containsKey(result.Customer_Journey__c)) {
                resultsByJourney.put(result.Customer_Journey__c, new List<Attribution_Result__c>());
            }
            resultsByJourney.get(result.Customer_Journey__c).add(result);
        }
        
        for (Id journeyId : resultsByJourney.keySet()) {
            Decimal totalWeight = 0;
            for (Attribution_Result__c result : resultsByJourney.get(journeyId)) {
                totalWeight += result.Attribution_Weight__c;
            }
            System.assert(Math.abs(totalWeight - 1.0) < 0.001, 
                         'Total weights should sum to 100% for journey ' + journeyId);
        }
    }
}