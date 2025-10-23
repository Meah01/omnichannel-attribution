/**
 * LinearAttributionTest
 * 
 * Comprehensive test class for LinearAttribution and LinearAttributionBatch classes.
 * Provides test coverage for all methods and edge cases to ensure reliable attribution
 * calculation functionality.
 * 
 * Test Coverage:
 * - Single journey attribution calculation
 * - Batch processing for multiple journeys
 * - Error handling and validation
 * - Edge cases and boundary conditions
 * - Linear attribution logic with multiple channels
 * - Channel statistics and comparison features
 * 
 * Author: Alexandru Constantinescu (www.linkedin.com/in/alexandru-constantinescu-6a18b6117)
 * Project: Omnichannel Attribution Platform
 * Date: 2025
 * Version: 1.0
 */
@isTest
public class LinearAttributionTest {
    
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
        
        // Journey 1: 3 touchpoints, 3 different channels (Google -> Facebook -> Email)
        Customer_Journey__c journey1 = new Customer_Journey__c();
        journey1.Journey_ID__c = 'JOURNEY_001';
        journey1.Customer_ID__c = 'CUSTOMER_001';
        journey1.Customer_Type__c = 'B2C';
        journey1.Converted__c = true;
        journey1.Conversion_Value__c = 300.00;
        journey1.Journey_Start_Date__c = DateTime.now().addDays(-5);
        journey1.Journey_End_Date__c = DateTime.now().addDays(-1);
        journey1.Total_Touchpoints__c = 3;
        journey1.Confidence_Score__c = 0.85;
        journey1.Confidence_Level__c = 'High';
        journeys.add(journey1);
        
        // Journey 2: 4 touchpoints, with Google appearing twice (Google -> Facebook -> Google -> Email)
        Customer_Journey__c journey2 = new Customer_Journey__c();
        journey2.Journey_ID__c = 'JOURNEY_002';
        journey2.Customer_ID__c = 'CUSTOMER_002';
        journey2.Customer_Type__c = 'B2B';
        journey2.Converted__c = true;
        journey2.Conversion_Value__c = 400.00;
        journey2.Journey_Start_Date__c = DateTime.now().addDays(-10);
        journey2.Journey_End_Date__c = DateTime.now().addDays(-2);
        journey2.Total_Touchpoints__c = 4;
        journey2.Confidence_Score__c = 0.90;
        journey2.Confidence_Level__c = 'High';
        journeys.add(journey2);
        
        // Journey 3: Single touchpoint (Google only)
        Customer_Journey__c journey3 = new Customer_Journey__c();
        journey3.Journey_ID__c = 'JOURNEY_003';
        journey3.Customer_ID__c = 'CUSTOMER_003';
        journey3.Customer_Type__c = 'B2C';
        journey3.Converted__c = true;
        journey3.Conversion_Value__c = 200.00;
        journey3.Journey_Start_Date__c = DateTime.now().addDays(-3);
        journey3.Journey_End_Date__c = DateTime.now().addDays(-1);
        journey3.Total_Touchpoints__c = 1;
        journey3.Confidence_Score__c = 0.75;
        journey3.Confidence_Level__c = 'Medium';
        journeys.add(journey3);
        
        // Journey 4: 2 touchpoints, same channel twice (Email -> Email)
        Customer_Journey__c journey4 = new Customer_Journey__c();
        journey4.Journey_ID__c = 'JOURNEY_004';
        journey4.Customer_ID__c = 'CUSTOMER_004';
        journey4.Customer_Type__c = 'B2C';
        journey4.Converted__c = true;
        journey4.Conversion_Value__c = 100.00;
        journey4.Journey_Start_Date__c = DateTime.now().addDays(-4);
        journey4.Journey_End_Date__c = DateTime.now().addDays(-2);
        journey4.Total_Touchpoints__c = 2;
        journey4.Confidence_Score__c = 0.80;
        journey4.Confidence_Level__c = 'High';
        journeys.add(journey4);
        
        insert journeys;
        
        // Create touchpoints for Journey 1 (Google -> Facebook -> Email)
        List<Touchpoint__c> touchpoints1 = new List<Touchpoint__c>();
        
        Touchpoint__c tp1 = new Touchpoint__c();
        tp1.Touchpoint_ID__c = 'TP_001_001';
        tp1.Customer_Journey__c = journeys[0].Id;
        tp1.Channel__c = 'Google_Ads';
        tp1.Touchpoint_Timestamp__c = DateTime.now().addDays(-5);
        tp1.Campaign__c = campaigns[0].Id;
        touchpoints1.add(tp1);
        
        Touchpoint__c tp2 = new Touchpoint__c();
        tp2.Touchpoint_ID__c = 'TP_001_002';
        tp2.Customer_Journey__c = journeys[0].Id;
        tp2.Channel__c = 'Facebook_Ads';
        tp2.Touchpoint_Timestamp__c = DateTime.now().addDays(-3);
        tp2.Campaign__c = campaigns[1].Id;
        touchpoints1.add(tp2);
        
        Touchpoint__c tp3 = new Touchpoint__c();
        tp3.Touchpoint_ID__c = 'TP_001_003';
        tp3.Customer_Journey__c = journeys[0].Id;
        tp3.Channel__c = 'Email_Marketing';
        tp3.Touchpoint_Timestamp__c = DateTime.now().addDays(-1);
        tp3.Campaign__c = campaigns[2].Id;
        touchpoints1.add(tp3);
        
        // Create touchpoints for Journey 2 (Google -> Facebook -> Google -> Email)
        List<Touchpoint__c> touchpoints2 = new List<Touchpoint__c>();
        
        Touchpoint__c tp4 = new Touchpoint__c();
        tp4.Touchpoint_ID__c = 'TP_002_001';
        tp4.Customer_Journey__c = journeys[1].Id;
        tp4.Channel__c = 'Google_Ads';
        tp4.Touchpoint_Timestamp__c = DateTime.now().addDays(-10);
        tp4.Campaign__c = campaigns[0].Id;
        touchpoints2.add(tp4);
        
        Touchpoint__c tp5 = new Touchpoint__c();
        tp5.Touchpoint_ID__c = 'TP_002_002';
        tp5.Customer_Journey__c = journeys[1].Id;
        tp5.Channel__c = 'Facebook_Ads';
        tp5.Touchpoint_Timestamp__c = DateTime.now().addDays(-7);
        tp5.Campaign__c = campaigns[1].Id;
        touchpoints2.add(tp5);
        
        Touchpoint__c tp6 = new Touchpoint__c();
        tp6.Touchpoint_ID__c = 'TP_002_003';
        tp6.Customer_Journey__c = journeys[1].Id;
        tp6.Channel__c = 'Google_Ads';
        tp6.Touchpoint_Timestamp__c = DateTime.now().addDays(-4);
        tp6.Campaign__c = campaigns[0].Id;
        touchpoints2.add(tp6);
        
        Touchpoint__c tp7 = new Touchpoint__c();
        tp7.Touchpoint_ID__c = 'TP_002_004';
        tp7.Customer_Journey__c = journeys[1].Id;
        tp7.Channel__c = 'Email_Marketing';
        tp7.Touchpoint_Timestamp__c = DateTime.now().addDays(-2);
        tp7.Campaign__c = campaigns[2].Id;
        touchpoints2.add(tp7);
        
        // Create touchpoints for Journey 3 (Single Google touchpoint)
        List<Touchpoint__c> touchpoints3 = new List<Touchpoint__c>();
        
        Touchpoint__c tp8 = new Touchpoint__c();
        tp8.Touchpoint_ID__c = 'TP_003_001';
        tp8.Customer_Journey__c = journeys[2].Id;
        tp8.Channel__c = 'Google_Ads';
        tp8.Touchpoint_Timestamp__c = DateTime.now().addDays(-2);
        tp8.Campaign__c = campaigns[0].Id;
        touchpoints3.add(tp8);
        
        // Create touchpoints for Journey 4 (Email -> Email)
        List<Touchpoint__c> touchpoints4 = new List<Touchpoint__c>();
        
        Touchpoint__c tp9 = new Touchpoint__c();
        tp9.Touchpoint_ID__c = 'TP_004_001';
        tp9.Customer_Journey__c = journeys[3].Id;
        tp9.Channel__c = 'Email_Marketing';
        tp9.Touchpoint_Timestamp__c = DateTime.now().addDays(-4);
        tp9.Campaign__c = campaigns[2].Id;
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
     * Test linear attribution for journey with 3 different channels
     */
    @isTest
    static void testLinearAttributionThreeChannels() {
        // Get journey with 3 touchpoints, 3 different channels
        Customer_Journey__c journey = [SELECT Id FROM Customer_Journey__c WHERE Journey_ID__c = 'JOURNEY_001' LIMIT 1];
        
        Test.startTest();
        List<Attribution_Result__c> results = LinearAttribution.calculateAttribution(journey.Id);
        Test.stopTest();
        
        // Verify results
        System.assertNotEquals(null, results, 'Results should not be null');
        System.assertEquals(3, results.size(), 'Should have results for 3 channels');
        
        // Each channel should get 33.33% (1/3) attribution
        Map<String, Attribution_Result__c> resultsByChannel = new Map<String, Attribution_Result__c>();
        for (Attribution_Result__c result : results) {
            resultsByChannel.put(result.Channel__c, result);
        }
        
        Decimal expectedWeight = 1.0 / 3.0; // 33.33%
        Decimal expectedValue = 300.00 / 3.0; // €100 each
        Decimal tolerance = 0.001; // Allow for floating point precision issues
        
        System.assert(Math.abs(resultsByChannel.get('Google_Ads').Attribution_Weight__c - expectedWeight) < tolerance, 
                     'Google Ads should get 33.33% attribution');
        System.assert(Math.abs(resultsByChannel.get('Facebook_Ads').Attribution_Weight__c - expectedWeight) < tolerance, 
                     'Facebook Ads should get 33.33% attribution');
        System.assert(Math.abs(resultsByChannel.get('Email_Marketing').Attribution_Weight__c - expectedWeight) < tolerance, 
                     'Email Marketing should get 33.33% attribution');
        
        // Verify attribution values
        System.assert(Math.abs(resultsByChannel.get('Google_Ads').Attribution_Value__c - expectedValue) < tolerance, 
                     'Google Ads should get €100 value');
        System.assert(Math.abs(resultsByChannel.get('Facebook_Ads').Attribution_Value__c - expectedValue) < tolerance, 
                     'Facebook Ads should get €100 value');
        System.assert(Math.abs(resultsByChannel.get('Email_Marketing').Attribution_Value__c - expectedValue) < tolerance, 
                     'Email Marketing should get €100 value');
    }
    
    /**
     * Test linear attribution for journey with repeated channels
     */
    @isTest
    static void testLinearAttributionRepeatedChannels() {
        // Get journey with 4 touchpoints, Google appears twice
        Customer_Journey__c journey = [SELECT Id FROM Customer_Journey__c WHERE Journey_ID__c = 'JOURNEY_002' LIMIT 1];
        
        Test.startTest();
        List<Attribution_Result__c> results = LinearAttribution.calculateAttribution(journey.Id);
        Test.stopTest();
        
        // Verify results - should have 3 channels (Google, Facebook, Email)
        System.assertNotEquals(null, results, 'Results should not be null');
        System.assertEquals(3, results.size(), 'Should have results for 3 unique channels');
        
        Map<String, Attribution_Result__c> resultsByChannel = new Map<String, Attribution_Result__c>();
        for (Attribution_Result__c result : results) {
            resultsByChannel.put(result.Channel__c, result);
        }
        
        // Each touchpoint gets 25% (1/4), Google appears twice so gets 50% (2/4)
        Decimal expectedGoogleWeight = 2.0 / 4.0; // 50% (appears twice)
        Decimal expectedOtherWeight = 1.0 / 4.0;  // 25% (appears once)
        
        System.assertEquals(expectedGoogleWeight, resultsByChannel.get('Google_Ads').Attribution_Weight__c, 
                           'Google Ads should get 50% attribution (2 touchpoints)');
        System.assertEquals(expectedOtherWeight, resultsByChannel.get('Facebook_Ads').Attribution_Weight__c, 
                           'Facebook Ads should get 25% attribution (1 touchpoint)');
        System.assertEquals(expectedOtherWeight, resultsByChannel.get('Email_Marketing').Attribution_Weight__c, 
                           'Email Marketing should get 25% attribution (1 touchpoint)');
        
        // Verify total attribution sums to 100%
        Decimal totalAttribution = 0;
        for (Attribution_Result__c result : results) {
            totalAttribution += result.Attribution_Weight__c;
        }
        System.assert(Math.abs(totalAttribution - 1.0) < 0.001, 
                     'Total attribution should be 100%. Actual: ' + totalAttribution);
    }
    
    /**
     * Test linear attribution for single touchpoint journey
     */
    @isTest
    static void testLinearAttributionSingleTouchpoint() {
        // Get journey with single touchpoint
        Customer_Journey__c journey = [SELECT Id FROM Customer_Journey__c WHERE Journey_ID__c = 'JOURNEY_003' LIMIT 1];
        
        Test.startTest();
        List<Attribution_Result__c> results = LinearAttribution.calculateAttribution(journey.Id);
        Test.stopTest();
        
        // Verify single touchpoint gets 100% attribution
        System.assertEquals(1, results.size(), 'Should have result for single channel');
        System.assertEquals(1.0, results[0].Attribution_Weight__c, 'Single touchpoint should get 100% attribution');
        System.assertEquals('Google_Ads', results[0].Channel__c, 'Should be Google Ads channel');
        System.assertEquals(200.0, results[0].Attribution_Value__c, 'Should get full conversion value');
    }
    
    /**
     * Test linear attribution for same channel multiple times
     */
    @isTest
    static void testLinearAttributionSameChannelTwice() {
        // Get journey with Email appearing twice
        Customer_Journey__c journey = [SELECT Id FROM Customer_Journey__c WHERE Journey_ID__c = 'JOURNEY_004' LIMIT 1];
        
        Test.startTest();
        List<Attribution_Result__c> results = LinearAttribution.calculateAttribution(journey.Id);
        Test.stopTest();
        
        // Verify Email gets 100% attribution (appears in both touchpoints)
        System.assertEquals(1, results.size(), 'Should have result for single unique channel');
        System.assertEquals(1.0, results[0].Attribution_Weight__c, 'Channel appearing twice should get 100% attribution');
        System.assertEquals('Email_Marketing', results[0].Channel__c, 'Should be Email Marketing channel');
        System.assertEquals(100.0, results[0].Attribution_Value__c, 'Should get full conversion value');
    }
    
    /**
     * Test batch processing for multiple journeys
     */
    @isTest
    static void testBatchAttribution() {
        // Get all test journeys
        List<Customer_Journey__c> journeys = [SELECT Id FROM Customer_Journey__c ORDER BY Journey_ID__c];
        List<Id> journeyIds = new List<Id>();
        for (Customer_Journey__c journey : journeys) {
            journeyIds.add(journey.Id);
        }
        
        Test.startTest();
        List<Attribution_Result__c> results = LinearAttribution.calculateAttributionBatch(journeyIds);
        Test.stopTest();
        
        // Verify results for all journeys
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
        
        // Each journey's attribution should sum to 100%
        for (Id journeyId : resultsByJourney.keySet()) {
            Decimal totalAttribution = 0;
            
            for (Attribution_Result__c result : resultsByJourney.get(journeyId)) {
                totalAttribution += result.Attribution_Weight__c;
            }
            
            // Use tolerance for floating point comparison
            System.assert(Math.abs(totalAttribution - 1.0) < 0.001, 
                         'Total attribution should be 100% for journey ' + journeyId + 
                         '. Actual: ' + totalAttribution);
        }
    }
    
    /**
     * Test channel statistics functionality
     */
    @isTest
    static void testChannelStatistics() {
        // Get touchpoints for journey with repeated channels
        Customer_Journey__c journey = [SELECT Id FROM Customer_Journey__c WHERE Journey_ID__c = 'JOURNEY_002' LIMIT 1];
        List<Touchpoint__c> touchpoints = [
            SELECT Id, Channel__c, Touchpoint_Timestamp__c
            FROM Touchpoint__c 
            WHERE Customer_Journey__c = :journey.Id
            ORDER BY Touchpoint_Timestamp__c ASC
        ];
        
        Test.startTest();
        Map<String, Object> statistics = LinearAttribution.getChannelStatistics(touchpoints);
        Test.stopTest();
        
        // Verify statistics
        System.assertEquals(4, statistics.get('totalTouchpoints'), 'Should have 4 total touchpoints');
        System.assertEquals(3, statistics.get('uniqueChannels'), 'Should have 3 unique channels');
        System.assertEquals(0.25, statistics.get('attributionPerTouchpoint'), 'Each touchpoint should get 25%');
        
        Map<String, Integer> channelCounts = (Map<String, Integer>) statistics.get('channelCounts');
        System.assertEquals(2, channelCounts.get('Google_Ads'), 'Google Ads should appear 2 times');
        System.assertEquals(1, channelCounts.get('Facebook_Ads'), 'Facebook Ads should appear 1 time');
        System.assertEquals(1, channelCounts.get('Email_Marketing'), 'Email Marketing should appear 1 time');
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
            Attribution_Model__c = 'Linear',
            Channel__c = 'Google_Ads',
            Attribution_Weight__c = 0.3333,
            Attribution_Value__c = 100.0
        ));
        validResults.add(new Attribution_Result__c(
            Customer_Journey__c = journey.Id,
            Attribution_Model__c = 'Linear',
            Channel__c = 'Facebook_Ads',
            Attribution_Weight__c = 0.3333,
            Attribution_Value__c = 100.0
        ));
        validResults.add(new Attribution_Result__c(
            Customer_Journey__c = journey.Id,
            Attribution_Model__c = 'Linear',
            Channel__c = 'Email_Marketing',
            Attribution_Weight__c = 0.3334,
            Attribution_Value__c = 100.0
        ));
        
        Test.startTest();
        Boolean validationResult = LinearAttribution.validateAttributionResults(validResults);
        Test.stopTest();
        
        System.assertEquals(true, validationResult, 'Valid attribution results should pass validation');
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
        LinearAttribution.triggerAttributionCalculation(journeyIds);
        Test.stopTest();
        
        // Verify attribution results were created
        List<Attribution_Result__c> results = [
            SELECT Id, Customer_Journey__c, Channel__c, Attribution_Weight__c, Attribution_Value__c
            FROM Attribution_Result__c
            WHERE Customer_Journey__c = :journey.Id
        ];
        
        System.assertEquals(3, results.size(), 'Should have created attribution results for all channels');
        
        // Verify attribution totals 100%
        Decimal totalAttribution = 0;
        for (Attribution_Result__c result : results) {
            totalAttribution += result.Attribution_Weight__c;
        }
        System.assert(Math.abs(totalAttribution - 1.0) < 0.001, 
                     'Total attribution should be 100%. Actual: ' + totalAttribution);
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
        Id batchId = LinearAttributionBatch.executeBatch(journeyIds, 200);
        Test.stopTest();
        
        // Verify batch was created
        System.assertNotEquals(null, batchId, 'Batch job should be created');
        
        // Verify attribution results were created
        List<Attribution_Result__c> results = [
            SELECT Id, Customer_Journey__c, Channel__c, Attribution_Weight__c
            FROM Attribution_Result__c
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
            LinearAttribution.calculateAttribution(null);
            System.assert(false, 'Should have thrown exception for null journey ID');
        } catch (IllegalArgumentException e) {
            System.assertEquals('Journey ID cannot be null', e.getMessage());
        }
        
        // Test with empty journey list
        List<Attribution_Result__c> results = LinearAttribution.calculateAttributionBatch(new List<Id>());
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
        List<Attribution_Result__c> results = LinearAttribution.calculateAttribution(emptyJourney.Id);
        Test.stopTest();
        
        System.assertEquals(0, results.size(), 'Should return empty results for journey with no touchpoints');
    }
    
    /**
     * Test model comparison functionality
     */
    @isTest
    static void testModelComparison() {
        // First create some existing attribution results for comparison
        Customer_Journey__c journey = [SELECT Id FROM Customer_Journey__c WHERE Journey_ID__c = 'JOURNEY_001' LIMIT 1];
        
        // Create mock LastTouch and FirstTouch results
        List<Attribution_Result__c> existingResults = new List<Attribution_Result__c>();
        existingResults.add(new Attribution_Result__c(
            Customer_Journey__c = journey.Id,
            Attribution_Model__c = 'LastTouch',
            Channel__c = 'Email_Marketing',
            Attribution_Weight__c = 1.0,
            Attribution_Value__c = 300.0
        ));
        existingResults.add(new Attribution_Result__c(
            Customer_Journey__c = journey.Id,
            Attribution_Model__c = 'FirstTouch',
            Channel__c = 'Google_Ads',
            Attribution_Weight__c = 1.0,
            Attribution_Value__c = 300.0
        ));
        insert existingResults;
        
        Test.startTest();
        Map<String, Object> comparison = LinearAttribution.compareWithOtherModels(journey.Id);
        Test.stopTest();
        
        // Verify comparison data structure
        System.assertNotEquals(null, comparison, 'Comparison should not be null');
        System.assert(comparison.containsKey('attributionByModel'), 'Should contain attribution by model');
        System.assert(comparison.containsKey('linearResults'), 'Should contain linear results');
        
        Map<String, Map<String, Decimal>> attributionByModel = 
            (Map<String, Map<String, Decimal>>) comparison.get('attributionByModel');
        
        // Verify Linear model shows equal distribution
        Map<String, Decimal> linearWeights = attributionByModel.get('Linear');
        Decimal expectedLinearWeight = 1.0 / 3.0; // 33.33%
        Decimal tolerance = 0.001;
        
        System.assert(Math.abs(linearWeights.get('Google_Ads') - expectedLinearWeight) < tolerance, 
                     'Google should get 33.33% in Linear. Actual: ' + linearWeights.get('Google_Ads'));
        System.assert(Math.abs(linearWeights.get('Facebook_Ads') - expectedLinearWeight) < tolerance, 
                     'Facebook should get 33.33% in Linear. Actual: ' + linearWeights.get('Facebook_Ads'));
        System.assert(Math.abs(linearWeights.get('Email_Marketing') - expectedLinearWeight) < tolerance, 
                     'Email should get 33.33% in Linear. Actual: ' + linearWeights.get('Email_Marketing'));
    }
    
    /**
     * Test batch with large dataset simulation
     */
    @isTest
    static void testBatchLargeDataset() {
        // Create additional journeys to simulate larger dataset
        List<Customer_Journey__c> additionalJourneys = new List<Customer_Journey__c>();
        for (Integer i = 0; i < 30; i++) { // Smaller number to avoid test limits
            Customer_Journey__c journey = new Customer_Journey__c();
            journey.Journey_ID__c = 'JOURNEY_BULK_' + i;
            journey.Customer_ID__c = 'CUSTOMER_BULK_' + i;
            journey.Customer_Type__c = 'B2C';
            journey.Converted__c = true;
            journey.Conversion_Value__c = 50.00;
            journey.Journey_Start_Date__c = DateTime.now().addDays(-2);
            journey.Journey_End_Date__c = DateTime.now().addDays(-1);
            journey.Total_Touchpoints__c = 2;
            journey.Confidence_Score__c = 0.80;
            journey.Confidence_Level__c = 'High';
            additionalJourneys.add(journey);
        }
        insert additionalJourneys;
        
        // Create 2 touchpoints per journey
        List<Touchpoint__c> additionalTouchpoints = new List<Touchpoint__c>();
        for (Integer i = 0; i < additionalJourneys.size(); i++) {
            // First touchpoint
            Touchpoint__c touchpoint1 = new Touchpoint__c();
            touchpoint1.Touchpoint_ID__c = 'TP_BULK_' + i + '_1';
            touchpoint1.Customer_Journey__c = additionalJourneys[i].Id;
            touchpoint1.Channel__c = 'Google_Ads';
            touchpoint1.Touchpoint_Timestamp__c = DateTime.now().addDays(-2);
            additionalTouchpoints.add(touchpoint1);
            
            // Second touchpoint
            Touchpoint__c touchpoint2 = new Touchpoint__c();
            touchpoint2.Touchpoint_ID__c = 'TP_BULK_' + i + '_2';
            touchpoint2.Customer_Journey__c = additionalJourneys[i].Id;
            touchpoint2.Channel__c = 'Email_Marketing';
            touchpoint2.Touchpoint_Timestamp__c = DateTime.now().addDays(-1);
            additionalTouchpoints.add(touchpoint2);
        }
        insert additionalTouchpoints;
        
        Test.startTest();
        
        // Test batch class instantiation and synchronous processing
        List<Id> journeyIds = new List<Id>();
        for (Customer_Journey__c journey : additionalJourneys) {
            journeyIds.add(journey.Id);
        }
        
        // Test batch class creation
        LinearAttributionBatch batch = new LinearAttributionBatch(journeyIds);
        System.assertNotEquals(null, batch, 'Batch should be instantiated');
        
        // Test synchronous processing for large datasets
        List<Attribution_Result__c> results = LinearAttribution.calculateAttributionBatch(journeyIds);
        System.assert(results.size() > 0, 'Should generate attribution results for large dataset');
        
        Test.stopTest();
        
        // Each journey should have 2 channels, each getting 50%
        System.assertEquals(60, results.size(), 'Should have 2 attribution results per journey (30 journeys * 2 channels)');
    }
}