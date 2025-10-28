/**
 * AttributionOrchestrator
 * 
 * Orchestrates the execution of all Heuristic Attribution Models (HAMs) for customer journeys.
 * This class runs all 5 attribution models in sequence and stores results in Attribution_Result__c.
 * 
 * Optimizations:
 * - Inserts results per model to avoid 10,000 DML row limit
 * - Supports chunked processing for large datasets
 * - Batch processing option for datasets >500 journeys
 * 
 * Supported Models:
 * - First Touch Attribution
 * - Last Touch Attribution
 * - Linear Attribution
 * - Time Decay Attribution
 * - Position Based Attribution
 * 
 * Usage:
 * AttributionOrchestrator.calculateAllModels(); // All journeys (chunked automatically)
 * AttributionOrchestrator.calculateAllModelsChunked(200); // Custom chunk size
 * AttributionOrchestrator.calculateSingleModel('FirstTouch'); // Single model
 * 
 * Author: Alexandru Constantinescu
 * Project: Omnichannel Attribution Platform
 * Date: 2025
 */
public class AttributionOrchestrator {
    
    private static final List<String> ATTRIBUTION_MODELS = new List<String>{
        'FirstTouch',
        'LastTouch',
        'Linear',
        'TimeDecay',
        'PositionBased'
    };
    
    private static final Integer DEFAULT_CHUNK_SIZE = 200;
    private static final Integer DML_INSERT_CHUNK_SIZE = 2000;
    
    public static void calculateAllModels() {
        List<Id> allJourneyIds = getAllJourneyIds();
        
        if (allJourneyIds.size() > 500) {
            System.debug('Large dataset detected (' + allJourneyIds.size() + ' journeys). Using chunked processing.');
            calculateAllModelsChunked(DEFAULT_CHUNK_SIZE);
        } else {
            calculateAllModelsOptimized(allJourneyIds);
        }
    }
    
    public static void calculateAllModelsChunked(Integer chunkSize) {
        List<Id> allJourneyIds = getAllJourneyIds();
        Integer totalJourneys = allJourneyIds.size();
        Integer processedJourneys = 0;
        
        System.debug('Processing ' + totalJourneys + ' journeys in chunks of ' + chunkSize);
        
        for (Integer i = 0; i < totalJourneys; i += chunkSize) {
            Integer endIndex = Math.min(i + chunkSize, totalJourneys);
            List<Id> journeyChunk = new List<Id>();
            
            for (Integer j = i; j < endIndex; j++) {
                journeyChunk.add(allJourneyIds[j]);
            }
            
            calculateAllModelsOptimized(journeyChunk);
            processedJourneys += journeyChunk.size();
            
            System.debug('Progress: ' + processedJourneys + '/' + totalJourneys + ' journeys processed');
        }
        
        System.debug('All chunks processed successfully');
    }
    
    private static void calculateAllModelsOptimized(List<Id> journeyIds) {
        if (journeyIds == null || journeyIds.isEmpty()) {
            System.debug('No journey IDs provided for attribution calculation');
            return;
        }
        
        System.debug('Starting attribution calculation for ' + journeyIds.size() + ' journeys');
        
        deleteExistingResults(journeyIds);
        
        Integer totalResultsInserted = 0;
        
        try {
            System.debug('Running FirstTouchAttribution...');
            List<Attribution_Result__c> firstTouchResults = 
                FirstTouchAttribution.calculateAttributionBatch(journeyIds);
            insertResultsInChunks(firstTouchResults);
            totalResultsInserted += firstTouchResults.size();
            System.debug('FirstTouch: Inserted ' + firstTouchResults.size() + ' results');
            
        } catch (Exception e) {
            System.debug('Error in FirstTouchAttribution: ' + e.getMessage());
            System.debug('Stack trace: ' + e.getStackTraceString());
        }
        
        try {
            System.debug('Running LastTouchAttribution...');
            List<Attribution_Result__c> lastTouchResults = 
                LastTouchAttribution.calculateAttributionBatch(journeyIds);
            insertResultsInChunks(lastTouchResults);
            totalResultsInserted += lastTouchResults.size();
            System.debug('LastTouch: Inserted ' + lastTouchResults.size() + ' results');
            
        } catch (Exception e) {
            System.debug('Error in LastTouchAttribution: ' + e.getMessage());
            System.debug('Stack trace: ' + e.getStackTraceString());
        }
        
        try {
            System.debug('Running LinearAttribution...');
            List<Attribution_Result__c> linearResults = 
                LinearAttribution.calculateAttributionBatch(journeyIds);
            insertResultsInChunks(linearResults);
            totalResultsInserted += linearResults.size();
            System.debug('Linear: Inserted ' + linearResults.size() + ' results');
            
        } catch (Exception e) {
            System.debug('Error in LinearAttribution: ' + e.getMessage());
            System.debug('Stack trace: ' + e.getStackTraceString());
        }
        
        try {
            System.debug('Running TimeDecayAttribution...');
            List<Attribution_Result__c> timeDecayResults = 
                TimeDecayAttribution.calculateAttributionBatch(journeyIds);
            insertResultsInChunks(timeDecayResults);
            totalResultsInserted += timeDecayResults.size();
            System.debug('TimeDecay: Inserted ' + timeDecayResults.size() + ' results');
            
        } catch (Exception e) {
            System.debug('Error in TimeDecayAttribution: ' + e.getMessage());
            System.debug('Stack trace: ' + e.getStackTraceString());
        }
        
        try {
            System.debug('Running PositionBasedAttribution...');
            List<Attribution_Result__c> positionBasedResults = 
                PositionBasedAttribution.calculateAttributionBatch(journeyIds);
            insertResultsInChunks(positionBasedResults);
            totalResultsInserted += positionBasedResults.size();
            System.debug('PositionBased: Inserted ' + positionBasedResults.size() + ' results');
            
        } catch (Exception e) {
            System.debug('Error in PositionBasedAttribution: ' + e.getMessage());
            System.debug('Stack trace: ' + e.getStackTraceString());
        }
        
        System.debug('Total attribution results inserted: ' + totalResultsInserted);
    }
    
    private static void insertResultsInChunks(List<Attribution_Result__c> results) {
        if (results == null || results.isEmpty()) {
            return;
        }
        
        if (results.size() <= DML_INSERT_CHUNK_SIZE) {
            insert results;
            return;
        }
        
        Integer totalInserted = 0;
        for (Integer i = 0; i < results.size(); i += DML_INSERT_CHUNK_SIZE) {
            Integer endIndex = Math.min(i + DML_INSERT_CHUNK_SIZE, results.size());
            List<Attribution_Result__c> chunk = new List<Attribution_Result__c>();
            
            for (Integer j = i; j < endIndex; j++) {
                chunk.add(results[j]);
            }
            
            insert chunk;
            totalInserted += chunk.size();
        }
        
        System.debug('Inserted ' + totalInserted + ' results in chunks of ' + DML_INSERT_CHUNK_SIZE);
    }
    
    private static List<Id> getAllJourneyIds() {
        List<Id> journeyIds = new List<Id>();
        
        for (Customer_Journey__c journey : [
            SELECT Id 
            FROM Customer_Journey__c 
            WHERE Total_Touchpoints__c > 0
            ORDER BY Journey_Start_Date__c DESC
        ]) {
            journeyIds.add(journey.Id);
        }
        
        return journeyIds;
    }
    
    private static void deleteExistingResults(List<Id> journeyIds) {
        try {
            List<Attribution_Result__c> existingResults = [
                SELECT Id 
                FROM Attribution_Result__c 
                WHERE Customer_Journey__c IN :journeyIds
                AND Attribution_Model__c IN :ATTRIBUTION_MODELS
            ];
            
            if (!existingResults.isEmpty()) {
                if (existingResults.size() > 10000) {
                    System.debug('Large deletion detected. Deleting in chunks...');
                    deleteInChunks(existingResults);
                } else {
                    delete existingResults;
                }
                System.debug('Deleted ' + existingResults.size() + ' existing attribution results');
            }
            
        } catch (Exception e) {
            System.debug('Error deleting existing results: ' + e.getMessage());
        }
    }
    
    private static void deleteInChunks(List<Attribution_Result__c> results) {
        Integer chunkSize = 10000;
        for (Integer i = 0; i < results.size(); i += chunkSize) {
            Integer endIndex = Math.min(i + chunkSize, results.size());
            List<Attribution_Result__c> chunk = new List<Attribution_Result__c>();
            
            for (Integer j = i; j < endIndex; j++) {
                chunk.add(results[j]);
            }
            
            delete chunk;
        }
    }
    
    public static void calculateSingleModel(String modelName) {
        List<Id> allJourneyIds = getAllJourneyIds();
        calculateSingleModel(modelName, allJourneyIds);
    }
    
    public static void calculateSingleModel(String modelName, List<Id> journeyIds) {
        if (String.isBlank(modelName) || journeyIds == null || journeyIds.isEmpty()) {
            return;
        }
        
        System.debug('Calculating ' + modelName + ' for ' + journeyIds.size() + ' journeys');
        
        List<Attribution_Result__c> results = new List<Attribution_Result__c>();
        
        List<Attribution_Result__c> existingResults = [
            SELECT Id 
            FROM Attribution_Result__c 
            WHERE Customer_Journey__c IN :journeyIds 
            AND Attribution_Model__c = :modelName
        ];
        
        if (!existingResults.isEmpty()) {
            if (existingResults.size() > 10000) {
                deleteInChunks(existingResults);
            } else {
                delete existingResults;
            }
            System.debug('Deleted ' + existingResults.size() + ' existing results');
        }
        
        if (modelName == 'FirstTouch') {
            results = FirstTouchAttribution.calculateAttributionBatch(journeyIds);
        } else if (modelName == 'LastTouch') {
            results = LastTouchAttribution.calculateAttributionBatch(journeyIds);
        } else if (modelName == 'Linear') {
            results = LinearAttribution.calculateAttributionBatch(journeyIds);
        } else if (modelName == 'TimeDecay') {
            results = TimeDecayAttribution.calculateAttributionBatch(journeyIds);
        } else if (modelName == 'PositionBased') {
            results = PositionBasedAttribution.calculateAttributionBatch(journeyIds);
        }
        
        if (!results.isEmpty()) {
            insertResultsInChunks(results);
            System.debug('Inserted ' + results.size() + ' results for model: ' + modelName);
        }
    }
    
    public static Map<String, Object> getAttributionStatus() {
        Map<String, Object> status = new Map<String, Object>();
        
        AggregateResult[] journeyStats = [
            SELECT COUNT(Id) totalJourneys, 
                   SUM(Total_Touchpoints__c) totalTouchpoints
            FROM Customer_Journey__c
        ];
        
        AggregateResult[] touchpointStats = [
            SELECT COUNT(Id) totalTouchpoints
            FROM Touchpoint__c
        ];
        
        AggregateResult[] attributionStats = [
            SELECT Attribution_Model__c model, COUNT(Id) resultCount
            FROM Attribution_Result__c
            GROUP BY Attribution_Model__c
        ];
        
        status.put('totalJourneys', journeyStats[0].get('totalJourneys'));
        status.put('totalTouchpoints', touchpointStats[0].get('totalTouchpoints'));
        
        Map<String, Integer> modelCounts = new Map<String, Integer>();
        for (AggregateResult ar : attributionStats) {
            modelCounts.put((String)ar.get('model'), (Integer)ar.get('resultCount'));
        }
        status.put('modelResults', modelCounts);
        
        return status;
    }
    
    public static void executeQuickTest() {
        List<Customer_Journey__c> testJourneys = [
            SELECT Id 
            FROM Customer_Journey__c 
            WHERE Total_Touchpoints__c > 0 
            LIMIT 10
        ];
        
        List<Id> testJourneyIds = new List<Id>();
        for (Customer_Journey__c journey : testJourneys) {
            testJourneyIds.add(journey.Id);
        }
        
        System.debug('Running quick test with ' + testJourneyIds.size() + ' journeys');
        calculateAllModelsOptimized(testJourneyIds);
        
        Map<String, Object> status = getAttributionStatus();
        System.debug('Test complete. Status: ' + status);
    }
}