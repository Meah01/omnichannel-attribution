/**
 * FirstTouchAttributionBatch
 * 
 * Batch class for processing First Touch Attribution calculations on large datasets.
 * This class handles bulk processing of customer journeys while respecting Salesforce
 * governor limits and providing proper error handling and monitoring.
 * 
 * Features:
 * - Processes customer journeys in configurable batch sizes
 * - Handles cleanup of existing attribution results
 * - Provides progress tracking and error reporting
 * - Supports both full and incremental processing modes
 * 
 * Author: Alexandru Constantinescu (www.linkedin.com/in/alexandru-constantinescu-6a18b6117)
 * Project: Omnichannel Attribution Platform
 * Date: 2025
 * Version: 1.2
 */
global class FirstTouchAttributionBatch implements Database.Batchable<sObject>, Database.Stateful {
    
    // Batch configuration
    private static final String ATTRIBUTION_MODEL = 'FirstTouch';
    private static final Integer DEFAULT_BATCH_SIZE = 200;
    
    // Instance variables for tracking
    private List<Id> journeyIds;
    private Integer totalProcessed = 0;
    private Integer totalErrors = 0;
    private List<String> errorMessages = new List<String>();
    private DateTime processingStartTime;
    
    /**
     * Constructor for processing specific journey IDs
     * 
     * @param journeyIds List of Customer Journey IDs to process
     */
    global FirstTouchAttributionBatch(List<Id> journeyIds) {
        this.journeyIds = journeyIds;
        this.processingStartTime = DateTime.now();
    }
    
    /**
     * Default constructor for processing all journeys
     */
    global FirstTouchAttributionBatch() {
        this.journeyIds = new List<Id>();
        this.processingStartTime = DateTime.now();
    }
    
    /**
     * Start method - defines the scope of records to process
     * 
     * @param context Database.BatchableContext
     * @return Database.QueryLocator for customer journeys
     */
    global Database.QueryLocator start(Database.BatchableContext context) {
        String query = 'SELECT Id, Customer_ID__c, Customer_Type__c, Converted__c, ' +
                      'Conversion_Value__c, Journey_Start_Date__c, Journey_End_Date__c ' +
                      'FROM Customer_Journey__c';
        
        // If specific journey IDs provided, filter by them
        if (!journeyIds.isEmpty()) {
            query += ' WHERE Id IN :journeyIds';
        } else {
            // Process all journeys - removed Processing_Status__c filter for deployment safety
            query += ' WHERE Id != null';
        }
        
        query += ' ORDER BY Journey_Start_Date__c ASC';
        
        System.debug('FirstTouchAttributionBatch start query: ' + query);
        
        return Database.getQueryLocator(query);
    }
    
    /**
     * Execute method - processes each batch of records
     * 
     * @param context Database.BatchableContext
     * @param scope List of Customer_Journey__c records to process
     */
    global void execute(Database.BatchableContext context, List<Customer_Journey__c> scope) {
        System.debug('Processing batch of ' + scope.size() + ' journeys');
        
        List<Id> batchJourneyIds = new List<Id>();
        for (Customer_Journey__c journey : scope) {
            batchJourneyIds.add(journey.Id);
        }
        
        try {
            // Clean up existing First Touch attribution results for this batch
            cleanupExistingResults(batchJourneyIds);
            
            // Calculate new attribution results using the main class
            List<Attribution_Result__c> attributionResults = FirstTouchAttribution.calculateAttributionBatch(batchJourneyIds);
            
            // Validate results before saving
            if (!FirstTouchAttribution.validateAttributionResults(attributionResults)) {
                throw new BatchProcessingException('Attribution validation failed for batch');
            }
            
            // Save attribution results
            if (!attributionResults.isEmpty()) {
                List<Database.SaveResult> saveResults = Database.insert(attributionResults, false);
                handleSaveResults(saveResults, attributionResults);
            }
            
            totalProcessed += scope.size();
            
        } catch (Exception e) {
            totalErrors += scope.size();
            String errorMessage = 'Error processing batch: ' + e.getMessage() + ' Stack: ' + e.getStackTraceString();
            errorMessages.add(errorMessage);
            System.debug(errorMessage);
        }
    }
    
    /**
     * Finish method - performs cleanup and reporting after all batches complete
     * 
     * @param context Database.BatchableContext
     */
    global void finish(Database.BatchableContext context) {
        DateTime processingEndTime = DateTime.now();
        Long processingDuration = processingEndTime.getTime() - processingStartTime.getTime();
        
        System.debug('FirstTouchAttributionBatch completed. Total processed: ' + totalProcessed + 
                    ', Total errors: ' + totalErrors + ', Duration: ' + processingDuration + 'ms');
        
        // Log processing metrics
        logProcessingMetrics(processingDuration);
        
        // Send notification if there were errors
        if (totalErrors > 0) {
            sendErrorNotification();
        }
        
        // Create data quality metrics record
        createDataQualityMetrics();
    }
    
    /**
     * Clean up existing attribution results for the batch
     * 
     * @param journeyIds List of journey IDs to clean up
     */
    private void cleanupExistingResults(List<Id> journeyIds) {
        try {
            List<Attribution_Result__c> existingResults = [
                SELECT Id 
                FROM Attribution_Result__c 
                WHERE Customer_Journey__c IN :journeyIds 
                AND Attribution_Model__c = :ATTRIBUTION_MODEL
            ];
            
            if (!existingResults.isEmpty()) {
                delete existingResults;
                System.debug('Deleted ' + existingResults.size() + ' existing First Touch attribution results');
            }
            
        } catch (Exception e) {
            System.debug('Warning: Could not clean up existing results: ' + e.getMessage());
        }
    }
    
    /**
     * Handle save results and log any errors
     * 
     * @param saveResults List of Database.SaveResult from DML operation
     * @param attributionResults List of Attribution_Result__c records that were saved
     */
    private void handleSaveResults(List<Database.SaveResult> saveResults, List<Attribution_Result__c> attributionResults) {
        for (Integer i = 0; i < saveResults.size(); i++) {
            Database.SaveResult result = saveResults[i];
            
            if (!result.isSuccess()) {
                String errorMessage = 'Failed to save attribution result for journey ' + 
                                    attributionResults[i].Customer_Journey__c + ': ';
                
                for (Database.Error error : result.getErrors()) {
                    errorMessage += error.getMessage() + ' ';
                }
                
                errorMessages.add(errorMessage);
                System.debug(errorMessage);
            }
        }
    }
    
    /**
     * Update journey processing status
     * 
     * @param journeys List of Customer_Journey__c records to update
     * @param status The status to set
     */
    private void updateJourneyStatus(List<Customer_Journey__c> journeys, String status) {
        // Processing status update disabled for deployment simplicity
        // Can be re-enabled once Processing_Status__c field is confirmed to exist
        System.debug('Journey status update skipped - Processing_Status__c field handling disabled for deployment');
    }
    
    /**
     * Log processing metrics for monitoring
     * 
     * @param processingDuration Duration of the batch processing in milliseconds
     */
    private void logProcessingMetrics(Long processingDuration) {
        System.debug('=== First Touch Attribution Batch Processing Metrics ===');
        System.debug('Start Time: ' + processingStartTime);
        System.debug('End Time: ' + DateTime.now());
        System.debug('Duration: ' + processingDuration + 'ms');
        System.debug('Total Processed: ' + totalProcessed);
        System.debug('Total Errors: ' + totalErrors);
        System.debug('Success Rate: ' + (totalProcessed > 0 ? (totalProcessed - totalErrors) * 100 / totalProcessed : 0) + '%');
        
        if (!errorMessages.isEmpty()) {
            System.debug('Error Messages:');
            for (String errorMessage : errorMessages) {
                System.debug('  - ' + errorMessage);
            }
        }
    }
    
    /**
     * Send error notification to administrators
     */
    private void sendErrorNotification() {
        // Create a simple debug log entry for now
        // In production, this could send actual emails or create cases
        System.debug('ERROR NOTIFICATION: First Touch Attribution batch processing completed with ' + 
                    totalErrors + ' errors out of ' + totalProcessed + ' total records processed.');
    }
    
    /**
     * Create data quality metrics record for monitoring
     */
    private void createDataQualityMetrics() {
        try {
            Data_Quality_Metrics__c metrics = new Data_Quality_Metrics__c();
            metrics.Metric_Date__c = Date.today();
            metrics.Processing_Time_Seconds__c = (DateTime.now().getTime() - processingStartTime.getTime()) / 1000;
            metrics.Total_Touchpoints__c = totalProcessed; // Using processed journeys as approximation
            metrics.Error_Rate__c = totalProcessed > 0 ? (totalErrors * 100.0 / totalProcessed) : 0;
            
            insert metrics;
            
        } catch (Exception e) {
            System.debug('Could not create data quality metrics: ' + e.getMessage());
        }
    }
    
    /**
     * Static method to execute the batch with default settings
     * 
     * @return Database.BatchableContext for monitoring
     */
    global static Id executeBatch() {
        return executeBatch(DEFAULT_BATCH_SIZE);
    }
    
    /**
     * Static method to execute the batch with custom batch size
     * 
     * @param batchSize Number of records to process per batch
     * @return Database.BatchableContext for monitoring
     */
    global static Id executeBatch(Integer batchSize) {
        FirstTouchAttributionBatch batch = new FirstTouchAttributionBatch();
        return Database.executeBatch(batch, batchSize);
    }
    
    /**
     * Static method to execute the batch for specific journeys
     * 
     * @param journeyIds List of journey IDs to process
     * @return Database.BatchableContext for monitoring
     */
    global static Id executeBatch(List<Id> journeyIds) {
        return executeBatch(journeyIds, DEFAULT_BATCH_SIZE);
    }
    
    /**
     * Static method to execute the batch for specific journeys with custom batch size
     * 
     * @param journeyIds List of journey IDs to process
     * @param batchSize Number of records to process per batch
     * @return Database.BatchableContext for monitoring
     */
    global static Id executeBatch(List<Id> journeyIds, Integer batchSize) {
        FirstTouchAttributionBatch batch = new FirstTouchAttributionBatch(journeyIds);
        return Database.executeBatch(batch, batchSize);
    }
    
    /**
     * Custom exception class for batch processing errors
     */
    public class BatchProcessingException extends Exception {}
}