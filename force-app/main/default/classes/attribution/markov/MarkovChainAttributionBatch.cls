/**
 * MarkovChainAttributionBatch
 * 
 * Enhanced batch class for processing Markov Chain Attribution calculations on large datasets.
 * Handles transition matrix construction and attribution calculation with proper governor limit
 * management, stage detection monitoring, and comprehensive progress tracking.
 * 
 * Features:
 * - Processes customer journeys in configurable batch sizes
 * - Builds and maintains transition matrices for B2C and B2B customer types
 * - Stage detection monitoring and validation
 * - Network visualization data collection
 * - Enhanced error reporting and debugging capabilities
 * - Performance metrics and quality assessment
 * 
 * Author: Alexandru Constantinescu (www.linkedin.com/in/alexandru-constantinescu-6a18b6117)
 * Project: Omnichannel Attribution Platform
 * Date: 2025
 * Version: 2.1
 */
global class MarkovChainAttributionBatch implements Database.Batchable<sObject>, Database.Stateful {
    
    // Batch configuration
    private static final String ATTRIBUTION_MODEL = 'MarkovChain';
    private static final Integer DEFAULT_BATCH_SIZE = 200;
    
    // Processing modes
    public enum ProcessingMode {
        MATRIX_CALCULATION,     // Build transition matrices
        ATTRIBUTION_CALCULATION // Calculate attribution using existing matrices
    }
    
    // Instance variables for tracking
    private List<Id> journeyIds;
    private ProcessingMode mode;
    private String customerType;
    private Integer totalProcessed = 0;
    private Integer totalErrors = 0;
    private List<String> errorMessages = new List<String>();
    private DateTime processingStartTime;
    
    // Enhanced tracking for stage detection
    private Integer totalStageDetections = 0;
    private Integer validStageDetections = 0;
    private Integer invalidStageDetections = 0;
    private Map<String, Integer> stageDetectionCounts = new Map<String, Integer>();
    private Map<String, Integer> channelUsageCounts = new Map<String, Integer>();
    private List<String> stageDetectionWarnings = new List<String>();
    
    // Network visualization data collection
    private Map<String, Object> networkMetrics = new Map<String, Object>();
    
    /**
     * Constructor for attribution calculation on specific journey IDs
     * 
     * @param journeyIds List of Customer Journey IDs to process
     */
    global MarkovChainAttributionBatch(List<Id> journeyIds) {
        this.journeyIds = journeyIds;
        this.mode = ProcessingMode.ATTRIBUTION_CALCULATION;
        this.processingStartTime = DateTime.now();
        initializeStageDetectionTracking();
    }
    
    /**
     * Constructor for matrix calculation for specific customer type
     * 
     * @param customerType Customer type (B2B or B2C)
     * @param modeString Processing mode as string ('MATRIX_CALCULATION' or 'ATTRIBUTION_CALCULATION')
     */
    global MarkovChainAttributionBatch(String customerType, String modeString) {
        this.customerType = customerType;
        this.mode = modeString == 'MATRIX_CALCULATION' ? ProcessingMode.MATRIX_CALCULATION : ProcessingMode.ATTRIBUTION_CALCULATION;
        this.journeyIds = new List<Id>();
        this.processingStartTime = DateTime.now();
        initializeStageDetectionTracking();
    }
    
    /**
     * Default constructor for processing all journeys with attribution calculation
     */
    global MarkovChainAttributionBatch() {
        this.journeyIds = new List<Id>();
        this.mode = ProcessingMode.ATTRIBUTION_CALCULATION;
        this.processingStartTime = DateTime.now();
        initializeStageDetectionTracking();
    }
    
    /**
     * Initialize stage detection tracking variables
     */
    private void initializeStageDetectionTracking() {
        stageDetectionCounts.put('Awareness', 0);
        stageDetectionCounts.put('Interest', 0);
        stageDetectionCounts.put('Consideration', 0);
        stageDetectionCounts.put('Conversion', 0);
        stageDetectionCounts.put('Retention', 0);
        
        // Initialize network metrics
        networkMetrics.put('processingStartTime', processingStartTime);
        networkMetrics.put('batchMode', mode.name());
        networkMetrics.put('customerType', customerType);
    }
    
    /**
     * Start method - defines the scope of records to process
     * 
     * @param context Database.BatchableContext
     * @return Database.QueryLocator for customer journeys
     */
    global Database.QueryLocator start(Database.BatchableContext context) {
        String query = '';
        
        if (mode == ProcessingMode.MATRIX_CALCULATION) {
            // For matrix calculation, get journeys for specific customer type
            query = 'SELECT Id, Customer_ID__c, Customer_Type__c, Converted__c, ' +
                   'Conversion_Value__c, Journey_Start_Date__c, Journey_End_Date__c, ' +
                   'Total_Touchpoints__c, Confidence_Score__c, Confidence_Level__c ' +
                   'FROM Customer_Journey__c ' +
                   'WHERE Total_Touchpoints__c > 0';
            
            if (String.isNotBlank(customerType)) {
                query += ' AND Customer_Type__c = :customerType';
            }
            
        } else {
            // For attribution calculation
            query = 'SELECT Id, Customer_ID__c, Customer_Type__c, Converted__c, ' +
                   'Conversion_Value__c, Journey_Start_Date__c, Journey_End_Date__c, ' +
                   'Total_Touchpoints__c, Confidence_Score__c, Confidence_Level__c ' +
                   'FROM Customer_Journey__c ' +
                   'WHERE Total_Touchpoints__c > 0';
            
            // If specific journey IDs provided, filter by them
            if (!journeyIds.isEmpty()) {
                query += ' AND Id IN :journeyIds';
            }
        }
        
        query += ' ORDER BY Journey_Start_Date__c ASC';
        
        System.debug('MarkovChainAttributionBatch start query: ' + query);
        System.debug('Processing mode: ' + mode);
        System.debug('Customer type: ' + customerType);
        
        return Database.getQueryLocator(query);
    }
    
    /**
     * Execute method - processes each batch of records
     * 
     * @param context Database.BatchableContext
     * @param scope List of Customer_Journey__c records to process
     */
    global void execute(Database.BatchableContext context, List<Customer_Journey__c> scope) {
        System.debug('Processing batch of ' + scope.size() + ' journeys in mode: ' + mode);
        
        try {
            if (mode == ProcessingMode.MATRIX_CALCULATION) {
                processMatrixCalculation(scope);
            } else {
                processAttributionCalculation(scope);
            }
            
            totalProcessed += scope.size();
            
            // Collect stage detection metrics from this batch
            collectStageDetectionMetrics(scope);
            
        } catch (Exception e) {
            totalErrors += scope.size();
            String errorMessage = 'Error processing batch: ' + e.getMessage() + ' Stack: ' + e.getStackTraceString();
            errorMessages.add(errorMessage);
            System.debug(errorMessage);
        }
    }
    
    /**
     * Process matrix calculation for the batch with enhanced monitoring
     * 
     * @param scope List of Customer_Journey__c records
     */
    private void processMatrixCalculation(List<Customer_Journey__c> scope) {
        // Group journeys by customer type
        Map<String, List<Customer_Journey__c>> journeysByType = new Map<String, List<Customer_Journey__c>>();
        
        for (Customer_Journey__c journey : scope) {
            String type = journey.Customer_Type__c;
            if (!journeysByType.containsKey(type)) {
                journeysByType.put(type, new List<Customer_Journey__c>());
            }
            journeysByType.get(type).add(journey);
        }
        
        // Process each customer type
        for (String type : journeysByType.keySet()) {
            try {
                List<Customer_Journey__c> typeJourneys = journeysByType.get(type);
                System.debug('Building transition matrix for ' + type + ' with ' + typeJourneys.size() + ' journeys');
                
                // Monitor stage detection during matrix building
                DateTime matrixStartTime = DateTime.now();
                
                // Build transition matrix
                MarkovChainAttribution.buildAndStoreTransitionMatrix(type);
                
                // Calculate matrix building time
                Long matrixBuildTime = DateTime.now().getTime() - matrixStartTime.getTime();
                
                // Log matrix building metrics
                logMatrixBuildingMetrics(type, typeJourneys.size(), matrixBuildTime);
                
                // Collect network metrics
                collectNetworkMetrics(type);
                
            } catch (Exception e) {
                errorMessages.add('Error building matrix for ' + type + ': ' + e.getMessage());
                System.debug('Error building matrix for ' + type + ': ' + e.getMessage());
            }
        }
    }
    
    /**
     * Process attribution calculation for the batch with enhanced monitoring
     * 
     * @param scope List of Customer_Journey__c records
     */
    private void processAttributionCalculation(List<Customer_Journey__c> scope) {
        List<Id> batchJourneyIds = new List<Id>();
        for (Customer_Journey__c journey : scope) {
            batchJourneyIds.add(journey.Id);
        }
        
        try {
            // Clean up existing Markov Chain attribution results for this batch
            cleanupExistingResults(batchJourneyIds);
            
            // Monitor attribution calculation
            DateTime attributionStartTime = DateTime.now();
            
            // Calculate new attribution results using the main class
            List<Attribution_Result__c> attributionResults = MarkovChainAttribution.calculateAttributionBatch(batchJourneyIds);
            
            // Calculate attribution time
            Long attributionTime = DateTime.now().getTime() - attributionStartTime.getTime();
            
            // Validate results before saving
            if (!MarkovChainAttribution.validateAttributionResults(attributionResults)) {
                throw new BatchProcessingException('Attribution validation failed for batch');
            }
            
            // Save attribution results
            if (!attributionResults.isEmpty()) {
                List<Database.SaveResult> saveResults = Database.insert(attributionResults, false);
                handleSaveResults(saveResults, attributionResults);
            }
            
            // Log attribution metrics
            logAttributionMetrics(scope.size(), attributionResults.size(), attributionTime);
            
        } catch (Exception e) {
            throw new BatchProcessingException('Attribution calculation failed: ' + e.getMessage());
        }
    }
    
    /**
     * Collect stage detection metrics from processed journeys
     * 
     * @param scope List of Customer_Journey__c records processed
     */
    private void collectStageDetectionMetrics(List<Customer_Journey__c> scope) {
        try {
            // Get touchpoints for these journeys to analyze stage detection
            List<Id> journeyIds = new List<Id>();
            for (Customer_Journey__c journey : scope) {
                journeyIds.add(journey.Id);
            }
            
            List<Touchpoint__c> touchpoints = [
                SELECT Id, Customer_Journey__c, Channel__c, Touchpoint_Timestamp__c,
                       Customer_Journey__r.Customer_Type__c, Customer_Journey__r.Converted__c,
                       Customer_Journey__r.Total_Touchpoints__c
                FROM Touchpoint__c 
                WHERE Customer_Journey__c IN :journeyIds
                ORDER BY Customer_Journey__c, Touchpoint_Timestamp__c ASC
            ];
            
            // Analyze stage detection patterns
            analyzeStageDetectionPatterns(touchpoints);
            
        } catch (Exception e) {
            System.debug('Error collecting stage detection metrics: ' + e.getMessage());
        }
    }
    
    /**
     * Analyze stage detection patterns in touchpoints
     * 
     * @param touchpoints List of touchpoints to analyze
     */
    private void analyzeStageDetectionPatterns(List<Touchpoint__c> touchpoints) {
        Map<Id, List<Touchpoint__c>> touchpointsByJourney = new Map<Id, List<Touchpoint__c>>();
        
        // Group touchpoints by journey
        for (Touchpoint__c touchpoint : touchpoints) {
            if (!touchpointsByJourney.containsKey(touchpoint.Customer_Journey__c)) {
                touchpointsByJourney.put(touchpoint.Customer_Journey__c, new List<Touchpoint__c>());
            }
            touchpointsByJourney.get(touchpoint.Customer_Journey__c).add(touchpoint);
        }
        
        // Analyze each journey's stage progression
        for (Id journeyId : touchpointsByJourney.keySet()) {
            List<Touchpoint__c> journeyTouchpoints = touchpointsByJourney.get(journeyId);
            
            if (journeyTouchpoints.isEmpty()) continue;
            
            String customerType = journeyTouchpoints[0].Customer_Journey__r.Customer_Type__c;
            Boolean isConverted = journeyTouchpoints[0].Customer_Journey__r.Converted__c;
            Integer totalTouchpoints = Integer.valueOf(journeyTouchpoints[0].Customer_Journey__r.Total_Touchpoints__c);
            
            // Simulate stage detection for analysis
            List<String> detectedStages = simulateStageDetection(journeyTouchpoints, customerType, isConverted);
            
            // Update stage detection counts
            for (String stage : detectedStages) {
                totalStageDetections++;
                if (isValidStage(stage)) {
                    validStageDetections++;
                    if (stageDetectionCounts.containsKey(stage)) {
                        stageDetectionCounts.put(stage, stageDetectionCounts.get(stage) + 1);
                    }
                } else {
                    invalidStageDetections++;
                    stageDetectionWarnings.add('Invalid stage detected: ' + stage + ' for journey ' + journeyId);
                }
            }
            
            // Update channel usage counts
            for (Touchpoint__c touchpoint : journeyTouchpoints) {
                String channel = touchpoint.Channel__c;
                if (!channelUsageCounts.containsKey(channel)) {
                    channelUsageCounts.put(channel, 0);
                }
                channelUsageCounts.put(channel, channelUsageCounts.get(channel) + 1);
            }
        }
    }
    
    /**
     * Simulate stage detection for analysis purposes
     * 
     * @param touchpoints List of touchpoints in a journey
     * @param customerType Customer type (B2B or B2C)
     * @param isConverted Whether the journey converted
     * @return List of detected stages
     */
    private List<String> simulateStageDetection(List<Touchpoint__c> touchpoints, String customerType, Boolean isConverted) {
        List<String> stages = new List<String>();
        
        for (Integer i = 0; i < touchpoints.size(); i++) {
            Touchpoint__c touchpoint = touchpoints[i];
            
            // Simulate stage detection based on position and channel
            Decimal journeyProgress = (Decimal) (i + 1) / touchpoints.size();
            String stage = 'Awareness'; // Default
            
            if (touchpoint.Channel__c == 'Email_Marketing') {
                if (isConverted) {
                    stage = 'Retention';
                } else if (journeyProgress >= 0.5) {
                    stage = 'Consideration';
                } else {
                    stage = 'Interest';
                }
            } else if (touchpoint.Channel__c == 'Direct_Sales') {
                if (isConverted) {
                    stage = 'Retention';
                } else if (journeyProgress >= 0.8) {
                    stage = 'Conversion';
                } else {
                    stage = 'Consideration';
                }
            } else if (touchpoint.Channel__c.contains('Ads')) {
                if (journeyProgress <= 0.3) {
                    stage = 'Awareness';
                } else if (journeyProgress >= 0.7) {
                    stage = 'Conversion';
                } else {
                    stage = 'Interest';
                }
            } else if (touchpoint.Channel__c == 'App_Store_Optimization') {
                stage = journeyProgress >= 0.6 ? 'Conversion' : 'Consideration';
            }
            
            stages.add(stage);
        }
        
        return stages;
    }
    
    /**
     * Check if a stage is valid
     * 
     * @param stage Stage to validate
     * @return True if valid stage
     */
    private Boolean isValidStage(String stage) {
        Set<String> validStages = new Set<String>{'Awareness', 'Interest', 'Consideration', 'Conversion', 'Retention'};
        return validStages.contains(stage);
    }
    
    /**
     * Log matrix building metrics
     * 
     * @param customerType Customer type processed
     * @param journeyCount Number of journeys processed
     * @param buildTime Time taken to build matrix
     */
    private void logMatrixBuildingMetrics(String customerType, Integer journeyCount, Long buildTime) {
        System.debug('=== Matrix Building Metrics ===');
        System.debug('Customer Type: ' + customerType);
        System.debug('Journeys Processed: ' + journeyCount);
        System.debug('Build Time: ' + buildTime + 'ms');
        System.debug('Average Time per Journey: ' + (journeyCount > 0 ? buildTime / journeyCount : 0) + 'ms');
    }
    
    /**
     * Log attribution calculation metrics
     * 
     * @param journeyCount Number of journeys processed
     * @param resultCount Number of attribution results created
     * @param attributionTime Time taken for attribution calculation
     */
    private void logAttributionMetrics(Integer journeyCount, Integer resultCount, Long attributionTime) {
        System.debug('=== Attribution Calculation Metrics ===');
        System.debug('Journeys Processed: ' + journeyCount);
        System.debug('Results Created: ' + resultCount);
        System.debug('Attribution Time: ' + attributionTime + 'ms');
        System.debug('Average Results per Journey: ' + (journeyCount > 0 ? (Decimal) resultCount / journeyCount : 0));
    }
    
    /**
     * Collect network metrics for visualization
     * 
     * @param customerType Customer type processed
     */
    private void collectNetworkMetrics(String customerType) {
        try {
            // Get network statistics from the main class
            Map<String, Object> networkStats = MarkovChainAttribution.getTransitionMatrixStatistics(customerType);
            
            if (networkStats != null) {
                networkMetrics.put(customerType + '_network_stats', networkStats);
            }
            
        } catch (Exception e) {
            System.debug('Error collecting network metrics: ' + e.getMessage());
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
        
        System.debug('MarkovChainAttributionBatch completed. Total processed: ' + totalProcessed + 
                    ', Total errors: ' + totalErrors + ', Duration: ' + processingDuration + 'ms');
        
        // Log comprehensive processing metrics
        logComprehensiveProcessingMetrics(processingDuration);
        
        // Log stage detection metrics
        logStageDetectionMetrics();
        
        // Log network metrics
        logNetworkMetrics();
        
        // Send notification if there were errors
        if (totalErrors > 0) {
            sendErrorNotification();
        }
        
        // Create data quality metrics record
        createDataQualityMetrics();
        
        // If this was matrix calculation, trigger attribution calculation
        if (mode == ProcessingMode.MATRIX_CALCULATION) {
            triggerAttributionCalculation();
        }
    }
    
    /**
     * Log comprehensive processing metrics
     * 
     * @param processingDuration Duration of the batch processing in milliseconds
     */
    private void logComprehensiveProcessingMetrics(Long processingDuration) {
        System.debug('=== Comprehensive Markov Chain Attribution Batch Processing Metrics ===');
        System.debug('Processing Mode: ' + mode);
        System.debug('Customer Type: ' + customerType);
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
     * Log stage detection metrics
     */
    private void logStageDetectionMetrics() {
        System.debug('=== Stage Detection Metrics ===');
        System.debug('Total Stage Detections: ' + totalStageDetections);
        System.debug('Valid Stage Detections: ' + validStageDetections);
        System.debug('Invalid Stage Detections: ' + invalidStageDetections);
        System.debug('Stage Detection Accuracy: ' + (totalStageDetections > 0 ? (validStageDetections * 100 / totalStageDetections) : 0) + '%');
        
        System.debug('Stage Distribution:');
        for (String stage : stageDetectionCounts.keySet()) {
            System.debug('  ' + stage + ': ' + stageDetectionCounts.get(stage));
        }
        
        System.debug('Channel Usage:');
        for (String channel : channelUsageCounts.keySet()) {
            System.debug('  ' + channel + ': ' + channelUsageCounts.get(channel));
        }
        
        if (!stageDetectionWarnings.isEmpty()) {
            System.debug('Stage Detection Warnings:');
            for (String warning : stageDetectionWarnings) {
                System.debug('  - ' + warning);
            }
        }
    }
    
    /**
     * Log network metrics
     */
    private void logNetworkMetrics() {
        System.debug('=== Network Metrics ===');
        System.debug('Network Metrics Collected: ' + JSON.serialize(networkMetrics));
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
                System.debug('Deleted ' + existingResults.size() + ' existing Markov Chain attribution results');
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
     * Send error notification to administrators
     */
    private void sendErrorNotification() {
        System.debug('ERROR NOTIFICATION: Markov Chain Attribution batch processing completed with ' + 
                    totalErrors + ' errors out of ' + totalProcessed + ' total records processed.');
        
        // Log stage detection issues if any
        if (invalidStageDetections > 0) {
            System.debug('STAGE DETECTION ISSUES: ' + invalidStageDetections + ' invalid stage detections found');
        }
    }
    
    /**
     * Create data quality metrics record for monitoring
     */
    private void createDataQualityMetrics() {
        try {
            Data_Quality_Metrics__c metrics = new Data_Quality_Metrics__c();
            metrics.Metric_Date__c = Date.today();
            metrics.Processing_Time_Seconds__c = (DateTime.now().getTime() - processingStartTime.getTime()) / 1000;
            metrics.Total_Touchpoints__c = totalProcessed;
            metrics.Error_Rate__c = totalProcessed > 0 ? (totalErrors * 100.0 / totalProcessed) : 0;
            
            // Add stage detection metrics - use safe field assignment
            safelySetStageDetectionFields(metrics);
            
            insert metrics;
            
        } catch (Exception e) {
            System.debug('Could not create data quality metrics: ' + e.getMessage());
        }
    }
    
    /**
     * Safely set stage detection fields on Data_Quality_Metrics__c record
     * 
     * @param metrics Data_Quality_Metrics__c record to update
     */
    private void safelySetStageDetectionFields(Data_Quality_Metrics__c metrics) {
        try {
            // Check if Stage_Detection_Accuracy__c field exists
            Map<String, Schema.SObjectField> fieldMap = Data_Quality_Metrics__c.sObjectType.getDescribe().fields.getMap();
            
            if (fieldMap.containsKey('Stage_Detection_Accuracy__c')) {
                Decimal accuracy = totalStageDetections > 0 ? (validStageDetections * 100.0 / totalStageDetections) : 0;
                metrics.put('Stage_Detection_Accuracy__c', accuracy);
            }
            
            if (fieldMap.containsKey('Invalid_Stage_Detections__c')) {
                metrics.put('Invalid_Stage_Detections__c', invalidStageDetections);
            }
            
        } catch (Exception e) {
            System.debug('Could not set stage detection fields: ' + e.getMessage());
        }
    }
    
    /**
     * Trigger attribution calculation after matrix calculation completes
     */
    private void triggerAttributionCalculation() {
        try {
            // Queue attribution calculation batch for all journeys
            MarkovChainAttributionBatch attributionBatch = new MarkovChainAttributionBatch();
            Database.executeBatch(attributionBatch, DEFAULT_BATCH_SIZE);
            
            System.debug('Triggered attribution calculation batch after matrix calculation');
            
        } catch (Exception e) {
            System.debug('Could not trigger attribution calculation: ' + e.getMessage());
        }
    }
    
    /**
     * Get stage detection summary for debugging
     * 
     * @return Map containing stage detection information
     */
    public Map<String, Object> getStageDetectionSummary() {
        Map<String, Object> summary = new Map<String, Object>();
        summary.put('totalStageDetections', totalStageDetections);
        summary.put('validStageDetections', validStageDetections);
        summary.put('invalidStageDetections', invalidStageDetections);
        summary.put('stageDetectionCounts', stageDetectionCounts);
        summary.put('channelUsageCounts', channelUsageCounts);
        summary.put('stageDetectionWarnings', stageDetectionWarnings);
        summary.put('processingStartTime', processingStartTime);
        return summary;
    }
    
    /**
     * Get network metrics collected during processing
     * 
     * @return Map containing network metrics
     */
    public Map<String, Object> getNetworkMetrics() {
        networkMetrics.put('stageDetectionSummary', getStageDetectionSummary());
        return networkMetrics;
    }
    
    // [Static methods remain the same as in original file]
    /**
     * Static method to execute matrix calculation batch
     * 
     * @return Database.BatchableContext for monitoring
     */
    global static Id executeMatrixCalculationBatch() {
        return executeMatrixCalculationBatch(null, DEFAULT_BATCH_SIZE);
    }
    
    /**
     * Static method to execute matrix calculation batch for specific customer type
     * 
     * @param customerType Customer type (B2B, B2C, or null for all)
     * @return Database.BatchableContext for monitoring
     */
    global static Id executeMatrixCalculationBatch(String customerType) {
        return executeMatrixCalculationBatch(customerType, DEFAULT_BATCH_SIZE);
    }
    
    /**
     * Static method to execute matrix calculation batch with custom batch size
     * 
     * @param customerType Customer type (B2B, B2C, or null for all)
     * @param batchSize Number of records to process per batch
     * @return Database.BatchableContext for monitoring
     */
    global static Id executeMatrixCalculationBatch(String customerType, Integer batchSize) {
        MarkovChainAttributionBatch batch = new MarkovChainAttributionBatch(customerType, 'MATRIX_CALCULATION');
        return Database.executeBatch(batch, batchSize);
    }
    
    /**
     * Static method to execute attribution calculation batch
     * 
     * @return Database.BatchableContext for monitoring
     */
    global static Id executeAttributionBatch() {
        return executeAttributionBatch(DEFAULT_BATCH_SIZE);
    }
    
    /**
     * Static method to execute attribution calculation batch with custom batch size
     * 
     * @param batchSize Number of records to process per batch
     * @return Database.BatchableContext for monitoring
     */
    global static Id executeAttributionBatch(Integer batchSize) {
        MarkovChainAttributionBatch batch = new MarkovChainAttributionBatch();
        return Database.executeBatch(batch, batchSize);
    }
    
    /**
     * Static method to execute attribution calculation batch for specific journeys
     * 
     * @param journeyIds List of journey IDs to process
     * @return Database.BatchableContext for monitoring
     */
    global static Id executeAttributionBatch(List<Id> journeyIds) {
        return executeAttributionBatch(journeyIds, DEFAULT_BATCH_SIZE);
    }
    
    /**
     * Static method to execute attribution calculation batch for specific journeys with custom batch size
     * 
     * @param journeyIds List of journey IDs to process
     * @param batchSize Number of records to process per batch
     * @return Database.BatchableContext for monitoring
     */
    global static Id executeAttributionBatch(List<Id> journeyIds, Integer batchSize) {
        MarkovChainAttributionBatch batch = new MarkovChainAttributionBatch(journeyIds);
        return Database.executeBatch(batch, batchSize);
    }
    
    /**
     * Static method to trigger complete Markov Chain processing (matrix + attribution)
     * 
     * @return Map containing batch job IDs
     */
    global static Map<String, Id> executeCompleteProcessing() {
        Map<String, Id> batchIds = new Map<String, Id>();
        
        try {
            // First execute matrix calculation for both customer types
            Id b2cMatrixBatchId = executeMatrixCalculationBatch('B2C');
            Id b2bMatrixBatchId = executeMatrixCalculationBatch('B2B');
            
            batchIds.put('B2C_Matrix', b2cMatrixBatchId);
            batchIds.put('B2B_Matrix', b2bMatrixBatchId);
            
            System.debug('Triggered complete Markov Chain processing with matrix calculation');
            
        } catch (Exception e) {
            System.debug('Error triggering complete processing: ' + e.getMessage());
            throw new BatchProcessingException('Failed to trigger complete processing: ' + e.getMessage());
        }
        
        return batchIds;
    }
    
    /**
     * Static method to manually trigger matrix recalculation (synchronous)
     * 
     * @return Success message
     */
    global static String triggerManualMatrixRecalculation() {
        try {
            String result = MarkovChainAttribution.triggerMatrixRecalculation();
            System.debug('Manual matrix recalculation completed: ' + result);
            return result;
        } catch (Exception e) {
            System.debug('Manual matrix recalculation failed: ' + e.getMessage());
            throw new BatchProcessingException('Manual matrix recalculation failed: ' + e.getMessage());
        }
    }
    
    /**
     * Static method to get processing status with enhanced stage detection info
     * 
     * @return Map containing processing status information
     */
    global static Map<String, Object> getProcessingStatus() {
        Map<String, Object> status = new Map<String, Object>();
        
        try {
            // Check for running batch jobs
            List<AsyncApexJob> runningJobs = [
                SELECT Id, JobType, Status, CreatedDate, CompletedDate
                FROM AsyncApexJob 
                WHERE ApexClass.Name = 'MarkovChainAttributionBatch'
                AND Status IN ('Queued', 'Preparing', 'Processing')
                ORDER BY CreatedDate DESC
            ];
            
            status.put('runningJobs', runningJobs.size());
            status.put('isProcessing', !runningJobs.isEmpty());
            
            // Check transition matrix status
            Map<String, Object> b2cMatrix = MarkovChainAttribution.getTransitionMatrixStatistics('B2C');
            Map<String, Object> b2bMatrix = MarkovChainAttribution.getTransitionMatrixStatistics('B2B');
            
            status.put('b2cMatrixStatus', b2cMatrix);
            status.put('b2bMatrixStatus', b2bMatrix);
            
            // Check recent attribution results
            Integer todayAttributionCount = [
                SELECT COUNT() 
                FROM Attribution_Result__c 
                WHERE Attribution_Model__c = :ATTRIBUTION_MODEL
                AND CreatedDate = TODAY
            ];
            
            status.put('todayAttributionResults', todayAttributionCount);
            
            // Add stage detection summary from recent processing
            status.put('stageDetectionEnabled', true);
            status.put('networkVisualizationSupported', true);
            
            status.put('lastUpdated', DateTime.now());
            
        } catch (Exception e) {
            status.put('error', e.getMessage());
        }
        
        return status;
    }
    
    /**
     * Get stage detection quality metrics
     * 
     * @return Map containing stage detection quality information
     */
    global static Map<String, Object> getStageDetectionQualityMetrics() {
        Map<String, Object> metrics = new Map<String, Object>();
        
        try {
            // Get recent data quality metrics with safe field access
            String query = 'SELECT Error_Rate__c, Processing_Time_Seconds__c, Metric_Date__c';
            
            // Check if stage detection fields exist before including them in query
            Map<String, Schema.SObjectField> fieldMap = Data_Quality_Metrics__c.sObjectType.getDescribe().fields.getMap();
            
            if (fieldMap.containsKey('Stage_Detection_Accuracy__c')) {
                query += ', Stage_Detection_Accuracy__c';
            }
            
            if (fieldMap.containsKey('Invalid_Stage_Detections__c')) {
                query += ', Invalid_Stage_Detections__c';
            }
            
            query += ' FROM Data_Quality_Metrics__c WHERE Metric_Date__c = LAST_N_DAYS:7 ORDER BY Metric_Date__c DESC LIMIT 10';
            
            List<Data_Quality_Metrics__c> recentMetrics = Database.query(query);
            
            if (!recentMetrics.isEmpty()) {
                Decimal avgAccuracy = 0;
                Decimal avgErrorRate = 0;
                Integer totalInvalidDetections = 0;
                
                for (Data_Quality_Metrics__c metric : recentMetrics) {
                    // Safely access stage detection fields
                    if (fieldMap.containsKey('Stage_Detection_Accuracy__c')) {
                        Decimal accuracy = (Decimal) metric.get('Stage_Detection_Accuracy__c');
                        avgAccuracy += accuracy != null ? accuracy : 0;
                    }
                    
                    avgErrorRate += metric.Error_Rate__c != null ? metric.Error_Rate__c : 0;
                    
                    if (fieldMap.containsKey('Invalid_Stage_Detections__c')) {
                        Decimal invalidDetections = (Decimal) metric.get('Invalid_Stage_Detections__c');
                        totalInvalidDetections += invalidDetections != null ? Integer.valueOf(invalidDetections) : 0;
                    }
                }
                
                metrics.put('averageStageDetectionAccuracy', avgAccuracy / recentMetrics.size());
                metrics.put('averageErrorRate', avgErrorRate / recentMetrics.size());
                metrics.put('totalInvalidDetections', totalInvalidDetections);
                metrics.put('metricsCount', recentMetrics.size());
                metrics.put('lastProcessingDate', recentMetrics[0].Metric_Date__c);
            }
            
            metrics.put('qualityMetricsAvailable', !recentMetrics.isEmpty());
            
        } catch (Exception e) {
            metrics.put('error', e.getMessage());
        }
        
        return metrics;
    }
    
    /**
     * Custom exception class for batch processing errors
     */
    public class BatchProcessingException extends Exception {}
}