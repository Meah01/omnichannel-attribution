/**
 * MarkovChainMatrixScheduler
 * 
 * Enhanced scheduled class for automatic recalculation of Markov Chain transition matrices
 * with comprehensive stage detection monitoring and network visualization data management.
 * Runs on configurable schedules to ensure matrices stay current with latest customer journey data.
 * 
 * Features:
 * - Weekly automatic matrix recalculation
 * - Separate processing for B2C and B2B customer types
 * - Stage detection quality monitoring
 * - Network visualization data validation
 * - Enhanced error handling and logging
 * - Configurable scheduling with quality checks
 * - Integration with enhanced batch processing system
 * 
 * Usage:
 * - Schedule weekly: MarkovChainMatrixScheduler.scheduleWeekly()
 * - Schedule daily: MarkovChainMatrixScheduler.scheduleDaily()
 * - Manual execution: MarkovChainMatrixScheduler.executeNow()
 * - Network validation: MarkovChainMatrixScheduler.validateNetworkVisualization()
 * 
 * Author: Alexandru Constantinescu (www.linkedin.com/in/alexandru-constantinescu-6a18b6117)
 * Project: Omnichannel Attribution Platform
 * Date: 2025
 * Version: 2.0 (Improved for Stage Detection and Network Visualization)
 */
global class MarkovChainMatrixScheduler implements Schedulable {
    
    // Default scheduling configurations
    private static final String WEEKLY_CRON = '0 0 2 ? * SUN';  // Sunday at 2 AM
    private static final String DAILY_CRON = '0 0 1 * * ?';     // Daily at 1 AM
    private static final String MONTHLY_CRON = '0 0 3 1 * ?';   // First day of month at 3 AM
    
    // Network visualization update schedule
    private static final String NETWORK_UPDATE_CRON = '0 30 2 ? * SUN'; // Sunday at 2:30 AM (after matrix update)
    
    // Processing mode
    public enum SchedulingMode {
        MATRIX_ONLY,        // Only recalculate matrices
        COMPLETE_PROCESSING, // Recalculate matrices and run attribution
        NETWORK_VALIDATION  // Validate network visualization data
    }
    
    private SchedulingMode mode;
    
    /**
     * Default constructor - matrix-only mode
     */
    global MarkovChainMatrixScheduler() {
        this.mode = SchedulingMode.MATRIX_ONLY;
    }
    
    /**
     * Constructor with processing mode
     * 
     * @param modeString The scheduling mode as string
     */
    global MarkovChainMatrixScheduler(String modeString) {
        if (modeString == 'COMPLETE_PROCESSING') {
            this.mode = SchedulingMode.COMPLETE_PROCESSING;
        } else if (modeString == 'NETWORK_VALIDATION') {
            this.mode = SchedulingMode.NETWORK_VALIDATION;
        } else {
            this.mode = SchedulingMode.MATRIX_ONLY;
        }
    }
    
    /**
     * Execute method - called by Salesforce scheduler
     * 
     * @param context SchedulableContext provided by Salesforce
     */
    global void execute(SchedulableContext context) {
        System.debug('MarkovChainMatrixScheduler started at: ' + DateTime.now() + ' in mode: ' + mode);
        
        try {
            switch on mode {
                when COMPLETE_PROCESSING {
                    executeCompleteProcessing();
                }
                when NETWORK_VALIDATION {
                    executeNetworkValidation();
                }
                when else {
                    executeMatrixRecalculation();
                }
            }
            
            System.debug('MarkovChainMatrixScheduler completed successfully');
            
        } catch (Exception e) {
            System.debug('MarkovChainMatrixScheduler error: ' + e.getMessage());
            handleSchedulerError(e);
        }
    }
    
    /**
     * Execute matrix recalculation with stage detection monitoring
     */
    private void executeMatrixRecalculation() {
        System.debug('Starting matrix recalculation with stage detection monitoring');
        
        try {
            // Check if sufficient data exists for matrix calculation
            if (!hasMinimumDataForMatrixCalculation()) {
                System.debug('Insufficient data for matrix calculation - skipping');
                return;
            }
            
            // Pre-calculation validation
            Map<String, Object> preCalculationMetrics = collectPreCalculationMetrics();
            
            // Execute matrix calculation batches for both customer types
            Id b2cBatchId = MarkovChainAttributionBatch.executeMatrixCalculationBatch('B2C');
            Id b2bBatchId = MarkovChainAttributionBatch.executeMatrixCalculationBatch('B2B');
            
            System.debug('Matrix calculation batches started - B2C: ' + b2cBatchId + ', B2B: ' + b2bBatchId);
            
            // Log scheduling metrics with stage detection info
            logSchedulingMetrics('MATRIX_RECALCULATION', new Map<String, Id>{
                'B2C_Matrix' => b2cBatchId,
                'B2B_Matrix' => b2bBatchId
            });
            
            // Log pre-calculation metrics
            logPreCalculationMetrics(preCalculationMetrics);
            
        } catch (Exception e) {
            System.debug('Matrix recalculation failed: ' + e.getMessage());
            throw new SchedulerException('Matrix recalculation failed: ' + e.getMessage());
        }
    }
    
    /**
     * Execute complete processing with enhanced monitoring
     */
    private void executeCompleteProcessing() {
        System.debug('Starting complete Markov Chain processing with stage detection');
        
        try {
            // Execute complete processing
            Map<String, Id> batchIds = MarkovChainAttributionBatch.executeCompleteProcessing();
            
            System.debug('Complete processing started with batch IDs: ' + JSON.serialize(batchIds));
            
            // Log scheduling metrics
            logSchedulingMetrics('COMPLETE_PROCESSING', batchIds);
            
            // Schedule network validation to run after batch completion
            scheduleNetworkValidationAfterDelay();
            
        } catch (Exception e) {
            System.debug('Complete processing failed: ' + e.getMessage());
            throw new SchedulerException('Complete processing failed: ' + e.getMessage());
        }
    }
    
    /**
     * Execute network visualization validation
     */
    private void executeNetworkValidation() {
        System.debug('Starting network visualization validation');
        
        try {
            // Validate network visualization data for both customer types
            Map<String, Object> b2cNetworkStatus = validateNetworkVisualizationData('B2C');
            Map<String, Object> b2bNetworkStatus = validateNetworkVisualizationData('B2B');
            
            // Log network validation results
            logNetworkValidationResults(b2cNetworkStatus, b2bNetworkStatus);
            
            // Check stage detection quality
            Map<String, Object> stageDetectionQuality = MarkovChainAttributionBatch.getStageDetectionQualityMetrics();
            logStageDetectionQuality(stageDetectionQuality);
            
        } catch (Exception e) {
            System.debug('Network validation failed: ' + e.getMessage());
            throw new SchedulerException('Network validation failed: ' + e.getMessage());
        }
    }
    
    /**
     * Validate network visualization data for a customer type
     * 
     * @param customerType Customer type (B2B or B2C)
     * @return Map containing validation results
     */
    private Map<String, Object> validateNetworkVisualizationData(String customerType) {
        Map<String, Object> validationResults = new Map<String, Object>();
        
        try {
            // Get network visualization data
            Map<String, Object> networkData = MarkovChainAttribution.getNetworkVisualization(customerType);
            
            // Validate network data structure
            validationResults.put('customerType', customerType);
            validationResults.put('hasNetworkData', networkData != null);
            validationResults.put('hasError', networkData.containsKey('error'));
            
            if (networkData != null && !networkData.containsKey('error')) {
                // Validate required components
                validationResults.put('hasNetworkNodes', networkData.containsKey('networkNodes'));
                validationResults.put('hasTransitions', networkData.containsKey('allTransitions'));
                validationResults.put('hasConversionPaths', networkData.containsKey('conversionPaths'));
                validationResults.put('hasNetworkStatistics', networkData.containsKey('networkStatistics'));
                validationResults.put('hasStageAnalysis', networkData.containsKey('stageAnalysis'));
                
                // Validate data quality
                Integer totalStates = (Integer) networkData.get('totalStates');
                Integer totalTransitions = (Integer) networkData.get('totalTransitions');
                
                validationResults.put('totalStates', totalStates);
                validationResults.put('totalTransitions', totalTransitions);
                validationResults.put('hasMinimumStates', totalStates >= 5);
                validationResults.put('hasMinimumTransitions', totalTransitions >= 10);
                
                // Validate network statistics
                if (networkData.containsKey('networkStatistics')) {
                    Map<String, Object> networkStats = (Map<String, Object>) networkData.get('networkStatistics');
                    validationResults.put('hasConversionRate', networkStats.containsKey('conversionRate'));
                    validationResults.put('hasNetworkDensity', networkStats.containsKey('networkDensity'));
                    
                    if (networkStats.containsKey('conversionRate')) {
                        Decimal conversionRate = (Decimal) networkStats.get('conversionRate');
                        validationResults.put('conversionRate', conversionRate);
                        validationResults.put('hasReasonableConversionRate', conversionRate >= 0 && conversionRate <= 100);
                    }
                }
                
                // Validate stage analysis
                if (networkData.containsKey('stageAnalysis')) {
                    Map<String, Object> stageAnalysis = (Map<String, Object>) networkData.get('stageAnalysis');
                    validationResults.put('hasStageTransitions', stageAnalysis.containsKey('stageTransitions'));
                    validationResults.put('hasChannelStagePerformance', stageAnalysis.containsKey('channelStagePerformance'));
                }
                
                validationResults.put('validationStatus', 'PASSED');
                
            } else {
                validationResults.put('validationStatus', 'FAILED');
                if (networkData != null && networkData.containsKey('error')) {
                    validationResults.put('error', networkData.get('error'));
                }
            }
            
        } catch (Exception e) {
            validationResults.put('validationStatus', 'ERROR');
            validationResults.put('error', e.getMessage());
        }
        
        return validationResults;
    }
    
    /**
     * Collect pre-calculation metrics for monitoring
     * 
     * @return Map containing pre-calculation metrics
     */
    private Map<String, Object> collectPreCalculationMetrics() {
        Map<String, Object> metrics = new Map<String, Object>();
        
        try {
            // Get journey counts by customer type
            List<AggregateResult> journeyCounts = [
                SELECT Customer_Type__c, COUNT(Id) journeyCount
                FROM Customer_Journey__c 
                WHERE Total_Touchpoints__c > 0
                AND CreatedDate = LAST_N_DAYS:30
                GROUP BY Customer_Type__c
            ];
            
            for (AggregateResult result : journeyCounts) {
                String customerType = (String) result.get('Customer_Type__c');
                Integer count = (Integer) result.get('journeyCount');
                metrics.put(customerType + '_journey_count', count);
            }
            
            // Get touchpoint counts
            Integer totalTouchpoints = [
                SELECT COUNT() FROM Touchpoint__c 
                WHERE Customer_Journey__r.Total_Touchpoints__c > 0
                AND CreatedDate = LAST_N_DAYS:30
            ];
            
            metrics.put('total_touchpoints', totalTouchpoints);
            
            // Get existing matrix counts
            Integer existingB2CMatrices = [
                SELECT COUNT() FROM Transition_Matrix__c WHERE Customer_Type__c = 'B2C'
            ];
            
            Integer existingB2BMatrices = [
                SELECT COUNT() FROM Transition_Matrix__c WHERE Customer_Type__c = 'B2B'
            ];
            
            metrics.put('existing_b2c_matrices', existingB2CMatrices);
            metrics.put('existing_b2b_matrices', existingB2BMatrices);
            
        } catch (Exception e) {
            metrics.put('error', e.getMessage());
        }
        
        return metrics;
    }
    
    /**
     * Schedule network validation to run after batch completion
     */
    private void scheduleNetworkValidationAfterDelay() {
        try {
            // Schedule network validation to run 30 minutes after batch jobs should complete
            DateTime scheduledTime = DateTime.now().addMinutes(30);
            String cronExpression = '0 ' + scheduledTime.minute() + ' ' + scheduledTime.hour() + ' ' + 
                                  scheduledTime.day() + ' ' + scheduledTime.month() + ' ? ' + scheduledTime.year();
            
            MarkovChainMatrixScheduler networkValidator = new MarkovChainMatrixScheduler('NETWORK_VALIDATION');
            String jobId = System.schedule('Markov Chain Network Validation - ' + scheduledTime.format(), 
                                         cronExpression, networkValidator);
            
            System.debug('Scheduled network validation job: ' + jobId + ' for ' + scheduledTime);
            
        } catch (Exception e) {
            System.debug('Could not schedule network validation: ' + e.getMessage());
        }
    }
    
    /**
     * Check if minimum data exists for matrix calculation
     * 
     * @return True if sufficient data exists
     */
    private Boolean hasMinimumDataForMatrixCalculation() {
        try {
            // Check for minimum number of journeys per customer type
            List<AggregateResult> journeyCounts = [
                SELECT Customer_Type__c, COUNT(Id) journeyCount
                FROM Customer_Journey__c 
                WHERE Total_Touchpoints__c > 0
                AND CreatedDate = LAST_N_DAYS:30  // Only consider recent data
                GROUP BY Customer_Type__c
            ];
            
            Integer minJourneysRequired = 50; // Minimum journeys needed for reliable matrix
            
            Map<String, Integer> countsByType = new Map<String, Integer>();
            for (AggregateResult result : journeyCounts) {
                String customerType = (String) result.get('Customer_Type__c');
                Integer count = (Integer) result.get('journeyCount');
                countsByType.put(customerType, count);
            }
            
            Boolean hasB2CData = countsByType.get('B2C') != null && countsByType.get('B2C') >= minJourneysRequired;
            Boolean hasB2BData = countsByType.get('B2B') != null && countsByType.get('B2B') >= minJourneysRequired;
            
            System.debug('Data availability check - B2C: ' + countsByType.get('B2C') + ' journeys, B2B: ' + countsByType.get('B2B') + ' journeys');
            
            return hasB2CData || hasB2BData;
            
        } catch (Exception e) {
            System.debug('Error checking data availability: ' + e.getMessage());
            return false; // Conservative approach - skip if we can't check
        }
    }
    
    /**
     * Log scheduling metrics for monitoring
     * 
     * @param processType Type of processing executed
     * @param batchIds Map of batch job IDs
     */
    private void logSchedulingMetrics(String processType, Map<String, Id> batchIds) {
        try {
            System.debug('=== Enhanced Markov Chain Scheduler Metrics ===');
            System.debug('Process Type: ' + processType);
            System.debug('Execution Time: ' + DateTime.now());
            System.debug('Mode: ' + mode);
            System.debug('Batch IDs: ' + JSON.serialize(batchIds));
            
            // Add stage detection status
            System.debug('Stage Detection Enabled: true');
            System.debug('Network Visualization Enabled: true');
            
        } catch (Exception e) {
            System.debug('Error logging scheduling metrics: ' + e.getMessage());
        }
    }
    
    /**
     * Log pre-calculation metrics
     * 
     * @param metrics Pre-calculation metrics
     */
    private void logPreCalculationMetrics(Map<String, Object> metrics) {
        try {
            System.debug('=== Pre-Calculation Metrics ===');
            System.debug('Metrics: ' + JSON.serialize(metrics));
            
        } catch (Exception e) {
            System.debug('Error logging pre-calculation metrics: ' + e.getMessage());
        }
    }
    
    /**
     * Log network validation results
     * 
     * @param b2cNetworkStatus B2C network validation results
     * @param b2bNetworkStatus B2B network validation results
     */
    private void logNetworkValidationResults(Map<String, Object> b2cNetworkStatus, Map<String, Object> b2bNetworkStatus) {
        try {
            System.debug('=== Network Validation Results ===');
            System.debug('B2C Network Status: ' + JSON.serialize(b2cNetworkStatus));
            System.debug('B2B Network Status: ' + JSON.serialize(b2bNetworkStatus));
            
            // Log summary
            String b2cStatus = (String) b2cNetworkStatus.get('validationStatus');
            String b2bStatus = (String) b2bNetworkStatus.get('validationStatus');
            
            System.debug('B2C Validation: ' + b2cStatus);
            System.debug('B2B Validation: ' + b2bStatus);
            
            // Log conversion rates if available
            if (b2cNetworkStatus.containsKey('conversionRate')) {
                System.debug('B2C Conversion Rate: ' + b2cNetworkStatus.get('conversionRate') + '%');
            }
            
            if (b2bNetworkStatus.containsKey('conversionRate')) {
                System.debug('B2B Conversion Rate: ' + b2bNetworkStatus.get('conversionRate') + '%');
            }
            
        } catch (Exception e) {
            System.debug('Error logging network validation results: ' + e.getMessage());
        }
    }
    
    /**
     * Log stage detection quality metrics
     * 
     * @param stageDetectionQuality Stage detection quality metrics
     */
    private void logStageDetectionQuality(Map<String, Object> stageDetectionQuality) {
        try {
            System.debug('=== Stage Detection Quality Metrics ===');
            System.debug('Quality Metrics: ' + JSON.serialize(stageDetectionQuality));
            
            if (stageDetectionQuality.containsKey('averageStageDetectionAccuracy')) {
                Decimal accuracy = (Decimal) stageDetectionQuality.get('averageStageDetectionAccuracy');
                System.debug('Average Stage Detection Accuracy: ' + accuracy + '%');
                
                if (accuracy < 80) {
                    System.debug('WARNING: Stage detection accuracy is below 80%');
                }
            }
            
        } catch (Exception e) {
            System.debug('Error logging stage detection quality: ' + e.getMessage());
        }
    }
    
    /**
     * Handle scheduler errors with enhanced error information
     * 
     * @param e The exception that occurred
     */
    private void handleSchedulerError(Exception e) {
        try {
            System.debug('ERROR: Enhanced Markov Chain Matrix Scheduler failed');
            System.debug('Error Message: ' + e.getMessage());
            System.debug('Stack Trace: ' + e.getStackTraceString());
            System.debug('Processing Mode: ' + mode);
            System.debug('Execution Time: ' + DateTime.now());
            
            // Log additional context based on mode
            if (mode == SchedulingMode.NETWORK_VALIDATION) {
                System.debug('Network validation failure - checking matrix status');
                try {
                    Map<String, Object> b2cStatus = MarkovChainAttribution.getTransitionMatrixStatistics('B2C');
                    Map<String, Object> b2bStatus = MarkovChainAttribution.getTransitionMatrixStatistics('B2B');
                    System.debug('B2C Matrix Status: ' + JSON.serialize(b2cStatus));
                    System.debug('B2B Matrix Status: ' + JSON.serialize(b2bStatus));
                } catch (Exception statusError) {
                    System.debug('Could not retrieve matrix status: ' + statusError.getMessage());
                }
            }
            
        } catch (Exception logError) {
            System.debug('Failed to log enhanced scheduler error: ' + logError.getMessage());
        }
    }
    
    /**
     * Schedule weekly matrix recalculation (recommended)
     * 
     * @return Scheduled job ID
     */
    global static String scheduleWeekly() {
        return scheduleWeekly('MATRIX_ONLY');
    }
    
    /**
     * Schedule weekly processing with specified mode
     * 
     * @param modeString The scheduling mode as string
     * @return Scheduled job ID
     */
    global static String scheduleWeekly(String modeString) {
        SchedulingMode mode = getSchedulingMode(modeString);
        String jobName = 'Markov Chain Matrix Scheduler - Weekly - ' + mode;
        MarkovChainMatrixScheduler scheduler = new MarkovChainMatrixScheduler(modeString);
        
        try {
            return System.schedule(jobName, WEEKLY_CRON, scheduler);
        } catch (Exception e) {
            System.debug('Failed to schedule weekly job: ' + e.getMessage());
            throw new SchedulerException('Failed to schedule weekly job: ' + e.getMessage());
        }
    }
    
    /**
     * Schedule daily matrix recalculation (for high-volume environments)
     * 
     * @return Scheduled job ID
     */
    global static String scheduleDaily() {
        return scheduleDaily('MATRIX_ONLY');
    }
    
    /**
     * Schedule daily processing with specified mode
     * 
     * @param modeString The scheduling mode as string
     * @return Scheduled job ID
     */
    global static String scheduleDaily(String modeString) {
        SchedulingMode mode = getSchedulingMode(modeString);
        String jobName = 'Markov Chain Matrix Scheduler - Daily - ' + mode;
        MarkovChainMatrixScheduler scheduler = new MarkovChainMatrixScheduler(modeString);
        
        try {
            return System.schedule(jobName, DAILY_CRON, scheduler);
        } catch (Exception e) {
            System.debug('Failed to schedule daily job: ' + e.getMessage());
            throw new SchedulerException('Failed to schedule daily job: ' + e.getMessage());
        }
    }
    
    /**
     * Schedule monthly matrix recalculation (for low-volume environments)
     * 
     * @return Scheduled job ID
     */
    global static String scheduleMonthly() {
        return scheduleMonthly('MATRIX_ONLY');
    }
    
    /**
     * Schedule monthly processing with specified mode
     * 
     * @param modeString The scheduling mode as string
     * @return Scheduled job ID
     */
    global static String scheduleMonthly(String modeString) {
        SchedulingMode mode = getSchedulingMode(modeString);
        String jobName = 'Markov Chain Matrix Scheduler - Monthly - ' + mode;
        MarkovChainMatrixScheduler scheduler = new MarkovChainMatrixScheduler(modeString);
        
        try {
            return System.schedule(jobName, MONTHLY_CRON, scheduler);
        } catch (Exception e) {
            System.debug('Failed to schedule monthly job: ' + e.getMessage());
            throw new SchedulerException('Failed to schedule monthly job: ' + e.getMessage());
        }
    }
    
    /**
     * Schedule network visualization validation
     * 
     * @return Scheduled job ID
     */
    global static String scheduleNetworkValidation() {
        String jobName = 'Markov Chain Network Validation - Weekly';
        MarkovChainMatrixScheduler scheduler = new MarkovChainMatrixScheduler('NETWORK_VALIDATION');
        
        try {
            return System.schedule(jobName, NETWORK_UPDATE_CRON, scheduler);
        } catch (Exception e) {
            System.debug('Failed to schedule network validation job: ' + e.getMessage());
            throw new SchedulerException('Failed to schedule network validation job: ' + e.getMessage());
        }
    }
    
    /**
     * Execute scheduler immediately (for testing and manual runs)
     * 
     * @return Success message
     */
    global static String executeNow() {
        return executeNow('MATRIX_ONLY');
    }
    
    /**
     * Execute scheduler immediately with specified mode
     * 
     * @param modeString The scheduling mode as string
     * @return Success message
     */
    global static String executeNow(String modeString) {
        try {
            SchedulingMode mode = getSchedulingMode(modeString);
            MarkovChainMatrixScheduler scheduler = new MarkovChainMatrixScheduler(modeString);
            scheduler.execute(null);
            return 'Markov Chain Matrix Scheduler executed successfully in mode: ' + mode;
        } catch (Exception e) {
            System.debug('Immediate execution failed: ' + e.getMessage());
            throw new SchedulerException('Immediate execution failed: ' + e.getMessage());
        }
    }
    
    /**
     * Validate network visualization data immediately
     * 
     * @return Network validation results
     */
    global static Map<String, Object> validateNetworkVisualization() {
        Map<String, Object> results = new Map<String, Object>();
        
        try {
            MarkovChainMatrixScheduler scheduler = new MarkovChainMatrixScheduler('NETWORK_VALIDATION');
            
            // Validate network data for both customer types
            Map<String, Object> b2cResults = scheduler.validateNetworkVisualizationData('B2C');
            Map<String, Object> b2bResults = scheduler.validateNetworkVisualizationData('B2B');
            
            results.put('b2c_validation', b2cResults);
            results.put('b2b_validation', b2bResults);
            results.put('validation_time', DateTime.now());
            results.put('overall_status', 'COMPLETED');
            
        } catch (Exception e) {
            results.put('error', e.getMessage());
            results.put('overall_status', 'ERROR');
        }
        
        return results;
    }
    
    /**
     * Get all scheduled Markov Chain jobs including network validation
     * 
     * @return List of scheduled jobs
     */
    global static List<CronTrigger> getScheduledJobs() {
        return [
            SELECT Id, CronJobDetail.Name, NextFireTime, PreviousFireTime, 
                   State, CronExpression, CreatedDate
            FROM CronTrigger 
            WHERE CronJobDetail.Name LIKE '%Markov Chain%'
            ORDER BY CreatedDate DESC
        ];
    }
    
    /**
     * Cancel all scheduled Markov Chain jobs
     * 
     * @return Number of jobs cancelled
     */
    global static Integer cancelAllScheduledJobs() {
        List<CronTrigger> scheduledJobs = getScheduledJobs();
        Integer cancelledCount = 0;
        
        for (CronTrigger job : scheduledJobs) {
            try {
                System.abortJob(job.Id);
                cancelledCount++;
            } catch (Exception e) {
                System.debug('Failed to cancel job ' + job.Id + ': ' + e.getMessage());
            }
        }
        
        System.debug('Cancelled ' + cancelledCount + ' scheduled Markov Chain jobs');
        return cancelledCount;
    }
    
    /**
     * Get scheduler status with enhanced network visualization information
     * 
     * @return Map containing scheduler status information
     */
    global static Map<String, Object> getSchedulerStatus() {
        Map<String, Object> status = new Map<String, Object>();
        
        try {
            List<CronTrigger> scheduledJobs = getScheduledJobs();
            
            status.put('totalScheduledJobs', scheduledJobs.size());
            status.put('scheduledJobs', scheduledJobs);
            
            // Find next execution time
            DateTime nextExecution = null;
            String nextJobType = null;
            
            for (CronTrigger job : scheduledJobs) {
                if (job.NextFireTime != null) {
                    if (nextExecution == null || job.NextFireTime < nextExecution) {
                        nextExecution = job.NextFireTime;
                        nextJobType = job.CronJobDetail.Name;
                    }
                }
            }
            
            status.put('nextExecution', nextExecution);
            status.put('nextJobType', nextJobType);
            status.put('isScheduled', scheduledJobs.size() > 0);
            
            // Get last execution status from async apex jobs
            List<AsyncApexJob> recentJobs = [
                SELECT Status, CompletedDate, NumberOfErrors, JobItemsProcessed
                FROM AsyncApexJob 
                WHERE ApexClass.Name = 'MarkovChainMatrixScheduler'
                AND CreatedDate = LAST_N_DAYS:7
                ORDER BY CreatedDate DESC
                LIMIT 5
            ];
            
            status.put('recentExecutions', recentJobs);
            
            // Add network visualization status
            status.put('networkVisualizationEnabled', true);
            status.put('stageDetectionEnabled', true);
            
            // Get network validation results
            Map<String, Object> networkValidation = validateNetworkVisualization();
            status.put('networkValidationResults', networkValidation);
            
            // Get stage detection quality
            Map<String, Object> stageDetectionQuality = MarkovChainAttributionBatch.getStageDetectionQualityMetrics();
            status.put('stageDetectionQuality', stageDetectionQuality);
            
            status.put('lastUpdated', DateTime.now());
            
        } catch (Exception e) {
            status.put('error', e.getMessage());
        }
        
        return status;
    }
    
    /**
     * Get network visualization summary for monitoring
     * 
     * @return Map containing network visualization summary
     */
    global static Map<String, Object> getNetworkVisualizationSummary() {
        Map<String, Object> summary = new Map<String, Object>();
        
        try {
            // Get network data for both customer types
            Map<String, Object> b2cNetworkData = MarkovChainAttribution.getNetworkVisualization('B2C');
            Map<String, Object> b2bNetworkData = MarkovChainAttribution.getNetworkVisualization('B2B');
            
            // Build summary
            summary.put('b2c_available', b2cNetworkData != null && !b2cNetworkData.containsKey('error'));
            summary.put('b2b_available', b2bNetworkData != null && !b2bNetworkData.containsKey('error'));
            
            if (b2cNetworkData != null && !b2cNetworkData.containsKey('error')) {
                summary.put('b2c_total_states', b2cNetworkData.get('totalStates'));
                summary.put('b2c_total_transitions', b2cNetworkData.get('totalTransitions'));
                
                if (b2cNetworkData.containsKey('networkStatistics')) {
                    Map<String, Object> b2cStats = (Map<String, Object>) b2cNetworkData.get('networkStatistics');
                    summary.put('b2c_conversion_rate', b2cStats.get('conversionRate'));
                }
            }
            
            if (b2bNetworkData != null && !b2bNetworkData.containsKey('error')) {
                summary.put('b2b_total_states', b2bNetworkData.get('totalStates'));
                summary.put('b2b_total_transitions', b2bNetworkData.get('totalTransitions'));
                
                if (b2bNetworkData.containsKey('networkStatistics')) {
                    Map<String, Object> b2bStats = (Map<String, Object>) b2bNetworkData.get('networkStatistics');
                    summary.put('b2b_conversion_rate', b2bStats.get('conversionRate'));
                }
            }
            
            summary.put('summary_generated', DateTime.now());
            
        } catch (Exception e) {
            summary.put('error', e.getMessage());
        }
        
        return summary;
    }
    
    /**
     * Helper method to get scheduling mode from string
     * 
     * @param modeString Mode string
     * @return SchedulingMode enum value
     */
    private static SchedulingMode getSchedulingMode(String modeString) {
        if (modeString == 'COMPLETE_PROCESSING') {
            return SchedulingMode.COMPLETE_PROCESSING;
        } else if (modeString == 'NETWORK_VALIDATION') {
            return SchedulingMode.NETWORK_VALIDATION;
        } else {
            return SchedulingMode.MATRIX_ONLY;
        }
    }
    
    /**
     * Custom exception class for scheduler errors
     */
    public class SchedulerException extends Exception {}
}