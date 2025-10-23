/**
 * AttributionDashboardController
 * 
 * Enhanced controller for the AttributionDashboard LWC component. Provides methods to
 * retrieve customer journeys, calculate attribution using all models including advanced
 * Markov Chain with stage detection, fetch results for visualization, and access
 * network visualization data for comprehensive attribution analysis.
 * 
 * Features:
 * - Customer journey retrieval with filtering
 * - Attribution calculation using all 6 models (including MarkovChain)
 * - Results aggregation and statistics
 * - Network visualization data access
 * - Stage detection monitoring and validation
 * - Transition matrix statistics and insights
 * - Enhanced error handling and validation
 * - Performance optimization
 * 
 * Author: Alexandru Constantinescu (www.linkedin.com/in/alexandru-constantinescu-6a18b6117)
 * Project: Omnichannel Attribution Platform
 * Date: 2025
 * Version: 1.11 
 */
public with sharing class AttributionDashboardController {
    
    private static final Integer MAX_JOURNEYS = 1000;
    private static final Integer DEFAULT_LIMIT = 100;
    
    /**
     * Get customer journeys for dashboard display
     * 
     * @return List of Customer_Journey__c records
     */
    @AuraEnabled(cacheable=true)
    public static List<Customer_Journey__c> getCustomerJourneys() {
        try {
            return [
                SELECT Id, Journey_ID__c, Customer_ID__c, Customer_Type__c, 
                       Converted__c, Conversion_Value__c, Total_Touchpoints__c,
                       Confidence_Score__c, Confidence_Level__c, 
                       Journey_Start_Date__c, Journey_End_Date__c
                FROM Customer_Journey__c 
                WHERE Total_Touchpoints__c > 0
                ORDER BY Journey_Start_Date__c DESC 
                LIMIT :DEFAULT_LIMIT
            ];
        } catch (Exception e) {
            throw new AuraHandledException('Failed to retrieve customer journeys: ' + e.getMessage());
        }
    }
    
    /**
     * Get customer journeys with filtering options
     * 
     * @param customerType Filter by customer type (B2B, B2C, or null for all)
     * @param convertedOnly Filter to only converted journeys
     * @param limitCount Maximum number of records to return
     * @return List of Customer_Journey__c records
     */
    @AuraEnabled(cacheable=true)
    public static List<Customer_Journey__c> getFilteredCustomerJourneys(
        String customerType, 
        Boolean convertedOnly, 
        Integer limitCount
    ) {
        try {
            // Build dynamic query
            String query = 'SELECT Id, Journey_ID__c, Customer_ID__c, Customer_Type__c, ' +
                          'Converted__c, Conversion_Value__c, Total_Touchpoints__c, ' +
                          'Confidence_Score__c, Confidence_Level__c, ' +
                          'Journey_Start_Date__c, Journey_End_Date__c ' +
                          'FROM Customer_Journey__c ' +
                          'WHERE Total_Touchpoints__c > 0';
            
            // Add filters
            if (String.isNotBlank(customerType)) {
                query += ' AND Customer_Type__c = :customerType';
            }
            
            if (convertedOnly != null && convertedOnly) {
                query += ' AND Converted__c = true';
            }
            
            query += ' ORDER BY Journey_Start_Date__c DESC';
            
            // Apply limit
            Integer safeLimit = limitCount != null ? 
                Math.min(limitCount, MAX_JOURNEYS) : DEFAULT_LIMIT;
            query += ' LIMIT :safeLimit';
            
            return Database.query(query);
            
        } catch (Exception e) {
            throw new AuraHandledException('Failed to retrieve filtered customer journeys: ' + e.getMessage());
        }
    }
    
    /**
     * Calculate attribution for selected journeys using specified model
     * 
     * @param journeyIds List of journey IDs to process
     * @param attributionModel Attribution model to use
     * @return Success message
     */
    @AuraEnabled
    public static String calculateAttribution(List<Id> journeyIds, String attributionModel) {
        if (journeyIds == null || journeyIds.isEmpty()) {
            throw new AuraHandledException('No journey IDs provided for attribution calculation');
        }
        
        if (String.isBlank(attributionModel)) {
            throw new AuraHandledException('Attribution model must be specified');
        }
        
        try {
            // Validate journey IDs
            List<Customer_Journey__c> journeys = [
                SELECT Id FROM Customer_Journey__c 
                WHERE Id IN :journeyIds
            ];
            
            if (journeys.size() != journeyIds.size()) {
                throw new AuraHandledException('Some journey IDs are invalid');
            }
            
            // Calculate attribution based on model
            switch on attributionModel {
                when 'LastTouch' {
                    LastTouchAttribution.triggerAttributionCalculation(journeyIds);
                }
                when 'FirstTouch' {
                    FirstTouchAttribution.triggerAttributionCalculation(journeyIds);
                }
                when 'Linear' {
                    LinearAttribution.triggerAttributionCalculation(journeyIds);
                }
                when 'TimeDecay' {
                    TimeDecayAttribution.triggerAttributionCalculation(journeyIds);
                }
                when 'PositionBased' {
                    PositionBasedAttribution.triggerAttributionCalculation(journeyIds);
                }
                when 'MarkovChain' {
                    MarkovChainAttribution.triggerAttributionCalculation(journeyIds);
                }
                when else {
                    throw new AuraHandledException('Unknown attribution model: ' + attributionModel);
                }
            }
            
            return 'Attribution calculated successfully for ' + journeyIds.size() + ' journeys';
            
        } catch (Exception e) {
            throw new AuraHandledException('Attribution calculation failed: ' + e.getMessage());
        }
    }
    
    /**
     * Calculate attribution for all selected models
     * 
     * @param journeyIds List of journey IDs to process
     * @param selectedModels List of attribution models to calculate
     * @return Batch processing status
     */
    @AuraEnabled
    public static Map<String, Object> calculateMultipleAttributions(List<Id> journeyIds, List<String> selectedModels) {
        if (journeyIds == null || journeyIds.isEmpty()) {
            throw new AuraHandledException('No journey IDs provided for attribution calculation');
        }
        
        if (selectedModels == null || selectedModels.isEmpty()) {
            throw new AuraHandledException('No attribution models selected');
        }
        
        Map<String, Object> results = new Map<String, Object>();
        List<String> successfulModels = new List<String>();
        List<String> failedModels = new List<String>();
        
        try {
            // Process each model
            for (String model : selectedModels) {
                try {
                    calculateAttribution(journeyIds, model);
                    successfulModels.add(model);
                } catch (Exception e) {
                    failedModels.add(model + ': ' + e.getMessage());
                }
            }
            
            results.put('successfulModels', successfulModels);
            results.put('failedModels', failedModels);
            results.put('totalJourneys', journeyIds.size());
            results.put('processedModels', successfulModels.size());
            results.put('failedCount', failedModels.size());
            
            // Check if MarkovChain was processed and provide additional info
            if (successfulModels.contains('MarkovChain')) {
                results.put('markovChainProcessed', true);
                results.put('networkVisualizationAvailable', true);
                results.put('stageDetectionEnabled', true);
            }
            
            return results;
            
        } catch (Exception e) {
            throw new AuraHandledException('Multi-model attribution calculation failed: ' + e.getMessage());
        }
    }
    
    /**
     * Get attribution results for specified journeys and model
     * 
     * @param journeyIds List of journey IDs
     * @param attributionModel Attribution model used
     * @return List of Attribution_Result__c records
     */
    @AuraEnabled(cacheable=true)
    public static List<Attribution_Result__c> getAttributionResults(
        List<Id> journeyIds, 
        String attributionModel
    ) {
        if (journeyIds == null || journeyIds.isEmpty()) {
            return new List<Attribution_Result__c>();
        }
        
        try {
            return [
                SELECT Id, Customer_Journey__c, Attribution_Model__c, 
                       Channel__c, Attribution_Weight__c, Attribution_Value__c,
                       Customer_Journey__r.Journey_ID__c
                FROM Attribution_Result__c 
                WHERE Customer_Journey__c IN :journeyIds
                AND Attribution_Model__c = :attributionModel
                ORDER BY Attribution_Value__c DESC
            ];
        } catch (Exception e) {
            throw new AuraHandledException('Failed to retrieve attribution results: ' + e.getMessage());
        }
    }
    
    /**
     * Get channel statistics for attribution results
     * 
     * @param journeyIds List of journey IDs
     * @param attributionModel Attribution model used
     * @return List of channel statistics
     */
    @AuraEnabled(cacheable=true)
    public static List<ChannelStatistic> getChannelStatistics(
        List<Id> journeyIds, 
        String attributionModel
    ) {
        if (journeyIds == null || journeyIds.isEmpty()) {
            return new List<ChannelStatistic>();
        }
        
        try {
            // Get attribution results
            List<Attribution_Result__c> results = [
                SELECT Channel__c, Attribution_Weight__c, Attribution_Value__c
                FROM Attribution_Result__c 
                WHERE Customer_Journey__c IN :journeyIds
                AND Attribution_Model__c = :attributionModel
            ];
            
            // Aggregate statistics by channel
            Map<String, ChannelStatistic> channelStats = new Map<String, ChannelStatistic>();
            
            for (Attribution_Result__c result : results) {
                String channel = result.Channel__c;
                
                if (!channelStats.containsKey(channel)) {
                    channelStats.put(channel, new ChannelStatistic(channel));
                }
                
                ChannelStatistic stat = channelStats.get(channel);
                stat.totalAttributions++;
                stat.totalValue += result.Attribution_Value__c != null ? result.Attribution_Value__c : 0;
                stat.totalWeight += result.Attribution_Weight__c != null ? result.Attribution_Weight__c : 0;
            }
            
            // Calculate averages
            List<ChannelStatistic> statisticsList = new List<ChannelStatistic>();
            for (ChannelStatistic stat : channelStats.values()) {
                if (stat.totalAttributions > 0) {
                    stat.averageWeight = stat.totalWeight / stat.totalAttributions;
                    stat.averageValue = stat.totalValue / stat.totalAttributions;
                }
                statisticsList.add(stat);
            }
            
            // Sort by total value descending
            statisticsList.sort();
            
            return statisticsList;
            
        } catch (Exception e) {
            throw new AuraHandledException('Failed to calculate channel statistics: ' + e.getMessage());
        }
    }
    
    /**
     * Get summary statistics for attribution results
     * 
     * @param journeyIds List of journey IDs
     * @param attributionModel Attribution model used
     * @return Attribution summary statistics
     */
    @AuraEnabled(cacheable=true)
    public static AttributionSummary getAttributionSummary(
        List<Id> journeyIds, 
        String attributionModel
    ) {
        if (journeyIds == null || journeyIds.isEmpty()) {
            return new AttributionSummary();
        }
        
        try {
            // Get attribution results
            List<Attribution_Result__c> results = [
                SELECT Attribution_Value__c, Attribution_Weight__c, Channel__c
                FROM Attribution_Result__c 
                WHERE Customer_Journey__c IN :journeyIds
                AND Attribution_Model__c = :attributionModel
            ];
            
            AttributionSummary summary = new AttributionSummary();
            summary.totalJourneys = journeyIds.size();
            summary.attributionModel = attributionModel;
            
            Set<String> uniqueChannels = new Set<String>();
            
            for (Attribution_Result__c result : results) {
                summary.totalAttributionValue += result.Attribution_Value__c != null ? result.Attribution_Value__c : 0;
                uniqueChannels.add(result.Channel__c);
            }
            
            summary.totalChannels = uniqueChannels.size();
            summary.averageAttributionPerJourney = summary.totalJourneys > 0 ? 
                summary.totalAttributionValue / summary.totalJourneys : 0;
            
            // Add Markov Chain specific information
            if (attributionModel == 'MarkovChain') {
                summary.isMarkovChain = true;
                summary.stageDetectionEnabled = true;
                summary.networkVisualizationAvailable = true;
            }
            
            return summary;
            
        } catch (Exception e) {
            throw new AuraHandledException('Failed to calculate attribution summary: ' + e.getMessage());
        }
    }
    
    /**
     * Get network visualization data for Markov Chain attribution
     * 
     * @param customerType Customer type (B2B, B2C, or null for both)
     * @return Network visualization data
     */
    @AuraEnabled(cacheable=true)
    public static Map<String, Object> getNetworkVisualizationData(String customerType) {
        if (String.isBlank(customerType)) {
            throw new AuraHandledException('Customer type must be specified for network visualization');
        }
        
        try {
            // Get network visualization from MarkovChainAttribution
            Map<String, Object> networkData = MarkovChainAttribution.getNetworkVisualization(customerType);
            
            if (networkData == null) {
                throw new AuraHandledException('No network visualization data available for ' + customerType);
            }
            
            // Add dashboard-specific enhancements
            networkData.put('dashboardTimestamp', DateTime.now());
            networkData.put('requestedCustomerType', customerType);
            
            return networkData;
            
        } catch (Exception e) {
            throw new AuraHandledException('Failed to retrieve network visualization data: ' + e.getMessage());
        }
    }
    
    /**
     * Get transition matrix statistics for dashboard display
     * 
     * @param customerType Customer type (B2B or B2C)
     * @return Transition matrix statistics
     */
    @AuraEnabled(cacheable=true)
    public static Map<String, Object> getTransitionMatrixStatistics(String customerType) {
        if (String.isBlank(customerType)) {
            throw new AuraHandledException('Customer type must be specified');
        }
        
        try {
            Map<String, Object> statistics = MarkovChainAttribution.getTransitionMatrixStatistics(customerType);
            
            if (statistics == null) {
                statistics = new Map<String, Object>();
                statistics.put('matrixExists', false);
                statistics.put('customerType', customerType);
            }
            
            // Add dashboard timestamp
            statistics.put('dashboardRequestTime', DateTime.now());
            
            return statistics;
            
        } catch (Exception e) {
            throw new AuraHandledException('Failed to retrieve transition matrix statistics: ' + e.getMessage());
        }
    }
    
    /**
     * Get stage detection summary for quality monitoring
     * 
     * @param customerType Customer type (B2B or B2C)
     * @return Stage detection summary
     */
    @AuraEnabled(cacheable=true)
    public static Map<String, Object> getStageDetectionSummary(String customerType) {
        if (String.isBlank(customerType)) {
            throw new AuraHandledException('Customer type must be specified');
        }
        
        try {
            Map<String, Object> summary = MarkovChainAttribution.getStageDetectionSummary(customerType);
            
            if (summary == null) {
                summary = new Map<String, Object>();
                summary.put('error', 'No stage detection data available');
            }
            
            // Add dashboard enhancements
            summary.put('dashboardRequestTime', DateTime.now());
            summary.put('stageDetectionEnabled', true);
            
            return summary;
            
        } catch (Exception e) {
            throw new AuraHandledException('Failed to retrieve stage detection summary: ' + e.getMessage());
        }
    }
    
    /**
     * Trigger Markov Chain matrix recalculation
     * 
     * @param customerType Customer type to recalculate (B2B, B2C, or null for both)
     * @return Success message
     */
    @AuraEnabled
    public static String triggerMatrixRecalculation(String customerType) {
        try {
            String result;
            
            if (String.isBlank(customerType)) {
                // Recalculate for both customer types
                result = MarkovChainAttribution.triggerMatrixRecalculation();
            } else {
                // Recalculate for specific customer type
                result = MarkovChainAttribution.triggerMatrixRecalculation(customerType);
            }
            
            return result;
            
        } catch (Exception e) {
            throw new AuraHandledException('Failed to trigger matrix recalculation: ' + e.getMessage());
        }
    }
    
    /**
     * Get Markov Chain processing status for dashboard monitoring
     * 
     * @return Processing status information
     */
    @AuraEnabled(cacheable=true)
    public static Map<String, Object> getMarkovChainProcessingStatus() {
        try {
            Map<String, Object> status = MarkovChainAttributionBatch.getProcessingStatus();
            
            // Add dashboard-specific information
            status.put('dashboardRequestTime', DateTime.now());
            status.put('processingStatusAvailable', true);
            
            return status;
            
        } catch (Exception e) {
            throw new AuraHandledException('Failed to retrieve processing status: ' + e.getMessage());
        }
    }
    
    /**
     * Get stage detection quality metrics for monitoring
     * 
     * @return Stage detection quality metrics
     */
    @AuraEnabled(cacheable=true)
    public static Map<String, Object> getStageDetectionQualityMetrics() {
        try {
            Map<String, Object> qualityMetrics = MarkovChainAttributionBatch.getStageDetectionQualityMetrics();
            
            // Add dashboard enhancements
            qualityMetrics.put('dashboardRequestTime', DateTime.now());
            qualityMetrics.put('qualityMonitoringEnabled', true);
            
            return qualityMetrics;
            
        } catch (Exception e) {
            throw new AuraHandledException('Failed to retrieve stage detection quality metrics: ' + e.getMessage());
        }
    }
    
    /**
     * Get comprehensive dashboard data for Markov Chain visualization
     * 
     * @param customerType Customer type (B2B or B2C)
     * @return Comprehensive dashboard data
     */
    @AuraEnabled(cacheable=true)
    public static Map<String, Object> getMarkovChainDashboardData(String customerType) {
        if (String.isBlank(customerType)) {
            throw new AuraHandledException('Customer type must be specified');
        }
        
        try {
            Map<String, Object> dashboardData = new Map<String, Object>();
            
            // Get network visualization data
            Map<String, Object> networkData = MarkovChainAttribution.getNetworkVisualization(customerType);
            dashboardData.put('networkVisualization', networkData);
            
            // Get transition matrix statistics
            Map<String, Object> matrixStats = MarkovChainAttribution.getTransitionMatrixStatistics(customerType);
            dashboardData.put('matrixStatistics', matrixStats);
            
            // Get stage detection summary
            Map<String, Object> stageDetection = MarkovChainAttribution.getStageDetectionSummary(customerType);
            dashboardData.put('stageDetectionSummary', stageDetection);
            
            // Get processing status
            Map<String, Object> processingStatus = MarkovChainAttributionBatch.getProcessingStatus();
            dashboardData.put('processingStatus', processingStatus);
            
            // Add metadata
            dashboardData.put('customerType', customerType);
            dashboardData.put('dashboardGeneratedTime', DateTime.now());
            dashboardData.put('dataAvailable', true);
            
            return dashboardData;
            
        } catch (Exception e) {
            throw new AuraHandledException('Failed to retrieve comprehensive dashboard data: ' + e.getMessage());
        }
    }
    
    /**
     * Compare attribution models for the same set of journeys
     * 
     * @param journeyIds List of journey IDs
     * @param models List of attribution models to compare
     * @return Model comparison data
     */
    @AuraEnabled(cacheable=true)
    public static Map<String, Object> compareAttributionModels(List<Id> journeyIds, List<String> models) {
        if (journeyIds == null || journeyIds.isEmpty()) {
            throw new AuraHandledException('Journey IDs are required for model comparison');
        }
        
        if (models == null || models.isEmpty()) {
            throw new AuraHandledException('Attribution models are required for comparison');
        }
        
        try {
            Map<String, Object> comparison = new Map<String, Object>();
            Map<String, Map<String, Object>> modelResults = new Map<String, Map<String, Object>>();
            
            // Get results for each model
            for (String model : models) {
                List<Attribution_Result__c> results = getAttributionResults(journeyIds, model);
                AttributionSummary summary = getAttributionSummary(journeyIds, model);
                List<ChannelStatistic> channelStats = getChannelStatistics(journeyIds, model);
                
                Map<String, Object> modelData = new Map<String, Object>();
                modelData.put('results', results);
                modelData.put('summary', summary);
                modelData.put('channelStatistics', channelStats);
                
                // Add Markov Chain specific data
                if (model == 'MarkovChain') {
                    try {
                        // Determine customer type from first journey
                        Customer_Journey__c firstJourney = [
                            SELECT Customer_Type__c FROM Customer_Journey__c WHERE Id = :journeyIds[0] LIMIT 1
                        ];
                        
                        if (firstJourney != null) {
                            Map<String, Object> networkData = MarkovChainAttribution.getNetworkVisualization(firstJourney.Customer_Type__c);
                            modelData.put('networkVisualization', networkData);
                        }
                    } catch (Exception e) {
                        System.debug('Could not retrieve network data for comparison: ' + e.getMessage());
                    }
                }
                
                modelResults.put(model, modelData);
            }
            
            comparison.put('modelResults', modelResults);
            comparison.put('comparedModels', models);
            comparison.put('totalJourneys', journeyIds.size());
            comparison.put('comparisonTimestamp', DateTime.now());
            
            return comparison;
            
        } catch (Exception e) {
            throw new AuraHandledException('Failed to compare attribution models: ' + e.getMessage());
        }
    }
    
    /**
     * Delete attribution results for specified journeys and model
     * 
     * @param journeyIds List of journey IDs
     * @param attributionModel Attribution model to delete
     * @return Success message
     */
    @AuraEnabled
    public static String deleteAttributionResults(List<Id> journeyIds, String attributionModel) {
        if (journeyIds == null || journeyIds.isEmpty()) {
            return 'No journeys specified for deletion';
        }
        
        try {
            List<Attribution_Result__c> resultsToDelete = [
                SELECT Id FROM Attribution_Result__c 
                WHERE Customer_Journey__c IN :journeyIds
                AND Attribution_Model__c = :attributionModel
            ];
            
            if (!resultsToDelete.isEmpty()) {
                delete resultsToDelete;
                return 'Deleted ' + resultsToDelete.size() + ' attribution results';
            } else {
                return 'No attribution results found to delete';
            }
            
        } catch (Exception e) {
            throw new AuraHandledException('Failed to delete attribution results: ' + e.getMessage());
        }
    }
    
    /**
     * Get available attribution models including Markov Chain
     * 
     * @return List of available attribution models
     */
    @AuraEnabled(cacheable=true)
    public static List<AttributionModel> getAvailableAttributionModels() {
        List<AttributionModel> models = new List<AttributionModel>();
        
        models.add(new AttributionModel('LastTouch', 'Last Touch Attribution', 
            'Assigns 100% credit to the final touchpoint in each journey'));
        models.add(new AttributionModel('FirstTouch', 'First Touch Attribution', 
            'Assigns 100% credit to the first touchpoint in each journey'));
        models.add(new AttributionModel('Linear', 'Linear Attribution', 
            'Distributes credit equally across all touchpoints'));
        models.add(new AttributionModel('TimeDecay', 'Time Decay Attribution', 
            'Applies exponential decay favoring recent touchpoints'));
        models.add(new AttributionModel('PositionBased', 'Position-Based Attribution', 
            'Assigns 40% to first and last touchpoints, 20% to middle touchpoints'));
        models.add(new AttributionModel('MarkovChain', 'Markov Chain Attribution', 
            'Uses probabilistic state transitions with stage detection for advanced attribution modeling'));
        
        return models;
    }
    
    /**
     * Get dashboard configuration for UI components
     * 
     * @return Dashboard configuration
     */
    @AuraEnabled(cacheable=true)
    public static Map<String, Object> getDashboardConfiguration() {
        Map<String, Object> config = new Map<String, Object>();
        
        try {
            // Basic configuration
            config.put('maxJourneys', MAX_JOURNEYS);
            config.put('defaultLimit', DEFAULT_LIMIT);
            config.put('availableModels', getAvailableAttributionModels());
            
            // Advanced features availability
            config.put('markovChainEnabled', true);
            config.put('networkVisualizationEnabled', true);
            config.put('stageDetectionEnabled', true);
            config.put('batchProcessingEnabled', true);
            
            // Customer types
            config.put('customerTypes', new List<String>{'B2C', 'B2B'});
            
            // Dashboard features
            config.put('modelComparisonEnabled', true);
            config.put('channelStatisticsEnabled', true);
            config.put('qualityMonitoringEnabled', true);
            
            // Performance settings
            config.put('networkVisualizationCacheTime', 300); // 5 minutes
            config.put('matrixStatisticsCacheTime', 600); // 10 minutes
            
            config.put('configurationTimestamp', DateTime.now());
            
        } catch (Exception e) {
            config.put('error', e.getMessage());
        }
        
        return config;
    }
    
    /**
     * Wrapper class for channel statistics
     */
    public class ChannelStatistic implements Comparable {
        @AuraEnabled public String channel { get; set; }
        @AuraEnabled public Integer totalAttributions { get; set; }
        @AuraEnabled public Decimal totalValue { get; set; }
        @AuraEnabled public Decimal totalWeight { get; set; }
        @AuraEnabled public Decimal averageWeight { get; set; }
        @AuraEnabled public Decimal averageValue { get; set; }
        
        public ChannelStatistic(String channel) {
            this.channel = channel;
            this.totalAttributions = 0;
            this.totalValue = 0.0;
            this.totalWeight = 0.0;
            this.averageWeight = 0.0;
            this.averageValue = 0.0;
        }
        
        public Integer compareTo(Object other) {
            ChannelStatistic otherStat = (ChannelStatistic) other;
            // Sort by total value descending
            if (otherStat.totalValue > this.totalValue) {
                return 1;
            } else if (otherStat.totalValue < this.totalValue) {
                return -1;
            } else {
                return 0;
            }
        }
    }
    
    /**
     * Wrapper class for attribution summary
     */
    public class AttributionSummary {
        @AuraEnabled public Integer totalJourneys { get; set; }
        @AuraEnabled public Integer totalChannels { get; set; }
        @AuraEnabled public Decimal totalAttributionValue { get; set; }
        @AuraEnabled public Decimal averageAttributionPerJourney { get; set; }
        @AuraEnabled public String attributionModel { get; set; }
        @AuraEnabled public Boolean isMarkovChain { get; set; }
        @AuraEnabled public Boolean stageDetectionEnabled { get; set; }
        @AuraEnabled public Boolean networkVisualizationAvailable { get; set; }
        
        public AttributionSummary() {
            this.totalJourneys = 0;
            this.totalChannels = 0;
            this.totalAttributionValue = 0;
            this.averageAttributionPerJourney = 0;
            this.attributionModel = '';
            this.isMarkovChain = false;
            this.stageDetectionEnabled = false;
            this.networkVisualizationAvailable = false;
        }
    }
    
    /**
     * Wrapper class for attribution model info
     */
    public class AttributionModel {
        @AuraEnabled public String value { get; set; }
        @AuraEnabled public String label { get; set; }
        @AuraEnabled public String description { get; set; }
        
        public AttributionModel(String value, String label, String description) {
            this.value = value;
            this.label = label;
            this.description = description;
        }
    }

    // For HAMs comparison - variance analysis
    @AuraEnabled(cacheable=true)
    public static List<ChannelVarianceData> getChannelVarianceAnalysis(
        List<Id> journeyIds, List<String> models
    );

    // For model correlation matrix
    @AuraEnabled(cacheable=true) 
    public static Map<String, Object> getModelCorrelationMatrix(
        List<Id> journeyIds, List<String> models
    );

    // For timeline attribution data
    @AuraEnabled(cacheable=true)
    public static Map<String, Object> getAttributionTimeline(
        List<Id> journeyIds, String attributionModel, Integer months
    );
}