/**
 * LinearAttribution
 * 
 * Implements Linear Attribution model that assigns equal credit to all touchpoints
 * in each customer journey. This class provides both synchronous and batch processing
 * capabilities to handle large datasets while respecting Salesforce governor limits.
 * 
 * Business Logic:
 * - Distributes attribution credit equally across all touchpoints in the journey
 * - Each touchpoint receives (1 / total_touchpoints) attribution weight
 * - Conversion value is distributed proportionally across all touchpoints
 * - Handles both B2B and B2C customer journeys
 * 
 * Author: Alexandru Constantinescu (www.linkedin.com/in/alexandru-constantinescu-6a18b6117)
 * Project: Omnichannel Attribution Platform
 * Date: 2025
 * Version: 1.0
 */
public class LinearAttribution {
    
    // Constants for attribution model configuration
    private static final String ATTRIBUTION_MODEL = 'Linear';
    private static final Integer BATCH_SIZE = 200;
    
    /**
     * Calculate Linear Attribution for a single customer journey
     * 
     * @param journeyId The ID of the customer journey to process
     * @return List of Attribution_Result__c records for the journey
     */
    public static List<Attribution_Result__c> calculateAttribution(Id journeyId) {
        if (journeyId == null) {
            throw new IllegalArgumentException('Journey ID cannot be null');
        }
        
        List<Id> journeyIds = new List<Id>{journeyId};
        return calculateAttributionBatch(journeyIds);
    }
    
    /**
     * Calculate Linear Attribution for multiple customer journeys
     * 
     * @param journeyIds List of customer journey IDs to process
     * @return List of Attribution_Result__c records for all journeys
     */
    public static List<Attribution_Result__c> calculateAttributionBatch(List<Id> journeyIds) {
        if (journeyIds == null || journeyIds.isEmpty()) {
            return new List<Attribution_Result__c>();
        }
        
        List<Attribution_Result__c> attributionResults = new List<Attribution_Result__c>();
        
        try {
            // Query customer journeys
            List<Customer_Journey__c> journeys = [
                SELECT Id, Customer_ID__c, Customer_Type__c, Converted__c, 
                       Conversion_Value__c, Journey_Start_Date__c, Journey_End_Date__c
                FROM Customer_Journey__c 
                WHERE Id IN :journeyIds
            ];
            
            // Query touchpoints separately to avoid relationship issues
            List<Touchpoint__c> allTouchpoints = [
                SELECT Id, Customer_Journey__c, Channel__c, Touchpoint_Timestamp__c, Campaign__c
                FROM Touchpoint__c 
                WHERE Customer_Journey__c IN :journeyIds
                ORDER BY Customer_Journey__c, Touchpoint_Timestamp__c ASC
            ];
            
            // Group touchpoints by journey
            Map<Id, List<Touchpoint__c>> touchpointsByJourney = new Map<Id, List<Touchpoint__c>>();
            for (Touchpoint__c touchpoint : allTouchpoints) {
                if (!touchpointsByJourney.containsKey(touchpoint.Customer_Journey__c)) {
                    touchpointsByJourney.put(touchpoint.Customer_Journey__c, new List<Touchpoint__c>());
                }
                touchpointsByJourney.get(touchpoint.Customer_Journey__c).add(touchpoint);
            }
            
            // Process each journey
            for (Customer_Journey__c journey : journeys) {
                List<Touchpoint__c> journeyTouchpoints = touchpointsByJourney.get(journey.Id);
                if (journeyTouchpoints != null) {
                    List<Attribution_Result__c> journeyResults = processJourney(journey, journeyTouchpoints);
                    attributionResults.addAll(journeyResults);
                }
            }
            
        } catch (Exception e) {
            System.debug('Error in calculateAttributionBatch: ' + e.getMessage());
            throw new AttributionException('Failed to calculate Linear Attribution: ' + e.getMessage());
        }
        
        return attributionResults;
    }
    
    /**
     * Process a single customer journey to determine linear attribution
     * 
     * @param journey The customer journey record
     * @param touchpoints List of touchpoints for the journey
     * @return List of Attribution_Result__c records for the journey
     */
    private static List<Attribution_Result__c> processJourney(Customer_Journey__c journey, List<Touchpoint__c> touchpoints) {
        List<Attribution_Result__c> results = new List<Attribution_Result__c>();
        
        if (touchpoints == null || touchpoints.isEmpty()) {
            System.debug('No touchpoints found for journey: ' + journey.Id);
            return results;
        }
        
        // Calculate linear attribution weights for each channel
        Map<String, Decimal> channelAttribution = calculateChannelAttribution(touchpoints);
        
        // Convert to Attribution_Result__c records
        for (String channel : channelAttribution.keySet()) {
            Attribution_Result__c result = new Attribution_Result__c();
            result.Customer_Journey__c = journey.Id;
            result.Attribution_Model__c = ATTRIBUTION_MODEL;
            result.Channel__c = channel;
            result.Attribution_Weight__c = channelAttribution.get(channel);
            result.Attribution_Value__c = calculateAttributionValue(
                journey.Conversion_Value__c, channelAttribution.get(channel)
            );
            
            results.add(result);
        }
        
        return results;
    }
    
    /**
     * Calculate linear attribution weights for each channel
     * 
     * @param touchpoints List of all touchpoints in the journey
     * @return Map of channel to attribution weight
     */
    private static Map<String, Decimal> calculateChannelAttribution(List<Touchpoint__c> touchpoints) {
        Map<String, Decimal> channelAttributionMap = new Map<String, Decimal>();
        
        if (touchpoints == null || touchpoints.isEmpty()) {
            return channelAttributionMap;
        }
        
        // Count touchpoints per channel
        Map<String, Integer> touchpointCountByChannel = new Map<String, Integer>();
        for (Touchpoint__c touchpoint : touchpoints) {
            String channel = touchpoint.Channel__c;
            if (!touchpointCountByChannel.containsKey(channel)) {
                touchpointCountByChannel.put(channel, 0);
            }
            touchpointCountByChannel.put(channel, touchpointCountByChannel.get(channel) + 1);
        }
        
        // Calculate attribution weight per touchpoint
        Integer totalTouchpoints = touchpoints.size();
        Decimal attributionPerTouchpoint = 1.0 / totalTouchpoints;
        
        // Calculate attribution weight per channel
        for (String channel : touchpointCountByChannel.keySet()) {
            Integer channelTouchpoints = touchpointCountByChannel.get(channel);
            Decimal channelAttributionWeight = attributionPerTouchpoint * channelTouchpoints;
            channelAttributionMap.put(channel, channelAttributionWeight);
        }
        
        return channelAttributionMap;
    }
    
    /**
     * Calculate attribution value based on conversion value and attribution weight
     * 
     * @param conversionValue The total conversion value of the journey
     * @param attributionWeight The attribution weight for the channel
     * @return The calculated attribution value
     */
    private static Decimal calculateAttributionValue(Decimal conversionValue, Decimal attributionWeight) {
        if (conversionValue == null || attributionWeight == null) {
            return 0;
        }
        
        return conversionValue * attributionWeight;
    }
    
    /**
     * Get channel statistics for a journey
     * 
     * @param touchpoints List of touchpoints in the journey
     * @return Map containing channel statistics
     */
    public static Map<String, Object> getChannelStatistics(List<Touchpoint__c> touchpoints) {
        Map<String, Object> statistics = new Map<String, Object>();
        
        if (touchpoints == null || touchpoints.isEmpty()) {
            statistics.put('totalTouchpoints', 0);
            statistics.put('uniqueChannels', 0);
            statistics.put('channelCounts', new Map<String, Integer>());
            return statistics;
        }
        
        Map<String, Integer> channelCounts = new Map<String, Integer>();
        Set<String> uniqueChannels = new Set<String>();
        
        for (Touchpoint__c touchpoint : touchpoints) {
            String channel = touchpoint.Channel__c;
            uniqueChannels.add(channel);
            
            if (!channelCounts.containsKey(channel)) {
                channelCounts.put(channel, 0);
            }
            channelCounts.put(channel, channelCounts.get(channel) + 1);
        }
        
        statistics.put('totalTouchpoints', touchpoints.size());
        statistics.put('uniqueChannels', uniqueChannels.size());
        statistics.put('channelCounts', channelCounts);
        statistics.put('attributionPerTouchpoint', 1.0 / touchpoints.size());
        
        return statistics;
    }
    
    /**
     * Trigger Linear Attribution calculation for journeys
     * This method is designed to be called from triggers or flows
     * 
     * @param journeyIds List of journey IDs to process
     */
    public static void triggerAttributionCalculation(List<Id> journeyIds) {
        if (journeyIds == null || journeyIds.isEmpty()) {
            return;
        }
        
        // Process synchronously for all datasets initially
        // Batch processing can be added later via separate batch class
        List<Attribution_Result__c> results = calculateAttributionBatch(journeyIds);
        
        if (!results.isEmpty()) {
            try {
                // Delete existing Linear results for these journeys
                delete [SELECT Id FROM Attribution_Result__c 
                       WHERE Customer_Journey__c IN :journeyIds 
                       AND Attribution_Model__c = :ATTRIBUTION_MODEL];
                
                // Insert new results
                insert results;
                
            } catch (DMLException e) {
                System.debug('Error saving attribution results: ' + e.getMessage());
                throw new AttributionException('Failed to save Linear Attribution results: ' + e.getMessage());
            }
        }
    }
    
    /**
     * Trigger Linear Attribution calculation for large datasets
     * This method can be used when batch processing is needed
     * 
     * @param journeyIds List of journey IDs to process
     */
    public static void triggerAttributionCalculationBatch(List<Id> journeyIds) {
        if (journeyIds == null || journeyIds.isEmpty()) {
            return;
        }
        
        System.debug('Batch processing requested for ' + journeyIds.size() + ' journeys');
        System.debug('Note: Deploy LinearAttributionBatch class to enable true batch processing');
        
        // For now, fall back to synchronous processing
        // This can be enhanced once batch class is deployed
        triggerAttributionCalculation(journeyIds);
    }
    
    /**
     * Validate attribution results for quality assurance
     * 
     * @param results List of attribution results to validate
     * @return True if validation passes, false otherwise
     */
    public static Boolean validateAttributionResults(List<Attribution_Result__c> results) {
        if (results == null || results.isEmpty()) {
            return true;
        }
        
        // Group results by journey
        Map<Id, List<Attribution_Result__c>> resultsByJourney = new Map<Id, List<Attribution_Result__c>>();
        
        for (Attribution_Result__c result : results) {
            if (!resultsByJourney.containsKey(result.Customer_Journey__c)) {
                resultsByJourney.put(result.Customer_Journey__c, new List<Attribution_Result__c>());
            }
            resultsByJourney.get(result.Customer_Journey__c).add(result);
        }
        
        // Validate each journey's attribution sums to 100%
        for (Id journeyId : resultsByJourney.keySet()) {
            List<Attribution_Result__c> journeyResults = resultsByJourney.get(journeyId);
            Decimal totalWeight = 0;
            
            for (Attribution_Result__c result : journeyResults) {
                totalWeight += result.Attribution_Weight__c;
            }
            
            // Allow for small floating point differences
            if (Math.abs(totalWeight - 1.0) > 0.001) {
                System.debug('Attribution validation failed for journey ' + journeyId + 
                           ': total weight = ' + totalWeight);
                return false;
            }
        }
        
        return true;
    }
    
    /**
     * Compare Linear Attribution with other models
     * 
     * @param journeyId The journey to analyze
     * @return Map containing comparison data
     */
    public static Map<String, Object> compareWithOtherModels(Id journeyId) {
        Map<String, Object> comparison = new Map<String, Object>();
        
        try {
            // Calculate Linear Attribution
            List<Attribution_Result__c> linearResults = calculateAttribution(journeyId);
            
            // Get existing attribution results for comparison
            List<Attribution_Result__c> existingResults = [
                SELECT Id, Attribution_Model__c, Channel__c, Attribution_Weight__c, Attribution_Value__c
                FROM Attribution_Result__c
                WHERE Customer_Journey__c = :journeyId
                AND Attribution_Model__c IN ('LastTouch', 'FirstTouch')
            ];
            
            // Organize results by model
            Map<String, Map<String, Decimal>> resultsByModel = new Map<String, Map<String, Decimal>>();
            
            // Add Linear results
            Map<String, Decimal> linearWeights = new Map<String, Decimal>();
            for (Attribution_Result__c result : linearResults) {
                linearWeights.put(result.Channel__c, result.Attribution_Weight__c);
            }
            resultsByModel.put('Linear', linearWeights);
            
            // Add existing model results
            for (Attribution_Result__c result : existingResults) {
                if (!resultsByModel.containsKey(result.Attribution_Model__c)) {
                    resultsByModel.put(result.Attribution_Model__c, new Map<String, Decimal>());
                }
                resultsByModel.get(result.Attribution_Model__c).put(result.Channel__c, result.Attribution_Weight__c);
            }
            
            comparison.put('attributionByModel', resultsByModel);
            comparison.put('linearResults', linearResults);
            
        } catch (Exception e) {
            System.debug('Error in model comparison: ' + e.getMessage());
            comparison.put('error', e.getMessage());
        }
        
        return comparison;
    }
    
    /**
     * Custom exception class for attribution errors
     */
    public class AttributionException extends Exception {}
}