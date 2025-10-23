/**
 * PositionBasedAttribution
 * 
 * Implements Position-Based Attribution model (also known as U-shaped attribution)
 * that assigns 40% credit to the first touchpoint, 40% credit to the last touchpoint,
 * and distributes the remaining 20% equally among all middle touchpoints.
 * 
 * Business Logic:
 * - First touchpoint: 40% attribution weight
 * - Last touchpoint: 40% attribution weight
 * - Middle touchpoints: 20% distributed equally
 * - Single touchpoint: 100% attribution
 * - Two touchpoints: 40% each (no middle touchpoints)
 * Author: Alexandru Constantinescu (www.linkedin.com/in/alexandru-constantinescu-6a18b6117)
 * Project: Omnichannel Attribution Platform
 * Date: 2025
 * Version: 1.1
 */
public class PositionBasedAttribution {
    
    // Constants for attribution model configuration
    private static final String ATTRIBUTION_MODEL = 'PositionBased';
    private static final Decimal FIRST_TOUCHPOINT_WEIGHT = 0.4;
    private static final Decimal LAST_TOUCHPOINT_WEIGHT = 0.4;
    private static final Decimal MIDDLE_TOUCHPOINTS_TOTAL_WEIGHT = 0.2;
    private static final Decimal SINGLE_TOUCHPOINT_WEIGHT = 1.0;
    private static final Integer BATCH_SIZE = 200;
    
    /**
     * Calculate Position-Based Attribution for a single customer journey
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
     * Calculate Position-Based Attribution for multiple customer journeys
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
            throw new AttributionException('Failed to calculate Position-Based Attribution: ' + e.getMessage());
        }
        
        return attributionResults;
    }
    
    /**
     * Process a single customer journey to determine position-based attribution
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
        
        // Sort touchpoints by timestamp to ensure correct ordering
        touchpoints = sortTouchpointsByTimestamp(touchpoints);
        
        // Calculate position-based attribution weights
        Map<String, Decimal> channelWeights = calculatePositionBasedWeights(touchpoints);
        
        // Convert to Attribution_Result__c records
        for (String channel : channelWeights.keySet()) {
            Attribution_Result__c result = new Attribution_Result__c();
            result.Customer_Journey__c = journey.Id;
            result.Attribution_Model__c = ATTRIBUTION_MODEL;
            result.Channel__c = channel;
            result.Attribution_Weight__c = channelWeights.get(channel);
            result.Attribution_Value__c = calculateAttributionValue(
                journey.Conversion_Value__c, channelWeights.get(channel)
            );
            
            results.add(result);
        }
        
        return results;
    }
    
    /**
     * Calculate position-based attribution weights for all channels
     * 
     * @param touchpoints List of touchpoints ordered by timestamp
     * @return Map of channel to attribution weight
     */
    private static Map<String, Decimal> calculatePositionBasedWeights(List<Touchpoint__c> touchpoints) {
        Map<String, Decimal> channelWeights = new Map<String, Decimal>();
        
        if (touchpoints == null || touchpoints.isEmpty()) {
            return channelWeights;
        }
        
        // Debug output
        System.debug('=== Position-Based Attribution Debug ===');
        System.debug('Total touchpoints: ' + touchpoints.size());
        for (Integer i = 0; i < touchpoints.size(); i++) {
            System.debug('Position ' + i + ': ' + touchpoints[i].Channel__c + ' at ' + touchpoints[i].Touchpoint_Timestamp__c);
        }
        
        // Handle single touchpoint case
        if (touchpoints.size() == 1) {
            String channel = touchpoints[0].Channel__c;
            channelWeights.put(channel, SINGLE_TOUCHPOINT_WEIGHT);
            return channelWeights;
        }
        
        // Initialize channel weights
        Set<String> allChannels = new Set<String>();
        for (Touchpoint__c touchpoint : touchpoints) {
            allChannels.add(touchpoint.Channel__c);
        }
        
        for (String channel : allChannels) {
            channelWeights.put(channel, 0.0);
        }
        
        // Identify first and last touchpoints
        Touchpoint__c firstTouchpoint = touchpoints[0];
        Touchpoint__c lastTouchpoint = touchpoints[touchpoints.size() - 1];
        
        System.debug('First touchpoint: ' + firstTouchpoint.Channel__c + ' at ' + firstTouchpoint.Touchpoint_Timestamp__c);
        System.debug('Last touchpoint: ' + lastTouchpoint.Channel__c + ' at ' + lastTouchpoint.Touchpoint_Timestamp__c);
        
        // Assign first touchpoint weight
        String firstChannel = firstTouchpoint.Channel__c;
        channelWeights.put(firstChannel, channelWeights.get(firstChannel) + FIRST_TOUCHPOINT_WEIGHT);
        System.debug('Assigned ' + FIRST_TOUCHPOINT_WEIGHT + ' to ' + firstChannel + ' (first)');
        
        // Assign last touchpoint weight (only if different from first)
        String lastChannel = lastTouchpoint.Channel__c;
        if (touchpoints.size() > 1) {
            channelWeights.put(lastChannel, channelWeights.get(lastChannel) + LAST_TOUCHPOINT_WEIGHT);
            System.debug('Assigned ' + LAST_TOUCHPOINT_WEIGHT + ' to ' + lastChannel + ' (last)');
        }
        
        // Handle middle touchpoints
        if (touchpoints.size() > 2) {
            List<Touchpoint__c> middleTouchpoints = new List<Touchpoint__c>();
            for (Integer i = 1; i < touchpoints.size() - 1; i++) {
                middleTouchpoints.add(touchpoints[i]);
            }
            
            System.debug('Middle touchpoints: ' + middleTouchpoints.size());
            
            // Calculate weight per middle touchpoint
            Decimal weightPerMiddleTouchpoint = MIDDLE_TOUCHPOINTS_TOTAL_WEIGHT / middleTouchpoints.size();
            System.debug('Weight per middle touchpoint: ' + weightPerMiddleTouchpoint);
            
            // Distribute middle touchpoint weights
            for (Touchpoint__c middleTouchpoint : middleTouchpoints) {
                String middleChannel = middleTouchpoint.Channel__c;
                channelWeights.put(middleChannel, channelWeights.get(middleChannel) + weightPerMiddleTouchpoint);
                System.debug('Assigned ' + weightPerMiddleTouchpoint + ' to ' + middleChannel + ' (middle)');
            }
        }
        
        // Debug final weights
        System.debug('Final channel weights:');
        for (String channel : channelWeights.keySet()) {
            System.debug('  ' + channel + ': ' + channelWeights.get(channel));
        }
        
        return channelWeights;
    }
    
    /**
     * Sort touchpoints by timestamp using a more reliable sorting approach
     * 
     * @param touchpoints List of touchpoints to sort
     * @return List of touchpoints sorted by timestamp
     */
    private static List<Touchpoint__c> sortTouchpointsByTimestamp(List<Touchpoint__c> touchpoints) {
        // Handle edge cases
        if (touchpoints == null || touchpoints.size() <= 1) {
            return touchpoints;
        }
        
        // Create a list of wrapper objects to sort
        List<TouchpointWrapper> wrappers = new List<TouchpointWrapper>();
        for (Touchpoint__c touchpoint : touchpoints) {
            wrappers.add(new TouchpointWrapper(touchpoint));
        }
        
        // Sort using simple bubble sort (reliable for small lists)
        for (Integer i = 0; i < wrappers.size() - 1; i++) {
            for (Integer j = 0; j < wrappers.size() - i - 1; j++) {
                if (wrappers[j].timestamp > wrappers[j + 1].timestamp) {
                    // Swap
                    TouchpointWrapper temp = wrappers[j];
                    wrappers[j] = wrappers[j + 1];
                    wrappers[j + 1] = temp;
                }
            }
        }
        
        // Extract sorted touchpoints
        List<Touchpoint__c> sortedTouchpoints = new List<Touchpoint__c>();
        for (TouchpointWrapper wrapper : wrappers) {
            sortedTouchpoints.add(wrapper.touchpoint);
        }
        
        return sortedTouchpoints;
    }
    
    /**
     * Wrapper class for touchpoint sorting
     */
    private class TouchpointWrapper {
        public Touchpoint__c touchpoint;
        public DateTime timestamp;
        
        public TouchpointWrapper(Touchpoint__c touchpoint) {
            this.touchpoint = touchpoint;
            this.timestamp = touchpoint.Touchpoint_Timestamp__c;
        }
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
     * Trigger Position-Based Attribution calculation for journeys
     * This method is designed to be called from triggers or flows
     * 
     * @param journeyIds List of journey IDs to process
     */
    public static void triggerAttributionCalculation(List<Id> journeyIds) {
        if (journeyIds == null || journeyIds.isEmpty()) {
            return;
        }
        
        // Process synchronously for all datasets initially
        List<Attribution_Result__c> results = calculateAttributionBatch(journeyIds);
        
        if (!results.isEmpty()) {
            try {
                // Delete existing Position-Based results for these journeys
                delete [SELECT Id FROM Attribution_Result__c 
                       WHERE Customer_Journey__c IN :journeyIds 
                       AND Attribution_Model__c = :ATTRIBUTION_MODEL];
                
                // Insert new results
                insert results;
                
            } catch (DMLException e) {
                System.debug('Error saving attribution results: ' + e.getMessage());
                throw new AttributionException('Failed to save Position-Based Attribution results: ' + e.getMessage());
            }
        }
    }
    
    /**
     * Trigger Position-Based Attribution calculation for large datasets
     * This method can be used when batch processing is needed
     * 
     * @param journeyIds List of journey IDs to process
     */
    public static void triggerAttributionCalculationBatch(List<Id> journeyIds) {
        if (journeyIds == null || journeyIds.isEmpty()) {
            return;
        }
        
        System.debug('Batch processing requested for ' + journeyIds.size() + ' journeys');
        System.debug('Note: Deploy PositionBasedAttributionBatch class to enable true batch processing');
        
        // For now, fall back to synchronous processing
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
     * Get channel statistics for Position-Based Attribution model
     * Provides insights into first/last/middle touchpoint distribution
     * 
     * @param journeyIds List of journey IDs to analyze
     * @return Map with channel statistics
     */
    public static Map<String, Object> getChannelStatistics(List<Id> journeyIds) {
        Map<String, Object> statistics = new Map<String, Object>();
        
        try {
            List<Attribution_Result__c> results = [
                SELECT Channel__c, Attribution_Weight__c, Attribution_Value__c
                FROM Attribution_Result__c 
                WHERE Customer_Journey__c IN :journeyIds 
                AND Attribution_Model__c = :ATTRIBUTION_MODEL
            ];
            
            Map<String, List<Decimal>> channelWeights = new Map<String, List<Decimal>>();
            
            for (Attribution_Result__c result : results) {
                String channel = result.Channel__c;
                if (!channelWeights.containsKey(channel)) {
                    channelWeights.put(channel, new List<Decimal>());
                }
                channelWeights.get(channel).add(result.Attribution_Weight__c);
            }
            
            // Calculate statistics per channel
            for (String channel : channelWeights.keySet()) {
                List<Decimal> weights = channelWeights.get(channel);
                Decimal sum = 0;
                for (Decimal weight : weights) {
                    sum += weight;
                }
                
                statistics.put(channel + '_average_weight', sum / weights.size());
                statistics.put(channel + '_total_attributions', weights.size());
            }
            
        } catch (Exception e) {
            System.debug('Error calculating channel statistics: ' + e.getMessage());
        }
        
        return statistics;
    }
    
    /**
     * Get position-based model insights for business analysis
     * 
     * @param journeyIds List of journey IDs to analyze
     * @return Map with position-based insights
     */
    public static Map<String, Object> getPositionBasedInsights(List<Id> journeyIds) {
        Map<String, Object> insights = new Map<String, Object>();
        
        try {
            // Count touchpoints by position across all journeys
            List<Touchpoint__c> allTouchpoints = [
                SELECT Id, Customer_Journey__c, Channel__c, Touchpoint_Timestamp__c
                FROM Touchpoint__c 
                WHERE Customer_Journey__c IN :journeyIds
                ORDER BY Customer_Journey__c, Touchpoint_Timestamp__c ASC
            ];
            
            Map<String, Integer> firstTouchpointCounts = new Map<String, Integer>();
            Map<String, Integer> lastTouchpointCounts = new Map<String, Integer>();
            Map<String, Integer> middleTouchpointCounts = new Map<String, Integer>();
            
            // Group touchpoints by journey
            Map<Id, List<Touchpoint__c>> touchpointsByJourney = new Map<Id, List<Touchpoint__c>>();
            for (Touchpoint__c touchpoint : allTouchpoints) {
                if (!touchpointsByJourney.containsKey(touchpoint.Customer_Journey__c)) {
                    touchpointsByJourney.put(touchpoint.Customer_Journey__c, new List<Touchpoint__c>());
                }
                touchpointsByJourney.get(touchpoint.Customer_Journey__c).add(touchpoint);
            }
            
            // Analyze position patterns
            for (Id journeyId : touchpointsByJourney.keySet()) {
                List<Touchpoint__c> journeyTouchpoints = touchpointsByJourney.get(journeyId);
                
                if (journeyTouchpoints.size() >= 1) {
                    // First touchpoint
                    String firstChannel = journeyTouchpoints[0].Channel__c;
                    firstTouchpointCounts.put(firstChannel, 
                        firstTouchpointCounts.containsKey(firstChannel) ? 
                        firstTouchpointCounts.get(firstChannel) + 1 : 1);
                    
                    // Last touchpoint (if different from first)
                    if (journeyTouchpoints.size() > 1) {
                        String lastChannel = journeyTouchpoints[journeyTouchpoints.size() - 1].Channel__c;
                        lastTouchpointCounts.put(lastChannel, 
                            lastTouchpointCounts.containsKey(lastChannel) ? 
                            lastTouchpointCounts.get(lastChannel) + 1 : 1);
                    }
                    
                    // Middle touchpoints
                    for (Integer i = 1; i < journeyTouchpoints.size() - 1; i++) {
                        String middleChannel = journeyTouchpoints[i].Channel__c;
                        middleTouchpointCounts.put(middleChannel, 
                            middleTouchpointCounts.containsKey(middleChannel) ? 
                            middleTouchpointCounts.get(middleChannel) + 1 : 1);
                    }
                }
            }
            
            insights.put('first_touchpoint_counts', firstTouchpointCounts);
            insights.put('last_touchpoint_counts', lastTouchpointCounts);
            insights.put('middle_touchpoint_counts', middleTouchpointCounts);
            
        } catch (Exception e) {
            System.debug('Error calculating position-based insights: ' + e.getMessage());
        }
        
        return insights;
    }
    
    /**
     * Custom exception class for attribution errors
     */
    public class AttributionException extends Exception {}
}