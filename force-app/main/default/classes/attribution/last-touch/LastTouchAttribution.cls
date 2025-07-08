/**
 * LastTouchAttribution
 * 
 * Implements Last Touch Attribution model that assigns 100% credit to the final touchpoint
 * in each customer journey. This class provides both synchronous and batch processing
 * capabilities to handle large datasets while respecting Salesforce governor limits.
 * 
 * Business Logic:
 * - Identifies the chronologically last touchpoint in each customer journey
 * - Assigns 100% attribution weight and full conversion value to that touchpoint
 * - All other touchpoints in the journey receive 0% attribution
 * - Handles both B2B and B2C customer journeys
 * 
 * Author: Omnichannel Attribution Platform
 * Date: 2025
 * Version: 1.0
 */
public class LastTouchAttribution {
    
    // Constants for attribution model configuration
    private static final String ATTRIBUTION_MODEL = 'LastTouch';
    private static final Decimal FULL_ATTRIBUTION_WEIGHT = 1.0;
    private static final Decimal NO_ATTRIBUTION_WEIGHT = 0.0;
    private static final Integer BATCH_SIZE = 200;
    
    /**
     * Calculate Last Touch Attribution for a single customer journey
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
     * Calculate Last Touch Attribution for multiple customer journeys
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
            throw new AttributionException('Failed to calculate Last Touch Attribution: ' + e.getMessage());
        }
        
        return attributionResults;
    }
    
    /**
     * Process a single customer journey to determine last touch attribution
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
        
        // Find the last touchpoint chronologically
        Touchpoint__c lastTouchpoint = findLastTouchpoint(touchpoints);
        
        if (lastTouchpoint == null) {
            System.debug('Could not determine last touchpoint for journey: ' + journey.Id);
            return results;
        }
        
        // Create attribution results for each channel
        Map<String, Decimal> channelAttribution = calculateChannelAttribution(
            touchpoints, lastTouchpoint.Channel__c
        );
        
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
     * Find the chronologically last touchpoint in a journey
     * 
     * @param touchpoints List of touchpoints in the journey
     * @return The last touchpoint or null if none found
     */
    private static Touchpoint__c findLastTouchpoint(List<Touchpoint__c> touchpoints) {
        if (touchpoints == null || touchpoints.isEmpty()) {
            return null;
        }
        
        Touchpoint__c lastTouchpoint = touchpoints[0];
        
        for (Touchpoint__c touchpoint : touchpoints) {
            if (touchpoint.Touchpoint_Timestamp__c > lastTouchpoint.Touchpoint_Timestamp__c) {
                lastTouchpoint = touchpoint;
            }
        }
        
        return lastTouchpoint;
    }
    
    /**
     * Calculate attribution weights for each channel based on last touch model
     * 
     * @param touchpoints List of all touchpoints in the journey
     * @param lastTouchChannel The channel of the last touchpoint
     * @return Map of channel to attribution weight
     */
    private static Map<String, Decimal> calculateChannelAttribution(
        List<Touchpoint__c> touchpoints, String lastTouchChannel) {
        
        Map<String, Decimal> channelAttribution = new Map<String, Decimal>();
        
        // Get all unique channels in the journey
        Set<String> allChannels = new Set<String>();
        for (Touchpoint__c touchpoint : touchpoints) {
            allChannels.add(touchpoint.Channel__c);
        }
        
        // Assign attribution weights
        for (String channel : allChannels) {
            if (channel.equals(lastTouchChannel)) {
                channelAttribution.put(channel, FULL_ATTRIBUTION_WEIGHT);
            } else {
                channelAttribution.put(channel, NO_ATTRIBUTION_WEIGHT);
            }
        }
        
        return channelAttribution;
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
     * Trigger Last Touch Attribution calculation for journeys
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
                // Delete existing Last Touch results for these journeys
                delete [SELECT Id FROM Attribution_Result__c 
                       WHERE Customer_Journey__c IN :journeyIds 
                       AND Attribution_Model__c = :ATTRIBUTION_MODEL];
                
                // Insert new results
                insert results;
                
            } catch (DMLException e) {
                System.debug('Error saving attribution results: ' + e.getMessage());
                throw new AttributionException('Failed to save Last Touch Attribution results: ' + e.getMessage());
            }
        }
    }
    
    /**
     * Trigger Last Touch Attribution calculation for large datasets
     * This method can be used when batch processing is needed
     * 
     * @param journeyIds List of journey IDs to process
     */
    public static void triggerAttributionCalculationBatch(List<Id> journeyIds) {
        if (journeyIds == null || journeyIds.isEmpty()) {
            return;
        }
        
        System.debug('Batch processing requested for ' + journeyIds.size() + ' journeys');
        System.debug('Note: Deploy LastTouchAttributionBatch class to enable true batch processing');
        
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
     * Custom exception class for attribution errors
     */
    public class AttributionException extends Exception {}
}