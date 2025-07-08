/**
 * TimeDecayAttribution
 * 
 * Implements Time Decay Attribution model that applies exponential decay to touchpoints
 * based on their temporal distance from conversion. Recent touchpoints receive higher
 * attribution weights while older touchpoints receive exponentially decreasing weights.
 * 
 * Business Logic:
 * - Calculates time difference between each touchpoint and journey end
 * - Applies exponential decay formula: weight = decay_factor ^ (days_since_touchpoint)
 * - Normalizes weights to sum to 100% across all touchpoints
 * - Supports configurable decay factors (default: 0.7)
 * 
 * Author: Omnichannel Attribution Platform
 * Date: 2025
 * Version: 1.0
 */
public class TimeDecayAttribution {
    
    // Constants for attribution model configuration
    private static final String ATTRIBUTION_MODEL = 'TimeDecay';
    private static final Decimal DEFAULT_DECAY_FACTOR = 0.7;
    private static final Integer BATCH_SIZE = 200;
    private static final Integer HOURS_PER_DAY = 24;
    
    // Configurable decay factors for different scenarios
    private static final Map<String, Decimal> DECAY_FACTORS = new Map<String, Decimal>{
        'AGGRESSIVE' => 0.5,    // Strong preference for recent touchpoints
        'MODERATE' => 0.7,      // Balanced decay (default)
        'GENTLE' => 0.9         // Mild preference for recent touchpoints
    };
    
    /**
     * Calculate Time Decay Attribution for a single customer journey
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
     * Calculate Time Decay Attribution for multiple customer journeys
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
            throw new AttributionException('Failed to calculate Time Decay Attribution: ' + e.getMessage());
        }
        
        return attributionResults;
    }
    
    /**
     * Process a single customer journey to determine time decay attribution
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
        
        // Determine the reference date for decay calculation
        DateTime referenceDate = determineReferenceDate(journey, touchpoints);
        
        // Determine decay factor based on customer type
        Decimal decayFactor = determineDecayFactor(journey.Customer_Type__c);
        
        // Calculate time decay weights for each touchpoint
        Map<String, Decimal> channelWeights = calculateTimeDecayWeights(
            touchpoints, referenceDate, decayFactor
        );
        
        // Normalize weights to sum to 100%
        channelWeights = normalizeWeights(channelWeights);
        
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
     * Determine the reference date for time decay calculation
     * Uses journey end date as the reference point for calculating time decay
     * 
     * @param journey The customer journey record
     * @param touchpoints List of touchpoints in the journey (unused but kept for consistency)
     * @return Reference DateTime for decay calculation
     */
    private static DateTime determineReferenceDate(Customer_Journey__c journey, List<Touchpoint__c> touchpoints) {
        // Use journey end date as reference since it's required
        return journey.Journey_End_Date__c;
    }
    
    /**
     * Determine decay factor based on customer type and journey characteristics
     * 
     * @param customerType The customer type (B2B or B2C)
     * @return Decimal decay factor to use
     */
    private static Decimal determineDecayFactor(String customerType) {
        // B2B customers typically have longer consideration periods
        // Use gentler decay for B2B, more aggressive for B2C
        if (customerType == 'B2B') {
            return DECAY_FACTORS.get('GENTLE');
        } else {
            return DECAY_FACTORS.get('MODERATE');
        }
    }
    
    /**
     * Calculate time decay weights for each channel based on touchpoint timing
     * 
     * @param touchpoints List of touchpoints in the journey
     * @param referenceDate Reference date for decay calculation
     * @param decayFactor Exponential decay factor
     * @return Map of channel to raw attribution weight
     */
    private static Map<String, Decimal> calculateTimeDecayWeights(
        List<Touchpoint__c> touchpoints, DateTime referenceDate, Decimal decayFactor) {
        
        Map<String, Decimal> channelWeights = new Map<String, Decimal>();
        
        for (Touchpoint__c touchpoint : touchpoints) {
            // Calculate time difference in days
            Long timeDifferenceMs = referenceDate.getTime() - touchpoint.Touchpoint_Timestamp__c.getTime();
            Decimal daysDifference = timeDifferenceMs / (1000 * 60 * 60 * HOURS_PER_DAY);
            
            // Ensure non-negative time difference
            daysDifference = Math.max(0, daysDifference);
            
            // Calculate decay weight using exponential decay formula
            // Convert to Double for Math.pow, then back to Decimal
            Double decayFactorDouble = Double.valueOf(decayFactor);
            Double daysDifferenceDouble = Double.valueOf(daysDifference);
            Decimal weight = Decimal.valueOf(Math.pow(decayFactorDouble, daysDifferenceDouble));
            
            // Aggregate weights by channel
            String channel = touchpoint.Channel__c;
            if (channelWeights.containsKey(channel)) {
                channelWeights.put(channel, channelWeights.get(channel) + weight);
            } else {
                channelWeights.put(channel, weight);
            }
        }
        
        return channelWeights;
    }
    
    /**
     * Normalize weights to sum to 100%
     * 
     * @param rawWeights Map of channel to raw attribution weight
     * @return Map of channel to normalized attribution weight
     */
    private static Map<String, Decimal> normalizeWeights(Map<String, Decimal> rawWeights) {
        Map<String, Decimal> normalizedWeights = new Map<String, Decimal>();
        
        if (rawWeights == null || rawWeights.isEmpty()) {
            return normalizedWeights;
        }
        
        // Calculate total weight
        Decimal totalWeight = 0;
        for (Decimal weight : rawWeights.values()) {
            totalWeight += weight;
        }
        
        // Handle edge case where total weight is zero
        if (totalWeight == 0) {
            // Fallback to equal distribution
            Decimal equalWeight = 1.0 / rawWeights.size();
            for (String channel : rawWeights.keySet()) {
                normalizedWeights.put(channel, equalWeight);
            }
            return normalizedWeights;
        }
        
        // Normalize each weight
        for (String channel : rawWeights.keySet()) {
            Decimal normalizedWeight = rawWeights.get(channel) / totalWeight;
            normalizedWeights.put(channel, normalizedWeight);
        }
        
        return normalizedWeights;
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
     * Trigger Time Decay Attribution calculation for journeys
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
                // Delete existing Time Decay results for these journeys
                delete [SELECT Id FROM Attribution_Result__c 
                       WHERE Customer_Journey__c IN :journeyIds 
                       AND Attribution_Model__c = :ATTRIBUTION_MODEL];
                
                // Insert new results
                insert results;
                
            } catch (DMLException e) {
                System.debug('Error saving attribution results: ' + e.getMessage());
                throw new AttributionException('Failed to save Time Decay Attribution results: ' + e.getMessage());
            }
        }
    }
    
    /**
     * Trigger Time Decay Attribution calculation for large datasets
     * This method can be used when batch processing is needed
     * 
     * @param journeyIds List of journey IDs to process
     */
    public static void triggerAttributionCalculationBatch(List<Id> journeyIds) {
        if (journeyIds == null || journeyIds.isEmpty()) {
            return;
        }
        
        System.debug('Batch processing requested for ' + journeyIds.size() + ' journeys');
        System.debug('Note: Deploy TimeDecayAttributionBatch class to enable true batch processing');
        
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
     * Get channel statistics for Time Decay Attribution model
     * Provides insights into decay patterns and channel performance
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
     * Custom exception class for attribution errors
     */
    public class AttributionException extends Exception {}
}