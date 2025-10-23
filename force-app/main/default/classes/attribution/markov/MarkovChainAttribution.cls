/**
 * MarkovChainAttribution
 * 
 * Implements true Markov Chain Attribution model using probabilistic state transitions
 * between channel-stage combinations. Creates a network structure modeling how customers
 * move through Awareness → Interest → Consideration → Conversion stages across channels.
 * 
 * Business Logic:
 * - Detects journey stages based on touchpoint context, sequence, and timing
 * - Builds transition matrices showing how customers move between channel-stage states
 * - Calculates removal effects by excluding specific channel-stage combinations
 * - Assigns attribution credit based on state contribution to conversion probability
 * - Supports both B2C (25 states) and B2B (15 states) customer journeys
 * 
 * Mathematical Implementation:
 * - State transitions: P(j|i) = N(i→j) / N(i)
 * - Removal effect: RE(k) = CV_baseline - CV_without_k
 * - Attribution credit: Attribution(k) = RE(k) / Σ(RE(all_channels))
 * 
 * Author: Omnichannel Attribution Platform
 * Date: 2025
 * Version: 2.0 (Complete Rewrite)
 */
public class MarkovChainAttribution {
    
    // Constants for attribution model configuration
    private static final String ATTRIBUTION_MODEL = 'MarkovChain';
    private static final Integer BATCH_SIZE = 200;
    private static final Decimal MINIMUM_TRANSITION_THRESHOLD = 0.005;
    private static final Integer MAX_ITERATIONS = 1000;
    private static final Decimal CONVERGENCE_THRESHOLD = 0.0001;
    
    // Journey stages
    private static final String AWARENESS_STAGE = 'Awareness';
    private static final String INTEREST_STAGE = 'Interest';
    private static final String CONSIDERATION_STAGE = 'Consideration';
    private static final String CONVERSION_STAGE = 'Conversion';
    private static final String RETENTION_STAGE = 'Retention';
    
    // B2C State Space Definition (25 states)
    private static final Set<String> B2C_STATES = new Set<String>{
        // Awareness Stage
        'Google_Ads_Awareness', 'Facebook_Ads_Awareness', 'LinkedIn_Organic_Awareness',
        'Instagram_Organic_Awareness', 'Facebook_Organic_Awareness', 'X_Organic_Awareness',
        'TikTok_Organic_Awareness', 'Influencer_Marketing_Awareness', 'PR_Awareness', 'Events_Awareness',
        // Interest Stage
        'Google_Ads_Interest', 'Facebook_Ads_Interest', 'Content_Website_SEO_Interest',
        'Influencer_Marketing_Interest', 'Email_Marketing_Interest',
        // Consideration Stage
        'Content_Website_SEO_Consideration', 'Email_Marketing_Consideration', 
        'Referral_Consideration', 'App_Store_Optimization_Consideration',
        // Conversion Stage
        'App_Store_Optimization_Conversion', 'Referral_Conversion', 
        'Google_Ads_Conversion', 'Facebook_Ads_Conversion',
        // Retention Stage
        'Email_Marketing_Retention', 'Content_Website_SEO_Retention'
    };
    
    // B2B State Space Definition (15 states)
    private static final Set<String> B2B_STATES = new Set<String>{
        // Awareness Stage
        'Industry_Events_Awareness', 'LinkedIn_Organic_Awareness', 'LinkedIn_Ads_Awareness',
        'Google_Ads_Awareness', 'Content_Website_SEO_Awareness',
        // Interest Stage
        'LinkedIn_Ads_Interest', 'Email_Marketing_Interest', 'Content_Website_SEO_Interest',
        // Consideration Stage
        'Direct_Sales_Consideration', 'Email_Marketing_Consideration', 'Content_Website_SEO_Consideration',
        // Conversion Stage
        'Direct_Sales_Conversion',
        // Retention Stage
        'Email_Marketing_Retention', 'Direct_Sales_Retention'
    };
    
    // Channel-to-stage mapping for B2C
    private static final Map<String, List<String>> B2C_CHANNEL_STAGES = new Map<String, List<String>>{
        'Google_Ads' => new List<String>{AWARENESS_STAGE, INTEREST_STAGE, CONVERSION_STAGE},
        'Facebook_Ads' => new List<String>{AWARENESS_STAGE, INTEREST_STAGE, CONVERSION_STAGE},
        'LinkedIn_Organic' => new List<String>{AWARENESS_STAGE},
        'Instagram_Organic' => new List<String>{AWARENESS_STAGE},
        'Facebook_Organic' => new List<String>{AWARENESS_STAGE},
        'X_Organic' => new List<String>{AWARENESS_STAGE},
        'TikTok_Organic' => new List<String>{AWARENESS_STAGE},
        'Influencer_Marketing' => new List<String>{AWARENESS_STAGE, INTEREST_STAGE},
        'PR' => new List<String>{AWARENESS_STAGE},
        'Events' => new List<String>{AWARENESS_STAGE},
        'Content_Website_SEO' => new List<String>{INTEREST_STAGE, CONSIDERATION_STAGE, RETENTION_STAGE},
        'Email_Marketing' => new List<String>{INTEREST_STAGE, CONSIDERATION_STAGE, RETENTION_STAGE},
        'Referral' => new List<String>{CONSIDERATION_STAGE, CONVERSION_STAGE},
        'App_Store_Optimization' => new List<String>{CONSIDERATION_STAGE, CONVERSION_STAGE}
    };
    
    // Channel-to-stage mapping for B2B
    private static final Map<String, List<String>> B2B_CHANNEL_STAGES = new Map<String, List<String>>{
        'Industry_Events' => new List<String>{AWARENESS_STAGE},
        'LinkedIn_Organic' => new List<String>{AWARENESS_STAGE},
        'LinkedIn_Ads' => new List<String>{AWARENESS_STAGE, INTEREST_STAGE},
        'Google_Ads' => new List<String>{AWARENESS_STAGE},
        'Content_Website_SEO' => new List<String>{AWARENESS_STAGE, INTEREST_STAGE, CONSIDERATION_STAGE},
        'Email_Marketing' => new List<String>{INTEREST_STAGE, CONSIDERATION_STAGE, RETENTION_STAGE},
        'Direct_Sales' => new List<String>{CONSIDERATION_STAGE, CONVERSION_STAGE, RETENTION_STAGE}
    };
    
    // Special states
    private static final String START_STATE = 'START';
    private static final String CONVERSION_STATE = 'CONVERSION';
    private static final String NULL_STATE = 'NULL';
    
    /**
     * Journey stage detection context
     */
    public class JourneyContext {
        public Integer totalTouchpoints;
        public Integer currentPosition;
        public Long timeSinceJourneyStart;
        public Long timeSinceLastTouchpoint;
        public String previousChannel;
        public String previousStage;
        public Boolean isConverted;
        public String customerType;
        public List<String> allChannelsInJourney;
    }
    
    /**
     * Calculate Markov Chain Attribution for a single customer journey
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
     * Calculate Markov Chain Attribution for multiple customer journeys
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
                       Conversion_Value__c, Journey_Start_Date__c, Journey_End_Date__c,
                       Total_Touchpoints__c
                FROM Customer_Journey__c 
                WHERE Id IN :journeyIds
            ];
            
            // Group journeys by customer type for appropriate transition matrix
            Map<String, List<Customer_Journey__c>> journeysByType = new Map<String, List<Customer_Journey__c>>();
            
            for (Customer_Journey__c journey : journeys) {
                String customerType = journey.Customer_Type__c;
                if (!journeysByType.containsKey(customerType)) {
                    journeysByType.put(customerType, new List<Customer_Journey__c>());
                }
                journeysByType.get(customerType).add(journey);
            }
            
            // Process each customer type separately
            for (String customerType : journeysByType.keySet()) {
                List<Customer_Journey__c> typeJourneys = journeysByType.get(customerType);
                List<Attribution_Result__c> typeResults = processJourneysByType(typeJourneys, customerType);
                attributionResults.addAll(typeResults);
            }
            
        } catch (Exception e) {
            System.debug('Error in calculateAttributionBatch: ' + e.getMessage());
            throw new AttributionException('Failed to calculate Markov Chain Attribution: ' + e.getMessage());
        }
        
        return attributionResults;
    }
    
    /**
     * Process journeys of a specific customer type
     * 
     * @param journeys List of journeys for the customer type
     * @param customerType The customer type (B2B or B2C)
     * @return List of Attribution_Result__c records
     */
    private static List<Attribution_Result__c> processJourneysByType(
        List<Customer_Journey__c> journeys, 
        String customerType
    ) {
        List<Attribution_Result__c> results = new List<Attribution_Result__c>();
        
        // Get transition matrix for customer type
        Map<String, Map<String, Decimal>> transitionMatrix = getTransitionMatrix(customerType);
        
        // If no transition matrix exists, trigger calculation
        if (transitionMatrix.isEmpty()) {
            System.debug('No transition matrix found for ' + customerType + '. Triggering matrix calculation.');
            buildAndStoreTransitionMatrix(customerType);
            transitionMatrix = getTransitionMatrix(customerType);
        }
        
        if (transitionMatrix.isEmpty()) {
            System.debug('Warning: Could not build transition matrix for ' + customerType);
            return results;
        }
        
        // Calculate baseline conversion probability
        Decimal baselineConversionProbability = calculateConversionProbability(transitionMatrix);
        
        // Get all channels present in the journeys
        Set<String> allChannels = getAllChannelsFromJourneys(journeys);
        
        // Calculate removal effects for each channel
        Map<String, Decimal> removalEffects = new Map<String, Decimal>();
        
        for (String channel : allChannels) {
            Map<String, Map<String, Decimal>> modifiedMatrix = removeChannelFromMatrix(transitionMatrix, channel);
            Decimal modifiedConversionProb = calculateConversionProbability(modifiedMatrix);
            Decimal removalEffect = baselineConversionProbability - modifiedConversionProb;
            removalEffects.put(channel, Math.max(0, removalEffect)); // Ensure non-negative
        }
        
        // Normalize attribution credits
        Decimal totalRemovalEffect = 0;
        for (Decimal effect : removalEffects.values()) {
            totalRemovalEffect += effect;
        }
        
        // Create attribution results for each journey
        for (Customer_Journey__c journey : journeys) {
            if (totalRemovalEffect > 0) {
                for (String channel : allChannels) {
                    Decimal attributionWeight = removalEffects.get(channel) / totalRemovalEffect;
                    
                    Attribution_Result__c result = new Attribution_Result__c();
                    result.Customer_Journey__c = journey.Id;
                    result.Attribution_Model__c = ATTRIBUTION_MODEL;
                    result.Channel__c = channel;
                    result.Attribution_Weight__c = attributionWeight;
                    result.Attribution_Value__c = calculateAttributionValue(
                        journey.Conversion_Value__c, attributionWeight
                    );
                    
                    results.add(result);
                }
            }
        }
        
        return results;
    }
    
    /**
     * Detect journey stage for a touchpoint based on context
     * 
     * @param touchpoint The touchpoint to analyze
     * @param context Journey context information
     * @return The detected journey stage
     */
    private static String detectJourneyStage(Touchpoint__c touchpoint, JourneyContext context) {
        String channel = touchpoint.Channel__c;
        String customerType = context.customerType;
        
        // Get possible stages for this channel
        Map<String, List<String>> channelStages = customerType == 'B2C' ? B2C_CHANNEL_STAGES : B2B_CHANNEL_STAGES;
        List<String> possibleStages = channelStages.get(channel);
        
        if (possibleStages == null || possibleStages.isEmpty()) {
            return AWARENESS_STAGE; // Default fallback
        }
        
        // If channel only has one possible stage, return it
        if (possibleStages.size() == 1) {
            return possibleStages[0];
        }
        
        // Multi-stage detection logic
        return detectMultiStageChannel(touchpoint, context, possibleStages);
    }
    
    /**
     * Detect stage for channels that can appear in multiple stages
     * 
     * @param touchpoint The touchpoint to analyze
     * @param context Journey context information
     * @param possibleStages List of possible stages for this channel
     * @return The most likely stage
     */
    private static String detectMultiStageChannel(
        Touchpoint__c touchpoint, 
        JourneyContext context, 
        List<String> possibleStages
    ) {
        String channel = touchpoint.Channel__c;
        
        // Stage detection based on position in journey
        Decimal journeyProgress = (Decimal) context.currentPosition / context.totalTouchpoints;
        
        // Early in journey (0-30%) - likely Awareness
        if (journeyProgress <= 0.3 && possibleStages.contains(AWARENESS_STAGE)) {
            return AWARENESS_STAGE;
        }
        
        // Late in journey (70-100%) - likely Conversion or Consideration
        if (journeyProgress >= 0.7) {
            if (possibleStages.contains(CONVERSION_STAGE)) {
                return CONVERSION_STAGE;
            } else if (possibleStages.contains(CONSIDERATION_STAGE)) {
                return CONSIDERATION_STAGE;
            }
        }
        
        // Channel-specific logic
        if (channel == 'Google_Ads' || channel == 'Facebook_Ads') {
            // Paid ads can be any stage - use journey progression
            if (journeyProgress <= 0.3) {
                return AWARENESS_STAGE;
            } else if (journeyProgress >= 0.7) {
                return CONVERSION_STAGE;
            } else {
                return INTEREST_STAGE;
            }
        }
        
        if (channel == 'Email_Marketing') {
            // Email progression: Interest → Consideration → Retention
            if (context.isConverted) {
                return RETENTION_STAGE;
            } else if (journeyProgress >= 0.5) {
                return CONSIDERATION_STAGE;
            } else {
                return INTEREST_STAGE;
            }
        }
        
        if (channel == 'Content_Website_SEO') {
            // Content progression: Interest → Consideration → Retention
            if (context.isConverted) {
                return RETENTION_STAGE;
            } else if (journeyProgress >= 0.6) {
                return CONSIDERATION_STAGE;
            } else {
                return INTEREST_STAGE;
            }
        }
        
        if (channel == 'Direct_Sales') {
            // Direct sales progression: Consideration → Conversion → Retention
            if (context.isConverted) {
                return RETENTION_STAGE;
            } else if (journeyProgress >= 0.8) {
                return CONVERSION_STAGE;
            } else {
                return CONSIDERATION_STAGE;
            }
        }
        
        // Default to middle stage if available
        if (possibleStages.contains(INTEREST_STAGE)) {
            return INTEREST_STAGE;
        } else if (possibleStages.contains(CONSIDERATION_STAGE)) {
            return CONSIDERATION_STAGE;
        } else {
            return possibleStages[0];
        }
    }
    
    /**
     * Get transition matrix from stored records
     * 
     * @param customerType The customer type (B2B or B2C)
     * @return Map representing transition matrix
     */
    private static Map<String, Map<String, Decimal>> getTransitionMatrix(String customerType) {
        Map<String, Map<String, Decimal>> matrix = new Map<String, Map<String, Decimal>>();
        
        try {
            List<Transition_Matrix__c> transitions = [
                SELECT From_State__c, To_State__c, Probability__c
                FROM Transition_Matrix__c
                WHERE Customer_Type__c = :customerType
                AND Last_Calculated__c = LAST_N_DAYS:7  // Get recent matrix
                ORDER BY Last_Calculated__c DESC
            ];
            
            for (Transition_Matrix__c transition : transitions) {
                String fromState = transition.From_State__c;
                String toState = transition.To_State__c;
                Decimal probability = transition.Probability__c;
                
                if (!matrix.containsKey(fromState)) {
                    matrix.put(fromState, new Map<String, Decimal>());
                }
                matrix.get(fromState).put(toState, probability);
            }
            
        } catch (Exception e) {
            System.debug('Error loading transition matrix: ' + e.getMessage());
        }
        
        return matrix;
    }
    
    /**
     * Build and store transition matrix for customer type
     * 
     * @param customerType The customer type (B2B or B2C)
     */
    public static void buildAndStoreTransitionMatrix(String customerType) {
        try {
            // Get all journeys for customer type
            List<Customer_Journey__c> journeys = [
                SELECT Id, Customer_Type__c, Converted__c, Journey_Start_Date__c, 
                       Journey_End_Date__c, Total_Touchpoints__c
                FROM Customer_Journey__c 
                WHERE Customer_Type__c = :customerType
                AND Total_Touchpoints__c > 0
                LIMIT 10000  // Process in chunks for large datasets
            ];
            
            if (journeys.isEmpty()) {
                System.debug('No journeys found for customer type: ' + customerType);
                return;
            }
            
            // Get touchpoints for these journeys
            List<Id> journeyIds = new List<Id>();
            for (Customer_Journey__c journey : journeys) {
                journeyIds.add(journey.Id);
            }
            
            List<Touchpoint__c> touchpoints = [
                SELECT Id, Customer_Journey__c, Channel__c, Touchpoint_Timestamp__c,
                       Customer_Journey__r.Customer_Type__c, Customer_Journey__r.Converted__c,
                       Customer_Journey__r.Journey_Start_Date__c, Customer_Journey__r.Total_Touchpoints__c
                FROM Touchpoint__c 
                WHERE Customer_Journey__c IN :journeyIds
                ORDER BY Customer_Journey__c, Touchpoint_Timestamp__c ASC
            ];
            
            // Build transition matrix
            Map<String, Map<String, Decimal>> transitionMatrix = buildTransitionMatrix(touchpoints, customerType);
            
            // Store transition matrix
            storeTransitionMatrix(transitionMatrix, customerType);
            
        } catch (Exception e) {
            System.debug('Error building transition matrix: ' + e.getMessage());
            throw new AttributionException('Failed to build transition matrix: ' + e.getMessage());
        }
    }
    
    /**
     * Build transition matrix from touchpoint data with proper stage detection
     * 
     * @param touchpoints List of touchpoints to analyze
     * @param customerType The customer type (B2B or B2C)
     * @return Map representing transition matrix
     */
    private static Map<String, Map<String, Decimal>> buildTransitionMatrix(
        List<Touchpoint__c> touchpoints, 
        String customerType
    ) {
        // Count transitions
        Map<String, Map<String, Integer>> transitionCounts = new Map<String, Map<String, Integer>>();
        Map<String, Integer> stateCounts = new Map<String, Integer>();
        
        // Group touchpoints by journey
        Map<Id, List<Touchpoint__c>> touchpointsByJourney = new Map<Id, List<Touchpoint__c>>();
        for (Touchpoint__c touchpoint : touchpoints) {
            if (!touchpointsByJourney.containsKey(touchpoint.Customer_Journey__c)) {
                touchpointsByJourney.put(touchpoint.Customer_Journey__c, new List<Touchpoint__c>());
            }
            touchpointsByJourney.get(touchpoint.Customer_Journey__c).add(touchpoint);
        }
        
        // Process each journey to count state transitions
        for (Id journeyId : touchpointsByJourney.keySet()) {
            List<Touchpoint__c> journeyTouchpoints = touchpointsByJourney.get(journeyId);
            
            if (journeyTouchpoints.isEmpty()) continue;
            
            Boolean isConverted = journeyTouchpoints[0].Customer_Journey__r.Converted__c;
            Integer totalTouchpoints = Integer.valueOf(journeyTouchpoints[0].Customer_Journey__r.Total_Touchpoints__c);
            DateTime journeyStart = journeyTouchpoints[0].Customer_Journey__r.Journey_Start_Date__c;
            
            // Convert touchpoints to states with proper stage detection
            List<String> states = convertTouchpointsToStatesWithStageDetection(
                journeyTouchpoints, customerType, isConverted, totalTouchpoints, journeyStart
            );
            
            // Add START state at beginning
            states.add(0, START_STATE);
            
            // Add end state (CONVERSION or NULL)
            states.add(isConverted ? CONVERSION_STATE : NULL_STATE);
            
            // Count transitions
            for (Integer i = 0; i < states.size() - 1; i++) {
                String fromState = states[i];
                String toState = states[i + 1];
                
                // Count state occurrences
                if (!stateCounts.containsKey(fromState)) {
                    stateCounts.put(fromState, 0);
                }
                stateCounts.put(fromState, stateCounts.get(fromState) + 1);
                
                // Count transitions
                if (!transitionCounts.containsKey(fromState)) {
                    transitionCounts.put(fromState, new Map<String, Integer>());
                }
                if (!transitionCounts.get(fromState).containsKey(toState)) {
                    transitionCounts.get(fromState).put(toState, 0);
                }
                transitionCounts.get(fromState).put(toState, transitionCounts.get(fromState).get(toState) + 1);
            }
        }
        
        // Convert counts to probabilities
        Map<String, Map<String, Decimal>> transitionMatrix = new Map<String, Map<String, Decimal>>();
        
        for (String fromState : transitionCounts.keySet()) {
            Integer totalFromState = stateCounts.get(fromState);
            if (totalFromState > 0) {
                Map<String, Decimal> fromStateProbabilities = new Map<String, Decimal>();
                
                for (String toState : transitionCounts.get(fromState).keySet()) {
                    Integer transitionCount = transitionCounts.get(fromState).get(toState);
                    Decimal probability = (Decimal) transitionCount / totalFromState;
                    
                    // Apply minimum threshold
                    if (probability >= MINIMUM_TRANSITION_THRESHOLD) {
                        fromStateProbabilities.put(toState, probability);
                    }
                }
                
                // Normalize probabilities to sum to 1.0
                fromStateProbabilities = normalizeProbabilities(fromStateProbabilities);
                transitionMatrix.put(fromState, fromStateProbabilities);
            }
        }
        
        return transitionMatrix;
    }
    
    /**
     * Convert touchpoints to state sequence with proper stage detection
     * 
     * @param touchpoints List of touchpoints for a journey
     * @param customerType The customer type (B2B or B2C)
     * @param isConverted Whether the journey converted
     * @param totalTouchpoints Total touchpoints in the journey
     * @param journeyStart Journey start date
     * @return List of state names
     */
    private static List<String> convertTouchpointsToStatesWithStageDetection(
        List<Touchpoint__c> touchpoints, 
        String customerType, 
        Boolean isConverted,
        Integer totalTouchpoints,
        DateTime journeyStart
    ) {
        List<String> states = new List<String>();
        
        // Get valid states for customer type
        Set<String> validStates = customerType == 'B2C' ? B2C_STATES : B2B_STATES;
        
        // Build journey context
        JourneyContext context = new JourneyContext();
        context.totalTouchpoints = totalTouchpoints;
        context.isConverted = isConverted;
        context.customerType = customerType;
        context.allChannelsInJourney = new List<String>();
        
        for (Touchpoint__c tp : touchpoints) {
            context.allChannelsInJourney.add(tp.Channel__c);
        }
        
        // Process each touchpoint with stage detection
        for (Integer i = 0; i < touchpoints.size(); i++) {
            Touchpoint__c touchpoint = touchpoints[i];
            
            // Update context for current touchpoint
            context.currentPosition = i + 1;
            context.timeSinceJourneyStart = journeyStart.getTime() - touchpoint.Touchpoint_Timestamp__c.getTime();
            
            if (i > 0) {
                context.timeSinceLastTouchpoint = touchpoints[i-1].Touchpoint_Timestamp__c.getTime() - touchpoint.Touchpoint_Timestamp__c.getTime();
                context.previousChannel = touchpoints[i-1].Channel__c;
            }
            
            // Detect stage for this touchpoint
            String stage = detectJourneyStage(touchpoint, context);
            String state = touchpoint.Channel__c + '_' + stage;
            
            // Validate state exists in valid states
            if (validStates.contains(state)) {
                states.add(state);
                context.previousStage = stage;
            } else {
                // Fallback to awareness stage if state not found
                String fallbackState = touchpoint.Channel__c + '_' + AWARENESS_STAGE;
                if (validStates.contains(fallbackState)) {
                    states.add(fallbackState);
                    context.previousStage = AWARENESS_STAGE;
                }
            }
        }
        
        return states;
    }
    
    /**
     * Normalize probabilities to sum to 1.0
     * 
     * @param probabilities Map of state probabilities
     * @return Normalized probabilities
     */
    private static Map<String, Decimal> normalizeProbabilities(Map<String, Decimal> probabilities) {
        Decimal total = 0;
        for (Decimal prob : probabilities.values()) {
            total += prob;
        }
        
        Map<String, Decimal> normalized = new Map<String, Decimal>();
        if (total > 0) {
            for (String state : probabilities.keySet()) {
                normalized.put(state, probabilities.get(state) / total);
            }
        }
        
        return normalized;
    }
    
    /**
     * Store transition matrix in Transition_Matrix__c records
     * 
     * @param transitionMatrix The transition matrix to store
     * @param customerType The customer type (B2B or B2C)
     */
    private static void storeTransitionMatrix(
        Map<String, Map<String, Decimal>> transitionMatrix, 
        String customerType
    ) {
        try {
            // Delete existing matrix for customer type
            delete [SELECT Id FROM Transition_Matrix__c WHERE Customer_Type__c = :customerType];
            
            // Create new matrix records
            List<Transition_Matrix__c> matrixRecords = new List<Transition_Matrix__c>();
            String matrixVersion = DateTime.now().format('yyyy-MM-dd_HH-mm-ss');
            
            for (String fromState : transitionMatrix.keySet()) {
                Map<String, Decimal> transitions = transitionMatrix.get(fromState);
                
                for (String toState : transitions.keySet()) {
                    Transition_Matrix__c record = new Transition_Matrix__c();
                    record.From_State__c = fromState;
                    record.To_State__c = toState;
                    record.Probability__c = transitions.get(toState);
                    record.Customer_Type__c = customerType;
                    record.Matrix_Version__c = matrixVersion;
                    record.Last_Calculated__c = DateTime.now();
                    
                    matrixRecords.add(record);
                }
            }
            
            if (!matrixRecords.isEmpty()) {
                insert matrixRecords;
                System.debug('Stored ' + matrixRecords.size() + ' transition matrix records for ' + customerType);
            }
            
        } catch (Exception e) {
            System.debug('Error storing transition matrix: ' + e.getMessage());
            throw new AttributionException('Failed to store transition matrix: ' + e.getMessage());
        }
    }
    
    /**
     * Calculate conversion probability using transition matrix
     * 
     * @param transitionMatrix The transition matrix
     * @return Conversion probability
     */
    private static Decimal calculateConversionProbability(Map<String, Map<String, Decimal>> transitionMatrix) {
        // Use matrix power method to calculate steady-state conversion probability
        if (!transitionMatrix.containsKey(START_STATE)) {
            return 0;
        }
        
        // Calculate probability of reaching conversion state
        return calculateReachabilityProbability(transitionMatrix, START_STATE, CONVERSION_STATE, new Set<String>(), 0);
    }
    
    /**
     * Calculate probability of reaching target state from source state
     * 
     * @param matrix The transition matrix
     * @param currentState Current state
     * @param targetState Target state to reach
     * @param visited Set of visited states (to prevent infinite loops)
     * @param depth Current recursion depth
     * @return Probability of reaching target state
     */
    private static Decimal calculateReachabilityProbability(
        Map<String, Map<String, Decimal>> matrix,
        String currentState,
        String targetState,
        Set<String> visited,
        Integer depth
    ) {
        // Prevent infinite recursion
        if (depth > 10 || visited.contains(currentState)) {
            return 0;
        }
        
        if (currentState == targetState) {
            return 1;
        }
        
        if (!matrix.containsKey(currentState)) {
            return 0;
        }
        
        visited.add(currentState);
        
        Decimal totalProbability = 0;
        Map<String, Decimal> transitions = matrix.get(currentState);
        
        for (String nextState : transitions.keySet()) {
            Decimal transitionProb = transitions.get(nextState);
            Decimal reachProb = calculateReachabilityProbability(matrix, nextState, targetState, visited, depth + 1);
            totalProbability += transitionProb * reachProb;
        }
        
        visited.remove(currentState);
        
        return totalProbability;
    }
    
    /**
     * Remove channel from transition matrix for removal effect calculation
     * 
     * @param originalMatrix The original transition matrix
     * @param channelToRemove Channel to remove
     * @return Modified transition matrix without the channel
     */
    private static Map<String, Map<String, Decimal>> removeChannelFromMatrix(
        Map<String, Map<String, Decimal>> originalMatrix,
        String channelToRemove
    ) {
        Map<String, Map<String, Decimal>> modifiedMatrix = new Map<String, Map<String, Decimal>>();
        
        for (String fromState : originalMatrix.keySet()) {
            // Skip states related to the channel being removed
            if (fromState.contains(channelToRemove)) {
                continue;
            }
            
            Map<String, Decimal> originalTransitions = originalMatrix.get(fromState);
            Map<String, Decimal> modifiedTransitions = new Map<String, Decimal>();
            
            Decimal totalProbToKeep = 0;
            
            // First pass: calculate total probability for states we're keeping
            for (String toState : originalTransitions.keySet()) {
                if (!toState.contains(channelToRemove)) {
                    totalProbToKeep += originalTransitions.get(toState);
                    modifiedTransitions.put(toState, originalTransitions.get(toState));
                }
            }
            
            // Normalize remaining probabilities
            if (totalProbToKeep > 0) {
                for (String toState : modifiedTransitions.keySet()) {
                    Decimal normalizedProb = modifiedTransitions.get(toState) / totalProbToKeep;
                    modifiedTransitions.put(toState, normalizedProb);
                }
                modifiedMatrix.put(fromState, modifiedTransitions);
            }
        }
        
        return modifiedMatrix;
    }
    
    /**
     * Get all channels present in the journeys
     * 
     * @param journeys List of customer journeys
     * @return Set of channel names
     */
    private static Set<String> getAllChannelsFromJourneys(List<Customer_Journey__c> journeys) {
        Set<String> channels = new Set<String>();
        
        List<Id> journeyIds = new List<Id>();
        for (Customer_Journey__c journey : journeys) {
            journeyIds.add(journey.Id);
        }
        
        List<Touchpoint__c> touchpoints = [
            SELECT Channel__c FROM Touchpoint__c 
            WHERE Customer_Journey__c IN :journeyIds
        ];
        
        for (Touchpoint__c touchpoint : touchpoints) {
            channels.add(touchpoint.Channel__c);
        }
        
        return channels;
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
     * Trigger Markov Chain Attribution calculation for journeys
     * 
     * @param journeyIds List of journey IDs to process
     */
    public static void triggerAttributionCalculation(List<Id> journeyIds) {
        if (journeyIds == null || journeyIds.isEmpty()) {
            return;
        }
        
        List<Attribution_Result__c> results = calculateAttributionBatch(journeyIds);
        
        if (!results.isEmpty()) {
            try {
                // Delete existing Markov Chain results for these journeys
                delete [SELECT Id FROM Attribution_Result__c 
                       WHERE Customer_Journey__c IN :journeyIds 
                       AND Attribution_Model__c = :ATTRIBUTION_MODEL];
                
                // Insert new results
                insert results;
                
            } catch (DMLException e) {
                System.debug('Error saving attribution results: ' + e.getMessage());
                throw new AttributionException('Failed to save Markov Chain Attribution results: ' + e.getMessage());
            }
        }
    }
    
    /**
     * Trigger transition matrix recalculation for all customer types
     * 
     * @return Success message
     */
    public static String triggerMatrixRecalculation() {
        try {
            buildAndStoreTransitionMatrix('B2C');
            buildAndStoreTransitionMatrix('B2B');
            return 'Transition matrices recalculated successfully for B2C and B2B';
        } catch (Exception e) {
            throw new AttributionException('Matrix recalculation failed: ' + e.getMessage());
        }
    }
    
    /**
     * Trigger transition matrix recalculation for specific customer type
     * 
     * @param customerType The customer type (B2B or B2C)
     * @return Success message
     */
    public static String triggerMatrixRecalculation(String customerType) {
        try {
            buildAndStoreTransitionMatrix(customerType);
            return 'Transition matrix recalculated successfully for ' + customerType;
        } catch (Exception e) {
            throw new AttributionException('Matrix recalculation failed for ' + customerType + ': ' + e.getMessage());
        }
    }
    
    /**
     * Get transition matrix statistics
     * 
     * @param customerType The customer type (B2B or B2C)
     * @return Map containing matrix statistics
     */
    public static Map<String, Object> getTransitionMatrixStatistics(String customerType) {
        Map<String, Object> statistics = new Map<String, Object>();
        
        try {
            List<Transition_Matrix__c> matrixRecords = [
                SELECT From_State__c, To_State__c, Probability__c, Last_Calculated__c
                FROM Transition_Matrix__c
                WHERE Customer_Type__c = :customerType
                ORDER BY Last_Calculated__c DESC
            ];
            
            if (!matrixRecords.isEmpty()) {
                Set<String> states = new Set<String>();
                Integer transitionCount = 0;
                DateTime lastCalculated = matrixRecords[0].Last_Calculated__c;
                
                for (Transition_Matrix__c record : matrixRecords) {
                    states.add(record.From_State__c);
                    states.add(record.To_State__c);
                    transitionCount++;
                }
                
                statistics.put('customerType', customerType);
                statistics.put('stateCount', states.size());
                statistics.put('transitionCount', transitionCount);
                statistics.put('lastCalculated', lastCalculated);
                statistics.put('matrixExists', true);
            } else {
                statistics.put('customerType', customerType);
                statistics.put('matrixExists', false);
            }
            
        } catch (Exception e) {
            statistics.put('error', e.getMessage());
        }
        
        return statistics;
    }
    
    /**
     * Get network visualization data showing actual transition matrix structure
     * 
     * @param customerType The customer type (B2B or B2C)
     * @return Map containing complete network structure and analysis
     */
    public static Map<String, Object> getNetworkVisualization(String customerType) {
        Map<String, Object> networkData = new Map<String, Object>();
        
        try {
            // Get actual transition matrix from database
            List<Transition_Matrix__c> matrixRecords = [
                SELECT From_State__c, To_State__c, Probability__c, Transition_Count__c,
                       Total_From_State__c, Last_Calculated__c
                FROM Transition_Matrix__c
                WHERE Customer_Type__c = :customerType
                ORDER BY From_State__c, Probability__c DESC
            ];
            
            if (matrixRecords.isEmpty()) {
                networkData.put('error', 'No transition matrix found for ' + customerType);
                return networkData;
            }
            
            // Build network structure
            Map<String, List<Map<String, Object>>> networkNodes = new Map<String, List<Map<String, Object>>>();
            Map<String, Map<String, Object>> nodeMetrics = new Map<String, Map<String, Object>>();
            List<Map<String, Object>> allTransitions = new List<Map<String, Object>>();
            
            Set<String> allStates = new Set<String>();
            Decimal totalTransitions = 0;
            
            // Process each transition
            for (Transition_Matrix__c record : matrixRecords) {
                String fromState = record.From_State__c;
                String toState = record.To_State__c;
                Decimal probability = record.Probability__c;
                Integer transitionCount = Integer.valueOf(record.Transition_Count__c != null ? record.Transition_Count__c : 0);
                
                allStates.add(fromState);
                allStates.add(toState);
                totalTransitions += transitionCount;
                
                // Build transitions list
                if (!networkNodes.containsKey(fromState)) {
                    networkNodes.put(fromState, new List<Map<String, Object>>());
                }
                
                Map<String, Object> transition = new Map<String, Object>();
                transition.put('to', toState);
                transition.put('probability', probability);
                transition.put('count', transitionCount);
                transition.put('percentage', Math.round(probability * 100 * 100) / 100); // Round to 2 decimals
                
                networkNodes.get(fromState).add(transition);
                
                // All transitions for analysis
                Map<String, Object> fullTransition = new Map<String, Object>();
                fullTransition.put('from', fromState);
                fullTransition.put('to', toState);
                fullTransition.put('probability', probability);
                fullTransition.put('count', transitionCount);
                fullTransition.put('weight', probability); // For network analysis
                
                allTransitions.add(fullTransition);
            }
            
            // Calculate node metrics
            for (String state : allStates) {
                Map<String, Object> metrics = new Map<String, Object>();
                
                // Outgoing transitions
                Integer outgoingCount = 0;
                Decimal outgoingTotalProb = 0;
                List<Map<String, Object>> outgoing = networkNodes.get(state);
                if (outgoing != null) {
                    outgoingCount = outgoing.size();
                    for (Map<String, Object> trans : outgoing) {
                        outgoingTotalProb += (Decimal) trans.get('probability');
                    }
                }
                
                // Incoming transitions
                Integer incomingCount = 0;
                Decimal incomingTotalProb = 0;
                for (Map<String, Object> trans : allTransitions) {
                    if (trans.get('to') == state) {
                        incomingCount++;
                        incomingTotalProb += (Decimal) trans.get('probability');
                    }
                }
                
                metrics.put('outgoingTransitions', outgoingCount);
                metrics.put('incomingTransitions', incomingCount);
                metrics.put('outgoingProbability', Math.round(outgoingTotalProb * 100 * 100) / 100);
                metrics.put('incomingProbability', Math.round(incomingTotalProb * 100 * 100) / 100);
                metrics.put('isHub', (outgoingCount >= 3 || incomingCount >= 3));
                
                // Classify node type
                if (state == 'START') {
                    metrics.put('nodeType', 'Entry');
                } else if (state == 'CONVERSION') {
                    metrics.put('nodeType', 'Success');
                } else if (state == 'NULL') {
                    metrics.put('nodeType', 'Exit');
                } else if (state.contains('_Awareness')) {
                    metrics.put('nodeType', 'Awareness');
                } else if (state.contains('_Interest')) {
                    metrics.put('nodeType', 'Interest');
                } else if (state.contains('_Consideration')) {
                    metrics.put('nodeType', 'Consideration');
                } else if (state.contains('_Conversion')) {
                    metrics.put('nodeType', 'Conversion');
                } else if (state.contains('_Retention')) {
                    metrics.put('nodeType', 'Retention');
                } else {
                    metrics.put('nodeType', 'Unknown');
                }
                
                nodeMetrics.put(state, metrics);
            }
            
            // Find most common paths
            List<Map<String, Object>> commonPaths = findMostCommonPaths(allTransitions, 5);
            
            // Find conversion paths
            List<Map<String, Object>> conversionPaths = findConversionPaths(allTransitions, 10);
            
            // Calculate network statistics
            Map<String, Object> networkStats = calculateNetworkStatistics(allTransitions, allStates);
            
            // Build stage analysis
            Map<String, Object> stageAnalysis = analyzeStageTransitions(allTransitions);
            
            // Compile final network data
            networkData.put('customerType', customerType);
            networkData.put('totalStates', allStates.size());
            networkData.put('totalTransitions', allTransitions.size());
            networkData.put('lastCalculated', matrixRecords[0].Last_Calculated__c);
            
            networkData.put('networkNodes', networkNodes);
            networkData.put('nodeMetrics', nodeMetrics);
            networkData.put('allTransitions', allTransitions);
            networkData.put('commonPaths', commonPaths);
            networkData.put('conversionPaths', conversionPaths);
            networkData.put('networkStatistics', networkStats);
            networkData.put('stageAnalysis', stageAnalysis);
            
            // Top transitions for quick overview
            List<Map<String, Object>> topTransitions = new List<Map<String, Object>>();
            for (Integer i = 0; i < Math.min(20, allTransitions.size()); i++) {
                topTransitions.add(allTransitions[i]);
            }
            networkData.put('topTransitions', topTransitions);
            
        } catch (Exception e) {
            networkData.put('error', 'Error building network visualization: ' + e.getMessage());
            System.debug('Error in getNetworkVisualization: ' + e.getMessage());
        }
        
        return networkData;
    }
    
    /**
     * Find most common transition paths in the network
     * 
     * @param allTransitions List of all transitions
     * @param maxPaths Maximum number of paths to return
     * @return List of most common paths
     */
    private static List<Map<String, Object>> findMostCommonPaths(
        List<Map<String, Object>> allTransitions, 
        Integer maxPaths
    ) {
        List<Map<String, Object>> commonPaths = new List<Map<String, Object>>();
        
        // Build path frequency map
        Map<String, Integer> pathFrequency = new Map<String, Integer>();
        Map<String, Decimal> pathProbability = new Map<String, Decimal>();
        
        // Find 2-step paths
        for (Map<String, Object> trans1 : allTransitions) {
            String fromState1 = (String) trans1.get('from');
            String toState1 = (String) trans1.get('to');
            Decimal prob1 = (Decimal) trans1.get('probability');
            
            for (Map<String, Object> trans2 : allTransitions) {
                String fromState2 = (String) trans2.get('from');
                String toState2 = (String) trans2.get('to');
                Decimal prob2 = (Decimal) trans2.get('probability');
                
                if (toState1 == fromState2) {
                    String pathKey = fromState1 + ' → ' + toState1 + ' → ' + toState2;
                    Decimal pathProb = prob1 * prob2;
                    
                    if (!pathFrequency.containsKey(pathKey)) {
                        pathFrequency.put(pathKey, 0);
                        pathProbability.put(pathKey, 0);
                    }
                    
                    pathFrequency.put(pathKey, pathFrequency.get(pathKey) + 1);
                    pathProbability.put(pathKey, pathProbability.get(pathKey) + pathProb);
                }
            }
        }
        
        // Sort by probability and create result
        List<String> sortedPaths = new List<String>(pathProbability.keySet());
        // Simple bubble sort for small datasets
        for (Integer i = 0; i < sortedPaths.size() - 1; i++) {
            for (Integer j = 0; j < sortedPaths.size() - i - 1; j++) {
                if (pathProbability.get(sortedPaths[j]) < pathProbability.get(sortedPaths[j + 1])) {
                    String temp = sortedPaths[j];
                    sortedPaths[j] = sortedPaths[j + 1];
                    sortedPaths[j + 1] = temp;
                }
            }
        }
        
        // Build result
        for (Integer i = 0; i < Math.min(maxPaths, sortedPaths.size()); i++) {
            String path = sortedPaths[i];
            Map<String, Object> pathData = new Map<String, Object>();
            pathData.put('path', path);
            pathData.put('probability', Math.round(pathProbability.get(path) * 100 * 100) / 100);
            pathData.put('frequency', pathFrequency.get(path));
            commonPaths.add(pathData);
        }
        
        return commonPaths;
    }
    
    /**
     * Find paths that lead to conversion
     * 
     * @param allTransitions List of all transitions
     * @param maxPaths Maximum number of paths to return
     * @return List of conversion paths
     */
    private static List<Map<String, Object>> findConversionPaths(
        List<Map<String, Object>> allTransitions, 
        Integer maxPaths
    ) {
        List<Map<String, Object>> conversionPaths = new List<Map<String, Object>>();
        
        // Find all transitions that lead to CONVERSION
        for (Map<String, Object> trans : allTransitions) {
            String toState = (String) trans.get('to');
            if (toState == 'CONVERSION') {
                String fromState = (String) trans.get('from');
                Decimal probability = (Decimal) trans.get('probability');
                
                Map<String, Object> pathData = new Map<String, Object>();
                pathData.put('path', fromState + ' → CONVERSION');
                pathData.put('probability', Math.round(probability * 100 * 100) / 100);
                pathData.put('fromState', fromState);
                pathData.put('channel', fromState.contains('_') ? fromState.split('_')[0] : fromState);
                pathData.put('stage', fromState.contains('_') ? fromState.split('_')[1] : 'Unknown');
                
                conversionPaths.add(pathData);
            }
        }
        
        // Sort by probability
        for (Integer i = 0; i < conversionPaths.size() - 1; i++) {
            for (Integer j = 0; j < conversionPaths.size() - i - 1; j++) {
                Decimal prob1 = (Decimal) conversionPaths[j].get('probability');
                Decimal prob2 = (Decimal) conversionPaths[j + 1].get('probability');
                if (prob1 < prob2) {
                    Map<String, Object> temp = conversionPaths[j];
                    conversionPaths[j] = conversionPaths[j + 1];
                    conversionPaths[j + 1] = temp;
                }
            }
        }
        
        // Return top paths
        List<Map<String, Object>> result = new List<Map<String, Object>>();
        for (Integer i = 0; i < Math.min(maxPaths, conversionPaths.size()); i++) {
            result.add(conversionPaths[i]);
        }
        
        return result;
    }
    
    /**
     * Calculate network statistics
     * 
     * @param allTransitions List of all transitions
     * @param allStates Set of all states
     * @return Map containing network statistics
     */
    private static Map<String, Object> calculateNetworkStatistics(
        List<Map<String, Object>> allTransitions, 
        Set<String> allStates
    ) {
        Map<String, Object> stats = new Map<String, Object>();
        
        // Basic network metrics
        stats.put('totalNodes', allStates.size());
        stats.put('totalEdges', allTransitions.size());
        stats.put('networkDensity', Math.round(((Decimal) allTransitions.size() / (allStates.size() * (allStates.size() - 1))) * 100 * 100) / 100);
        
        // Probability distribution
        Decimal totalProbability = 0;
        Decimal maxProbability = 0;
        Decimal minProbability = 1;
        
        for (Map<String, Object> trans : allTransitions) {
            Decimal prob = (Decimal) trans.get('probability');
            totalProbability += prob;
            maxProbability = Math.max(maxProbability, prob);
            minProbability = Math.min(minProbability, prob);
        }
        
        stats.put('averageProbability', Math.round((totalProbability / allTransitions.size()) * 100 * 100) / 100);
        stats.put('maxProbability', Math.round(maxProbability * 100 * 100) / 100);
        stats.put('minProbability', Math.round(minProbability * 100 * 100) / 100);
        
        // Node degree distribution
        Map<String, Integer> nodeDegrees = new Map<String, Integer>();
        for (String state : allStates) {
            nodeDegrees.put(state, 0);
        }
        
        for (Map<String, Object> trans : allTransitions) {
            String fromState = (String) trans.get('from');
            nodeDegrees.put(fromState, nodeDegrees.get(fromState) + 1);
        }
        
        Integer maxDegree = 0;
        Integer totalDegree = 0;
        for (Integer degree : nodeDegrees.values()) {
            maxDegree = Math.max(maxDegree, degree);
            totalDegree += degree;
        }
        
        stats.put('maxNodeDegree', maxDegree);
        stats.put('averageNodeDegree', Math.round(((Decimal) totalDegree / allStates.size()) * 100) / 100);
        
        // Conversion analysis
        Integer conversionTransitions = 0;
        Integer nullTransitions = 0;
        
        for (Map<String, Object> trans : allTransitions) {
            String toState = (String) trans.get('to');
            if (toState == 'CONVERSION') {
                conversionTransitions++;
            } else if (toState == 'NULL') {
                nullTransitions++;
            }
        }
        
        stats.put('conversionTransitions', conversionTransitions);
        stats.put('nullTransitions', nullTransitions);
        stats.put('conversionRate', Math.round(((Decimal) conversionTransitions / (conversionTransitions + nullTransitions)) * 100 * 100) / 100);
        
        return stats;
    }
    
    /**
     * Analyze stage transitions in the network
     * 
     * @param allTransitions List of all transitions
     * @return Map containing stage analysis
     */
    private static Map<String, Object> analyzeStageTransitions(List<Map<String, Object>> allTransitions) {
        Map<String, Object> stageAnalysis = new Map<String, Object>();
        
        // Stage transition counters
        Map<String, Integer> stageTransitionCounts = new Map<String, Integer>();
        Map<String, Decimal> stageTransitionProbs = new Map<String, Decimal>();
        
        // Channel performance by stage
        Map<String, Map<String, Decimal>> channelByStage = new Map<String, Map<String, Decimal>>();
        
        for (Map<String, Object> trans : allTransitions) {
            String fromState = (String) trans.get('from');
            String toState = (String) trans.get('to');
            Decimal probability = (Decimal) trans.get('probability');
            
            // Skip special states
            if (fromState == 'START' || toState == 'CONVERSION' || toState == 'NULL') {
                continue;
            }
            
            // Extract stages
            String fromStage = fromState.contains('_') ? fromState.split('_')[1] : 'Unknown';
            String toStage = toState.contains('_') ? toState.split('_')[1] : 'Unknown';
            
            // Count stage transitions
            String stageTransition = fromStage + ' → ' + toStage;
            if (!stageTransitionCounts.containsKey(stageTransition)) {
                stageTransitionCounts.put(stageTransition, 0);
                stageTransitionProbs.put(stageTransition, 0);
            }
            stageTransitionCounts.put(stageTransition, stageTransitionCounts.get(stageTransition) + 1);
            stageTransitionProbs.put(stageTransition, stageTransitionProbs.get(stageTransition) + probability);
            
            // Channel performance
            String channel = fromState.contains('_') ? fromState.split('_')[0] : fromState;
            if (!channelByStage.containsKey(channel)) {
                channelByStage.put(channel, new Map<String, Decimal>());
            }
            if (!channelByStage.get(channel).containsKey(fromStage)) {
                channelByStage.get(channel).put(fromStage, 0);
            }
            channelByStage.get(channel).put(fromStage, channelByStage.get(channel).get(fromStage) + probability);
        }
        
        // Build stage transition summary
        List<Map<String, Object>> stageTransitionSummary = new List<Map<String, Object>>();
        for (String stageTransition : stageTransitionCounts.keySet()) {
            Map<String, Object> summary = new Map<String, Object>();
            summary.put('transition', stageTransition);
            summary.put('count', stageTransitionCounts.get(stageTransition));
            summary.put('totalProbability', Math.round(stageTransitionProbs.get(stageTransition) * 100 * 100) / 100);
            stageTransitionSummary.add(summary);
        }
        
        stageAnalysis.put('stageTransitions', stageTransitionSummary);
        stageAnalysis.put('channelStagePerformance', channelByStage);
        
        return stageAnalysis;
    }
    
    /**
     * Get stage detection summary for debugging
     * 
     * @param customerType The customer type (B2B or B2C)
     * @return Map containing stage detection information
     */
    public static Map<String, Object> getStageDetectionSummary(String customerType) {
        Map<String, Object> summary = new Map<String, Object>();
        
        try {
            // Get sample journeys for analysis
            List<Customer_Journey__c> journeys = [
                SELECT Id, Customer_Type__c, Converted__c, Total_Touchpoints__c
                FROM Customer_Journey__c 
                WHERE Customer_Type__c = :customerType
                LIMIT 100
            ];
            
            List<Id> journeyIds = new List<Id>();
            for (Customer_Journey__c journey : journeys) {
                journeyIds.add(journey.Id);
            }
            
            List<Transition_Matrix__c> matrixRecords = [
                SELECT From_State__c, To_State__c, Probability__c
                FROM Transition_Matrix__c
                WHERE Customer_Type__c = :customerType
                AND From_State__c != 'START'
                AND To_State__c != 'NULL'
                AND To_State__c != 'CONVERSION'
                ORDER BY Probability__c DESC
                LIMIT 50
            ];
            
            Map<String, List<String>> stageTransitions = new Map<String, List<String>>();
            
            for (Transition_Matrix__c record : matrixRecords) {
                String fromState = record.From_State__c;
                String toState = record.To_State__c;
                
                if (!stageTransitions.containsKey(fromState)) {
                    stageTransitions.put(fromState, new List<String>());
                }
                stageTransitions.get(fromState).add(toState + ' (' + record.Probability__c + ')');
            }
            
            summary.put('customerType', customerType);
            summary.put('totalJourneys', journeys.size());
            summary.put('topTransitions', stageTransitions);
            summary.put('matrixRecords', matrixRecords.size());
            
        } catch (Exception e) {
            summary.put('error', e.getMessage());
        }
        
        return summary;
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