/**
 * TouchpointGenerationBatch
 * 
 * Generates realistic touchpoint sequences for existing Customer_Journey__c records
 * based on Phase 0 business logic, Dutch market patterns, and synergistic effects.
 * 
 * This batch class:
 * - Processes existing journeys that have no touchpoints
 * - Generates touchpoint sequences based on Total_Touchpoints__c
 * - Applies B2B vs B2C journey patterns
 * - Implements synergistic channel effects
 * - Respects Dutch market seasonality
 * 
 * Author: Alexandru Constantinescu
 * Project: Omnichannel Attribution Platform
 * Date: 2025
 */
public class TouchpointGenerationBatch implements Database.Batchable<sObject> {
    
    private static final Integer BATCH_SIZE = 50;
    
    private static final List<String> B2C_CHANNELS = new List<String>{
        'Google_Ads', 'Facebook_Ads', 'Email_Marketing', 'Events',
        'Content_Website_SEO', 'App_Store', 'Organic_Social'
    };
    
    private static final List<String> B2B_CHANNELS = new List<String>{
        'LinkedIn_Ads', 'Email_Marketing', 'Events', 'Content_Website_SEO', 'Google_Ads'
    };
    
    private static final Map<String, List<String>> SYNERGISTIC_PATTERNS_B2C = new Map<String, List<String>>{
        'Events' => new List<String>{'Organic_Social', 'Content_Website_SEO', 'Google_Ads'},
        'Google_Ads' => new List<String>{'Content_Website_SEO'},
        'Facebook_Ads' => new List<String>{'Content_Website_SEO', 'App_Store'},
        'Email_Marketing' => new List<String>{'Content_Website_SEO', 'App_Store'},
        'Organic_Social' => new List<String>{'App_Store'}
    };
    
    private static final Map<String, List<String>> SYNERGISTIC_PATTERNS_B2B = new Map<String, List<String>>{
        'LinkedIn_Ads' => new List<String>{'Email_Marketing', 'Content_Website_SEO'},
        'Events' => new List<String>{'Email_Marketing', 'Content_Website_SEO'},
        'Google_Ads' => new List<String>{'Content_Website_SEO'},
        'Email_Marketing' => new List<String>{'Content_Website_SEO'}
    };
    
    public TouchpointGenerationBatch() {
    }
    
    public Database.QueryLocator start(Database.BatchableContext bc) {
        return Database.getQueryLocator([
            SELECT Id, Customer_ID__c, Customer_Type__c, Total_Touchpoints__c,
                   Converted__c, Conversion_Value__c, Journey_Start_Date__c, 
                   Journey_End_Date__c
            FROM Customer_Journey__c
            WHERE Total_Touchpoints__c > 0
            ORDER BY CreatedDate
        ]);
    }
    
    public void execute(Database.BatchableContext bc, List<Customer_Journey__c> scope) {
        List<Touchpoint__c> touchpointsToInsert = new List<Touchpoint__c>();
        
        Set<Id> journeyIds = new Set<Id>();
        for (Customer_Journey__c journey : scope) {
            journeyIds.add(journey.Id);
        }
        
        Map<Id, List<Touchpoint__c>> existingTouchpoints = getExistingTouchpoints(journeyIds);
        
        for (Customer_Journey__c journey : scope) {
            if (existingTouchpoints.containsKey(journey.Id) && 
                !existingTouchpoints.get(journey.Id).isEmpty()) {
                continue;
            }
            
            List<Touchpoint__c> journeyTouchpoints = generateTouchpointsForJourney(journey);
            touchpointsToInsert.addAll(journeyTouchpoints);
        }
        
        if (!touchpointsToInsert.isEmpty()) {
            try {
                Database.SaveResult[] results = Database.insert(touchpointsToInsert, false);
                
                Integer successCount = 0;
                Integer errorCount = 0;
                
                for (Integer i = 0; i < results.size(); i++) {
                    if (results[i].isSuccess()) {
                        successCount++;
                    } else {
                        errorCount++;
                        for (Database.Error error : results[i].getErrors()) {
                            System.debug('Error inserting touchpoint ' + i + ': ' + error.getMessage());
                            System.debug('Fields: ' + error.getFields());
                        }
                    }
                }
                
                System.debug('Successfully inserted ' + successCount + ' touchpoints');
                if (errorCount > 0) {
                    System.debug('Failed to insert ' + errorCount + ' touchpoints');
                }
            } catch (Exception e) {
                System.debug('Error inserting touchpoints: ' + e.getMessage());
                System.debug('Stack trace: ' + e.getStackTraceString());
            }
        } else {
            System.debug('No touchpoints to insert for this batch');
        }
    }
    
    public void finish(Database.BatchableContext bc) {
        System.debug('TouchpointGenerationBatch completed');
        
        AsyncApexJob job = [
            SELECT Id, Status, NumberOfErrors, JobItemsProcessed, TotalJobItems
            FROM AsyncApexJob
            WHERE Id = :bc.getJobId()
        ];
        
        System.debug('Batch Job Status: ' + job.Status);
        System.debug('Items Processed: ' + job.JobItemsProcessed + '/' + job.TotalJobItems);
        System.debug('Errors: ' + job.NumberOfErrors);
        
        Integer totalTouchpoints = [SELECT COUNT() FROM Touchpoint__c];
        System.debug('Total touchpoints in system: ' + totalTouchpoints);
    }
    
    private Map<Id, List<Touchpoint__c>> getExistingTouchpoints(Set<Id> journeyIds) {
        Map<Id, List<Touchpoint__c>> touchpointsByJourney = new Map<Id, List<Touchpoint__c>>();
        
        List<Touchpoint__c> existingTouchpoints = [
            SELECT Id, Customer_Journey__c
            FROM Touchpoint__c
            WHERE Customer_Journey__c IN :journeyIds
        ];
        
        for (Touchpoint__c tp : existingTouchpoints) {
            if (!touchpointsByJourney.containsKey(tp.Customer_Journey__c)) {
                touchpointsByJourney.put(tp.Customer_Journey__c, new List<Touchpoint__c>());
            }
            touchpointsByJourney.get(tp.Customer_Journey__c).add(tp);
        }
        
        return touchpointsByJourney;
    }
    
    private List<Touchpoint__c> generateTouchpointsForJourney(Customer_Journey__c journey) {
        List<Touchpoint__c> touchpoints = new List<Touchpoint__c>();
        
        Integer numTouchpoints = Integer.valueOf(journey.Total_Touchpoints__c);
        Boolean isB2B = journey.Customer_Type__c == 'B2B';
        
        List<String> channelSequence = generateChannelSequence(numTouchpoints, isB2B, journey.Converted__c);
        
        DateTime currentTimestamp = journey.Journey_Start_Date__c;
        Long journeyDurationSeconds = (journey.Journey_End_Date__c.getTime() - journey.Journey_Start_Date__c.getTime()) / 1000;
        Long intervalSeconds = journeyDurationSeconds / numTouchpoints;
        
        for (Integer i = 0; i < channelSequence.size(); i++) {
            String channel = channelSequence[i];
            
            Touchpoint__c touchpoint = new Touchpoint__c();
            touchpoint.Customer_Journey__c = journey.Id;
            touchpoint.Channel__c = channel;
            touchpoint.Touchpoint_Order__c = i + 1;
            touchpoint.Touchpoint_Timestamp__c = currentTimestamp;
            touchpoint.Touchpoint_ID__c = generateTouchpointId(journey.Id, i + 1);
            
            touchpoints.add(touchpoint);
            
            currentTimestamp = currentTimestamp.addSeconds(Integer.valueOf(intervalSeconds));
        }
        
        return touchpoints;
    }
    
    private String generateTouchpointId(Id journeyId, Integer touchpointOrder) {
        String journeyIdStr = String.valueOf(journeyId);
        String orderStr = String.valueOf(touchpointOrder).leftPad(3, '0');
        return journeyIdStr + '_TP' + orderStr;
    }
    
    private List<String> generateChannelSequence(Integer numTouchpoints, Boolean isB2B, Boolean converted) {
        List<String> sequence = new List<String>();
        List<String> availableChannels = isB2B ? B2B_CHANNELS : B2C_CHANNELS;
        Map<String, List<String>> synergisticPatterns = isB2B ? SYNERGISTIC_PATTERNS_B2B : SYNERGISTIC_PATTERNS_B2C;
        
        String firstChannel = selectRandomChannel(availableChannels);
        sequence.add(firstChannel);
        
        for (Integer i = 1; i < numTouchpoints; i++) {
            String previousChannel = sequence[i - 1];
            String nextChannel;
            
            if (synergisticPatterns.containsKey(previousChannel) && Math.random() < 0.6) {
                List<String> synergisticChannels = synergisticPatterns.get(previousChannel);
                nextChannel = selectRandomChannel(synergisticChannels);
            } else {
                nextChannel = selectRandomChannel(availableChannels);
            }
            
            sequence.add(nextChannel);
        }
        
        if (converted && !isB2B && Math.random() < 0.7) {
            sequence[sequence.size() - 1] = 'App_Store';
        }
        
        return sequence;
    }
    
    private String selectRandomChannel(List<String> channels) {
        Integer randomIndex = Integer.valueOf(Math.random() * channels.size());
        if (randomIndex >= channels.size()) {
            randomIndex = channels.size() - 1;
        }
        return channels[randomIndex];
    }
    
    public static Id executeBatch() {
        return executeBatch(BATCH_SIZE);
    }
    
    public static Id executeBatch(Integer batchSize) {
        TouchpointGenerationBatch batch = new TouchpointGenerationBatch();
        return Database.executeBatch(batch, batchSize);
    }
}