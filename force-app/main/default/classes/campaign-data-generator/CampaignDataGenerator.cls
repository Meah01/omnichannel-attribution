/**
 * CampaignDataGenerator
 * 
 * Generates synthetic Campaign__c records based on Dutch market patterns from Phase 0.
 * Creates campaigns following the annual campaign calendar framework with proper
 * seasonality, channel distribution, and B2B/B2C segmentation.
 * 
 * This is a one-time data generation class to create campaign structure for
 * touchpoint attribution. Campaigns are created based on:
 * - Dutch Market Annual Campaign Calendar Framework
 * - 8 marketing channels
 * - B2B and B2C customer types
 * - Seasonal patterns (Q1-Q4 variations)
 * 
 * Author: Alexandru Constantinescu
 * Project: Omnichannel Attribution Platform
 * Date: 2025
 */
public class CampaignDataGenerator {
    
    private static final Integer CAMPAIGNS_PER_CHANNEL = 4;
    
    private static final List<String> CHANNELS = new List<String>{
        'Google_Ads',
        'Facebook_Ads',
        'Email_Marketing',
        'LinkedIn_Ads',
        'Events',
        'Content_Website_SEO',
        'App_Store',
        'Organic_Social'
    };
    
    private static final List<String> QUARTERS = new List<String>{'Q1', 'Q2', 'Q3', 'Q4'};
    
    private static final Map<String, String> CHANNEL_FOCUS = new Map<String, String>{
        'Google_Ads' => 'B2C',
        'Facebook_Ads' => 'B2C',
        'Email_Marketing' => 'Both',
        'LinkedIn_Ads' => 'B2B',
        'Events' => 'Both',
        'Content_Website_SEO' => 'Both',
        'App_Store' => 'B2C',
        'Organic_Social' => 'B2C'
    };
    
    public static List<Campaign__c> generateCampaigns() {
        List<Campaign__c> campaigns = new List<Campaign__c>();
        
        for (String channel : CHANNELS) {
            for (String quarter : QUARTERS) {
                Campaign__c campaign = createCampaign(channel, quarter);
                campaigns.add(campaign);
            }
        }
        
        return campaigns;
    }
    
    private static Campaign__c createCampaign(String channel, String quarter) {
        Campaign__c campaign = new Campaign__c();
        
        String channelLabel = channel.replace('_', ' ');
        campaign.Name = channelLabel + ' - ' + quarter + ' 2025';
        campaign.Channel__c = channel;
        campaign.Customer_Type__c = CHANNEL_FOCUS.get(channel);
        campaign.Campaign_Quarter__c = quarter;
        campaign.Status__c = 'Active';
        campaign.Start_Date__c = getQuarterStartDate(quarter);
        campaign.End_Date__c = getQuarterEndDate(quarter);
        campaign.Campaign_ID_External__c = generateExternalCampaignId(channel, quarter);
        
        return campaign;
    }
    
    private static String generateExternalCampaignId(String channel, String quarter) {
        String channelPrefix = channel.substring(0, Math.min(3, channel.length())).toUpperCase();
        String yearQuarter = '2025_' + quarter;
        Long timestamp = System.currentTimeMillis();
        String randomSuffix = String.valueOf(Math.abs(Math.random() * 10000).intValue());
        
        return channelPrefix + '_' + yearQuarter + '_' + randomSuffix;
    }
    
    private static Date getQuarterStartDate(String quarter) {
        if (quarter == 'Q1') return Date.newInstance(2025, 1, 1);
        if (quarter == 'Q2') return Date.newInstance(2025, 4, 1);
        if (quarter == 'Q3') return Date.newInstance(2025, 7, 1);
        return Date.newInstance(2025, 10, 1);
    }
    
    private static Date getQuarterEndDate(String quarter) {
        if (quarter == 'Q1') return Date.newInstance(2025, 3, 31);
        if (quarter == 'Q2') return Date.newInstance(2025, 6, 30);
        if (quarter == 'Q3') return Date.newInstance(2025, 9, 30);
        return Date.newInstance(2025, 12, 31);
    }
    
    public static void insertCampaigns() {
        List<Campaign__c> existingCampaigns = [SELECT Id FROM Campaign__c LIMIT 1];
        
        if (!existingCampaigns.isEmpty()) {
            System.debug('Campaigns already exist. Skipping generation.');
            return;
        }
        
        List<Campaign__c> campaigns = generateCampaigns();
        insert campaigns;
        
        System.debug('Successfully created ' + campaigns.size() + ' campaigns');
    }
}