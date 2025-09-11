/**
 * @description Clean REST API endpoint for importing customer journey data
 * @author Omnichannel Attribution Platform
 * @version 3.0 - Simplified for actual deployment
 * 
 * Endpoint: /services/apexrest/attribution/import/journeys
 */

@RestResource(urlMapping='/attribution/import/*')
global class JourneyDataImportService {
    
    // Simple response wrapper
    global class ImportResponse {
        public Boolean success;
        public String message;
        public Integer totalProcessed;
        public Integer successCount;
        public Integer errorCount;
        public List<String> errors;
        
        public ImportResponse() {
            this.success = false;
            this.totalProcessed = 0;
            this.successCount = 0;
            this.errorCount = 0;
            this.errors = new List<String>();
        }
    }
    
    // Simple data structures matching your actual fields
    global class JourneyData {
        public String journeyId;
        public String customerId;
        public String customerType;
        public String startDate;
        public String endDate;
        public Boolean converted;
        public Decimal conversionValue;
        public Integer totalTouchpoints;
        public Decimal confidenceScore;
        public String confidenceLevel;
    }
    
    global class TouchpointData {
        public String touchpointId;
        public String journeyId;
        public String channel;
        public String timestamp;
        public String campaign;
    }
    
    global class ImportData {
        public List<JourneyData> journeys;
        public List<TouchpointData> touchpoints;
    }
    
    /**
     * @description Import journey data with upsert logic
     */
    @HttpPost
    global static ImportResponse importJourneyData() {
        ImportResponse response = new ImportResponse();
        
        try {
            RestRequest request = RestContext.request;
            String requestBody = request.requestBody.toString();
            
            if (String.isBlank(requestBody)) {
                response.message = 'Request body is empty';
                return response;
            }
            
            // Parse the JSON data
            ImportData data = parseImportData(requestBody);
            
            if (data.journeys == null || data.journeys.isEmpty()) {
                response.message = 'No journey data provided';
                return response;
            }
            
            // Process the data
            response = processImportData(data);
            
        } catch (Exception e) {
            response.message = 'Import error: ' + e.getMessage();
            response.errors.add('System error: ' + e.getMessage());
            System.debug(LoggingLevel.ERROR, 'Import error: ' + e.getMessage());
        }
        
        return response;
    }
    
    /**
     * @description Parse JSON data from different possible formats
     */
    private static ImportData parseImportData(String jsonData) {
        ImportData result = new ImportData();
        result.journeys = new List<JourneyData>();
        result.touchpoints = new List<TouchpointData>();
        
        try {
            // Try to parse as generic JSON object
            Map<String, Object> jsonMap = (Map<String, Object>) JSON.deserializeUntyped(jsonData);
            
            // Handle salesforce_ready_data format (from GitHub Actions)
            if (jsonMap.containsKey('salesforce_ready_data')) {
                List<Object> readyData = (List<Object>) jsonMap.get('salesforce_ready_data');
                
                for (Object journeyObj : readyData) {
                    Map<String, Object> journeyWrapper = (Map<String, Object>) journeyObj;
                    
                    // Extract journey data
                    if (journeyWrapper.containsKey('Customer_Journey__c')) {
                        Map<String, Object> journeyMap = (Map<String, Object>) journeyWrapper.get('Customer_Journey__c');
                        JourneyData journey = extractJourneyFromMap(journeyMap);
                        if (journey != null) {
                            result.journeys.add(journey);
                        }
                    }
                    
                    // Extract touchpoints
                    if (journeyWrapper.containsKey('Touchpoints')) {
                        List<Object> touchpointsList = (List<Object>) journeyWrapper.get('Touchpoints');
                        for (Object tpObj : touchpointsList) {
                            Map<String, Object> tpMap = (Map<String, Object>) tpObj;
                            TouchpointData tp = extractTouchpointFromMap(tpMap);
                            if (tp != null) {
                                result.touchpoints.add(tp);
                            }
                        }
                    }
                }
            }
            // Handle simple journeys array format
            else if (jsonMap.containsKey('journeys')) {
                List<Object> journeysList = (List<Object>) jsonMap.get('journeys');
                for (Object journeyObj : journeysList) {
                    Map<String, Object> journeyMap = (Map<String, Object>) journeyObj;
                    JourneyData journey = extractJourneyFromMap(journeyMap);
                    if (journey != null) {
                        result.journeys.add(journey);
                    }
                }
                
                if (jsonMap.containsKey('touchpoints')) {
                    List<Object> touchpointsList = (List<Object>) jsonMap.get('touchpoints');
                    for (Object tpObj : touchpointsList) {
                        Map<String, Object> tpMap = (Map<String, Object>) tpObj;
                        TouchpointData tp = extractTouchpointFromMap(tpMap);
                        if (tp != null) {
                            result.touchpoints.add(tp);
                        }
                    }
                }
            }
            
        } catch (Exception e) {
            System.debug(LoggingLevel.ERROR, 'Parse error: ' + e.getMessage());
        }
        
        return result;
    }
    
    /**
     * @description Extract journey data from map with flexible field names
     */
    private static JourneyData extractJourneyFromMap(Map<String, Object> journeyMap) {
        try {
            JourneyData journey = new JourneyData();
            
            // Extract journey ID from Name field or direct field
            String nameField = (String) journeyMap.get('Name');
            if (String.isNotBlank(nameField) && nameField.startsWith('Journey ')) {
                journey.journeyId = nameField.substring(8); // Remove "Journey " prefix
            } else {
                journey.journeyId = (String) getFieldValue(journeyMap, new List<String>{'journeyId', 'journey_id', 'Journey_ID__c'});
            }
            
            // Extract other fields with flexible naming
            journey.customerId = (String) getFieldValue(journeyMap, new List<String>{'customerId', 'customer_id', 'Customer_ID__c'});
            journey.customerType = (String) getFieldValue(journeyMap, new List<String>{'customerType', 'customer_type', 'Customer_Type__c'});
            journey.startDate = (String) getFieldValue(journeyMap, new List<String>{'startDate', 'journey_start', 'Journey_Start_Date__c'});
            journey.endDate = (String) getFieldValue(journeyMap, new List<String>{'endDate', 'journey_end', 'Journey_End_Date__c'});
            journey.converted = (Boolean) getFieldValue(journeyMap, new List<String>{'converted', 'Converted__c'});
            journey.conversionValue = (Decimal) getFieldValue(journeyMap, new List<String>{'conversionValue', 'conversion_value', 'Conversion_Value__c'});
            
            Object touchpointsObj = getFieldValue(journeyMap, new List<String>{'totalTouchpoints', 'total_touchpoints', 'Total_Touchpoints__c'});
            if (touchpointsObj != null) {
                journey.totalTouchpoints = Integer.valueOf(touchpointsObj);
            }
            
            journey.confidenceScore = (Decimal) getFieldValue(journeyMap, new List<String>{'confidenceScore', 'confidence_score', 'Confidence_Score__c'});
            journey.confidenceLevel = (String) getFieldValue(journeyMap, new List<String>{'confidenceLevel', 'confidence_level', 'Confidence_Level__c'});
            
            return journey;
            
        } catch (Exception e) {
            System.debug(LoggingLevel.ERROR, 'Journey extraction error: ' + e.getMessage());
            return null;
        }
    }
    
    /**
     * @description Extract touchpoint data from map with flexible field names
     */
    private static TouchpointData extractTouchpointFromMap(Map<String, Object> tpMap) {
        try {
            TouchpointData tp = new TouchpointData();
            
            // Extract touchpoint ID from Name field or direct field
            String nameField = (String) tpMap.get('Name');
            if (String.isNotBlank(nameField) && nameField.startsWith('Touchpoint ')) {
                tp.touchpointId = nameField.substring(11); // Remove "Touchpoint " prefix
            } else {
                tp.touchpointId = (String) getFieldValue(tpMap, new List<String>{'touchpointId', 'touchpoint_id', 'Touchpoint_ID__c'});
            }
            
            tp.journeyId = (String) getFieldValue(tpMap, new List<String>{'journeyId', 'journey_id', 'Journey_ID__c'});
            tp.channel = (String) getFieldValue(tpMap, new List<String>{'channel', 'Channel__c'});
            tp.timestamp = (String) getFieldValue(tpMap, new List<String>{'timestamp', 'touchpoint_timestamp', 'Touchpoint_Timestamp__c'});
            tp.campaign = (String) getFieldValue(tpMap, new List<String>{'campaign', 'campaign_id', 'Campaign__c'});
            
            return tp;
            
        } catch (Exception e) {
            System.debug(LoggingLevel.ERROR, 'Touchpoint extraction error: ' + e.getMessage());
            return null;
        }
    }
    
    /**
     * @description Get field value using multiple possible field names
     */
    private static Object getFieldValue(Map<String, Object> dataMap, List<String> possibleNames) {
        for (String fieldName : possibleNames) {
            if (dataMap.containsKey(fieldName)) {
                return dataMap.get(fieldName);
            }
        }
        return null;
    }
    
    /**
     * @description Process the parsed data and save to Salesforce
     */
    private static ImportResponse processImportData(ImportData data) {
        ImportResponse response = new ImportResponse();
        response.totalProcessed = data.journeys.size();
        
        try {
            // Convert to Salesforce objects
            List<Customer_Journey__c> journeysToUpsert = new List<Customer_Journey__c>();
            
            for (JourneyData journeyData : data.journeys) {
                Customer_Journey__c journey = convertToSalesforceJourney(journeyData);
                if (journey != null) {
                    journeysToUpsert.add(journey);
                }
            }
            
            // Upsert journeys
            if (!journeysToUpsert.isEmpty()) {
                List<Database.UpsertResult> results = Database.upsert(journeysToUpsert, Customer_Journey__c.Journey_ID__c, false);
                
                // Process results
                Map<String, Id> journeyIdMap = new Map<String, Id>();
                for (Integer i = 0; i < results.size(); i++) {
                    Database.UpsertResult result = results[i];
                    if (result.isSuccess()) {
                        response.successCount++;
                        journeyIdMap.put(journeysToUpsert[i].Journey_ID__c, result.getId());
                    } else {
                        response.errorCount++;
                        String errorMsg = 'Journey ' + journeysToUpsert[i].Journey_ID__c + ': ';
                        for (Database.Error error : result.getErrors()) {
                            errorMsg += error.getMessage() + '; ';
                        }
                        response.errors.add(errorMsg);
                    }
                }
                
                // Process touchpoints if we have successful journeys
                if (!journeyIdMap.isEmpty() && data.touchpoints != null && !data.touchpoints.isEmpty()) {
                    processTouchpoints(data.touchpoints, journeyIdMap, response);
                }
            }
            
            response.success = (response.errorCount == 0);
            response.message = response.success ? 
                'Successfully imported ' + response.successCount + ' journeys' :
                'Completed with ' + response.errorCount + ' errors out of ' + response.totalProcessed + ' journeys';
                
        } catch (Exception e) {
            response.success = false;
            response.message = 'Processing error: ' + e.getMessage();
            response.errors.add('Processing error: ' + e.getMessage());
        }
        
        return response;
    }
    
    /**
     * @description Convert journey data to Salesforce object
     */
    private static Customer_Journey__c convertToSalesforceJourney(JourneyData journeyData) {
        try {
            Customer_Journey__c journey = new Customer_Journey__c();
            
            journey.Journey_ID__c = journeyData.journeyId;
            journey.Customer_ID__c = journeyData.customerId;
            journey.Customer_Type__c = journeyData.customerType;
            journey.Converted__c = journeyData.converted != null ? journeyData.converted : false;
            journey.Conversion_Value__c = journeyData.conversionValue;
            journey.Total_Touchpoints__c = journeyData.totalTouchpoints;
            journey.Confidence_Score__c = journeyData.confidenceScore;
            journey.Confidence_Level__c = journeyData.confidenceLevel;
            journey.Processing_Status__c = 'Completed';
            
            // Parse dates
            if (String.isNotBlank(journeyData.startDate)) {
                journey.Journey_Start_Date__c = parseDateTime(journeyData.startDate);
            }
            if (String.isNotBlank(journeyData.endDate)) {
                journey.Journey_End_Date__c = parseDateTime(journeyData.endDate);
            }
            
            return journey;
            
        } catch (Exception e) {
            System.debug(LoggingLevel.ERROR, 'Journey conversion error: ' + e.getMessage());
            return null;
        }
    }
    
    /**
     * @description Process touchpoints
     */
    private static void processTouchpoints(List<TouchpointData> touchpoints, Map<String, Id> journeyIdMap, ImportResponse response) {
        List<Touchpoint__c> touchpointsToUpsert = new List<Touchpoint__c>();
        
        for (TouchpointData tpData : touchpoints) {
            Id journeyId = journeyIdMap.get(tpData.journeyId);
            if (journeyId != null) {
                Touchpoint__c tp = convertToSalesforceTouchpoint(tpData, journeyId);
                if (tp != null) {
                    touchpointsToUpsert.add(tp);
                }
            }
        }
        
        if (!touchpointsToUpsert.isEmpty()) {
            List<Database.UpsertResult> tpResults = Database.upsert(touchpointsToUpsert, Touchpoint__c.Touchpoint_ID__c, false);
            
            for (Integer i = 0; i < tpResults.size(); i++) {
                Database.UpsertResult result = tpResults[i];
                if (!result.isSuccess()) {
                    String errorMsg = 'Touchpoint ' + touchpointsToUpsert[i].Touchpoint_ID__c + ': ';
                    for (Database.Error error : result.getErrors()) {
                        errorMsg += error.getMessage() + '; ';
                    }
                    response.errors.add(errorMsg);
                }
            }
        }
    }
    
    /**
     * @description Convert touchpoint data to Salesforce object
     */
    private static Touchpoint__c convertToSalesforceTouchpoint(TouchpointData tpData, Id journeyId) {
        try {
            Touchpoint__c touchpoint = new Touchpoint__c();
            
            touchpoint.Touchpoint_ID__c = tpData.touchpointId;
            touchpoint.Customer_Journey__c = journeyId;
            touchpoint.Channel__c = tpData.channel;
            touchpoint.Campaign__c = tpData.campaign;
            
            if (String.isNotBlank(tpData.timestamp)) {
                touchpoint.Touchpoint_Timestamp__c = parseDateTime(tpData.timestamp);
            }
            
            return touchpoint;
            
        } catch (Exception e) {
            System.debug(LoggingLevel.ERROR, 'Touchpoint conversion error: ' + e.getMessage());
            return null;
        }
    }
    
    /**
     * @description Parse datetime string with flexible formats
     */
    private static DateTime parseDateTime(String dateTimeStr) {
        try {
            if (String.isBlank(dateTimeStr)) {
                return null;
            }
            
            // Handle different formats
            String cleanStr = dateTimeStr.replace('T', ' ').replace('Z', '');
            return DateTime.valueOf(cleanStr);
            
        } catch (Exception e) {
            System.debug(LoggingLevel.ERROR, 'DateTime parse error: ' + e.getMessage() + ' for: ' + dateTimeStr);
            return null;
        }
    }
    
    // Status response wrapper
    global class StatusResponse {
        public Integer total_journeys;
        public Integer total_touchpoints;
        public Integer b2c_journeys;
        public Integer b2b_journeys;
        public Integer converted_journeys;
        public DateTime last_import;
        public String status;
        public String error_message;
        
        public StatusResponse() {
            this.total_journeys = 0;
            this.total_touchpoints = 0;
            this.b2c_journeys = 0;
            this.b2b_journeys = 0;
            this.converted_journeys = 0;
            this.last_import = System.now();
            this.status = 'active';
        }
    }
    
    /**
     * @description Get import status
     */
    @HttpGet
    global static StatusResponse getImportStatus() {
        StatusResponse response = new StatusResponse();
        
        try {
            List<AggregateResult> totalJourneys = [SELECT COUNT(Id) totalCount FROM Customer_Journey__c WHERE CreatedDate = TODAY];
            response.total_journeys = totalJourneys.size() > 0 ? (Integer)totalJourneys[0].get('totalCount') : 0;
            
            List<AggregateResult> totalTouchpoints = [SELECT COUNT(Id) totalCount FROM Touchpoint__c WHERE CreatedDate = TODAY];
            response.total_touchpoints = totalTouchpoints.size() > 0 ? (Integer)totalTouchpoints[0].get('totalCount') : 0;
            
            List<AggregateResult> b2cJourneys = [SELECT COUNT(Id) totalCount FROM Customer_Journey__c WHERE Customer_Type__c = 'B2C' AND CreatedDate = TODAY];
            response.b2c_journeys = b2cJourneys.size() > 0 ? (Integer)b2cJourneys[0].get('totalCount') : 0;
            
            List<AggregateResult> b2bJourneys = [SELECT COUNT(Id) totalCount FROM Customer_Journey__c WHERE Customer_Type__c = 'B2B' AND CreatedDate = TODAY];
            response.b2b_journeys = b2bJourneys.size() > 0 ? (Integer)b2bJourneys[0].get('totalCount') : 0;
            
            List<AggregateResult> convertedJourneys = [SELECT COUNT(Id) totalCount FROM Customer_Journey__c WHERE Converted__c = true AND CreatedDate = TODAY];
            response.converted_journeys = convertedJourneys.size() > 0 ? (Integer)convertedJourneys[0].get('totalCount') : 0;
            
            response.last_import = System.now();
            response.status = 'active';
            
        } catch (Exception e) {
            response.error_message = e.getMessage();
            response.status = 'error';
        }
        
        return response;
    }
}