/**
 * @description REST API endpoint for importing customer journey data from synthetic generators
 * @author System Generated - Omnichannel Attribution Platform
 * @version 2.0
 * 
 * This service receives JSON data from the Journey Assembler API and imports it into
 * Salesforce custom objects with upsert logic and version tracking.
 * 
 * Endpoint: /services/apexrest/attribution/import/*
 * 
 * Usage:
 * POST /services/apexrest/attribution/import/journeys
 * Body: JSON from Journey Assembler API (salesforce_ready_data format)
 */

@RestResource(urlMapping='/attribution/import/*')
global class JourneyDataImportService {
    
    // Response wrapper class
    global class ImportResponse {
        public Boolean success;
        public String message;
        public Integer totalProcessed;
        public Integer successCount;
        public Integer errorCount;
        public List<String> errors;
        public String batchId;
        public DateTime processing_timestamp;
        
        public ImportResponse() {
            this.success = false;
            this.totalProcessed = 0;
            this.successCount = 0;
            this.errorCount = 0;
            this.errors = new List<String>();
            this.processing_timestamp = System.now();
        }
    }
    
    // Journey assembler response wrapper matching actual output
    global class AssemblerResponse {
        public String status;
        public Integer processed_journeys;
        public List<SalesforceJourneyWrapper> salesforce_ready_data;
    }
    
    global class SalesforceJourneyWrapper {
        public CustomerJourneyRecord Customer_Journey__c;
        public List<TouchpointRecord> Touchpoints;
    }
    
    global class CustomerJourneyRecord {
        public String Name;
        public String Customer_ID__c;
        public String Journey_Start_Date__c;
        public String Journey_End_Date__c;
        public Integer Total_Touchpoints__c;
        public Boolean Converted__c;
        public Decimal Conversion_Value__c;
        public Integer Journey_Duration_Days__c;
        public String Customer_Type__c;
        public Decimal Confidence_Score__c;
        public String Channel_Sequence__c;
        public String Synergistic_Patterns__c;
    }
    
    global class TouchpointRecord {
        public String Name;
        public String Channel__c;
        public String Touchpoint_Timestamp__c;
        public String Campaign_ID__c;
        public String Device_Type__c;
        public String Stage__c;
        public Decimal Conversion_Value__c;
    }
    
    /**
     * @description Import customer journeys and touchpoints with upsert logic
     * @return ImportResponse with processing results
     */
    @HttpPost
    global static ImportResponse importJourneyData() {
        ImportResponse response = new ImportResponse();
        
        try {
            // Get request body
            RestRequest request = RestContext.request;
            String requestBody = request.requestBody.toString();
            
            if (String.isBlank(requestBody)) {
                response.message = 'Request body is empty';
                return response;
            }
            
            // Parse JSON data from assembler format
            AssemblerResponse assemblerData;
            try {
                assemblerData = (AssemblerResponse) JSON.deserialize(requestBody, AssemblerResponse.class);
            } catch (Exception e) {
                response.message = 'Invalid JSON format: ' + e.getMessage();
                response.errors.add('JSON parsing error: ' + e.getMessage());
                return response;
            }
            
            // Validate input data
            if (assemblerData.salesforce_ready_data == null || assemblerData.salesforce_ready_data.isEmpty()) {
                response.message = 'No salesforce_ready_data provided';
                return response;
            }
            
            // Process data with upsert logic
            response = processBatchedUpsert(assemblerData);
            
        } catch (Exception e) {
            response.message = 'Unexpected error: ' + e.getMessage();
            response.errors.add('System error: ' + e.getMessage());
            System.debug(LoggingLevel.ERROR, 'JourneyDataImportService error: ' + e.getMessage());
            System.debug(LoggingLevel.ERROR, 'Stack trace: ' + e.getStackTraceString());
        }
        
        return response;
    }
    
    /**
     * @description Process import with upsert logic to handle duplicates and version tracking
     * @param assemblerData The parsed assembler response data
     * @return ImportResponse with batch processing results
     */
    private static ImportResponse processBatchedUpsert(AssemblerResponse assemblerData) {
        ImportResponse response = new ImportResponse();
        
        try {
            response.totalProcessed = assemblerData.salesforce_ready_data.size();
            
            // Convert assembler data to Salesforce objects
            List<Customer_Journey__c> journeysToUpsert = new List<Customer_Journey__c>();
            List<Touchpoint__c> touchpointsToUpsert = new List<Touchpoint__c>();
            
            // Extract Journey IDs from the assembler data
            Map<String, String> journeyNameToIdMap = new Map<String, String>();
            
            for (SalesforceJourneyWrapper journeyWrapper : assemblerData.salesforce_ready_data) {
                Customer_Journey__c journey = convertToJourneyObject(journeyWrapper.Customer_Journey__c);
                if (journey != null) {
                    // Extract Journey ID from Name field (format: "Journey {journey_id}")
                    String journeyId = extractJourneyIdFromName(journeyWrapper.Customer_Journey__c.Name);
                    if (String.isNotBlank(journeyId)) {
                        journey.Journey_ID__c = journeyId;
                        journeyNameToIdMap.put(journeyWrapper.Customer_Journey__c.Name, journeyId);
                        journeysToUpsert.add(journey);
                    }
                }
            }
            
            // Upsert journeys using Journey_ID__c as external ID
            List<Database.UpsertResult> journeyResults = upsertJourneysBatch(journeysToUpsert);
            
            // Process journey results and build ID mapping
            Map<String, Id> journeyIdToSalesforceIdMap = new Map<String, Id>();
            for (Integer i = 0; i < journeyResults.size(); i++) {
                Database.UpsertResult result = journeyResults[i];
                if (result.isSuccess()) {
                    response.successCount++;
                    journeyIdToSalesforceIdMap.put(journeysToUpsert[i].Journey_ID__c, result.getId());
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
            if (!journeyIdToSalesforceIdMap.isEmpty()) {
                for (SalesforceJourneyWrapper journeyWrapper : assemblerData.salesforce_ready_data) {
                    String journeyId = journeyNameToIdMap.get(journeyWrapper.Customer_Journey__c.Name);
                    Id salesforceJourneyId = journeyIdToSalesforceIdMap.get(journeyId);
                    
                    if (salesforceJourneyId != null && journeyWrapper.Touchpoints != null) {
                        for (TouchpointRecord touchpointData : journeyWrapper.Touchpoints) {
                            Touchpoint__c touchpoint = convertToTouchpointObject(touchpointData, salesforceJourneyId, journeyId);
                            if (touchpoint != null) {
                                touchpointsToUpsert.add(touchpoint);
                            }
                        }
                    }
                }
                
                // Upsert touchpoints using Touchpoint_ID__c as external ID
                if (!touchpointsToUpsert.isEmpty()) {
                    List<Database.UpsertResult> touchpointResults = upsertTouchpointsBatch(touchpointsToUpsert);
                    
                    // Process touchpoint results
                    for (Integer i = 0; i < touchpointResults.size(); i++) {
                        Database.UpsertResult result = touchpointResults[i];
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
            
            // Set success status
            response.success = (response.errorCount == 0);
            response.message = response.success ? 
                'Successfully processed ' + response.successCount + ' journeys' :
                'Completed with ' + response.errorCount + ' errors out of ' + response.totalProcessed + ' journeys';
                
        } catch (Exception e) {
            response.success = false;
            response.message = 'Batch processing error: ' + e.getMessage();
            response.errors.add('Batch error: ' + e.getMessage());
        }
        
        return response;
    }
    
    /**
     * @description Extract journey ID from Name field format "Journey {journey_id}"
     * @param journeyName The journey name from assembler
     * @return Extracted journey ID or null
     */
    private static String extractJourneyIdFromName(String journeyName) {
        if (String.isBlank(journeyName)) {
            return null;
        }
        
        // Expected format: "Journey journey_a1b2c3d4e5f6"
        if (journeyName.startsWith('Journey ')) {
            return journeyName.substring(8); // Remove "Journey " prefix
        }
        
        return journeyName; // Fallback to full name
    }
    
    /**
     * @description Convert assembler journey data to Salesforce Customer_Journey__c object
     * @param journeyData The journey data from assembler
     * @return Customer_Journey__c object or null if conversion fails
     */
    private static Customer_Journey__c convertToJourneyObject(CustomerJourneyRecord journeyData) {
        try {
            Customer_Journey__c journey = new Customer_Journey__c();
            
            // Core fields
            journey.Name = journeyData.Name;
            journey.Customer_ID__c = journeyData.Customer_ID__c;
            journey.Customer_Type__c = journeyData.Customer_Type__c;
            
            // Date fields with proper parsing
            if (String.isNotBlank(journeyData.Journey_Start_Date__c)) {
                journey.Journey_Start_Date__c = DateTime.valueOf(journeyData.Journey_Start_Date__c.replace('T', ' '));
            }
            if (String.isNotBlank(journeyData.Journey_End_Date__c)) {
                journey.Journey_End_Date__c = DateTime.valueOf(journeyData.Journey_End_Date__c.replace('T', ' '));
            }
            
            // Conversion data
            journey.Converted__c = journeyData.Converted__c != null ? journeyData.Converted__c : false;
            journey.Conversion_Value__c = journeyData.Conversion_Value__c;
            
            // Journey metrics
            journey.Total_Touchpoints__c = journeyData.Total_Touchpoints__c;
            journey.Journey_Duration_Days__c = journeyData.Journey_Duration_Days__c;
            
            // Data quality metrics
            journey.Confidence_Score__c = journeyData.Confidence_Score__c;
            
            // Attribution ready fields
            journey.Channel_Sequence__c = journeyData.Channel_Sequence__c;
            journey.Synergistic_Patterns__c = journeyData.Synergistic_Patterns__c;
            
            // Version tracking - update timestamp for tracking updates
            journey.Last_Updated__c = System.now();
            
            // Processing status
            journey.Processing_Status__c = 'Completed';
            
            return journey;
            
        } catch (Exception e) {
            System.debug(LoggingLevel.ERROR, 'Error converting journey data: ' + e.getMessage());
            return null;
        }
    }
    
    /**
     * @description Convert assembler touchpoint data to Salesforce Touchpoint__c object
     * @param touchpointData The touchpoint data from assembler
     * @param journeyId The Salesforce ID of the related Customer_Journey__c
     * @param journeyExternalId The external journey ID for touchpoint ID generation
     * @return Touchpoint__c object or null if conversion fails
     */
    private static Touchpoint__c convertToTouchpointObject(TouchpointRecord touchpointData, Id journeyId, String journeyExternalId) {
        try {
            Touchpoint__c touchpoint = new Touchpoint__c();
            
            // Generate unique Touchpoint ID from name (format: "Touchpoint {tp.touchpoint_id}")
            String touchpointId = touchpointData.Name;
            if (touchpointData.Name.startsWith('Touchpoint ')) {
                touchpointId = touchpointData.Name.substring(11); // Remove "Touchpoint " prefix
            }
            
            // Required fields
            touchpoint.Touchpoint_ID__c = touchpointId;
            touchpoint.Customer_Journey__c = journeyId;
            touchpoint.Channel__c = touchpointData.Channel__c;
            
            // Parse timestamp
            if (String.isNotBlank(touchpointData.Touchpoint_Timestamp__c)) {
                touchpoint.Touchpoint_Timestamp__c = DateTime.valueOf(touchpointData.Touchpoint_Timestamp__c.replace('T', ' '));
            }
            
            // Optional fields
            touchpoint.Campaign_ID__c = touchpointData.Campaign_ID__c;
            touchpoint.Device_Type__c = touchpointData.Device_Type__c;
            touchpoint.Stage__c = touchpointData.Stage__c;
            touchpoint.Conversion_Value__c = touchpointData.Conversion_Value__c;
            
            return touchpoint;
            
        } catch (Exception e) {
            System.debug(LoggingLevel.ERROR, 'Error converting touchpoint data: ' + e.getMessage());
            return null;
        }
    }
    
    /**
     * @description Upsert customer journeys using Journey_ID__c as external ID
     * @param journeys List of Customer_Journey__c objects to upsert
     * @return List of Database.UpsertResult
     */
    private static List<Database.UpsertResult> upsertJourneysBatch(List<Customer_Journey__c> journeys) {
        List<Database.UpsertResult> results = new List<Database.UpsertResult>();
        
        if (journeys.isEmpty()) {
            return results;
        }
        
        try {
            // Use Database.upsert with Journey_ID__c as external ID field
            Schema.SObjectField externalIdField = Customer_Journey__c.Journey_ID__c;
            results = Database.upsert(journeys, externalIdField, false);
            
        } catch (Exception e) {
            System.debug(LoggingLevel.ERROR, 'Error upserting journeys batch: ' + e.getMessage());
            // Create error results for all records
            for (Customer_Journey__c journey : journeys) {
                Database.UpsertResult errorResult = (Database.UpsertResult) JSON.deserialize(
                    '{"success":false,"errors":[{"message":"' + e.getMessage() + '"}]}', 
                    Database.UpsertResult.class
                );
                results.add(errorResult);
            }
        }
        
        return results;
    }
    
    /**
     * @description Upsert touchpoints using Touchpoint_ID__c as external ID
     * @param touchpoints List of Touchpoint__c objects to upsert
     * @return List of Database.UpsertResult
     */
    private static List<Database.UpsertResult> upsertTouchpointsBatch(List<Touchpoint__c> touchpoints) {
        List<Database.UpsertResult> results = new List<Database.UpsertResult>();
        
        if (touchpoints.isEmpty()) {
            return results;
        }
        
        try {
            // Use Database.upsert with Touchpoint_ID__c as external ID field
            Schema.SObjectField externalIdField = Touchpoint__c.Touchpoint_ID__c;
            results = Database.upsert(touchpoints, externalIdField, false);
            
        } catch (Exception e) {
            System.debug(LoggingLevel.ERROR, 'Error upserting touchpoints batch: ' + e.getMessage());
            // Create error results for all records
            for (Touchpoint__c touchpoint : touchpoints) {
                Database.UpsertResult errorResult = (Database.UpsertResult) JSON.deserialize(
                    '{"success":false,"errors":[{"message":"' + e.getMessage() + '"}]}', 
                    Database.UpsertResult.class
                );
                results.add(errorResult);
            }
        }
        
        return results;
    }
    
    // Status response wrapper class
    global class StatusResponse {
        public Integer total_journeys;
        public Integer total_touchpoints;
        public Integer b2c_journeys;
        public Integer b2b_journeys;
        public Integer converted_journeys;
        public Integer updated_journeys;
        public DateTime last_import;
        public String status;
        public String error_message;
        
        public StatusResponse() {
            this.total_journeys = 0;
            this.total_touchpoints = 0;
            this.b2c_journeys = 0;
            this.b2b_journeys = 0;
            this.converted_journeys = 0;
            this.updated_journeys = 0;
            this.last_import = System.now();
            this.status = 'active';
        }
    }
    
    /**
     * @description Get import status and statistics with version tracking
     * @return StatusResponse with import statistics
     */
    @HttpGet
    global static StatusResponse getImportStatus() {
        StatusResponse response = new StatusResponse();
        
        try {
            // Get statistics using proper aggregate query syntax
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
            
            // Track updated journeys (where Last_Updated__c != CreatedDate)
            List<AggregateResult> updatedJourneys = [SELECT COUNT(Id) totalCount FROM Customer_Journey__c WHERE Last_Updated__c = TODAY AND CreatedDate != TODAY];
            response.updated_journeys = updatedJourneys.size() > 0 ? (Integer)updatedJourneys[0].get('totalCount') : 0;
            
            response.last_import = System.now();
            response.status = 'active';
            
        } catch (Exception e) {
            response.error_message = e.getMessage();
            response.status = 'error';
            System.debug(LoggingLevel.ERROR, 'Error getting import status: ' + e.getMessage());
        }
        
        return response;
    }
}