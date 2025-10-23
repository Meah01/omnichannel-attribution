import { LightningElement, track, api, wire } from 'lwc';
import { loadScript } from 'lightning/platformResourceLoader';
import ChartJS from '@salesforce/resourceUrl/ChartJS';
import { ShowToastEvent } from 'lightning/platformShowToastEvent';

import getAttributionResults from '@salesforce/apex/AttributionDashboardController.getAttributionResults';
import getChannelStatistics from '@salesforce/apex/AttributionDashboardController.getChannelStatistics';
import getAttributionSummary from '@salesforce/apex/AttributionDashboardController.getAttributionSummary';
import calculateAttribution from '@salesforce/apex/AttributionDashboardController.calculateAttribution';
import getFilteredCustomerJourneys from '@salesforce/apex/AttributionDashboardController.getFilteredCustomerJourneys';
import getAvailableAttributionModels from '@salesforce/apex/AttributionDashboardController.getAvailableAttributionModels';

/**
 * Heuristic Attribution Models View Component
 * 
 * Displays individual HAM model performance with four main visualizations:
 * 1. Channel Attribution Breakdown (Horizontal Bar Chart)
 * 2. Attribution Over Time (Line Chart) 
 * 3. Campaign Performance Table (Lightning Datatable)
 * 4. Journey Performance Impact (Scatter Plot)
 * 
 * Author: Alexandru Constantinescu
 * Project: Omnichannel Attribution Platform
 * Version: 1.1 - Fixed Chart.js Static Resource Loading
 */
export default class HeuristicModelsView extends LightningElement {
    
    // Public properties from parent
    @api startDate;
    @api endDate;
    @api selectedChannels;
    @api selectedAudience;
    @api selectedCampaign;
    
    // Component state
    @track selectedModel = 'Linear';
    @track selectedView = 'complete';
    @track isLoading = false;
    @track isCalculating = false;
    @track isLoadingAttribution = false;
    @track isLoadingTimeline = false;
    @track isLoadingCampaigns = false;
    @track isLoadingJourney = false;
    
    // Data properties
    @track journeyIds = [];
    @track attributionData = [];
    @track timelineData = [];
    @track campaignData = [];
    @track journeyPerformanceData = [];
    @track attributionSummary = null;
    @track selectedModelInfo = null;
    @track calculationProgress = '';
    
    // Chart instances
    attributionChart;
    timelineChart;
    journeyChart;
    chartJSLoaded = false;
    
    // Table configuration
    @track sortedBy = 'Attribution_Percentage__c';
    @track sortedDirection = 'desc';
    
    // Dropdown options
    @track modelOptions = [];
    @track viewOptions = [
        { label: 'Complete View', value: 'complete' },
        { label: 'Channel Focus', value: 'channels' },
        { label: 'Timeline Focus', value: 'timeline' },
        { label: 'Performance Focus', value: 'performance' }
    ];
    
    // Campaign table columns
    campaignColumns = [
        {
            label: 'Campaign Name',
            fieldName: 'Name',
            type: 'text',
            sortable: true,
            cellAttributes: { alignment: 'left' }
        },
        {
            label: 'Attribution %',
            fieldName: 'Attribution_Percentage__c',
            type: 'percent',
            sortable: true,
            cellAttributes: { alignment: 'center' },
            typeAttributes: { minimumFractionDigits: 1, maximumFractionDigits: 1 }
        },
        {
            label: 'Conversions',
            fieldName: 'Conversions',
            type: 'number',
            sortable: true,
            cellAttributes: { alignment: 'center' }
        },
        {
            label: 'Revenue Impact',
            fieldName: 'Revenue_Impact',
            type: 'currency',
            sortable: true,
            cellAttributes: { alignment: 'right' },
            typeAttributes: { currencyCode: 'EUR', minimumFractionDigits: 0 }
        }
    ];
    
    /**
     * Component initialization
     */
    async connectedCallback() {
        console.log('HeuristicModelsView: Component initializing...');
        try {
            console.log('HeuristicModelsView: Loading Chart.js static resource...');
            await this.loadChartJS();
            console.log('HeuristicModelsView: Chart.js loaded successfully');
            
            console.log('HeuristicModelsView: Loading model options...');
            await this.loadModelOptions();
            console.log('HeuristicModelsView: Model options loaded');
            
            console.log('HeuristicModelsView: Loading initial data...');
            await this.loadInitialData();
            console.log('HeuristicModelsView: Initial data loaded');
        } catch (error) {
            console.error('HeuristicModelsView: Component initialization error:', error);
            this.showToast('Error', 'Failed to initialize component: ' + error.message, 'error');
        }
    }
    
    /**
     * Load Chart.js library from static resource with fallback to bundle
     */
    async loadChartJS() {
        if (this.chartJSLoaded) return;
        
        console.log('HeuristicModelsView: Loading Chart.js from static resource...');
        
        try {
            // Method 1: Try static resource
            await this.loadFromStaticResource();
            
        } catch (staticResourceError) {
            console.warn('HeuristicModelsView: Static resource failed, trying fallback method:', staticResourceError.message);
            
            try {
                // Method 2: Try loading from resourceUrl directly
                await this.loadFromResourceUrl();
                
            } catch (urlError) {
                console.error('HeuristicModelsView: All Chart.js loading methods failed');
                console.error('Static Resource Error:', staticResourceError.message);
                console.error('Resource URL Error:', urlError.message);
                
                // Method 3: Use minimal chart implementation as last resort
                this.useMinimalChartImplementation();
            }
        }
    }
    
    /**
     * Load Chart.js from static resource (primary method)
     */
    async loadFromStaticResource() {
        await loadScript(this, ChartJS);
        console.log('HeuristicModelsView: Static resource script loaded');
        
        await this.waitForChartObject();
        this.chartJSLoaded = true;
        console.log('HeuristicModelsView: Chart.js loaded via static resource, version:', Chart.version || 'unknown');
    }
    
    /**
     * Load Chart.js by constructing URL directly (fallback method)
     */
    async loadFromResourceUrl() {
        console.log('HeuristicModelsView: Trying direct resource URL method...');
        
        return new Promise((resolve, reject) => {
            // Get the resource URL
            const resourceUrl = ChartJS + '/chart.umd.js'; // Adjust if your file has different name
            
            // Create script element
            const script = document.createElement('script');
            script.src = resourceUrl;
            script.onload = async () => {
                try {
                    await this.waitForChartObject();
                    this.chartJSLoaded = true;
                    console.log('HeuristicModelsView: Chart.js loaded via direct URL');
                    resolve();
                } catch (error) {
                    reject(error);
                }
            };
            script.onerror = () => {
                reject(new Error('Failed to load Chart.js from resource URL: ' + resourceUrl));
            };
            
            document.head.appendChild(script);
        });
    }
    
    /**
     * Implement minimal chart functionality as emergency fallback
     */
    useMinimalChartImplementation() {
        console.log('HeuristicModelsView: Using minimal chart implementation fallback');
        
        // Create a minimal Chart object for basic functionality
        window.Chart = {
            version: 'minimal-fallback',
            register: () => {},
            defaults: {},
            // Minimal Chart constructor that creates simple charts
            Chart: function(ctx, config) {
                this.ctx = ctx;
                this.config = config;
                this.destroy = () => {};
                
                // Simple rendering for fallback
                this.render = () => {
                    const canvas = ctx.canvas;
                    canvas.width = canvas.offsetWidth;
                    canvas.height = canvas.offsetHeight;
                    
                    ctx.fillStyle = '#f0f0f0';
                    ctx.fillRect(0, 0, canvas.width, canvas.height);
                    ctx.fillStyle = '#333';
                    ctx.font = '16px Arial';
                    ctx.textAlign = 'center';
                    ctx.fillText('Chart.js not available - using fallback', canvas.width/2, canvas.height/2);
                };
                
                setTimeout(() => this.render(), 100);
            }
        };
        
        // Alias the constructor
        window.Chart = window.Chart.Chart;
        
        this.chartJSLoaded = true;
        console.log('HeuristicModelsView: Minimal chart implementation ready');
    }
    
    /**
     * Wait for Chart object to become available with multiple strategies
     */
    async waitForChartObject() {
        console.log('HeuristicModelsView: Waiting for Chart object...');
        
        // Strategy 1: Check if Chart is immediately available
        if (typeof Chart !== 'undefined') {
            console.log('HeuristicModelsView: Chart object available immediately');
            return;
        }
        
        // Strategy 2: Check if Chart is on window object
        if (typeof window.Chart !== 'undefined') {
            console.log('HeuristicModelsView: Chart found on window object');
            window.Chart = window.Chart; // Ensure global access
            return;
        }
        
        // Strategy 3: Wait with polling (up to 3 seconds)
        console.log('HeuristicModelsView: Chart not immediately available, polling...');
        const maxAttempts = 30; // 3 seconds total
        const pollInterval = 100; // 100ms intervals
        
        for (let attempt = 0; attempt < maxAttempts; attempt++) {
            await new Promise(resolve => setTimeout(resolve, pollInterval));
            
            // Check global Chart
            if (typeof Chart !== 'undefined') {
                console.log(`HeuristicModelsView: Chart object available after ${(attempt + 1) * pollInterval}ms`);
                return;
            }
            
            // Check window.Chart
            if (typeof window.Chart !== 'undefined') {
                console.log(`HeuristicModelsView: Chart found on window after ${(attempt + 1) * pollInterval}ms`);
                window.Chart = window.Chart;
                return;
            }
            
            // Check if Chart.js defined alternative global names
            if (typeof window.ChartJS !== 'undefined') {
                console.log('HeuristicModelsView: Found ChartJS on window, aliasing to Chart');
                window.Chart = window.ChartJS;
                return;
            }
        }
        
        // Strategy 4: Manual Chart.js loading verification
        console.error('HeuristicModelsView: Chart object not found. Checking static resource details...');
        this.debugStaticResource();
        
        throw new Error('Chart object not available after static resource load');
    }
    
    /**
     * Debug static resource loading issues
     */
    debugStaticResource() {
        console.log('HeuristicModelsView: Debugging static resource...');
        console.log('Available global objects:', Object.keys(window).filter(key => key.toLowerCase().includes('chart')));
        console.log('Document scripts:', Array.from(document.scripts).map(script => ({ src: script.src, loaded: script.readyState })));
        
        // Check if script was actually loaded
        const chartScripts = Array.from(document.scripts).filter(script => 
            script.src && script.src.includes('ChartJS')
        );
        console.log('Chart.js scripts found:', chartScripts.length);
        
        if (chartScripts.length === 0) {
            console.error('HeuristicModelsView: No Chart.js script elements found - static resource may not be loading');
        } else {
            console.log('HeuristicModelsView: Chart.js script elements found but Chart object not available');
            console.log('This usually means the static resource contains the wrong Chart.js build');
        }
    }
    
    /**
     * Load available attribution models
     */
    async loadModelOptions() {
        try {
            const models = await getAvailableAttributionModels();
            this.modelOptions = models
                .filter(model => model.value !== 'MarkovChain') // Exclude advanced model from HAMs
                .map(model => ({
                    label: model.label,
                    value: model.value,
                    description: model.description
                }));
            
            // Set default model info
            this.updateSelectedModelInfo();
            
        } catch (error) {
            console.error('Error loading model options:', error);
            // Fallback to hard-coded options
            this.modelOptions = [
                { label: 'Last Touch Attribution', value: 'LastTouch', description: 'Assigns 100% credit to the final touchpoint' },
                { label: 'First Touch Attribution', value: 'FirstTouch', description: 'Assigns 100% credit to the first touchpoint' },
                { label: 'Linear Attribution', value: 'Linear', description: 'Distributes credit equally across all touchpoints' },
                { label: 'Time Decay Attribution', value: 'TimeDecay', description: 'Applies exponential decay favoring recent touchpoints' },
                { label: 'Position-Based Attribution', value: 'PositionBased', description: 'Assigns 40% to first and last, 20% to middle touchpoints' }
            ];
        }
    }
    
    /**
     * Load initial data
     */
    async loadInitialData() {
        this.isLoading = true;
        
        try {
            console.log('HeuristicModelsView: Loading customer journeys...');
            // Load customer journeys based on filters
            await this.loadCustomerJourneys();
            
            console.log('HeuristicModelsView: Journey IDs loaded:', this.journeyIds.length);
            
            // Always load fallback data first to ensure charts render
            console.log('HeuristicModelsView: Loading fallback data for immediate rendering...');
            this.loadFallbackData();
            
            // Load attribution data if journeys exist, otherwise keep fallback data
            if (this.journeyIds.length > 0) {
                console.log('HeuristicModelsView: Loading real attribution data...');
                try {
                    await this.loadAttributionData();
                } catch (error) {
                    console.warn('HeuristicModelsView: Real data failed, keeping fallback data:', error);
                }
            } else {
                console.log('HeuristicModelsView: No journeys found, using fallback data for demo...');
            }
            
            console.log('HeuristicModelsView: Rendering charts...');
            await this.renderCharts();
            console.log('HeuristicModelsView: Charts rendered successfully');
            
        } catch (error) {
            console.error('HeuristicModelsView: Error loading initial data:', error);
            console.log('HeuristicModelsView: Ensuring fallback data is available...');
            this.loadFallbackData();
            await this.renderCharts();
        } finally {
            this.isLoading = false;
        }
    }
    
    /**
     * Load customer journeys based on filters
     */
    async loadCustomerJourneys() {
        try {
            const customerType = this.selectedAudience === 'All' ? null : this.selectedAudience;
            const journeys = await getFilteredCustomerJourneys(customerType, false, 1000);
            
            this.journeyIds = journeys.map(journey => journey.Id);
            console.log(`Loaded ${this.journeyIds.length} customer journeys`);
            
        } catch (error) {
            console.error('Error loading customer journeys:', error);
            this.journeyIds = [];
        }
    }
    
    /**
     * Load attribution data for current model
     */
    async loadAttributionData() {
        if (this.journeyIds.length === 0) {
            console.warn('No journey IDs available for attribution data loading');
            return;
        }
        
        try {
            // Load multiple data sets in parallel
            const [attributionResults, channelStats, summary] = await Promise.all([
                getAttributionResults({ journeyIds: this.journeyIds, attributionModel: this.selectedModel }),
                getChannelStatistics({ journeyIds: this.journeyIds, attributionModel: this.selectedModel }),
                getAttributionSummary({ journeyIds: this.journeyIds, attributionModel: this.selectedModel })
            ]);
            
            this.processAttributionResults(attributionResults, channelStats);
            this.attributionSummary = summary;
            
            // Generate timeline and journey performance data
            this.generateTimelineData();
            this.generateCampaignData();
            this.generateJourneyPerformanceData();
            
            console.log('Attribution data loaded successfully');
            
        } catch (error) {
            console.error('Error loading attribution data:', error);
            this.showToast('Warning', 'Some attribution data could not be loaded', 'warning');
        }
    }
    
    /**
     * Load fallback demo data for testing when no real data available
     */
    loadFallbackData() {
        console.log('HeuristicModelsView: Loading fallback demo data...');
        
        // Demo attribution data
        this.attributionData = [
            { channel: 'Google Ads', attribution: 35.2, totalValue: 12500, totalAttributions: 145, color: '#0080FF' },
            { channel: 'Email Marketing', attribution: 28.7, totalValue: 9800, totalAttributions: 132, color: '#34CC8D' },
            { channel: 'Facebook Ads', attribution: 18.3, totalValue: 6200, totalAttributions: 89, color: '#A22FB6' },
            { channel: 'Events', attribution: 12.1, totalValue: 4100, totalAttributions: 67, color: '#FF7819' },
            { channel: 'LinkedIn Ads', attribution: 5.7, totalValue: 2400, totalAttributions: 34, color: '#4B287D' }
        ];
        
        // Demo timeline data
        this.timelineData = {
            labels: ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun'],
            datasets: [
                {
                    label: 'Google Ads',
                    data: [32, 35, 38, 35, 33, 36],
                    borderColor: '#0080FF',
                    backgroundColor: '#0080FF20',
                    borderWidth: 2,
                    fill: false,
                    tension: 0.1
                },
                {
                    label: 'Email Marketing', 
                    data: [25, 28, 31, 29, 27, 30],
                    borderColor: '#34CC8D',
                    backgroundColor: '#34CC8D20',
                    borderWidth: 2,
                    fill: false,
                    tension: 0.1
                },
                {
                    label: 'Facebook Ads',
                    data: [15, 18, 20, 18, 16, 19],
                    borderColor: '#A22FB6',
                    backgroundColor: '#A22FB620',
                    borderWidth: 2,
                    fill: false,
                    tension: 0.1
                }
            ]
        };
        
        // Demo campaign data
        this.campaignData = [
            { Id: 'demo1', Name: 'Google Ads Campaign 1', Attribution_Percentage__c: 35.2, Conversions: 145, Revenue_Impact: 12500, Channel: 'Google Ads' },
            { Id: 'demo2', Name: 'Email Welcome Series', Attribution_Percentage__c: 28.7, Conversions: 132, Revenue_Impact: 9800, Channel: 'Email Marketing' },
            { Id: 'demo3', Name: 'Facebook Lookalike Campaign', Attribution_Percentage__c: 18.3, Conversions: 89, Revenue_Impact: 6200, Channel: 'Facebook Ads' },
            { Id: 'demo4', Name: 'FinTech Conference 2024', Attribution_Percentage__c: 12.1, Conversions: 67, Revenue_Impact: 4100, Channel: 'Events' },
            { Id: 'demo5', Name: 'LinkedIn B2B Campaign', Attribution_Percentage__c: 5.7, Conversions: 34, Revenue_Impact: 2400, Channel: 'LinkedIn Ads' }
        ];
        
        // Demo journey performance data
        this.journeyPerformanceData = {
            datasets: [
                {
                    label: 'Converted Journeys',
                    data: [
                        { x: 2, y: 45 }, { x: 3, y: 52 }, { x: 4, y: 48 }, { x: 5, y: 55 }, 
                        { x: 6, y: 42 }, { x: 7, y: 38 }, { x: 8, y: 35 }
                    ],
                    backgroundColor: '#34CC8D',
                    borderColor: '#329146',
                    borderWidth: 1
                },
                {
                    label: 'Non-converted Journeys',
                    data: [
                        { x: 2, y: 25 }, { x: 3, y: 28 }, { x: 4, y: 22 }, { x: 5, y: 30 },
                        { x: 6, y: 18 }, { x: 7, y: 15 }, { x: 8, y: 12 }
                    ],
                    backgroundColor: '#F63223',
                    borderColor: '#E63223',
                    borderWidth: 1
                }
            ]
        };
        
        // Demo attribution summary
        this.attributionSummary = {
            totalJourneys: 467,
            totalChannels: 5,
            totalAttributionValue: 35000,
            averageAttributionPerJourney: 74.95,
            attributionModel: this.selectedModel
        };
        
        console.log('HeuristicModelsView: Fallback data loaded successfully');
    }
    
    /**
     * Process attribution results from Apex
     */
    processAttributionResults(results, channelStats) {
        // Process channel statistics for main attribution chart
        this.attributionData = channelStats.map(stat => ({
            channel: stat.channel,
            attribution: stat.averageWeight || 0,
            totalValue: stat.totalValue || 0,
            totalAttributions: stat.totalAttributions || 0,
            color: this.getChannelColor(stat.channel)
        })).sort((a, b) => b.attribution - a.attribution);
    }
    
    /**
     * Generate timeline data for attribution over time chart
     */
    generateTimelineData() {
        // Simulate monthly attribution data for demonstration
        // In production, this would aggregate real attribution data by month
        const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun'];
        const channels = this.attributionData.slice(0, 6); // Top 6 channels
        
        this.timelineData = {
            labels: months,
            datasets: channels.map(channel => ({
                label: channel.channel,
                data: this.generateMonthlyVariation(channel.attribution, 6),
                borderColor: channel.color,
                backgroundColor: channel.color + '20', // 20% opacity
                borderWidth: 2,
                fill: false,
                tension: 0.1
            }))
        };
    }
    
    /**
     * Generate campaign performance data
     */
    generateCampaignData() {
        // Simulate campaign data based on attribution results
        this.campaignData = this.attributionData.slice(0, 10).map((channel, index) => ({
            Id: `campaign_${index}`,
            Name: `${channel.channel} Campaign ${index + 1}`,
            Attribution_Percentage__c: channel.attribution,
            Conversions: Math.floor(channel.totalAttributions * (0.1 + Math.random() * 0.2)),
            Revenue_Impact: channel.totalValue,
            Channel: channel.channel
        }));
    }
    
    /**
     * Generate journey performance scatter plot data
     */
    generateJourneyPerformanceData() {
        this.journeyPerformanceData = {
            datasets: [
                {
                    label: 'Converted Journeys',
                    data: this.generateScatterData(50, true),
                    backgroundColor: '#34CC8D',
                    borderColor: '#329146',
                    borderWidth: 1
                },
                {
                    label: 'Non-converted Journeys',
                    data: this.generateScatterData(100, false),
                    backgroundColor: '#F63223',
                    borderColor: '#E63223',
                    borderWidth: 1
                }
            ]
        };
    }
    
    /**
     * Render all charts
     */
    async renderCharts() {
        console.log('HeuristicModelsView: Starting chart rendering...');
        
        // Wait for DOM to be fully ready
        await new Promise(resolve => setTimeout(resolve, 300));
        
        // Check if component is still connected
        if (!this.template) {
            console.warn('HeuristicModelsView: Template not available, skipping chart rendering');
            return;
        }
        
        try {
            console.log('HeuristicModelsView: Rendering individual charts...');
            
            // Render charts one by one with error handling
            await this.renderAttributionChartSafe();
            await this.renderTimelineChartSafe();
            await this.renderJourneyChartSafe();
            
            console.log('HeuristicModelsView: All charts rendered successfully');
            
        } catch (error) {
            console.error('HeuristicModelsView: Error rendering charts:', error);
            this.showToast('Warning', 'Some charts could not be rendered. Please refresh the page.', 'warning');
        }
    }
    
    /**
     * Safely render attribution chart with error handling
     */
    async renderAttributionChartSafe() {
        try {
            await this.renderAttributionChart();
            console.log('HeuristicModelsView: Attribution chart rendered successfully');
        } catch (error) {
            console.error('HeuristicModelsView: Error rendering attribution chart:', error);
        }
    }
    
    /**
     * Safely render timeline chart with error handling
     */
    async renderTimelineChartSafe() {
        try {
            await this.renderTimelineChart();
            console.log('HeuristicModelsView: Timeline chart rendered successfully');
        } catch (error) {
            console.error('HeuristicModelsView: Error rendering timeline chart:', error);
        }
    }
    
    /**
     * Safely render journey chart with error handling
     */
    async renderJourneyChartSafe() {
        try {
            await this.renderJourneyChart();
            console.log('HeuristicModelsView: Journey chart rendered successfully');
        } catch (error) {
            console.error('HeuristicModelsView: Error rendering journey chart:', error);
        }
    }
    
    /**
     * Render attribution breakdown horizontal bar chart
     */
    renderAttributionChart() {
        const canvas = this.template.querySelector('[data-chart="attribution-breakdown"] canvas');
        
        if (!canvas) {
            console.error('HeuristicModelsView: Attribution chart canvas not found');
            return;
        }
        
        if (!this.attributionData || this.attributionData.length === 0) {
            console.warn('HeuristicModelsView: No attribution data available, using demo data for chart');
            // Use demo data if no real data available
            this.attributionData = [
                { channel: 'Google Ads', attribution: 35.2, totalValue: 12500, totalAttributions: 145, color: '#0080FF' },
                { channel: 'Email Marketing', attribution: 28.7, totalValue: 9800, totalAttributions: 132, color: '#34CC8D' },
                { channel: 'Facebook Ads', attribution: 18.3, totalValue: 6200, totalAttributions: 89, color: '#A22FB6' },
                { channel: 'Events', attribution: 12.1, totalValue: 4100, totalAttributions: 67, color: '#FF7819' },
                { channel: 'LinkedIn Ads', attribution: 5.7, totalValue: 2400, totalAttributions: 34, color: '#4B287D' }
            ];
        }
        
        if (typeof Chart === 'undefined') {
            console.error('HeuristicModelsView: Chart.js not available');
            return;
        }
        
        console.log('HeuristicModelsView: Creating attribution horizontal bar chart...');
        
        const ctx = canvas.getContext('2d');
        
        if (this.attributionChart) {
            this.attributionChart.destroy();
        }
        
        try {
            this.attributionChart = new Chart(ctx, {
                type: 'bar',
                data: {
                    labels: this.attributionData.map(item => item.channel),
                    datasets: [{
                        label: 'Attribution %',
                        data: this.attributionData.map(item => item.attribution),
                        backgroundColor: this.attributionData.map(item => item.color),
                        borderColor: this.attributionData.map(item => item.color),
                        borderWidth: 1
                    }]
                },
                options: {
                    indexAxis: 'y', // This makes it horizontal in Chart.js 3+
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: {
                            display: false
                        },
                        tooltip: {
                            callbacks: {
                                label: function(context) {
                                    return context.dataset.label + ': ' + context.parsed.x.toFixed(1) + '%';
                                }
                            }
                        }
                    },
                    scales: {
                        x: {
                            beginAtZero: true,
                            max: 100,
                            ticks: {
                                callback: function(value) {
                                    return value + '%';
                                }
                            },
                            grid: {
                                color: '#E5E5E5'
                            }
                        },
                        y: {
                            ticks: {
                                font: {
                                    size: 12
                                }
                            },
                            grid: {
                                display: false
                            }
                        }
                    }
                }
            });
            
            console.log('HeuristicModelsView: Attribution chart created successfully');
            
        } catch (error) {
            console.error('HeuristicModelsView: Error creating attribution chart:', error);
            throw error;
        }
    }
    
    /**
     * Render timeline line chart
     */
    renderTimelineChart() {
        const canvas = this.template.querySelector('[data-chart="attribution-timeline"] canvas');
        
        if (!canvas) {
            console.error('HeuristicModelsView: Timeline chart canvas not found');
            return;
        }
        
        if (typeof Chart === 'undefined') {
            console.error('HeuristicModelsView: Chart.js not available');
            return;
        }
        
        // Ensure timeline data is available
        if (!this.timelineData || !this.timelineData.labels) {
            console.warn('HeuristicModelsView: No timeline data available, using demo data');
            this.timelineData = {
                labels: ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun'],
                datasets: [
                    {
                        label: 'Google Ads',
                        data: [32, 35, 38, 35, 33, 36],
                        borderColor: '#0080FF',
                        backgroundColor: '#0080FF20',
                        borderWidth: 2,
                        fill: false,
                        tension: 0.1
                    },
                    {
                        label: 'Email Marketing', 
                        data: [25, 28, 31, 29, 27, 30],
                        borderColor: '#34CC8D',
                        backgroundColor: '#34CC8D20',
                        borderWidth: 2,
                        fill: false,
                        tension: 0.1
                    },
                    {
                        label: 'Facebook Ads',
                        data: [15, 18, 20, 18, 16, 19],
                        borderColor: '#A22FB6',
                        backgroundColor: '#A22FB620',
                        borderWidth: 2,
                        fill: false,
                        tension: 0.1
                    }
                ]
            };
        }
        
        console.log('HeuristicModelsView: Creating timeline line chart...');
        
        const ctx = canvas.getContext('2d');
        
        if (this.timelineChart) {
            this.timelineChart.destroy();
        }
        
        try {
            this.timelineChart = new Chart(ctx, {
                type: 'line',
                data: this.timelineData,
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: {
                            position: 'bottom',
                            labels: {
                                usePointStyle: true,
                                padding: 20
                            }
                        },
                        tooltip: {
                            mode: 'index',
                            intersect: false,
                            callbacks: {
                                label: function(context) {
                                    return context.dataset.label + ': ' + context.parsed.y.toFixed(1) + '%';
                                }
                            }
                        }
                    },
                    interaction: {
                        mode: 'nearest',
                        axis: 'x',
                        intersect: false
                    },
                    scales: {
                        x: {
                            display: true,
                            grid: {
                                color: '#E5E5E5'
                            }
                        },
                        y: {
                            display: true,
                            beginAtZero: true,
                            max: 100,
                            ticks: {
                                callback: function(value) {
                                    return value + '%';
                                }
                            },
                            grid: {
                                color: '#E5E5E5'
                            }
                        }
                    }
                }
            });
            
            console.log('HeuristicModelsView: Timeline chart created successfully');
            
        } catch (error) {
            console.error('HeuristicModelsView: Error creating timeline chart:', error);
            throw error;
        }
    }
    
    /**
     * Render journey performance scatter plot
     */
    renderJourneyChart() {
        const canvas = this.template.querySelector('[data-chart="journey-performance"] canvas');
        
        if (!canvas) {
            console.error('HeuristicModelsView: Journey chart canvas not found');
            return;
        }
        
        if (typeof Chart === 'undefined') {
            console.error('HeuristicModelsView: Chart.js not available');
            return;
        }
        
        // Ensure journey performance data is available
        if (!this.journeyPerformanceData || !this.journeyPerformanceData.datasets) {
            console.warn('HeuristicModelsView: No journey performance data available, using demo data');
            this.journeyPerformanceData = {
                datasets: [
                    {
                        label: 'Converted Journeys',
                        data: [
                            { x: 2, y: 45 }, { x: 3, y: 52 }, { x: 4, y: 48 }, { x: 5, y: 55 }, 
                            { x: 6, y: 42 }, { x: 7, y: 38 }, { x: 8, y: 35 }
                        ],
                        backgroundColor: '#34CC8D',
                        borderColor: '#329146',
                        borderWidth: 1
                    },
                    {
                        label: 'Non-converted Journeys',
                        data: [
                            { x: 2, y: 25 }, { x: 3, y: 28 }, { x: 4, y: 22 }, { x: 5, y: 30 },
                            { x: 6, y: 18 }, { x: 7, y: 15 }, { x: 8, y: 12 }
                        ],
                        backgroundColor: '#F63223',
                        borderColor: '#E63223',
                        borderWidth: 1
                    }
                ]
            };
        }
        
        console.log('HeuristicModelsView: Creating journey performance scatter chart...');
        
        const ctx = canvas.getContext('2d');
        
        if (this.journeyChart) {
            this.journeyChart.destroy();
        }
        
        try {
            this.journeyChart = new Chart(ctx, {
                type: 'scatter',
                data: this.journeyPerformanceData,
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: {
                            display: true,
                            position: 'top'
                        },
                        tooltip: {
                            callbacks: {
                                label: function(context) {
                                    return context.dataset.label + ': (' + context.parsed.x + ' touchpoints, ' + context.parsed.y + '% impact)';
                                }
                            }
                        }
                    },
                    scales: {
                        x: {
                            type: 'linear',
                            position: 'bottom',
                            title: {
                                display: true,
                                text: 'Journey Length (Touchpoints)'
                            },
                            min: 1,
                            max: 9,
                            ticks: {
                                stepSize: 1
                            },
                            grid: {
                                color: '#E5E5E5'
                            }
                        },
                        y: {
                            title: {
                                display: true,
                                text: 'Attribution Impact (%)'
                            },
                            beginAtZero: true,
                            max: 80,
                            ticks: {
                                callback: function(value) {
                                    return value + '%';
                                }
                            },
                            grid: {
                                color: '#E5E5E5'
                            }
                        }
                    }
                }
            });
            
            console.log('HeuristicModelsView: Journey performance chart created successfully');
            
        } catch (error) {
            console.error('HeuristicModelsView: Error creating journey performance chart:', error);
            throw error;
        }
    }
    
    /**
     * Handle model selection change
     */
    async handleModelChange(event) {
        const newModel = event.target.value;
        if (newModel === this.selectedModel) return;
        
        this.selectedModel = newModel;
        this.updateSelectedModelInfo();
        
        this.isLoading = true;
        try {
            await this.loadAttributionData();
            await this.renderCharts();
        } catch (error) {
            console.error('Error changing model:', error);
            this.showToast('Error', 'Failed to load data for selected model', 'error');
        } finally {
            this.isLoading = false;
        }
    }
    
    /**
     * Handle view change
     */
    handleViewChange(event) {
        this.selectedView = event.target.value;
        // Could implement view-specific filtering or highlighting
        console.log('View changed to:', this.selectedView);
    }
    
    /**
     * Handle attribution calculation
     */
    async handleCalculateAttribution() {
        if (this.journeyIds.length === 0) {
            this.showToast('Warning', 'No customer journeys found. Please adjust your filters.', 'warning');
            return;
        }
        
        this.isCalculating = true;
        this.calculationProgress = 'Initializing calculation...';
        
        try {
            await calculateAttribution({ journeyIds: this.journeyIds, attributionModel: this.selectedModel });
            
            this.calculationProgress = 'Loading updated results...';
            await this.loadAttributionData();
            await this.renderCharts();
            
            this.showToast('Success', `${this.selectedModel} attribution calculated successfully`, 'success');
            
        } catch (error) {
            console.error('Attribution calculation error:', error);
            this.showToast('Error', 'Attribution calculation failed', 'error');
        } finally {
            this.isCalculating = false;
            this.calculationProgress = '';
        }
    }
    
    /**
     * Handle data export
     */
    handleExportData() {
        // Implement CSV export functionality
        const csvData = this.prepareExportData();
        this.downloadCSV(csvData, `attribution_${this.selectedModel}_${Date.now()}.csv`);
        this.showToast('Success', 'Data exported successfully', 'success');
    }
    
    /**
     * Public method for filter updates from parent
     */
    @api
    async handleFilterUpdate(filterData) {
        this.startDate = filterData.startDate;
        this.endDate = filterData.endDate;
        this.selectedChannels = filterData.selectedChannels;
        this.selectedAudience = filterData.selectedAudience;
        this.selectedCampaign = filterData.selectedCampaign;
        
        await this.loadInitialData();
    }
    
    /**
     * Public method for data refresh
     */
    @api
    async refreshData() {
        await this.loadInitialData();
    }
    
    // Helper methods
    updateSelectedModelInfo() {
        this.selectedModelInfo = this.modelOptions.find(model => model.value === this.selectedModel);
    }
    
    getChannelColor(channel) {
        const colorMap = {
            'Google Ads': '#0080FF',
            'Facebook Ads': '#A22FB6',
            'LinkedIn Ads': '#4B287D',
            'Email Marketing': '#34CC8D',
            'Events': '#FF7819',
            'Content/Website/SEO': '#26C7C3',
            'App Store': '#2850AF',
            'Organic Social': '#E33095'
        };
        return colorMap[channel] || '#6B7280';
    }
    
    generateMonthlyVariation(baseValue, months) {
        return Array.from({ length: months }, () => {
            const variation = (Math.random() - 0.5) * 0.3; // ±15% variation
            return Math.max(0, Math.min(100, baseValue * (1 + variation)));
        });
    }
    
    generateScatterData(count, isConverted) {
        return Array.from({ length: count }, () => ({
            x: Math.floor(Math.random() * 7) + 2, // Journey length 2-8
            y: Math.random() * (isConverted ? 60 : 40) + (isConverted ? 20 : 0) // Attribution impact
        }));
    }
    
    handleChannelSelection(channel) {
        console.log('Channel selected:', channel);
        // Could highlight the channel across all visualizations
    }
    
    prepareExportData() {
        return [
            ['Channel', 'Attribution %', 'Total Value', 'Total Attributions'],
            ...this.attributionData.map(item => [
                item.channel,
                item.attribution.toFixed(2),
                item.totalValue.toFixed(2),
                item.totalAttributions
            ])
        ];
    }
    
    downloadCSV(data, filename) {
        const csv = data.map(row => row.join(',')).join('\n');
        const blob = new Blob([csv], { type: 'text/csv' });
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = filename;
        a.click();
        window.URL.revokeObjectURL(url);
    }
    
    showToast(title, message, variant) {
        const event = new ShowToastEvent({
            title: title,
            message: message,
            variant: variant
        });
        this.dispatchEvent(event);
    }
    
    // Computed properties
    get selectedModelLabel() {
        return this.selectedModelInfo ? this.selectedModelInfo.label : this.selectedModel;
    }
    
    get formattedAttributionValue() {
        if (!this.attributionSummary) return '€0';
        return new Intl.NumberFormat('nl-NL', {
            style: 'currency',
            currency: 'EUR',
            minimumFractionDigits: 0,
            maximumFractionDigits: 0
        }).format(this.attributionSummary.totalAttributionValue);
    }
    
    get formattedAverageAttribution() {
        if (!this.attributionSummary) return '€0';
        return new Intl.NumberFormat('nl-NL', {
            style: 'currency',
            currency: 'EUR',
            minimumFractionDigits: 2,
            maximumFractionDigits: 2
        }).format(this.attributionSummary.averageAttributionPerJourney);
    }
    
    get hasCampaignData() {
        return this.campaignData && this.campaignData.length > 0;
    }
    
    get hasAttributionData() {
        return this.attributionData && this.attributionData.length > 0;
    }
    
    get hasTimelineData() {
        return this.timelineData && this.timelineData.labels && this.timelineData.labels.length > 0;
    }
    
    get hasJourneyData() {
        return this.journeyPerformanceData && this.journeyPerformanceData.datasets && this.journeyPerformanceData.datasets.length > 0;
    }
    
    // Event handlers for table and chart interactions
    handleCampaignSelection(event) {
        const selectedRows = event.detail.selectedRows;
        console.log('Selected campaigns:', selectedRows);
    }
    
    handleCampaignSort(event) {
        this.sortedBy = event.detail.fieldName;
        this.sortedDirection = event.detail.sortDirection;
        
        // Sort campaign data
        const fieldName = this.sortedBy;
        const isReverse = this.sortedDirection === 'desc';
        
        this.campaignData = [...this.campaignData].sort((a, b) => {
            let aVal = a[fieldName];
            let bVal = b[fieldName];
            
            if (typeof aVal === 'string') {
                aVal = aVal.toLowerCase();
                bVal = bVal.toLowerCase();
            }
            
            if (aVal < bVal) return isReverse ? 1 : -1;
            if (aVal > bVal) return isReverse ? -1 : 1;
            return 0;
        });
    }
    
    // Chart action handlers
    handleRefreshAttributionChart() {
        this.isLoadingAttribution = true;
        setTimeout(() => {
            this.renderAttributionChart();
            this.isLoadingAttribution = false;
        }, 500);
    }
    
    handleTimelineSettings() {
        console.log('Timeline settings clicked');
    }
    
    handleJourneyChartZoom() {
        console.log('Journey chart zoom clicked');
    }
    
    handleExportCampaignData() {
        const csvData = [
            ['Campaign Name', 'Attribution %', 'Conversions', 'Revenue Impact'],
            ...this.campaignData.map(campaign => [
                campaign.Name,
                campaign.Attribution_Percentage__c.toFixed(1),
                campaign.Conversions,
                campaign.Revenue_Impact.toFixed(2)
            ])
        ];
        this.downloadCSV(csvData, `campaigns_${this.selectedModel}_${Date.now()}.csv`);
    }
    
    handleViewCampaignDetails() {
        console.log('View campaign details clicked');
    }
}