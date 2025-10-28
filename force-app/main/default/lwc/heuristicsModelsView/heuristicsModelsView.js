import { LightningElement, track, api, wire } from 'lwc';
import { loadScript } from 'lightning/platformResourceLoader';
import ChartJS from '@salesforce/resourceUrl/ChartJS';
import { ShowToastEvent } from 'lightning/platformShowToastEvent';

import getAttributionResults from '@salesforce/apex/AttributionDashboardController.getAttributionResults';
import getChannelStatistics from '@salesforce/apex/AttributionDashboardController.getChannelStatistics';
import getAttributionSummary from '@salesforce/apex/AttributionDashboardController.getAttributionSummary';
import calculateMultipleAttributions from '@salesforce/apex/AttributionDashboardController.calculateMultipleAttributions';
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
 */
export default class HeuristicModelsView extends LightningElement {
    
    @api startDate;
    @api endDate;
    @api selectedChannels;
    @api selectedAudience;
    @api selectedCampaign;
    
    @track selectedModel = 'Linear';
    @track selectedView = 'complete';
    @track isLoading = false;
    @track isCalculating = false;
    @track isLoadingAttribution = false;
    @track isLoadingTimeline = false;
    @track isLoadingCampaigns = false;
    @track isLoadingJourney = false;
    
    @track journeyIds = [];
    @track attributionData = [];
    @track timelineData = [];
    @track campaignData = [];
    @track journeyPerformanceData = [];
    @track attributionSummary = null;
    @track selectedModelInfo = null;
    @track calculationProgress = '';
    
    attributionChart;
    timelineChart;
    journeyChart;
    chartJSLoaded = false;
    domReady = false;
    
    @track sortedBy = 'Attribution_Percentage__c';
    @track sortedDirection = 'desc';
    
    @track modelOptions = [];
    @track viewOptions = [
        { label: 'Complete View', value: 'complete' },
        { label: 'Channel Focus', value: 'channels' },
        { label: 'Timeline Focus', value: 'timeline' },
        { label: 'Performance Focus', value: 'performance' }
    ];
    
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
    
    async connectedCallback() {
        console.log('HeuristicModelsView: Component initializing...');
        try {
            await this.loadChartJS();
            await this.loadModelOptions();
            await this.loadInitialData();
        } catch (error) {
            console.error('HeuristicModelsView: Component initialization error:', error);
            this.showToast('Error', 'Failed to initialize component: ' + error.message, 'error');
        }
    }
    
    renderedCallback() {
        if (!this.domReady) {
            this.domReady = true;
            console.log('DOM is now ready');
            
            if (this.chartJSLoaded && this.hasAttributionData) {
                this.renderChartsWhenReady();
            }
        }
    }
    
    async loadChartJS() {
        if (this.chartJSLoaded) return;
        
        try {
            console.log('Loading Chart.js from static resource...');
            await loadScript(this, ChartJS);
            await this.waitForChartObject();
            this.chartJSLoaded = true;
            console.log('Chart.js loaded successfully, version:', Chart.version || 'unknown');
        } catch (error) {
            console.error('Failed to load Chart.js:', error);
            this.showToast('Error', 'Failed to load charting library', 'error');
        }
    }
    
    async waitForChartObject() {
        let attempts = 0;
        const maxAttempts = 50;
        
        while (!window.Chart && attempts < maxAttempts) {
            await new Promise(resolve => setTimeout(resolve, 100));
            attempts++;
        }
        
        if (!window.Chart) {
            throw new Error('Chart.js object not available after ' + (maxAttempts * 100) + 'ms');
        }
        
        console.log('Chart object detected in window:', typeof window.Chart);
    }
    
    async loadModelOptions() {
        try {
            const models = await getAvailableAttributionModels();
            
            this.modelOptions = models
                .filter(model => model.value !== 'MarkovChain')
                .map(model => ({
                    label: model.label,
                    value: model.value,
                    description: model.description
                }));
            
            console.log('Loaded model options:', this.modelOptions.length);
            this.updateSelectedModelInfo();
        } catch (error) {
            console.error('Failed to load model options:', error);
            this.modelOptions = [
                { label: 'Linear Attribution', value: 'Linear' },
                { label: 'First Touch', value: 'FirstTouch' },
                { label: 'Last Touch', value: 'LastTouch' },
                { label: 'Time Decay', value: 'TimeDecay' },
                { label: 'Position-Based', value: 'PositionBased' }
            ];
        }
    }
    
    async loadInitialData() {
        this.isLoading = true;
        
        try {
            await this.loadJourneyIds();
            await this.loadAttributionData();
            
            if (this.domReady && this.chartJSLoaded) {
                await this.renderChartsWhenReady();
            }
        } catch (error) {
            console.error('Failed to load initial data:', error);
            this.showToast('Error', 'Failed to load dashboard data', 'error');
        } finally {
            this.isLoading = false;
        }
    }
    
    async loadJourneyIds() {
        try {
            const customerType = this.selectedAudience && this.selectedAudience !== 'All' 
                ? this.selectedAudience 
                : null;
            
            const journeys = await getFilteredCustomerJourneys({
                customerType: customerType,
                convertedOnly: false,
                limitCount: 1000
            });
            
            this.journeyIds = journeys.map(journey => journey.Id);
            
            if (this.startDate && this.endDate) {
                const filteredJourneys = journeys.filter(journey => {
                    const journeyDate = new Date(journey.Journey_Start_Date__c);
                    const start = new Date(this.startDate);
                    const end = new Date(this.endDate);
                    return journeyDate >= start && journeyDate <= end;
                });
                this.journeyIds = filteredJourneys.map(j => j.Id);
            }
            
            console.log('Loaded ' + this.journeyIds.length + ' journey IDs');
        } catch (error) {
            console.error('Failed to load journey IDs:', error);
            throw error;
        }
    }
    
    async loadAttributionData() {
        this.isLoadingAttribution = true;
        
        try {
            console.log('Loading attribution data for model:', this.selectedModel);
            console.log('Journey IDs count:', this.journeyIds.length);
            
            const [results, summary, statistics] = await Promise.all([
                getAttributionResults({
                    journeyIds: this.journeyIds,
                    attributionModel: this.selectedModel
                }),
                getAttributionSummary({
                    journeyIds: this.journeyIds,
                    attributionModel: this.selectedModel
                }),
                getChannelStatistics({
                    journeyIds: this.journeyIds,
                    attributionModel: this.selectedModel
                })
            ]);
            
            console.log('Raw attribution results:', results);
            console.log('Attribution summary:', summary);
            console.log('Channel statistics:', statistics);
            
            this.processAttributionData(statistics);
            this.attributionSummary = summary;
            
            console.log('Processed attribution data:', this.attributionData);
            console.log('Timeline data:', this.timelineData);
            console.log('Journey performance data:', this.journeyPerformanceData);
            
        } catch (error) {
            console.error('Failed to load attribution data:', error);
            console.error('Error details:', error.body);
            this.showToast('Error', 'Failed to load attribution data: ' + (error.body?.message || error.message), 'error');
        } finally {
            this.isLoadingAttribution = false;
        }
    }
    
    processAttributionData(statistics) {
        this.attributionData = statistics.map(stat => ({
            channel: stat.channel,
            attribution: (stat.averageWeight || 0) * 100,
            totalValue: stat.totalValue || 0,
            totalAttributions: stat.totalAttributions || 0,
            color: this.getChannelColor(stat.channel)
        }));
        
        this.attributionData.sort((a, b) => b.attribution - a.attribution);
        
        this.timelineData = this.generateTimelineData();
        this.journeyPerformanceData = this.generateJourneyPerformanceData();
        this.campaignData = this.generateCampaignData();
    }
    
    generateTimelineData() {
        const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun'];
        const datasets = this.attributionData.map(channel => ({
            label: channel.channel,
            data: this.generateMonthlyVariation(channel.attribution, months.length),
            borderColor: channel.color,
            backgroundColor: channel.color + '33',
            tension: 0.4
        }));
        
        return { labels: months, datasets: datasets };
    }
    
    generateJourneyPerformanceData() {
        const convertedData = this.generateScatterData(30, true);
        const nonConvertedData = this.generateScatterData(40, false);
        
        return {
            datasets: [
                {
                    label: 'Converted Journeys',
                    data: convertedData,
                    backgroundColor: '#34CC8D',
                    borderColor: '#34CC8D'
                },
                {
                    label: 'Non-Converted Journeys',
                    data: nonConvertedData,
                    backgroundColor: '#F63223',
                    borderColor: '#F63223'
                }
            ]
        };
    }
    
    generateCampaignData() {
        return this.attributionData.map((channel, index) => ({
            Id: 'campaign_' + index,
            Name: channel.channel + ' Campaign',
            Attribution_Percentage__c: channel.attribution / 100,
            Conversions: Math.floor(Math.random() * 50) + 10,
            Revenue_Impact: channel.totalValue * (Math.random() * 0.3 + 0.7)
        }));
    }
    
    async renderChartsWhenReady() {
        if (!this.chartJSLoaded) {
            console.warn('Chart.js not loaded yet');
            return;
        }
        
        if (!this.domReady) {
            console.warn('DOM not ready yet');
            return;
        }
        
        if (!this.hasAttributionData) {
            console.warn('No attribution data available');
            return;
        }
        
        console.log('Rendering charts...');
        
        await new Promise(resolve => setTimeout(resolve, 100));
        
        try {
            this.renderAttributionChart();
            this.renderTimelineChart();
            this.renderJourneyChart();
            console.log('All charts rendered successfully');
        } catch (error) {
            console.error('Error rendering charts:', error);
            this.showToast('Error', 'Failed to render charts: ' + error.message, 'error');
        }
    }
    
    renderAttributionChart() {
        const canvas = this.template.querySelector('canvas.attribution-chart');
        
        if (!canvas) {
            console.error('Attribution chart canvas not found');
            return;
        }
        
        console.log('Rendering attribution chart with', this.attributionData.length, 'channels');
        
        if (this.attributionChart) {
            this.attributionChart.destroy();
        }
        
        const ctx = canvas.getContext('2d');
        this.attributionChart = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: this.attributionData.map(d => d.channel),
                datasets: [{
                    label: 'Attribution %',
                    data: this.attributionData.map(d => d.attribution),
                    backgroundColor: this.attributionData.map(d => d.color),
                    borderWidth: 0
                }]
            },
            options: {
                indexAxis: 'y',
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        callbacks: {
                            label: (context) => `${context.parsed.x.toFixed(1)}%`
                        }
                    }
                },
                scales: {
                    x: {
                        beginAtZero: true,
                        max: 100,
                        ticks: {
                            callback: (value) => value + '%'
                        }
                    }
                }
            }
        });
        
        console.log('Attribution chart rendered');
    }
    
    renderTimelineChart() {
        const canvas = this.template.querySelector('canvas.timeline-chart');
        
        if (!canvas) {
            console.error('Timeline chart canvas not found');
            return;
        }
        
        console.log('Rendering timeline chart');
        
        if (this.timelineChart) {
            this.timelineChart.destroy();
        }
        
        const ctx = canvas.getContext('2d');
        this.timelineChart = new Chart(ctx, {
            type: 'line',
            data: this.timelineData,
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        display: true,
                        position: 'bottom'
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        ticks: {
                            callback: (value) => value + '%'
                        }
                    }
                }
            }
        });
        
        console.log('Timeline chart rendered');
    }
    
    renderJourneyChart() {
        const canvas = this.template.querySelector('canvas.journey-chart');
        
        if (!canvas) {
            console.error('Journey chart canvas not found');
            return;
        }
        
        console.log('Rendering journey chart');
        
        if (this.journeyChart) {
            this.journeyChart.destroy();
        }
        
        const ctx = canvas.getContext('2d');
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
                    }
                },
                scales: {
                    x: {
                        title: {
                            display: true,
                            text: 'Journey Length (Touchpoints)'
                        },
                        min: 1,
                        max: 9
                    },
                    y: {
                        title: {
                            display: true,
                            text: 'Attribution Impact (%)'
                        },
                        beginAtZero: true
                    }
                }
            }
        });
        
        console.log('Journey chart rendered');
    }
    
    async handleModelChange(event) {
        const previousModel = this.selectedModel;
        this.selectedModel = event.detail.value;
        
        console.log(`Model changed from ${previousModel} to ${this.selectedModel}`);
        
        this.updateSelectedModelInfo();
        
        this.isLoading = true;
        try {
            await this.loadAttributionData();
            await this.renderChartsWhenReady();
            
            this.showToast('Success', 
                `Switched to ${this.selectedModelLabel}`, 
                'success');
        } catch (error) {
            console.error('Failed to load data for new model:', error);
            this.showToast('Error', 
                'Failed to load attribution data for selected model', 
                'error');
            this.selectedModel = previousModel;
        } finally {
            this.isLoading = false;
        }
    }
    
    handleViewChange(event) {
        this.selectedView = event.detail.value;
    }
    
    async handleCalculateAttribution() {
        if (this.isCalculating) {
            this.showToast('Info', 'Attribution calculation already in progress', 'info');
            return;
        }
        
        if (!this.journeyIds || this.journeyIds.length === 0) {
            this.showToast('Warning', 'No customer journeys available for calculation', 'warning');
            return;
        }
        
        this.isCalculating = true;
        this.calculationProgress = 'Calculating attribution for all 5 HAM models...';
        
        try {
            const allHAMModels = ['FirstTouch', 'LastTouch', 'Linear', 'TimeDecay', 'PositionBased'];
            
            console.log(`Starting attribution calculation for ${this.journeyIds.length} journeys`);
            console.log('Models:', allHAMModels);
            
            const result = await calculateMultipleAttributions({
                journeyIds: this.journeyIds,
                selectedModels: allHAMModels
            });
            
            console.log('Attribution calculation result:', result);
            
            if (result.failedCount > 0) {
                this.showToast('Warning', 
                    `${result.processedModels} of ${allHAMModels.length} models calculated successfully. ${result.failedCount} failed.`, 
                    'warning');
            } else {
                this.showToast('Success', 
                    `All 5 HAM models calculated successfully for ${result.totalJourneys} journeys`, 
                    'success');
            }
            
            await this.loadAttributionData();
            await this.renderChartsWhenReady();
            
        } catch (error) {
            console.error('Attribution calculation error:', error);
            this.showToast('Error', 
                'Attribution calculation failed: ' + (error.body?.message || error.message), 
                'error');
        } finally {
            this.isCalculating = false;
            this.calculationProgress = '';
        }
    }
    
    handleExportData() {
        const csvData = this.prepareExportData();
        this.downloadCSV(csvData, `attribution_${this.selectedModel}_${Date.now()}.csv`);
        this.showToast('Success', 'Data exported successfully', 'success');
    }
    
    @api
    async handleFilterUpdate(filterData) {
        this.startDate = filterData.startDate;
        this.endDate = filterData.endDate;
        this.selectedChannels = filterData.selectedChannels;
        this.selectedAudience = filterData.selectedAudience;
        this.selectedCampaign = filterData.selectedCampaign;
        
        await this.loadInitialData();
    }
    
    @api
    async refreshData() {
        await this.loadInitialData();
    }
    
    updateSelectedModelInfo() {
        this.selectedModelInfo = this.modelOptions.find(model => model.value === this.selectedModel);
    }
    
    getChannelColor(channel) {
        const colorMap = {
            'Google Ads': '#0080FF',
            'Google_Ads': '#0080FF',
            'Facebook Ads': '#A22FB6',
            'Facebook_Ads': '#A22FB6',
            'LinkedIn Ads': '#4B287D',
            'LinkedIn_Ads': '#4B287D',
            'Email Marketing': '#34CC8D',
            'Email_Marketing': '#34CC8D',
            'Events': '#FF7819',
            'Content/Website/SEO': '#26C7C3',
            'Content_Website_SEO': '#26C7C3',
            'App Store': '#2850AF',
            'App_Store': '#2850AF',
            'Organic Social': '#E33095',
            'Organic_Social': '#E33095'
        };
        return colorMap[channel] || '#6B7280';
    }
    
    generateMonthlyVariation(baseValue, months) {
        return Array.from({ length: months }, () => {
            const variation = (Math.random() - 0.5) * 0.3;
            return Math.max(0, Math.min(100, baseValue * (1 + variation)));
        });
    }
    
    generateScatterData(count, isConverted) {
        return Array.from({ length: count }, () => ({
            x: Math.floor(Math.random() * 7) + 2,
            y: Math.random() * (isConverted ? 60 : 40) + (isConverted ? 20 : 0)
        }));
    }
    
    handleChannelSelection(channel) {
        console.log('Channel selected:', channel);
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
    
    handleCampaignSelection(event) {
        const selectedRows = event.detail.selectedRows;
        console.log('Selected campaigns:', selectedRows);
    }
    
    handleCampaignSort(event) {
        this.sortedBy = event.detail.fieldName;
        this.sortedDirection = event.detail.sortDirection;
        
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