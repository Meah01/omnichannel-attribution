import { LightningElement, track, wire } from 'lwc';
import getDashboardConfiguration from '@salesforce/apex/AttributionDashboardController.getDashboardConfiguration';
import { ShowToastEvent } from 'lightning/platformShowToastEvent';

/**
 * Main Attribution Dashboard Component
 * 
 * Central dashboard orchestrating all attribution analysis views including
 * Heuristic Models, Channel Removal, Conversion Contribution, Model A/B Testing,
 * and Forecasting. Manages global filtering and data coordination across child components.
 * 
 * Author: Alexandru Constantinescu
 * Project: Omnichannel Attribution Platform
 * Version: 1.0
 */
export default class AttributionDashboard extends LightningElement {
    
    // Dashboard State
    @track activeTabValue = 'heuristics-models';
    @track isLoading = false;
    
    // Global Filters
    @track startDate = null;
    @track endDate = null;
    @track selectedChannels = [];
    @track selectedAudience = 'All';
    @track selectedCampaign = '';
    
    // Filter Options
    @track channelOptions = [];
    @track audienceOptions = [];
    
    // Configuration Data
    dashboardConfig = null;
    
    /**
     * Component initialization
     */
    connectedCallback() {
        this.initializeDateFilters();
        this.initializeFilterOptions();
    }
    
    /**
     * Wire dashboard configuration data
     */
    @wire(getDashboardConfiguration)
    wiredDashboardConfig({ error, data }) {
        if (data) {
            this.dashboardConfig = data;
            this.handleConfigurationSuccess(data);
        } else if (error) {
            this.handleConfigurationError(error);
        }
    }
    
    /**
     * Initialize default date range (last 90 days)
     */
    initializeDateFilters() {
        const today = new Date();
        const ninetyDaysAgo = new Date(today.getTime() - (90 * 24 * 60 * 60 * 1000));
        
        this.endDate = this.formatDateForInput(today);
        this.startDate = this.formatDateForInput(ninetyDaysAgo);
    }
    
    /**
     * Initialize filter option arrays
     */
    initializeFilterOptions() {
        // Channel options based on project specification
        this.channelOptions = [
            { label: 'Google Ads', value: 'Google Ads' },
            { label: 'Facebook Ads', value: 'Facebook Ads' },
            { label: 'LinkedIn Ads', value: 'LinkedIn Ads' },
            { label: 'Email Marketing', value: 'Email Marketing' },
            { label: 'Events', value: 'Events' },
            { label: 'Content/Website/SEO', value: 'Content/Website/SEO' },
            { label: 'App Store', value: 'App Store' },
            { label: 'Organic Social', value: 'Organic Social' }
        ];
        
        // Audience options
        this.audienceOptions = [
            { label: 'All', value: 'All' },
            { label: 'B2C', value: 'B2C' },
            { label: 'B2B', value: 'B2B' }
        ];
    }
    
    /**
     * Handle successful configuration load
     */
    handleConfigurationSuccess(config) {
        console.log('Dashboard configuration loaded:', config);
        
        // Update audience options if customer types are provided
        if (config.customerTypes && config.customerTypes.length > 0) {
            this.audienceOptions = [
                { label: 'All', value: 'All' },
                ...config.customerTypes.map(type => ({
                    label: type,
                    value: type
                }))
            ];
        }
    }
    
    /**
     * Handle configuration load error
     */
    handleConfigurationError(error) {
        console.error('Failed to load dashboard configuration:', error);
        this.showToast('Warning', 'Dashboard configuration could not be loaded. Some features may be limited.', 'warning');
    }
    
    /**
     * Handle tab change
     */
    handleTabChange(event) {
        this.activeTabValue = event.target.value;
        
        // Log analytics for tab usage
        console.log('Tab changed to:', this.activeTabValue);
        
        // Trigger data refresh for certain tabs that need fresh data
        if (this.activeTabValue === 'model-ab-testing') {
            this.refreshChildComponent('c-attribution-model-comparison');
        }
    }
    
    /**
     * Handle start date change
     */
    handleStartDateChange(event) {
        this.startDate = event.target.value;
        this.validateDateRange();
        this.propagateFilterChange();
    }
    
    /**
     * Handle end date change
     */
    handleEndDateChange(event) {
        this.endDate = event.target.value;
        this.validateDateRange();
        this.propagateFilterChange();
    }
    
    /**
     * Handle channel selection change
     */
    handleChannelChange(event) {
        this.selectedChannels = event.target.value || [];
        this.propagateFilterChange();
    }
    
    /**
     * Handle audience selection change
     */
    handleAudienceChange(event) {
        this.selectedAudience = event.target.value;
        this.propagateFilterChange();
    }
    
    /**
     * Handle campaign search change
     */
    handleCampaignSearchChange(event) {
        this.selectedCampaign = event.target.value;
        
        // Debounce campaign search
        if (this.campaignSearchTimeout) {
            clearTimeout(this.campaignSearchTimeout);
        }
        
        this.campaignSearchTimeout = setTimeout(() => {
            this.propagateFilterChange();
        }, 500); // 500ms debounce
    }
    
    /**
     * Handle filter change from child components
     */
    handleFilterChange(event) {
        const { filterType, filterValue } = event.detail;
        
        switch (filterType) {
            case 'dateRange':
                this.startDate = filterValue.startDate;
                this.endDate = filterValue.endDate;
                break;
            case 'channels':
                this.selectedChannels = filterValue;
                break;
            case 'audience':
                this.selectedAudience = filterValue;
                break;
            case 'campaign':
                this.selectedCampaign = filterValue;
                break;
        }
        
        this.propagateFilterChange();
    }
    
    /**
     * Handle manual data refresh
     */
    handleRefreshData() {
        this.isLoading = true;
        
        try {
            // Refresh active child component
            this.refreshActiveChildComponent();
            
            // Refresh configuration
            this.refreshConfiguration();
            
            this.showToast('Success', 'Dashboard data refreshed successfully', 'success');
            
        } catch (error) {
            console.error('Refresh data error:', error);
            this.showToast('Error', 'Failed to refresh dashboard data', 'error');
        } finally {
            // Set timeout to ensure loading state is visible
            setTimeout(() => {
                this.isLoading = false;
            }, 1000);
        }
    }
    
    /**
     * Validate date range
     */
    validateDateRange() {
        if (this.startDate && this.endDate) {
            const start = new Date(this.startDate);
            const end = new Date(this.endDate);
            
            if (start > end) {
                this.showToast('Error', 'Start date cannot be after end date', 'error');
                return false;
            }
            
            // Check for maximum date range (1 year)
            const daysDiff = (end - start) / (1000 * 60 * 60 * 24);
            if (daysDiff > 365) {
                this.showToast('Warning', 'Date range limited to maximum 365 days for optimal performance', 'warning');
                
                // Adjust start date to maintain 365 days range
                const adjustedStart = new Date(end.getTime() - (365 * 24 * 60 * 60 * 1000));
                this.startDate = this.formatDateForInput(adjustedStart);
            }
        }
        
        return true;
    }
    
    /**
     * Propagate filter changes to child components
     */
    propagateFilterChange() {
        // Create filter change event
        const filterData = {
            startDate: this.startDate,
            endDate: this.endDate,
            selectedChannels: this.selectedChannels,
            selectedAudience: this.selectedAudience,
            selectedCampaign: this.selectedCampaign
        };
        
        // Propagate to child components
        this.template.querySelectorAll('c-heuristics-models-view, c-attribution-model-comparison').forEach(child => {
            if (child.handleFilterUpdate && typeof child.handleFilterUpdate === 'function') {
                child.handleFilterUpdate(filterData);
            }
        });
        
        // Log filter change for analytics
        console.log('Filters updated:', filterData);
    }
    
    /**
     * Refresh active child component
     */
    refreshActiveChildComponent() {
        const activeChild = this.getActiveChildComponent();
        if (activeChild && activeChild.refreshData && typeof activeChild.refreshData === 'function') {
            activeChild.refreshData();
        } else if (this.activeTabValue === 'heuristics-models') {
            // Component not yet available - log for debugging
            console.log('heuristicsModelsView component not yet available');
        }
    }
    
    /**
     * Get active child component
     */
    getActiveChildComponent() {
        switch (this.activeTabValue) {
            case 'heuristics-models':
                return this.template.querySelector('c-heuristics-models-view');
            case 'model-ab-testing':
                return this.template.querySelector('c-attribution-model-comparison');
            default:
                return null;
        }
    }
    
    /**
     * Refresh specific child component
     */
    refreshChildComponent(selector) {
        const child = this.template.querySelector(selector);
        if (child && child.refreshData && typeof child.refreshData === 'function') {
            child.refreshData();
        }
    }
    
    /**
     * Refresh dashboard configuration
     */
    refreshConfiguration() {
        // Force refresh of wired configuration data
        // This will re-trigger the wire method
        return getDashboardConfiguration()
            .then(result => {
                this.dashboardConfig = result;
                this.handleConfigurationSuccess(result);
            })
            .catch(error => {
                this.handleConfigurationError(error);
            });
    }
    
    /**
     * Format date for HTML input element
     */
    formatDateForInput(date) {
        if (!date) return null;
        
        const d = new Date(date);
        const month = String(d.getMonth() + 1).padStart(2, '0');
        const day = String(d.getDate()).padStart(2, '0');
        const year = d.getFullYear();
        
        return `${year}-${month}-${day}`;
    }
    
    /**
     * Show toast message
     */
    showToast(title, message, variant) {
        const event = new ShowToastEvent({
            title: title,
            message: message,
            variant: variant
        });
        this.dispatchEvent(event);
    }
    
    /**
     * Get filter summary for display
     */
    get filterSummary() {
        const summary = [];
        
        if (this.startDate && this.endDate) {
            summary.push(`${this.startDate} to ${this.endDate}`);
        }
        
        if (this.selectedChannels && this.selectedChannels.length > 0) {
            summary.push(`${this.selectedChannels.length} channels`);
        }
        
        if (this.selectedAudience && this.selectedAudience !== 'All') {
            summary.push(this.selectedAudience);
        }
        
        if (this.selectedCampaign) {
            summary.push(`Campaign: ${this.selectedCampaign}`);
        }
        
        return summary.join(' • ');
    }
    
    /**
     * Check if dashboard is ready
     */
    get isDashboardReady() {
        return this.dashboardConfig !== null && !this.isLoading;
    }
    
    /**
     * Get active tab label for analytics
     */
    get activeTabLabel() {
        const tab = this.template.querySelector(`lightning-tab[value="${this.activeTabValue}"]`);
        return tab ? tab.label : this.activeTabValue;
    }
}