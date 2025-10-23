// markovChainDashboard.js
import { LightningElement, track, wire, api } from 'lwc';
import { loadScript } from 'lightning/platformResourceLoader';
import { ShowToastEvent } from 'lightning/platformShowToastEvent';
import { refreshApex } from '@salesforce/apex';
import { getRecord } from 'lightning/uiRecordApi';

// Import Apex methods
import getNetworkVisualizationData from '@salesforce/apex/AttributionDashboardController.getNetworkVisualizationData';
import getTransitionMatrixStatistics from '@salesforce/apex/AttributionDashboardController.getTransitionMatrixStatistics';
import getStageDetectionSummary from '@salesforce/apex/AttributionDashboardController.getStageDetectionSummary';
import getMarkovChainDashboardData from '@salesforce/apex/AttributionDashboardController.getMarkovChainDashboardData';
import getStageDetectionQualityMetrics from '@salesforce/apex/AttributionDashboardController.getStageDetectionQualityMetrics';
import triggerMatrixRecalculation from '@salesforce/apex/AttributionDashboardController.triggerMatrixRecalculation';

// Import static resources
import D3JS from '@salesforce/resourceUrl/D3JS';
import ChartJS from '@salesforce/resourceUrl/ChartJS';

export default class MarkovChainDashboard extends LightningElement {
    // API properties for configuration
    @api cardTitle = 'Markov Chain Attribution Analysis';
    @api defaultCustomerType = 'B2C';
    @api defaultVisualization = 'network';
    @api autoRefreshInterval = 0;
    @api showInsightsPanel = false;
    @api showQualityMetrics = false;
    @api enableNetworkInteraction = false;
    @api minProbabilityThreshold = 5;
    @api recordContextMode = false;
    @api relatedRecordField = '';
    @api recordId; // For record page context
    
    // State management
    @track customerType = 'B2C';
    @track currentVisualization = 'network';
    @track isLoading = true;
    @track error = null;
    
    // Data properties
    @track networkData = null;
    @track matrixStatistics = null;
    @track stageDetectionQuality = null;
    
    // Visualization controls
    @track zoomLevel = 1;
    @track minProbabilityFilter = 5;
    @track matrixSortBy = 'probability';
    @track flowType = 'conversion';
    @track sankeyGroupBy = 'stage';
    @track minFlowValue = 0.01;
    
    // Libraries loaded status
    d3Loaded = false;
    chartJsLoaded = false;
    
    // Visualization instances
    networkChart = null;
    matrixChart = null;
    flowChart = null;
    sankeyChart = null;
    
    // Auto-refresh interval ID
    refreshIntervalId = null;
    
    // Record context data
    @track recordData = null;
    
    // Wire method to get current record data when in record context mode
    @wire(getRecord, { recordId: '$recordId', fields: ['Id'] })
    wiredRecord({ error, data }) {
        if (data) {
            this.recordData = data;
            if (this.recordContextMode) {
                // Reload dashboard data when record context changes
                this.loadDashboardData();
            }
        } else if (error) {
            console.error('Error loading record data:', error);
        }
    }
    
    // Getters for conditional rendering
    get shouldShowInsightsPanel() {
        return this.showInsightsPanel && this.networkData;
    }
    
    get shouldShowQualityMetrics() {
        return this.showQualityMetrics && this.stageDetectionQuality;
    }
    
    // Getters for button variants
    get networkButtonVariant() { return this.currentVisualization === 'network' ? 'brand' : 'neutral'; }
    get matrixButtonVariant() { return this.currentVisualization === 'matrix' ? 'brand' : 'neutral'; }
    get flowButtonVariant() { return this.currentVisualization === 'flow' ? 'brand' : 'neutral'; }
    get sankeyButtonVariant() { return this.currentVisualization === 'sankey' ? 'brand' : 'neutral'; }
    get b2cButtonVariant() { return this.customerType === 'B2C' ? 'brand' : 'neutral'; }
    get b2bButtonVariant() { return this.customerType === 'B2B' ? 'brand' : 'neutral'; }
    
    // Getters for visualization display
    get showNetworkGraph() { return this.currentVisualization === 'network'; }
    get showTransitionMatrix() { return this.currentVisualization === 'matrix'; }
    get showFlowDiagram() { return this.currentVisualization === 'flow'; }
    get showSankeyChart() { return this.currentVisualization === 'sankey'; }
    
    // Data getters with safe access
    get totalStates() {
        return this.networkData?.totalStates || 0;
    }
    
    get totalTransitions() {
        return this.networkData?.totalTransitions || 0;
    }
    
    get conversionRate() {
        return this.networkData?.networkStatistics?.conversionRate || 0;
    }
    
    get lastUpdated() {
        if (this.networkData?.lastCalculated) {
            try {
                return new Date(this.networkData.lastCalculated).toLocaleString();
            } catch (e) {
                return 'Invalid date';
            }
        }
        return 'Never';
    }
    
    get conversionPaths() {
        return this.networkData?.conversionPaths?.slice(0, 5) || [];
    }
    
    get stageTransitions() {
        return this.networkData?.stageAnalysis?.stageTransitions?.slice(0, 10) || [];
    }
    
    get networkStatistics() {
        return this.networkData?.networkStatistics || {};
    }
    
    get networkDensity() {
        return this.networkStatistics?.networkDensity || 0;
    }
    
    get averageProbability() {
        return this.networkStatistics?.averageProbability || 0;
    }
    
    get maxNodeDegree() {
        return this.networkStatistics?.maxNodeDegree || 0;
    }
    
    get averageNodeDegree() {
        return this.networkStatistics?.averageNodeDegree || 0;
    }
    
    get stageDetectionAccuracy() {
        return this.stageDetectionQuality?.averageStageDetectionAccuracy || 0;
    }
    
    get errorRate() {
        return this.stageDetectionQuality?.averageErrorRate || 0;
    }
    
    get invalidDetections() {
        return this.stageDetectionQuality?.totalInvalidDetections || 0;
    }
    
    get accuracyIcon() {
        const accuracy = this.stageDetectionAccuracy;
        if (accuracy >= 90) return 'utility:success';
        if (accuracy >= 80) return 'utility:warning';
        return 'utility:error';
    }
    
    get accuracyVariant() {
        const accuracy = this.stageDetectionAccuracy;
        if (accuracy >= 90) return 'success';
        if (accuracy >= 80) return 'warning';
        return 'error';
    }
    
    // Control options
    get matrixSortOptions() {
        return [
            { label: 'Probability (Desc)', value: 'probability' },
            { label: 'Alphabetical', value: 'alphabetical' },
            { label: 'Count (Desc)', value: 'count' }
        ];
    }
    
    get flowTypeOptions() {
        return [
            { label: 'Conversion Paths', value: 'conversion' },
            { label: 'All Paths', value: 'all' },
            { label: 'Top Paths', value: 'top' }
        ];
    }
    
    get sankeyGroupOptions() {
        return [
            { label: 'By Stage', value: 'stage' },
            { label: 'By Channel', value: 'channel' },
            { label: 'By Time Period', value: 'time' }
        ];
    }
    
    // Lifecycle methods
    connectedCallback() {
        // Initialize from API properties with proper defaults
        this.customerType = this.defaultCustomerType || 'B2C';
        this.currentVisualization = this.defaultVisualization || 'network';
        this.minProbabilityFilter = this.minProbabilityThreshold || 5;
        
        // Boolean properties default to true (metadata handles this)
        // Only override if explicitly needed
        
        this.loadLibraries();
        this.setupAutoRefresh();
    }
    
    renderedCallback() {
        if (this.d3Loaded && this.chartJsLoaded && this.networkData && !this.isLoading) {
            // Use setTimeout to ensure DOM is ready
            setTimeout(() => {
                this.renderCurrentVisualization();
            }, 100);
        }
    }
    
    disconnectedCallback() {
        this.cleanupVisualization();
        this.clearAutoRefresh();
    }
    
    // Auto-refresh setup
    setupAutoRefresh() {
        if (this.autoRefreshInterval > 0) {
            this.refreshIntervalId = setInterval(() => {
                this.loadDashboardData();
            }, this.autoRefreshInterval * 60 * 1000); // Convert minutes to milliseconds
        }
    }
    
    clearAutoRefresh() {
        if (this.refreshIntervalId) {
            clearInterval(this.refreshIntervalId);
            this.refreshIntervalId = null;
        }
    }
    
    // Library loading with better error handling
    async loadLibraries() {
        try {
            // Load libraries sequentially to avoid conflicts
            await loadScript(this, D3JS);
            this.d3Loaded = true;
            
            await loadScript(this, ChartJS);
            this.chartJsLoaded = true;
            
            // Load initial data
            await this.loadDashboardData();
            
        } catch (error) {
            this.handleError('Failed to load visualization libraries', error);
        }
    }
    
    // Data loading with improved error handling
    async loadDashboardData() {
        this.isLoading = true;
        this.error = null;
        
        try {
            // Load comprehensive dashboard data with timeout
            const timeoutPromise = new Promise((_, reject) => 
                setTimeout(() => reject(new Error('Request timeout')), 30000)
            );
            
            const dataPromise = getMarkovChainDashboardData({ customerType: this.customerType });
            
            const dashboardData = await Promise.race([dataPromise, timeoutPromise]);
            
            if (dashboardData) {
                this.networkData = dashboardData.networkVisualization;
                this.matrixStatistics = dashboardData.matrixStatistics;
                
                // Load stage detection quality separately
                try {
                    const qualityData = await getStageDetectionQualityMetrics();
                    this.stageDetectionQuality = qualityData;
                } catch (qualityError) {
                    console.warn('Could not load stage detection quality metrics:', qualityError);
                    this.stageDetectionQuality = {};
                }
                
                // Render current visualization
                this.renderCurrentVisualization();
            } else {
                throw new Error('No data available for ' + this.customerType + ' customers');
            }
            
        } catch (error) {
            this.handleError('Failed to load dashboard data', error);
        } finally {
            this.isLoading = false;
        }
    }
    
    // Event handlers with improved validation
    handleVisualizationChange(event) {
        const newVisualization = event.target.dataset.visualization;
        if (newVisualization && newVisualization !== this.currentVisualization) {
            this.currentVisualization = newVisualization;
            this.cleanupVisualization();
            this.renderCurrentVisualization();
        }
    }
    
    async handleCustomerTypeChange(event) {
        const newCustomerType = event.target.dataset.customerType;
        if (newCustomerType && newCustomerType !== this.customerType) {
            this.customerType = newCustomerType;
            this.cleanupVisualization();
            await this.loadDashboardData();
        }
    }
    
    async handleRefresh() {
        try {
            await this.loadDashboardData();
            this.showToast('Success', 'Dashboard data refreshed successfully', 'success');
        } catch (error) {
            this.showToast('Error', 'Failed to refresh dashboard data', 'error');
        }
    }
    
    // Visualization control handlers with validation
    handleZoomChange(event) {
        if (!this.enableNetworkInteraction) return;
        
        const newZoom = parseFloat(event.target.value);
        if (!isNaN(newZoom) && newZoom > 0) {
            this.zoomLevel = newZoom;
            if (this.networkChart) {
                this.applyZoom();
            }
        }
    }
    
    handleProbabilityFilterChange(event) {
        const newFilter = parseInt(event.target.value, 10);
        if (!isNaN(newFilter) && newFilter >= 0 && newFilter <= 100) {
            this.minProbabilityFilter = newFilter;
            if (this.currentVisualization === 'matrix') {
                this.renderTransitionMatrix();
            }
        }
    }
    
    handleMatrixSortChange(event) {
        const newSort = event.target.value;
        if (newSort) {
            this.matrixSortBy = newSort;
            if (this.currentVisualization === 'matrix') {
                this.renderTransitionMatrix();
            }
        }
    }
    
    handleFlowTypeChange(event) {
        const newFlowType = event.target.value;
        if (newFlowType) {
            this.flowType = newFlowType;
            if (this.currentVisualization === 'flow') {
                this.renderFlowDiagram();
            }
        }
    }
    
    handleSankeyGroupChange(event) {
        const newGroup = event.target.value;
        if (newGroup) {
            this.sankeyGroupBy = newGroup;
            if (this.currentVisualization === 'sankey') {
                this.renderSankeyChart();
            }
        }
    }
    
    handleMinFlowChange(event) {
        const newMinFlow = parseFloat(event.target.value);
        if (!isNaN(newMinFlow) && newMinFlow >= 0) {
            this.minFlowValue = newMinFlow;
            if (this.currentVisualization === 'sankey') {
                this.renderSankeyChart();
            }
        }
    }
    
    handleFitToScreen() {
        if (!this.enableNetworkInteraction) return;
        
        if (this.networkChart) {
            this.fitNetworkToScreen();
        }
    }
    
    handleResetView() {
        if (!this.enableNetworkInteraction) return;
        
        this.zoomLevel = 1;
        if (this.networkChart) {
            this.resetNetworkView();
        }
    }
    
    handleShowAllPaths() {
        this.flowType = 'all';
        this.renderFlowDiagram();
    }
    
    handleShowTopPaths() {
        this.flowType = 'top';
        this.renderFlowDiagram();
    }
    
    // Visualization rendering with better error handling
    renderCurrentVisualization() {
        if (!this.networkData || !this.d3Loaded || this.isLoading) return;
        
        try {
            switch (this.currentVisualization) {
                case 'network':
                    this.renderNetworkGraph();
                    break;
                case 'matrix':
                    this.renderTransitionMatrix();
                    break;
                case 'flow':
                    this.renderFlowDiagram();
                    break;
                case 'sankey':
                    this.renderSankeyChart();
                    break;
                default:
                    console.warn('Unknown visualization type:', this.currentVisualization);
            }
        } catch (error) {
            this.handleError('Failed to render visualization', error);
        }
    }
    
    renderNetworkGraph() {
        const container = this.template.querySelector('.network-graph');
        if (!container || !this.networkData.allTransitions) return;
        
        try {
            // Clear previous chart
            container.innerHTML = '';
            
            const width = container.clientWidth || 800;
            const height = 600;
            
            // Check if D3 is available
            if (typeof window.d3 === 'undefined') {
                console.error('D3 library not loaded');
                return;
            }
            
            const d3 = window.d3;
            
            // Create SVG with better error handling
            const svg = d3.select(container)
                .append('svg')
                .attr('width', width)
                .attr('height', height)
                .attr('viewBox', `0 0 ${width} ${height}`)
                .style('max-width', '100%')
                .style('height', 'auto');
            
            // Process data for D3 with validation
            const nodes = this.processNodesForD3();
            const links = this.processLinksForD3();
            
            if (!nodes.length || !links.length) {
                this.showNoDataMessage(container, 'No network data available');
                return;
            }
            
            // Create force simulation
            const simulation = d3.forceSimulation(nodes)
                .force('link', d3.forceLink(links).id(d => d.id).distance(100))
                .force('charge', d3.forceManyBody().strength(-300))
                .force('center', d3.forceCenter(width / 2, height / 2))
                .force('collision', d3.forceCollide().radius(30));
            
            // Create links
            const link = svg.append('g')
                .attr('class', 'links')
                .selectAll('line')
                .data(links)
                .enter().append('line')
                .attr('stroke', '#999')
                .attr('stroke-opacity', 0.6)
                .attr('stroke-width', d => Math.sqrt(d.value * 10));
            
            // Create nodes with conditional interaction
            const node = svg.append('g')
                .attr('class', 'nodes')
                .selectAll('circle')
                .data(nodes)
                .enter().append('circle')
                .attr('r', d => Math.sqrt(d.size || 1) * 5)
                .attr('fill', d => this.getNodeColor(d.stage));
            
            // Add drag behavior only if network interaction is enabled
            if (this.enableNetworkInteraction) {
                node.call(d3.drag()
                    .on('start', dragstarted)
                    .on('drag', dragged)
                    .on('end', dragended));
            }
            
            // Add labels
            const label = svg.append('g')
                .attr('class', 'labels')
                .selectAll('text')
                .data(nodes)
                .enter().append('text')
                .attr('text-anchor', 'middle')
                .attr('dy', '.35em')
                .style('font-size', '10px')
                .style('fill', '#333')
                .text(d => d.label);
            
            // Add tooltips
            node.append('title')
                .text(d => `${d.id}\nType: ${d.stage}\nConnections: ${d.connections || 0}`);
            
            // Update positions on tick
            simulation.on('tick', () => {
                link
                    .attr('x1', d => d.source.x)
                    .attr('y1', d => d.source.y)
                    .attr('x2', d => d.target.x)
                    .attr('y2', d => d.target.y);
                
                node
                    .attr('cx', d => d.x)
                    .attr('cy', d => d.y);
                
                label
                    .attr('x', d => d.x)
                    .attr('y', d => d.y);
            });
            
            // Drag functions
            function dragstarted(event, d) {
                if (!event.active) simulation.alphaTarget(0.3).restart();
                d.fx = d.x;
                d.fy = d.y;
            }
            
            function dragged(event, d) {
                d.fx = event.x;
                d.fy = event.y;
            }
            
            function dragended(event, d) {
                if (!event.active) simulation.alphaTarget(0);
                d.fx = null;
                d.fy = null;
            }
            
            this.networkChart = { svg, simulation, nodes, links };
            
        } catch (error) {
            console.error('Error rendering network graph:', error);
            this.showErrorMessage(container, 'Failed to render network graph');
        }
    }
    
    renderTransitionMatrix() {
        const container = this.template.querySelector('.transition-matrix');
        if (!container || !this.networkData.allTransitions) return;
        
        try {
            // Clear previous content
            container.innerHTML = '';
            
            // Filter and sort transitions
            const filteredTransitions = this.networkData.allTransitions
                .filter(t => (t.probability * 100) >= this.minProbabilityFilter);
            
            const sortedTransitions = this.sortTransitions(filteredTransitions);
            
            if (!sortedTransitions.length) {
                this.showNoDataMessage(container, 'No transitions match the current filter');
                return;
            }
            
            // Create matrix table
            const table = document.createElement('table');
            table.className = 'matrix-table';
            
            // Create header
            const headerRow = table.insertRow();
            headerRow.insertCell().innerHTML = '<strong>From State</strong>';
            headerRow.insertCell().innerHTML = '<strong>To State</strong>';
            headerRow.insertCell().innerHTML = '<strong>Probability</strong>';
            headerRow.insertCell().innerHTML = '<strong>Count</strong>';
            
            // Add data rows with validation
            sortedTransitions.forEach(transition => {
                if (transition.from && transition.to && typeof transition.probability === 'number') {
                    const row = table.insertRow();
                    row.insertCell().textContent = transition.from;
                    row.insertCell().textContent = transition.to;
                    row.insertCell().textContent = `${(transition.probability * 100).toFixed(2)}%`;
                    row.insertCell().textContent = transition.count || 'N/A';
                    
                    // Color code based on probability
                    const probability = transition.probability;
                    if (probability > 0.1) {
                        row.style.backgroundColor = '#d4edda';
                    } else if (probability > 0.05) {
                        row.style.backgroundColor = '#fff3cd';
                    }
                }
            });
            
            container.appendChild(table);
            
        } catch (error) {
            console.error('Error rendering transition matrix:', error);
            this.showErrorMessage(container, 'Failed to render transition matrix');
        }
    }
    
    renderFlowDiagram() {
        const container = this.template.querySelector('.flow-diagram');
        if (!container) return;
        
        try {
            // Clear previous content
            container.innerHTML = '';
            
            // Process flow data
            const flowData = this.processFlowData();
            
            if (!flowData.length) {
                this.showNoDataMessage(container, 'No flow data available');
                return;
            }
            
            // Create simple flow visualization
            this.createSimpleFlowDiagram(container, flowData);
            
        } catch (error) {
            console.error('Error rendering flow diagram:', error);
            this.showErrorMessage(container, 'Failed to render flow diagram');
        }
    }
    
    renderSankeyChart() {
        const container = this.template.querySelector('.sankey-chart');
        if (!container) return;
        
        try {
            // Clear previous content
            container.innerHTML = '';
            
            // Process Sankey data
            const sankeyData = this.processSankeyData();
            
            if (!sankeyData.length) {
                this.showNoDataMessage(container, 'No Sankey data available');
                return;
            }
            
            // Create Sankey visualization
            this.createSankeyVisualization(container, sankeyData);
            
        } catch (error) {
            console.error('Error rendering Sankey chart:', error);
            this.showErrorMessage(container, 'Failed to render Sankey chart');
        }
    }
    
    // Helper methods for showing messages
    showNoDataMessage(container, message) {
        container.innerHTML = `
            <div style="display: flex; align-items: center; justify-content: center; height: 200px; color: #666;">
                <div style="text-align: center;">
                    <div style="font-size: 24px; margin-bottom: 8px;">📊</div>
                    <div>${message}</div>
                </div>
            </div>
        `;
    }
    
    showErrorMessage(container, message) {
        container.innerHTML = `
            <div style="display: flex; align-items: center; justify-content: center; height: 200px; color: #d32f2f;">
                <div style="text-align: center;">
                    <div style="font-size: 24px; margin-bottom: 8px;">⚠️</div>
                    <div>${message}</div>
                </div>
            </div>
        `;
    }
    
    // Data processing methods with better validation
    processNodesForD3() {
        const nodes = [];
        const nodeMap = new Map();
        
        if (this.networkData.allTransitions && Array.isArray(this.networkData.allTransitions)) {
            this.networkData.allTransitions.forEach(transition => {
                if (transition.from && !nodeMap.has(transition.from)) {
                    const stage = this.extractStage(transition.from);
                    nodeMap.set(transition.from, {
                        id: transition.from,
                        label: this.formatNodeLabel(transition.from),
                        stage: stage,
                        size: 1,
                        connections: 0
                    });
                }
                
                if (transition.to && !nodeMap.has(transition.to)) {
                    const stage = this.extractStage(transition.to);
                    nodeMap.set(transition.to, {
                        id: transition.to,
                        label: this.formatNodeLabel(transition.to),
                        stage: stage,
                        size: 1,
                        connections: 0
                    });
                }
                
                if (nodeMap.has(transition.from)) {
                    nodeMap.get(transition.from).connections++;
                }
                if (nodeMap.has(transition.to)) {
                    nodeMap.get(transition.to).connections++;
                }
            });
        }
        
        return Array.from(nodeMap.values());
    }
    
    processLinksForD3() {
        if (!this.networkData.allTransitions || !Array.isArray(this.networkData.allTransitions)) {
            return [];
        }
        
        return this.networkData.allTransitions
            .filter(transition => transition.from && transition.to && typeof transition.probability === 'number')
            .map(transition => ({
                source: transition.from,
                target: transition.to,
                value: transition.probability,
                weight: transition.weight || 1
            }));
    }
    
    processFlowData() {
        if (!this.networkData.conversionPaths) return [];
        
        switch (this.flowType) {
            case 'conversion':
                return this.networkData.conversionPaths || [];
            case 'top':
                return (this.networkData.conversionPaths || []).slice(0, 10);
            case 'all':
            default:
                return this.networkData.allTransitions || [];
        }
    }
    
    processSankeyData() {
        const data = this.networkData.allTransitions || [];
        
        switch (this.sankeyGroupBy) {
            case 'stage':
                return this.groupByStage(data);
            case 'channel':
                return this.groupByChannel(data);
            case 'time':
                return this.groupByTime(data);
            default:
                return data;
        }
    }
    
    // Helper methods
    getNodeColor(stage) {
        const colorMap = {
            'Awareness': '#ff7f0e',
            'Interest': '#2ca02c',
            'Consideration': '#d62728',
            'Conversion': '#9467bd',
            'Retention': '#17becf',
            'Entry': '#1f77b4',
            'Success': '#2ca02c',
            'Exit': '#d62728'
        };
        return colorMap[stage] || '#7f7f7f';
    }
    
    extractStage(stateName) {
        if (typeof stateName === 'string' && stateName.includes('_')) {
            const parts = stateName.split('_');
            return parts[parts.length - 1];
        }
        return 'Unknown';
    }
    
    formatNodeLabel(stateName) {
        if (typeof stateName === 'string') {
            if (stateName.length > 15) {
                return stateName.substring(0, 12) + '...';
            }
            return stateName;
        }
        return 'Unknown';
    }
    
    sortTransitions(transitions) {
        if (!Array.isArray(transitions)) return [];
        
        switch (this.matrixSortBy) {
            case 'probability':
                return transitions.sort((a, b) => (b.probability || 0) - (a.probability || 0));
            case 'alphabetical':
                return transitions.sort((a, b) => (a.from || '').localeCompare(b.from || ''));
            case 'count':
                return transitions.sort((a, b) => (b.count || 0) - (a.count || 0));
            default:
                return transitions;
        }
    }
    
    groupByStage(data) {
        const grouped = {};
        if (Array.isArray(data)) {
            data.forEach(transition => {
                const fromStage = this.extractStage(transition.from);
                const toStage = this.extractStage(transition.to);
                const key = `${fromStage} → ${toStage}`;
                
                if (!grouped[key]) {
                    grouped[key] = { value: 0, count: 0 };
                }
                grouped[key].value += transition.probability || 0;
                grouped[key].count += 1;
            });
        }
        
        return Object.entries(grouped).map(([key, value]) => ({
            source: key.split(' → ')[0],
            target: key.split(' → ')[1],
            value: value.value,
            count: value.count
        }));
    }
    
    groupByChannel(data) {
        const grouped = {};
        if (Array.isArray(data)) {
            data.forEach(transition => {
                const fromChannel = (transition.from || '').split('_')[0];
                const toChannel = (transition.to || '').split('_')[0];
                const key = `${fromChannel} → ${toChannel}`;
                
                if (!grouped[key]) {
                    grouped[key] = { value: 0, count: 0 };
                }
                grouped[key].value += transition.probability || 0;
                grouped[key].count += 1;
            });
        }
        
        return Object.entries(grouped).map(([key, value]) => ({
            source: key.split(' → ')[0],
            target: key.split(' → ')[1],
            value: value.value,
            count: value.count
        }));
    }
    
    groupByTime(data) {
        return data || [];
    }
    
    createSimpleFlowDiagram(container, data) {
        if (!Array.isArray(data) || !data.length) {
            this.showNoDataMessage(container, 'No flow data to display');
            return;
        }
        
        const flowDiv = document.createElement('div');
        flowDiv.style.padding = '20px';
        
        data.slice(0, 10).forEach((item, index) => {
            const itemDiv = document.createElement('div');
            itemDiv.style.marginBottom = '10px';
            itemDiv.style.padding = '10px';
            itemDiv.style.backgroundColor = '#f8f9fa';
            itemDiv.style.borderRadius = '4px';
            itemDiv.style.display = 'flex';
            itemDiv.style.justifyContent = 'space-between';
            
            const pathText = item.path || `${item.source || 'Unknown'} → ${item.target || 'Unknown'}`;
            const probability = item.probability || item.value || 0;
            
            itemDiv.innerHTML = `
                <span>${pathText}</span>
                <span style="font-weight: bold; color: #0070d2;">${(probability * 100).toFixed(2)}%</span>
            `;
            
            flowDiv.appendChild(itemDiv);
        });
        
        container.appendChild(flowDiv);
    }
    
    createSankeyVisualization(container, data) {
        if (!Array.isArray(data) || !data.length) {
            this.showNoDataMessage(container, 'No Sankey data to display');
            return;
        }
        
        const sankeyDiv = document.createElement('div');
        sankeyDiv.style.padding = '20px';
        
        data.slice(0, 15).forEach((item, index) => {
            const itemDiv = document.createElement('div');
            itemDiv.style.marginBottom = '8px';
            itemDiv.style.display = 'flex';
            itemDiv.style.alignItems = 'center';
            
            const barWidth = Math.max(50, (item.value || 0) * 300);
            
            itemDiv.innerHTML = `
                <div style="width: 150px; font-size: 12px;">${item.source || 'Unknown'} → ${item.target || 'Unknown'}</div>
                <div style="width: ${barWidth}px; height: 20px; background: linear-gradient(90deg, #0070d2, #00d4aa); margin-left: 10px; border-radius: 10px;"></div>
                <div style="margin-left: 10px; font-weight: bold;">${((item.value || 0) * 100).toFixed(1)}%</div>
            `;
            
            sankeyDiv.appendChild(itemDiv);
        });
        
        container.appendChild(sankeyDiv);
    }
    
    // Network-specific methods
    applyZoom() {
        if (this.networkChart && this.networkChart.svg) {
            this.networkChart.svg
                .transition()
                .duration(300)
                .attr('transform', `scale(${this.zoomLevel})`);
        }
    }
    
    fitNetworkToScreen() {
        this.zoomLevel = 1;
        this.applyZoom();
    }
    
    resetNetworkView() {
        if (this.networkChart && this.networkChart.simulation) {
            this.networkChart.simulation.alpha(1).restart();
        }
        this.applyZoom();
    }
    
    // Cleanup
    cleanupVisualization() {
        const containers = [
            '.network-graph',
            '.transition-matrix',
            '.flow-diagram',
            '.sankey-chart'
        ];
        
        containers.forEach(selector => {
            const container = this.template.querySelector(selector);
            if (container) {
                container.innerHTML = '';
            }
        });
        
        this.networkChart = null;
        this.matrixChart = null;
        this.flowChart = null;
        this.sankeyChart = null;
    }
    
    // Utility methods
    handleError(message, error) {
        console.error(message, error);
        this.error = `${message}: ${error.body?.message || error.message || 'Unknown error'}`;
        this.isLoading = false;
    }
    
    showToast(title, message, variant) {
        this.dispatchEvent(new ShowToastEvent({
            title,
            message,
            variant
        }));
    }
}