class CalendarApp {
    constructor() {
        this.currentData = [];
        this.metadata = null;
        this.init();
    }

    async init() {
        await this.loadMetadata();
        this.setupEventListeners();
        this.initializeFilters();
        this.loadTodayData();
    }

    async loadMetadata() {
        try {
            const response = await fetch('data/metadata.json');
            this.metadata = await response.json();
            this.populatePlatformSelect();
        } catch (error) {
            console.error('加载元数据失败:', error);
        }
    }

    populatePlatformSelect() {
        const select = document.getElementById('platformSelect');
        this.metadata.platforms.forEach(platform => {
            const option = document.createElement('option');
            option.value = platform.id;
            option.textContent = platform.name;
            select.appendChild(option);
        });
    }

    setupEventListeners() {
        document.getElementById('queryBtn').addEventListener('click', () => {
            this.loadData();
        });

        document.getElementById('copyJsonBtn').addEventListener('click', () => {
            this.copyFilteredData();
        });

        // 回车键查询
        document.getElementById('dateInput').addEventListener('keypress', (e) => {
            if (e.key === 'Enter') this.loadData();
        });
    }

    initializeFilters() {
        // 设置默认日期为今天
        const today = new Date().toISOString().split('T')[0];
        document.getElementById('dateInput').value = today;
    }

    loadTodayData() {
        this.loadData();
    }

    async loadData() {
        const date = document.getElementById('dateInput').value;
        if (!date) {
            alert('请选择日期');
            return;
        }

        this.showLoading(true);

        try {
            const response = await fetch(`data/events/${date}.json`);
            
            if (!response.ok) {
                this.showNoEvents(`${date} 暂无事件数据`);
                return;
            }

            const data = await response.json();
            this.currentData = data.events || [];
            this.renderEvents();
        } catch (error) {
            console.error('加载数据失败:', error);
            this.showNoEvents('数据加载失败');
        } finally {
            this.showLoading(false);
        }
    }

    filterEvents() {
        const platform = document.getElementById('platformSelect').value;
        const eventType = document.getElementById('eventTypeSelect').value;

        let filtered = [...this.currentData];

        // 平台筛选
        if (platform !== 'all') {
            filtered = filtered.filter(event => event.platform === platform);
        }

        // 事件类型筛选
        if (eventType === 'new_only') {
            filtered = filtered.filter(event => event.is_new === true);
        }

        return filtered;
    }

    renderEvents() {
        const filtered = this.filterEvents();
        const container = document.getElementById('eventsList');
        const statsContainer = document.getElementById('statsInfo');

        // 显示统计信息
        const newCount = filtered.filter(e => e.is_new).length;
        statsContainer.innerHTML = `
            <span>共找到 ${filtered.length} 个事件</span>
            ${newCount > 0 ? `<span style="color: #e74c3c; margin-left: 15px;">其中新增 ${newCount} 个</span>` : ''}
        `;

        if (filtered.length === 0) {
            container.innerHTML = '<div class="no-events">暂无符合条件的事件</div>';
            return;
        }

        // 按时间排序
        filtered.sort((a, b) => {
            const timeA = a.event_time || '00:00:00';
            const timeB = b.event_time || '00:00:00';
            return timeA.localeCompare(timeB);
        });

        // 渲染事件列表
        container.innerHTML = filtered.map(event => this.renderEventItem(event)).join('');
    }

    renderEventItem(event) {
        const platformName = this.getPlatformName(event.platform);
        const importance = '★'.repeat(event.importance || 1);
        const newBadge = event.is_new ? '<span class="new-badge">🆕 NEW</span>' : '';
        
        const details = [];
        if (event.category) details.push(`类别: ${event.category}`);
        if (event.country) details.push(`国家: ${event.country}`);
        if (event.city) details.push(`城市: ${event.city}`);
        if (event.stocks && event.stocks.length > 0) {
            const stocks = event.stocks.slice(0, 3).join(', ');
            details.push(`相关股票: ${stocks}${event.stocks.length > 3 ? ' 等' : ''}`);
        }
        if (event.is_new && event.discovery_date) {
            details.push(`发现日期: ${event.discovery_date}`);
        }

        return `
            <div class="event-item ${event.is_new ? 'new-event' : ''}">
                <div class="event-header">
                    ${newBadge}
                    <span class="platform-badge">${platformName}</span>
                    <span class="event-time">${event.event_date} ${event.event_time || ''}</span>
                    <span class="importance">${importance}</span>
                </div>
                <div class="event-title">${event.title}</div>
                ${event.content ? `<div class="event-details">${event.content}</div>` : ''}
                ${details.length > 0 ? `<div class="event-details">${details.join(' | ')}</div>` : ''}
            </div>
        `;
    }

    getPlatformName(platformId) {
        if (!this.metadata) return platformId;
        const platform = this.metadata.platforms.find(p => p.id === platformId);
        return platform ? platform.name : platformId;
    }

    showLoading(show) {
        document.getElementById('loading').style.display = show ? 'block' : 'none';
        document.getElementById('eventsList').style.display = show ? 'none' : 'block';
    }

    showNoEvents(message) {
        document.getElementById('eventsList').innerHTML = `<div class="no-events">${message}</div>`;
        document.getElementById('statsInfo').innerHTML = '';
    }

    async copyFilteredData() {
        const filtered = this.filterEvents();
        const jsonData = JSON.stringify(filtered, null, 2);
        
        try {
            await navigator.clipboard.writeText(jsonData);
            
            // 临时改变按钮文字显示复制成功
            const btn = document.getElementById('copyJsonBtn');
            const originalText = btn.textContent;
            btn.textContent = '已复制!';
            btn.style.backgroundColor = '#27ae60';
            
            setTimeout(() => {
                btn.textContent = originalText;
                btn.style.backgroundColor = '#95a5a6';
            }, 2000);
        } catch (error) {
            console.error('复制失败:', error);
            alert('复制失败，请手动复制控制台输出的数据');
            console.log('筛选结果JSON:', jsonData);
        }
    }
}

// 页面加载完成后初始化应用
document.addEventListener('DOMContentLoaded', () => {
    new CalendarApp();
});
