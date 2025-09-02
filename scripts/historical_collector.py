#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
投资日历历史数据采集器
一次性运行，采集所有平台的历史数据并归档
"""

import requests
import json
import hashlib
import time
import os
import re
import calendar
import urllib.parse
from datetime import datetime, timedelta
from typing import List, Dict, Any
from dataclasses import dataclass, asdict
from bs4 import BeautifulSoup

# ============================================================================
# 数据模型
# ============================================================================

@dataclass
class StandardizedEvent:
    """标准化事件模型"""
    platform: str
    event_id: str
    original_id: str
    event_date: str
    event_time: str = None
    event_datetime: str = None
    title: str = ""
    content: str = None
    category: str = None
    importance: int = None
    country: str = None
    city: str = None
    stocks: List[str] = None
    concepts: List[Dict[str, Any]] = None
    themes: List[str] = None
    data_status: str = "ARCHIVED"
    raw_data: Dict[str, Any] = None
    created_at: str = ""
    
    def __post_init__(self):
        if self.stocks is None:
            self.stocks = []
        if self.concepts is None:
            self.concepts = []
        if self.themes is None:
            self.themes = []
        if self.raw_data is None:
            self.raw_data = {}
        if not self.created_at:
            self.created_at = datetime.now().isoformat()
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

# ============================================================================
# 工具函数
# ============================================================================

def generate_sign(params_dict):
    """生成财联社API请求的签名"""
    sorted_data = sorted(params_dict.items(), key=lambda item: item[0])
    query_string = urllib.parse.urlencode(sorted_data)
    sha1_hash = hashlib.sha1(query_string.encode('utf-8')).hexdigest()
    sign = hashlib.md5(sha1_hash.encode('utf-8')).hexdigest()
    return sign

def extract_json_from_jsonp(jsonp_text: str, callback_name: str) -> Dict[str, Any]:
    """从JSONP响应中提取JSON数据"""
    try:
        pattern = f'{callback_name}\\((.+)\\);?$'
        match = re.search(pattern, jsonp_text.strip())
        if match:
            json_str = match.group(1)
            return json.loads(json_str)
    except:
        pass
    return {}

# ============================================================================
# 历史数据采集器
# ============================================================================

class HistoricalDataCollector:
    """历史数据采集器"""
    
    def __init__(self):
        self.base_path = "./data"
        self.archived_path = os.path.join(self.base_path, "archived")
        self._ensure_directories()
    
    def _ensure_directories(self):
        """创建目录结构"""
        os.makedirs(self.archived_path, exist_ok=True)
        
        # 创建年月目录结构
        for year in [2025, 2026]:
            year_path = os.path.join(self.archived_path, str(year))
            os.makedirs(year_path, exist_ok=True)
            
            max_month = 12 if year == 2025 else 12  # 不预设限制
            for month in range(1, max_month + 1):
                month_path = os.path.join(year_path, f"{month:02d}月")
                os.makedirs(month_path, exist_ok=True)
    
    def collect_all_historical_data(self):
        """采集所有平台的历史数据"""
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        print(f"🕒 开始采集历史数据（截止到 {yesterday}）")
        print("=" * 60)
        
        results = {}
        total_events = 0
        
        # 财联社
        print("\n📡 正在采集 cls 历史数据...")
        print(f"   时间范围: 2025-01-01 至 {yesterday}")
        cls_events = self._collect_cls_historical(yesterday)
        results['cls'] = {'total_events': cls_events, 'status': 'success' if cls_events > 0 else 'no_data'}
        total_events += cls_events
        print(f"✅ cls 采集完成，共 {cls_events} 个事件")
        
        # 韭研公社
        print("\n📡 正在采集 jiuyangongshe 历史数据...")
        print(f"   时间范围: 2025-01-01 至 {yesterday}")
        jiuyan_events = self._collect_jiuyan_historical(yesterday)
        results['jiuyangongshe'] = {'total_events': jiuyan_events, 'status': 'success' if jiuyan_events > 0 else 'no_data'}
        total_events += jiuyan_events
        print(f"✅ jiuyangongshe 采集完成，共 {jiuyan_events} 个事件")
        
        # 同花顺
        print("\n📡 正在采集 tonghuashun 历史数据...")
        print(f"   时间范围: 2025-01-01 至 {yesterday}")
        ths_events = self._collect_tonghuashun_historical(yesterday)
        results['tonghuashun'] = {'total_events': ths_events, 'status': 'success' if ths_events > 0 else 'no_data'}
        total_events += ths_events
        print(f"✅ tonghuashun 采集完成，共 {ths_events} 个事件")
        
        # 英为财情
        print("\n📡 正在采集 investing 历史数据...")
        print(f"   时间范围: 2025-01-01 至 {yesterday}")
        inv_events = self._collect_investing_historical(yesterday)
        results['investing'] = {'total_events': inv_events, 'status': 'success' if inv_events > 0 else 'no_data'}
        total_events += inv_events
        print(f"✅ investing 采集完成，共 {inv_events} 个事件")
        
        # 东方财富
        print("\n📡 正在采集 eastmoney 历史数据...")
        print(f"   时间范围: 2025-01-01 至 {yesterday}")
        em_events = self._collect_eastmoney_historical(yesterday)
        results['eastmoney'] = {'total_events': em_events, 'status': 'success' if em_events > 0 else 'no_data'}
        total_events += em_events
        print(f"✅ eastmoney 采集完成，共 {em_events} 个事件")
        
        # 生成汇总报告
        self._generate_historical_summary(results, total_events, yesterday)
        
        print(f"\n🎉 历史数据采集完成！")
        print("=" * 60)
        print(f"📊 总计采集 {total_events} 个历史事件")
        print("\n📈 各平台统计:")
        for platform, info in results.items():
            status_icon = "✅" if info['status'] == 'success' else "⚠️"
            print(f"  {status_icon} {platform:15}: {info['total_events']:4d} 个事件")
        
        return results
    
    def _collect_cls_historical(self, end_date: str) -> int:
        """采集财联社历史数据"""
        total_events = 0
        current_month = datetime.now().month
        
        # 财联社特殊逻辑：使用flag参数
        for month in range(1, current_month + 1):
            print(f"   📅 采集 2025年{month}月...")
            
            try:
                # 确定flag值
                if month < current_month:
                    flag = 1  # 历史月份，使用上月数据flag
                else:
                    flag = 2  # 当前月份，使用本月数据flag
                
                events = self._get_cls_data(flag, end_date, month)
                
                if events:
                    self._save_monthly_data('cls', 2025, month, events)
                    print(f"      ✅ 2025年{month}月: {len(events)} 个事件")
                    total_events += len(events)
                else:
                    print(f"      ⚠️ 2025年{month}月: 无数据")
                
                time.sleep(1)
                
            except Exception as e:
                print(f"      ❌ 2025年{month}月 采集失败: {e}")
        
        return total_events
    
    def _get_cls_data(self, flag: int, end_date: str, target_month: int) -> List[StandardizedEvent]:
        """获取财联社数据"""
        params = {
            "app": "CailianpressWeb",
            "flag": str(flag),
            "os": "web",
            "sv": "8.4.6",
            "token": "65EOORtcq9jy9667Vw0qugGnpcm4vU894112432",
            "type": "0",
            "uid": "4112432"
        }
        params["sign"] = generate_sign(params)
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://www.cls.cn/investKalendar"
        }
        
        try:
            response = requests.get(
                "https://www.cls.cn/api/calendar/web/list", 
                params=params, headers=headers, timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                events = []
                
                for day_data in data.get('data', []):
                    date = day_data.get('calendar_day')
                    
                    # 过滤：只要目标月份且小于今天的数据
                    if not date:
                        continue
                    
                    event_month = int(date.split('-')[1])
                    if event_month != target_month or date >= end_date:
                        continue
                    
                    for item in day_data.get('items', []):
                        event = StandardizedEvent(
                            platform="cls",
                            event_id=f"cls_{item.get('id')}_{date.replace('-', '')}_{item.get('type', 0)}",
                            original_id=str(item.get('id')),
                            event_date=date,
                            event_time=self._extract_time(item.get('calendar_time')),
                            event_datetime=item.get('calendar_time'),
                            title=item.get('title', ''),
                            category=self._get_cls_category(item.get('type')),
                            importance=self._get_cls_importance(item),
                            country=self._extract_cls_country(item),
                            raw_data=item
                        )
                        events.append(event)
                
                return events
            else:
                return []
        except Exception as e:
            print(f"财联社API请求失败: {e}")
            return []
    
    def _extract_time(self, datetime_str: str) -> str:
        if not datetime_str:
            return None
        try:
            dt = datetime.fromisoformat(datetime_str.replace('Z', '+00:00'))
            return dt.strftime('%H:%M:%S')
        except:
            return None
    
    def _get_cls_category(self, event_type: int) -> str:
        type_map = {1: '经济数据', 2: '事件公告', 3: '假日'}
        return type_map.get(event_type, '其他')
    
    def _get_cls_importance(self, item: Dict) -> int:
        if item.get('type') == 1 and item.get('economic'):
            return item.get('economic', {}).get('star', 3)
        elif item.get('type') == 2 and item.get('event'):
            return item.get('event', {}).get('star', 3)
        return 3
    
    def _extract_cls_country(self, item: Dict) -> str:
        if item.get('economic'):
            return item.get('economic', {}).get('country')
        elif item.get('event'):
            return item.get('event', {}).get('country')
        return None
    
    def _collect_jiuyan_historical(self, end_date: str) -> int:
        """采集韭研公社历史数据"""
        total_events = 0
        current_month = datetime.now().month
        
        # 按月循环采集
        for month in range(1, current_month + 1):
            print(f"   📅 采集 2025年{month}月...")
            
            try:
                events = self._get_jiuyan_month_data(2025, month, end_date)
                
                if events:
                    self._save_monthly_data('jiuyangongshe', 2025, month, events)
                    print(f"      ✅ 2025年{month}月: {len(events)} 个事件")
                    total_events += len(events)
                else:
                    print(f"      ⚠️ 2025年{month}月: 无数据")
                
                time.sleep(0.5)
                
            except Exception as e:
                print(f"      ❌ 2025年{month}月 采集失败: {e}")
        
        return total_events
    
    def _get_jiuyan_month_data(self, year: int, month: int, end_date: str) -> List[StandardizedEvent]:
        """获取韭研公社月度数据"""
        date_param = f"{year}-{month:02d}"
        
        headers = {
            'Content-Type': 'application/json',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'timestamp': str(int(time.time() * 1000)),
            'platform': '3',
            'token': '094cbed7fa79612f2ba4fcd34b191d99',
            'Origin': 'https://www.jiuyangongshe.com',
            'Referer': 'https://www.jiuyangongshe.com/'
        }
        
        payload = {"date": date_param}
        
        try:
            response = requests.post(
                "https://app.jiuyangongshe.com/jystock-app/api/v1/timeline/list",
                headers=headers, json=payload, timeout=10
            )
            
            if response.status_code == 200:
                month_data = response.json()
                events = []
                
                for day_data in month_data.get('data', []):
                    date = day_data.get('date')
                    
                    # 只处理历史数据（小于今天的数据）
                    if not date or date >= end_date:
                        continue
                    
                    for item in day_data.get('list', []):
                        timeline = item.get('timeline', {})
                        event = StandardizedEvent(
                            platform="jiuyangongshe",
                            event_id=f"jygs_{item.get('article_id', '')}_{timeline.get('timeline_id', '')}_{date.replace('-', '')}",
                            original_id=item.get('article_id', ''),
                            event_date=date,
                            title=item.get('title', ''),
                            content=item.get('content', ''),
                            category='投资事件',
                            importance=max(1, min(5, 7 - timeline.get('grade', 6))),
                            country='中国',
                            themes=[theme.get('name', '') for theme in timeline.get('theme_list', [])],
                            raw_data=item
                        )
                        events.append(event)
                
                return events
            else:
                return []
        except Exception as e:
            print(f"韭研公社API请求失败: {e}")
            return []
    
    def _collect_tonghuashun_historical(self, end_date: str) -> int:
        """采集同花顺历史数据"""
        total_events = 0
        current_month = datetime.now().month
        
        # 按月循环采集
        for month in range(1, current_month + 1):
            print(f"   📅 采集 2025年{month}月...")
            
            try:
                events = self._get_tonghuashun_month_data(2025, month, end_date)
                
                if events:
                    self._save_monthly_data('tonghuashun', 2025, month, events)
                    print(f"      ✅ 2025年{month}月: {len(events)} 个事件")
                    total_events += len(events)
                else:
                    print(f"      ⚠️ 2025年{month}月: 无数据")
                
                time.sleep(0.5)
                
            except Exception as e:
                print(f"      ❌ 2025年{month}月 采集失败: {e}")
        
        return total_events
    
    def _get_tonghuashun_month_data(self, year: int, month: int, end_date: str) -> List[StandardizedEvent]:
        """获取同花顺月度数据"""
        date_param = f"{year}{month:02d}"
        
        params = {
            'callback': 'callback_dt',
            'type': 'data',
            'date': date_param
        }
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'https://stock.10jqka.com.cn/',
            'Accept': '*/*'
        }
        
        try:
            response = requests.get(
                "https://comment.10jqka.com.cn/tzrl/getTzrlData.php",
                params=params, headers=headers, timeout=10
            )
            
            if response.status_code == 200:
                json_data = extract_json_from_jsonp(response.text, 'callback_dt')
                events = []
                
                for day_data in json_data.get('data', []):
                    date = day_data.get('date')
                    
                    # 只处理历史数据（小于今天的数据）
                    if not date or date >= end_date:
                        continue
                    
                    day_events = day_data.get('events', [])
                    concepts = day_data.get('concept', [])
                    
                    for i, event_data in enumerate(day_events):
                        if isinstance(event_data, list) and len(event_data) > 0:
                            title = event_data[0] if event_data[0] else ""
                            title_hash = hashlib.md5(title.encode('utf-8')).hexdigest()[:8]
                            concept_info = concepts[i] if i < len(concepts) else []
                            
                            event = StandardizedEvent(
                                platform="tonghuashun",
                                event_id=f"ths_{date.replace('-', '')}_{i}_{title_hash}",
                                original_id=f"{date}_{i}",
                                event_date=date,
                                title=title,
                                category='市场事件',
                                importance=3,
                                country='中国',
                                concepts=[{"code": c.get("code"), "name": c.get("name")} for c in concept_info] if concept_info else [],
                                raw_data={"event": event_data, "concept": concept_info}
                            )
                            events.append(event)
                
                return events
            else:
                return []
        except Exception as e:
            print(f"同花顺API请求失败: {e}")
            return []

    def _collect_investing_historical(self, end_date: str) -> int:
        """采集英为财情历史数据 - 按天采集版"""
        total_events = 0
        
        print(f"   🔄 英为财情按天采集历史数据: 2025-01-01 → {end_date}")
        
        # 按天采集
        start_dt = datetime(2025, 1, 1)
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        
        current_dt = start_dt
        day_count = 0
        
        # 按月保存数据
        monthly_events = {}
        
        while current_dt <= end_dt:
            day_count += 1
            day_str = current_dt.strftime('%Y-%m-%d')
            year, month = current_dt.year, current_dt.month
            
            if day_count % 10 == 1:  # 每10天显示一次进度
                print(f"   📅 进度: {day_str} (第 {day_count} 天)")
            
            try:
                # 采集单天数据
                day_events = self._request_investing_single_day_historical(day_str, end_date)
                
                if day_events:
                    # 按月分组
                    month_key = f"{year}-{month:02d}"
                    if month_key not in monthly_events:
                        monthly_events[month_key] = []
                    monthly_events[month_key].extend(day_events)
                    total_events += len(day_events)
                
                # 避免请求过快
                time.sleep(0.3)
                
            except Exception as e:
                print(f"      ❌ {day_str} 采集失败: {e}")
            
            # 移动到下一天
            current_dt += timedelta(days=1)
        
        # 保存按月分组的数据
        for month_key, events in monthly_events.items():
            year, month = month_key.split('-')
            year, month = int(year), int(month)
            
            if events:
                self._save_monthly_data('investing', year, month, events)
                print(f"   ✅ {year}年{month}月: {len(events)} 个事件已保存")
        
        print(f"   📊 英为财情历史数据总计: {total_events} 个事件 (分 {day_count} 天采集)")
        return total_events

    def _request_investing_single_day_historical(self, date: str, end_date: str) -> List[StandardizedEvent]:
        """请求英为财情单天历史数据"""
        countries = [37, 46, 6, 110, 14, 48, 32, 17, 10, 36, 43, 35, 72, 22, 41, 25, 12, 5, 4, 26, 178, 11, 39, 42]
        
        # 构建请求体（单天）
        payload = ""
        for country in countries:
            payload += f"country%5B%5D={country}&"
        payload += f"dateFrom={date}&dateTo={date}&timeZone=28&timeFilter=timeRemain&currentTab=custom&limit_from=0"
        
        headers = {
            "Host": "cn.investing.com",
            "Connection": "keep-alive",
            "sec-ch-ua": '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
            "sec-ch-ua-mobile": "?0",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "*/*",
            "X-Requested-With": "XMLHttpRequest",
            "sec-ch-ua-platform": '"Windows"',
            "Origin": "https://cn.investing.com",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Dest": "empty",
            "Referer": "https://cn.investing.com/economic-calendar/",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "zh-CN,zh;q=0.9"
        }
        
        try:
            response = requests.post(
                "https://cn.investing.com/economic-calendar/Service/getCalendarFilteredData",
                headers=headers, data=payload, timeout=15
            )
            
            if response.status_code == 200:
                html_content = response.text
                
                # 检查是否返回空结果
                if len(html_content.strip()) < 100:
                    return []
                
                # 处理可能的JSON嵌套
                try:
                    json_data = json.loads(html_content)
                    if 'data' in json_data:
                        html_content = json_data['data']
                    elif isinstance(json_data, dict) and len(json_data) == 0:
                        return []
                except json.JSONDecodeError:
                    pass
                
                # 处理转义字符
                if '\\u' in html_content or '\\"' in html_content:
                    html_content = html_content.encode().decode('unicode_escape')
                    html_content = html_content.replace('\\"', '"').replace('\\/', '/')
                
                # 检查HTML内容是否包含事件数据
                if 'js-event-item' not in html_content and 'eventRowId_' not in html_content:
                    return []
                
                return self._parse_investing_html_simple_historical(html_content, date, end_date)
            else:
                return []
        except Exception as e:
            return []

    def _parse_investing_html_simple_historical(self, html_content: str, date: str, end_date: str) -> List[StandardizedEvent]:
        """解析英为财情历史HTML数据（简化版）"""
        events = []
        
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            event_rows = soup.find_all('tr', class_=re.compile(r'js-event-item'))
            
            if not event_rows:
                event_rows = soup.find_all('tr', id=re.compile(r'eventRowId_'))
            
            for row in event_rows:
                event_data = self._extract_investing_event_enhanced(row)
                if event_data and event_data.get('event_name'):
                    event_date = self._extract_date_from_datetime(event_data.get('datetime'))
                    
                    # 只要当天且是历史数据
                    if event_date == date and event_date <= end_date:
                        # 构建内容字符串（包含数值信息）
                        content_parts = []
                        if event_data.get('actual'):
                            content_parts.append(f"实际值: {event_data.get('actual')}")
                        if event_data.get('forecast'):
                            content_parts.append(f"预测值: {event_data.get('forecast')}")
                        if event_data.get('previous'):
                            content_parts.append(f"前值: {event_data.get('previous')}")
                        
                        content = " | ".join(content_parts) if content_parts else None
                        
                        event = StandardizedEvent(
                            platform="investing",
                            event_id=f"inv_{event_data.get('event_attr_id', '')}_{event_data.get('datetime', '').replace('/', '').replace(' ', '').replace(':', '')}",
                            original_id=event_data.get('event_id', ''),
                            event_date=event_date,
                            event_time=event_data.get('time'),
                            event_datetime=event_data.get('datetime'),
                            title=event_data.get('event_name', ''),
                            content=content,
                            category='经济数据',
                            importance=event_data.get('importance', 1),
                            country=event_data.get('country'),
                            raw_data=event_data
                        )
                        events.append(event)
        except Exception as e:
            print(f"英为财情HTML解析失败: {e}")
        
        return events

        
    def _extract_investing_event_enhanced(self, row) -> Dict[str, Any]:
        """增强版英为财情事件数据提取（提取更多字段）"""
        event = {}
        cells = row.find_all('td')
        
        if len(cells) < 4:
            return None
        
        event['event_id'] = row.get('id', '')
        event['event_attr_id'] = row.get('event_attr_ID', '')
        event['datetime'] = row.get('data-event-datetime', '')
        
        try:
            # 第1列：时间
            if len(cells) > 0:
                event['time'] = cells[0].get_text(strip=True)
            
            # 第2列：国家
            if len(cells) > 1:
                flag_span = cells[1].find('span', class_=re.compile(r'ceFlags'))
                if flag_span:
                    event['country'] = flag_span.get('title', '')
            
            # 第3列：重要性
            if len(cells) > 2:
                full_icons = cells[2].find_all('i', class_='grayFullBullishIcon')
                event['importance'] = len(full_icons) if full_icons else 1
            
            # 第4列：事件名称
            if len(cells) > 3:
                event_cell = cells[3]
                link = event_cell.find('a')
                if link:
                    event['event_name'] = link.get_text(strip=True)
                else:
                    event['event_name'] = event_cell.get_text(strip=True)
            
            # 第5列：实际值（如果有）
            if len(cells) > 4:
                actual_value = cells[4].get_text(strip=True)
                if actual_value and actual_value not in ['--', '']:
                    event['actual'] = actual_value
            
            # 第6列：预测值（如果有）
            if len(cells) > 5:
                forecast_value = cells[5].get_text(strip=True)
                if forecast_value and forecast_value not in ['--', '']:
                    event['forecast'] = forecast_value
            
            # 第7列：前值（如果有）
            if len(cells) > 6:
                previous_value = cells[6].get_text(strip=True)
                if previous_value and previous_value not in ['--', '']:
                    event['previous'] = previous_value
            
            # 影响程度（从class或其他属性中提取）
            impact_cell = cells[2] if len(cells) > 2 else None
            if impact_cell:
                # 查找影响程度相关的class
                impact_spans = impact_cell.find_all('span', class_=re.compile(r'impact'))
                if impact_spans:
                    event['impact'] = impact_spans[0].get('class', [])
        
        except Exception as e:
            print(f"英为财情事件提取失败: {e}")
            return None
        
        return event

    
    def _extract_date_from_datetime(self, datetime_str: str) -> str:
        """从datetime字符串中提取日期"""
        if not datetime_str:
            return ""
        try:
            date_part = datetime_str.split(' ')[0]
            return date_part.replace('/', '-')
        except:
            return ""
    
    def _collect_eastmoney_historical(self, end_date: str) -> int:
        """采集东方财富历史数据"""
        total_events = 0
        current_month = datetime.now().month
        
        # 按月循环采集
        for month in range(1, current_month + 1):
            print(f"   📅 采集 2025年{month}月...")
            
            try:
                events = self._get_eastmoney_month_data(2025, month, end_date)
                
                if events:
                    self._save_monthly_data('eastmoney', 2025, month, events)
                    print(f"      ✅ 2025年{month}月: {len(events)} 个事件")
                    total_events += len(events)
                else:
                    print(f"      ⚠️ 2025年{month}月: 无数据")
                
                time.sleep(0.5)
                
            except Exception as e:
                print(f"      ❌ 2025年{month}月 采集失败: {e}")
        
        return total_events
    

    def _get_eastmoney_month_data(self, year: int, month: int, end_date: str) -> List[StandardizedEvent]:
        """获取东方财富月度数据 - 完整版（修复版）"""
        month_start = f"{year}-{month:02d}-01"
        month_end = f"{year}-{month:02d}-{calendar.monthrange(year, month)[1]}"
        
        # 如果月末超过结束日期，截取到结束日期
        if month_end > end_date:
            month_end = end_date
        
        params = {
            "fromdate": month_start,
            "todate": month_end,
            "option": "xsap,xgsg,tfpxx,hsgg,nbjb,jjsj,hyhy,gddh"
        }
        
        headers = {
            "Host": "data.eastmoney.com",
            "Connection": "keep-alive",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "X-Requested-With": "XMLHttpRequest",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": "https://data.eastmoney.com/dcrl/dashi.html"
        }
        
        try:
            response = requests.get(
                "https://data.eastmoney.com/dataapi/dcrl/dstx",
                params=params, headers=headers, timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                events = []
                
                # 安全获取数据长度，处理None值
                def safe_len(data_list):
                    return len(data_list) if data_list is not None else 0
                
                print(f"      🔍 东方财富 {year}年{month}月 原始数据统计:")
                print(f"         休市安排(xsap): {safe_len(data.get('xsap'))} 条")
                print(f"         新股申购(xgsg): {safe_len(data.get('xgsg'))} 条")
                print(f"         停复牌信息(tfpxx): {safe_len(data.get('tfpxx'))} 条")
                print(f"         A股公告(hsgg): {safe_len(data.get('hsgg'))} 条")
                print(f"         年报季报(nbjb): {safe_len(data.get('nbjb'))} 条")
                print(f"         经济数据(jjsj): {safe_len(data.get('jjsj'))} 条")
                print(f"         行业会议(hyhy): {safe_len(data.get('hyhy'))} 条")
                print(f"         股东大会(gddh): {safe_len(data.get('gddh'))} 条")
                
                # 1. 处理休市安排数据 (xsap)
                xsap_data = data.get('xsap') or []
                for item in xsap_data:
                    if not item:  # 跳过None或空项
                        continue
                    item_date = self._extract_date(item.get('SDATE'))
                    if item_date and item_date <= end_date:
                        event = StandardizedEvent(
                            platform="eastmoney",
                            event_id=f"em_xsap_{item_date.replace('-', '')}_{hash(item.get('HOLIDAY', ''))}",
                            original_id=f"xsap_{item.get('MKT', '')}_{item_date}",
                            event_date=item_date,
                            event_time=self._extract_time_from_datetime(item.get('SDATE')),
                            event_datetime=item.get('SDATE'),
                            title=f"{item.get('MKT', '')} - {item.get('HOLIDAY', '')}",
                            category='休市安排',
                            importance=2,
                            country='中国',
                            raw_data=item
                        )
                        events.append(event)
                
                # 2. 处理新股申购数据 (xgsg)
                xgsg_data = data.get('xgsg') or []
                for item in xgsg_data:
                    if not item:
                        continue
                    item_date = self._extract_date(item.get('APPLY_DATE'))
                    if item_date and item_date <= end_date:
                        event = StandardizedEvent(
                            platform="eastmoney",
                            event_id=f"em_xgsg_{item.get('SECURITY_CODE', '')}_{item_date.replace('-', '')}",
                            original_id=item.get('SECURITY_CODE', ''),
                            event_date=item_date,
                            title=f"{item.get('SECURITY_NAME_ABBR', '')}新股申购",
                            content=f"申购代码: {item.get('APPLY_CODE', '')}, 发行价: {item.get('ISSUE_PRICE', '')}, 发行量: {item.get('ONLINE_ISSUE_LWR', '')}万股",
                            category='新股申购',
                            importance=3,
                            country='中国',
                            stocks=[item.get('SECURITY_CODE', '')] if item.get('SECURITY_CODE') else [],
                            raw_data=item
                        )
                        events.append(event)
                
                # 3. 处理停复牌信息 (tfpxx)
                tfpxx_data = data.get('tfpxx') or []
                for item in tfpxx_data:
                    if not item:
                        continue
                    item_date = self._extract_date(item.get('Date'))
                    if item_date and item_date <= end_date:
                        stock_data = item.get('Data') or []
                        for stock in stock_data:
                            if not stock:
                                continue
                            event = StandardizedEvent(
                                platform="eastmoney",
                                event_id=f"em_tfpxx_{item_date.replace('-', '')}_{stock.get('Scode', '')}",
                                original_id=stock.get('Scode', ''),
                                event_date=item_date,
                                title=f"{stock.get('Sname', '')}停复牌",
                                content=f"股票代码: {stock.get('Scode', '')}, 停复牌原因: {stock.get('Reason', '')}",
                                category='停复牌信息',
                                importance=2,
                                country='中国',
                                stocks=[stock.get('Scode', '')] if stock.get('Scode') else [],
                                raw_data=stock
                            )
                            events.append(event)
                
                # 4. 处理A股公告 (hsgg)
                hsgg_data = data.get('hsgg') or []
                for item in hsgg_data:
                    if not item:
                        continue
                    item_date = self._extract_date(item.get('NOTICE_DATE'))
                    if item_date and item_date <= end_date:
                        event = StandardizedEvent(
                            platform="eastmoney",
                            event_id=f"em_hsgg_{item.get('SECUCODE', '')}_{item_date.replace('-', '')}_{hash(item.get('TITLE', ''))}",
                            original_id=item.get('SECUCODE', ''),
                            event_date=item_date,
                            title=f"{item.get('SECURITY_NAME_ABBR', '')} - {item.get('TITLE', '')}",
                            content=item.get('TITLE', ''),
                            category='A股公告',
                            importance=3,
                            country='中国',
                            stocks=[item.get('SECURITY_CODE', '')] if item.get('SECURITY_CODE') else [],
                            raw_data=item
                        )
                        events.append(event)
                
                # 5. 处理年报季报 (nbjb)
                nbjb_data = data.get('nbjb') or []
                for item in nbjb_data:
                    if not item:
                        continue
                    item_date = self._extract_date(item.get('REPORT_DATE'))
                    if item_date and item_date <= end_date:
                        event = StandardizedEvent(
                            platform="eastmoney",
                            event_id=f"em_nbjb_{item.get('SECURITY_CODE', '')}_{item_date.replace('-', '')}",
                            original_id=item.get('SECURITY_CODE', ''),
                            event_date=item_date,
                            title=f"{item.get('SECURITY_NAME_ABBR', '')} {item.get('REPORT_TYPE', '')}",
                            content=f"报告类型: {item.get('REPORT_TYPE', '')}, 报告期: {item.get('REPORT_PERIOD', '')}",
                            category='年报季报',
                            importance=4,
                            country='中国',
                            stocks=[item.get('SECURITY_CODE', '')] if item.get('SECURITY_CODE') else [],
                            raw_data=item
                        )
                        events.append(event)
                
                # 6. 处理经济数据 (jjsj)
                jjsj_data = data.get('jjsj') or []
                for item in jjsj_data:
                    if not item:
                        continue
                    item_date = self._extract_date(item.get('Date'))
                    if item_date and item_date <= end_date:
                        data_items = item.get('Data') or []
                        for data_item in data_items:
                            if not data_item:
                                continue
                            event = StandardizedEvent(
                                platform="eastmoney",
                                event_id=f"em_jjsj_{item.get('Date', '').replace('-', '').replace(' ', '').replace(':', '')}_{hash(data_item.get('Name', ''))}",
                                original_id=f"{item.get('Date')}_{data_item.get('Name')}",
                                event_date=item_date,
                                event_time=self._extract_time_from_datetime(item.get('Date')),
                                event_datetime=item.get('Date'),
                                title=data_item.get('Name', ''),
                                category='经济数据',
                                importance=4,
                                country=item.get('City', ''),
                                raw_data=data_item
                            )
                            events.append(event)
                
                # 7. 处理行业会议 (hyhy)
                hyhy_data = data.get('hyhy') or []
                for item in hyhy_data:
                    if not item:
                        continue
                    start_event_date = self._extract_date(item.get('START_DATE'))
                    if start_event_date and start_event_date <= end_date:
                        event = StandardizedEvent(
                            platform="eastmoney",
                            event_id=f"em_hyhy_{item.get('FE_CODE', '')}",
                            original_id=item.get('FE_CODE', ''),
                            event_date=start_event_date,
                            title=item.get('FE_NAME', ''),
                            content=item.get('CONTENT', ''),
                            category='行业会议',
                            importance=3,
                            country='中国',
                            city=item.get('CITY'),
                            raw_data=item
                        )
                        events.append(event)
                
                # 8. 处理股东大会 (gddh)
                gddh_data = data.get('gddh') or []
                for item in gddh_data:
                    if not item:
                        continue
                    item_date = self._extract_date(item.get('MEETING_DATE'))
                    if item_date and item_date <= end_date:
                        event = StandardizedEvent(
                            platform="eastmoney",
                            event_id=f"em_gddh_{item.get('SECURITY_CODE', '')}_{item_date.replace('-', '')}",
                            original_id=item.get('SECURITY_CODE', ''),
                            event_date=item_date,
                            event_time=self._extract_time_from_datetime(item.get('MEETING_DATE')),
                            event_datetime=item.get('MEETING_DATE'),
                            title=f"{item.get('SECURITY_NAME_ABBR', '')}股东大会",
                            content=f"会议类型: {item.get('MEETING_TYPE', '')}, 地点: {item.get('MEETING_PLACE', '')}",
                            category='股东大会',
                            importance=3,
                            country='中国',
                            city=item.get('MEETING_PLACE'),
                            stocks=[item.get('SECURITY_CODE', '')] if item.get('SECURITY_CODE') else [],
                            raw_data=item
                        )
                        events.append(event)
                
                print(f"      ✅ 东方财富 {year}年{month}月 过滤后事件: {len(events)} 个")
                
                # 按类别统计
                category_stats = {}
                for event in events:
                    category = event.category
                    category_stats[category] = category_stats.get(category, 0) + 1
                
                if category_stats:
                    print(f"      📊 按类别统计:")
                    for category, count in category_stats.items():
                        print(f"         {category}: {count} 个")
                
                return events
            else:
                print(f"      ❌ HTTP请求失败: {response.status_code}")
                return []
        except Exception as e:
            print(f"东方财富API请求失败: {e}")
            return []


    def _extract_time_from_datetime(self, datetime_str: str) -> str:
        """从datetime字符串中提取时间"""
        if not datetime_str:
            return None
        try:
            # 处理 "2025-12-31 09:30:00" 格式
            if ' ' in datetime_str:
                parts = datetime_str.split(' ')
                if len(parts) > 1 and parts[1] != "00:00:00":
                    return parts[1]
            return None
        except:
            return None



    def _extract_date(self, date_str: str) -> str:
        """提取日期 - 增强版"""
        if not date_str:
            return ""
        try:
            # 处理 "2025-12-31 00:00:00" 格式
            if ' ' in date_str:
                return date_str.split(' ')[0]
            # 处理 "2025-12-31" 格式
            return date_str
        except Exception as e:
            print(f"日期提取失败: {date_str}, 错误: {e}")
            return ""

    
    def _save_monthly_data(self, platform: str, year: int, month: int, events: List[StandardizedEvent]):
        """保存月度数据"""
        month_path = os.path.join(self.archived_path, str(year), f"{month:02d}月")
        file_path = os.path.join(month_path, f"{platform}.txt")
        
        # 如果文件已存在，合并数据
        existing_events = []
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    existing_data = json.loads(f.read())
                    for event_data in existing_data.get('events', []):
                        existing_events.append(StandardizedEvent(**event_data))
            except:
                pass
        
        # 合并并去重
        all_events = existing_events + events
        seen_ids = set()
        unique_events = []
        for event in all_events:
            if event.event_id not in seen_ids:
                unique_events.append(event)
                seen_ids.add(event.event_id)
        
        data = {
            "platform": platform,
            "year": year,
            "month": month,
            "total_events": len(unique_events),
            "data_status": "ARCHIVED",
            "date_type": "HISTORICAL",
            "last_update": datetime.now().isoformat(),
            "immutable": True,
            "events": [event.to_dict() for event in unique_events]
        }
        
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(json.dumps(data, ensure_ascii=False, indent=2))
        
        print(f"      💾 已保存到 {file_path}")
    
    def _generate_historical_summary(self, results: Dict[str, Any], total_events: int, end_date: str):
        """生成历史数据汇总"""
        summary = {
            "collection_type": "HISTORICAL",
            "collection_time": datetime.now().isoformat(),
            "historical_range": f"2025-01-01 至 {end_date}",
            "total_events": total_events,
            "platforms": results,
            "status": "completed"
        }
        
        # 保存汇总信息
        summary_path = os.path.join(self.archived_path, "historical_summary.txt")
        with open(summary_path, 'w', encoding='utf-8') as f:
            f.write(json.dumps(summary, ensure_ascii=False, indent=2))
        
        print(f"📊 历史数据汇总已保存到 {summary_path}")

# ============================================================================
# 程序入口
# ============================================================================
if __name__ == "__main__":
    # 检查依赖
    try:
        import requests
        from bs4 import BeautifulSoup
    except ImportError as e:
        print(f"❌ 缺少依赖包: {e}")
        print("请运行: pip install requests beautifulsoup4")
        exit(1)
    
    print("📚 投资日历历史数据采集器")
    print("=" * 50)
    
    # GitHub Actions环境检测
    if os.getenv('GITHUB_ACTIONS'):
        print("🤖 运行在GitHub Actions环境")
        # Actions环境下确保数据目录存在
        os.makedirs("./data/archived", exist_ok=True)
    
    collector = HistoricalDataCollector()
    collector.collect_all_historical_data()
    
    print("\n✅ 历史数据采集完成！")
    print("📁 数据已保存到 ./data/archived/ 目录")
    
    if not os.getenv('GITHUB_ACTIONS'):
        print("🔄 现在可以运行 daily_calendar.py 进行日常数据更新")
    else:
        print("🔄 Actions环境：历史数据采集完成，准备进行日常更新")

