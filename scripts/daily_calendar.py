#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
投资日历日常数据采集与变更检测
每日运行，采集未来数据并检测变更
"""

import requests
import json
import hashlib
import time
import os
import re
import shutil
import calendar
import urllib.parse
from datetime import datetime, timedelta
from typing import List, Dict, Any
from dataclasses import dataclass, asdict
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
import random


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
    data_status: str = "ACTIVE"
    is_new: bool = False
    discovery_date: str = ""
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
        if not self.discovery_date:
            self.discovery_date = datetime.now().strftime('%Y-%m-%d')
    
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

def load_platform_data(platform: str, data_path: str) -> List[StandardizedEvent]:
    """从txt文件加载平台数据"""
    file_path = os.path.join(data_path, f"{platform}.txt")
    
    if not os.path.exists(file_path):
        return []
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            json_content = f.read()
            data = json.loads(json_content)
        
        events = []
        for event_data in data.get('events', []):
            event = StandardizedEvent(**event_data)
            events.append(event)
        
        return events
    except Exception as e:
        print(f"加载 {platform} 数据失败: {e}")
        return []

# ============================================================================
# 平台数据范围动态检测器
# ============================================================================

class PlatformRangeDetector:
    """平台数据范围动态检测器"""
    
    def __init__(self):
        self.range_cache = {}
    
    def get_platform_date_range(self, platform: str, start_date: str) -> str:
        """获取平台的实际数据范围"""
        cache_key = f"{platform}_{start_date}"
        
        if cache_key in self.range_cache:
            return self.range_cache[cache_key]
        
        if platform == "cls":
            max_date = self._detect_cls_max_date(start_date)
        elif platform == "jiuyangongshe":
            max_date = self._detect_jiuyan_max_date(start_date)
        elif platform == "tonghuashun":
            max_date = self._detect_tonghuashun_max_date(start_date)
        elif platform == "investing":
            max_date = self._detect_investing_max_date(start_date)
        elif platform == "eastmoney":
            max_date = self._detect_eastmoney_max_date(start_date)
        else:
            max_date = start_date
        
        self.range_cache[cache_key] = max_date
        return max_date
    
    def _detect_cls_max_date(self, start_date: str) -> str:
        """财联社：通常提供未来6个月数据"""
        test_date = (datetime.strptime(start_date, '%Y-%m-%d') + timedelta(days=180)).strftime('%Y-%m-%d')
        return test_date
    
    def _detect_jiuyan_max_date(self, start_date: str) -> str:
        """韭研公社：测试到年底"""
        return "2025-12-31"
    
    def _detect_tonghuashun_max_date(self, start_date: str) -> str:
        """同花顺：测试到年底"""
        return "2025-12-31"
    
    def _detect_investing_max_date(self, start_date: str) -> str:
        """英为财情：测试到明年底"""
        return "2025-12-31"
    
    def _detect_eastmoney_max_date(self, start_date: str) -> str:
        """东方财富：测试到年底"""
        return "2025-12-31"

# ============================================================================
# 未来数据采集器
# ============================================================================

class FutureDataCollector:
    """未来数据采集器"""
    
    def __init__(self):
        self.base_path = "./data"
        self.current_path = os.path.join(self.base_path, "active", "current")
        self.range_detector = PlatformRangeDetector()
        self._ensure_directories()
    
    def _ensure_directories(self):
        """确保目录存在"""
        os.makedirs(self.current_path, exist_ok=True)
    
    def collect_all_future_data(self) -> Dict[str, List[StandardizedEvent]]:
        """采集所有平台的未来数据（动态范围）"""
        today = datetime.now().strftime('%Y-%m-%d')
        
        print(f"🔮 开始采集未来数据（从 {today} 开始，动态检测最远日期）")
        print("=" * 60)
        
        results = {}
        total_events = 0
        
        # 财联社
        print(f"\n📡 正在采集 cls 未来数据...")
        max_date = self.range_detector.get_platform_date_range('cls', today)
        print(f"   📅 cls 数据范围: {today} → {max_date}")
        cls_events = self._collect_cls_future_dynamic(today, max_date)
        results['cls'] = cls_events
        total_events += len(cls_events)
        print(f"✅ cls 采集完成，共 {len(cls_events)} 个事件")
        
        # 韭研公社
        print(f"\n📡 正在采集 jiuyangongshe 未来数据...")
        max_date = self.range_detector.get_platform_date_range('jiuyangongshe', today)
        print(f"   📅 jiuyangongshe 数据范围: {today} → {max_date}")
        jiuyan_events = self._collect_jiuyan_future_dynamic(today, max_date)
        results['jiuyangongshe'] = jiuyan_events
        total_events += len(jiuyan_events)
        print(f"✅ jiuyangongshe 采集完成，共 {len(jiuyan_events)} 个事件")
        
        # 同花顺
        print(f"\n📡 正在采集 tonghuashun 未来数据...")
        max_date = self.range_detector.get_platform_date_range('tonghuashun', today)
        print(f"   📅 tonghuashun 数据范围: {today} → {max_date}")
        ths_events = self._collect_tonghuashun_future_dynamic(today, max_date)
        results['tonghuashun'] = ths_events
        total_events += len(ths_events)
        print(f"✅ tonghuashun 采集完成，共 {len(ths_events)} 个事件")
        
        # 英为财情
        print(f"\n📡 正在采集 investing 未来数据...")
        max_date = self.range_detector.get_platform_date_range('investing', today)
        print(f"   📅 investing 数据范围: {today} → {max_date}")
        inv_events = self._collect_investing_future_dynamic(today, max_date)
        results['investing'] = inv_events
        total_events += len(inv_events)
        print(f"✅ investing 采集完成，共 {len(inv_events)} 个事件")
        
        # 东方财富
        print(f"\n📡 正在采集 eastmoney 未来数据...")
        max_date = self.range_detector.get_platform_date_range('eastmoney', today)
        print(f"   📅 eastmoney 数据范围: {today} → {max_date}")
        em_events = self._collect_eastmoney_future_dynamic(today, max_date)
        results['eastmoney'] = em_events
        total_events += len(em_events)
        print(f"✅ eastmoney 采集完成，共 {len(em_events)} 个事件")
        
        print(f"\n📊 总计采集 {total_events} 个未来事件")
        return results
    
    def _collect_cls_future_dynamic(self, start_date: str, end_date: str) -> List[StandardizedEvent]:
        """采集财联社未来数据"""
        params = {
            "app": "CailianpressWeb",
            "flag": "0",  # 从今以后的数据
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
                    
                    # 只要start_date及以后且不超过end_date的数据
                    if not date or date < start_date or date > end_date:
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
            
    def _collect_jiuyan_future_dynamic(self, start_date: str, end_date: str) -> List[StandardizedEvent]:
        """采集韭研公社未来数据 - 修复版"""
        all_events = []  # 改为事件列表
        
        # 按月采集到end_date
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        
        current_dt = start_dt.replace(day=1)  # 从月初开始，避免日期问题
        
        while current_dt <= end_dt:
            year, month = current_dt.year, current_dt.month
            print(f"   📅 采集 {year}年{month}月...")
            
            try:
                events = self._get_jiuyan_month_data(year, month, start_date, end_date, is_future=True)
                if events:
                    all_events.extend(events)  # 添加到总列表
                    print(f"      ✅ {year}年{month}月: {len(events)} 个事件")
                else:
                    print(f"      ⚠️ {year}年{month}月: 无数据")
                    
                time.sleep(0.5)
                
            except Exception as e:
                print(f"      ❌ {year}年{month}月采集失败: {e}")
            
            # 安全地移动到下个月
            try:
                if current_dt.month == 12:
                    current_dt = current_dt.replace(year=current_dt.year + 1, month=1)
                else:
                    current_dt = current_dt.replace(month=current_dt.month + 1)
            except ValueError as e:
                print(f"      ❌ 日期计算错误: {e}")
                break
        
        print(f"   📊 韭研公社总计: {len(all_events)} 个事件")
        return all_events  # 返回事件列表，不是数量
    
    def _collect_tonghuashun_future_dynamic(self, start_date: str, end_date: str) -> List[StandardizedEvent]:
        """采集同花顺未来数据 - 修复版"""
        all_events = []
        
        # 按月采集到end_date
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        
        current_dt = start_dt.replace(day=1)  # 从月初开始
        
        while current_dt <= end_dt:
            year, month = current_dt.year, current_dt.month
            print(f"   📅 采集 {year}年{month}月...")
            
            try:
                events = self._get_tonghuashun_month_data(year, month, start_date, end_date, is_future=True)
                if events:
                    all_events.extend(events)
                    print(f"      ✅ {year}年{month}月: {len(events)} 个事件")
                else:
                    print(f"      ⚠️ {year}年{month}月: 无数据")
                    
                time.sleep(0.5)
                
            except Exception as e:
                print(f"      ❌ {year}年{month}月采集失败: {e}")
            
            # 安全地移动到下个月
            try:
                if current_dt.month == 12:
                    current_dt = current_dt.replace(year=current_dt.year + 1, month=1)
                else:
                    current_dt = current_dt.replace(month=current_dt.month + 1)
            except ValueError as e:
                print(f"      ❌ 日期计算错误: {e}")
                break
        
        print(f"   📊 同花顺总计: {len(all_events)} 个事件")
        return all_events

    def _collect_investing_future_dynamic(self, start_date: str, end_date: str) -> List[StandardizedEvent]:
        """采集英为财情未来数据（并发按天采集）"""
        print(f"   🔄 英为财情并发按天采集: {start_date} → {end_date}")
        
        # 生成日期列表
        date_list = []
        current_date = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        
        while current_date <= end_dt:
            date_list.append(current_date.strftime('%Y-%m-%d'))
            current_date += timedelta(days=1)
        
        print(f"   📅 总共需要采集 {len(date_list)} 天的数据")
        
        all_events = []
        max_workers = 5  # 并发线程数
        
        # 使用线程池并发采集
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有任务
            future_to_date = {
                executor.submit(self._request_investing_single_day_with_retry, date): date 
                for date in date_list
            }
            
            completed_count = 0
            
            # 处理完成的任务
            for future in as_completed(future_to_date):
                date = future_to_date[future]
                completed_count += 1
                
                try:
                    day_events = future.result()
                    if day_events:
                        all_events.extend(day_events)
                    
                    # 每50天或最后显示进度
                    if completed_count % 50 == 0 or completed_count == len(date_list):
                        print(f"      📊 进度: {completed_count}/{len(date_list)} 天完成，已获取 {len(all_events)} 个事件")
                        
                except Exception as e:
                    print(f"      ❌ {date} 采集失败: {e}")
        
        print(f"   📊 英为财情总计: {len(all_events)} 个事件 (并发采集 {len(date_list)} 天)")
        return all_events

    def _request_investing_single_day_with_retry(self, date: str, max_retries: int = 3) -> List[StandardizedEvent]:
        """请求英为财情单天数据（带重试和随机延迟）"""
        for attempt in range(max_retries):
            try:
                # 添加随机延迟，避免并发请求过于集中
                delay = random.uniform(0.2, 0.8)
                time.sleep(delay)
                
                return self._request_investing_single_day(date)
                
            except Exception as e:
                if attempt == max_retries - 1:
                    # 最后一次重试失败，返回空列表
                    return []
                else:
                    # 重试前等待
                    retry_delay = random.uniform(1.0, 2.0)
                    time.sleep(retry_delay)
        
        return []

    
    def _collect_eastmoney_future_dynamic(self, start_date: str, end_date: str) -> List[StandardizedEvent]:
        """采集东方财富未来数据"""
        params = {
            "fromdate": start_date,
            "todate": end_date,
            "option": "xsap,xgsg,tfpxx,hsgg,nbjb,jjsj,hyhy,gddh"
        }
        
        headers = {
            "Host": "data.eastmoney.com",
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
                
                print(f"   🔍 东方财富原始数据统计:")
                print(f"      休市安排(xsap): {safe_len(data.get('xsap'))} 条")
                print(f"      新股申购(xgsg): {safe_len(data.get('xgsg'))} 条")
                print(f"      停复牌信息(tfpxx): {safe_len(data.get('tfpxx'))} 条")
                print(f"      A股公告(hsgg): {safe_len(data.get('hsgg'))} 条")
                print(f"      年报季报(nbjb): {safe_len(data.get('nbjb'))} 条")
                print(f"      经济数据(jjsj): {safe_len(data.get('jjsj'))} 条")
                print(f"      行业会议(hyhy): {safe_len(data.get('hyhy'))} 条")
                print(f"      股东大会(gddh): {safe_len(data.get('gddh'))} 条")
                
                # 处理各类数据
                events.extend(self._process_eastmoney_xsap(data.get('xsap') or [], start_date, end_date))
                events.extend(self._process_eastmoney_xgsg(data.get('xgsg') or [], start_date, end_date))
                events.extend(self._process_eastmoney_tfpxx(data.get('tfpxx') or [], start_date, end_date))
                events.extend(self._process_eastmoney_hsgg(data.get('hsgg') or [], start_date, end_date))
                events.extend(self._process_eastmoney_nbjb(data.get('nbjb') or [], start_date, end_date))
                events.extend(self._process_eastmoney_jjsj(data.get('jjsj') or [], start_date, end_date))
                events.extend(self._process_eastmoney_hyhy(data.get('hyhy') or [], start_date, end_date))
                events.extend(self._process_eastmoney_gddh(data.get('gddh') or [], start_date, end_date))
                
                print(f"   ✅ 东方财富过滤后事件: {len(events)} 个")
                return events
            else:
                return []
        except Exception as e:
            print(f"东方财富API请求失败: {e}")
            return []
    
    # 财联社辅助方法
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
    
    # 韭研公社辅助方法
    def _get_jiuyan_month_data(self, year: int, month: int, start_date: str, end_date: str, is_future: bool = False) -> List[StandardizedEvent]:
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
                    
                    # 过滤日期范围
                    if not date or date < start_date or date > end_date:
                        continue
                    
                    # 验证日期有效性
                    try:
                        datetime.strptime(date, '%Y-%m-%d')
                    except ValueError:
                        print(f"        ⚠️ 无效日期: {date}")
                        continue
                    
                    for item in day_data.get('list', []):
                        try:
                            timeline = item.get('timeline', {})
                            event = StandardizedEvent(
                                platform="jiuyangongshe",
                                event_id=f"jygs_{item.get('article_id', '')}_{timeline.get('timeline_id', '')}_{date.replace('-', '')}",
                                original_id=item.get('article_id', ''),
                                event_date=date,
                                title=item.get('title', ''),
                                content=item.get('content', ''),
                                importance=max(1, min(5, 7 - timeline.get('grade', 6))),
                                country='中国',
                                themes=[theme.get('name', '') for theme in timeline.get('theme_list', [])],
                                raw_data=item
                            )
                            events.append(event)
                        except Exception as e:
                            print(f"        ❌ 事件处理失败: {e}")
                            continue
                
                return events
            else:
                return []
        except Exception as e:
            print(f"韭研公社API请求失败: {e}")
            return []
    
    def _get_tonghuashun_month_data(self, year: int, month: int, start_date: str, end_date: str, is_future: bool = False) -> List[StandardizedEvent]:
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
                    
                    # 过滤日期范围
                    if not date or date < start_date or date > end_date:
                        continue
                    
                    # 验证日期有效性
                    try:
                        datetime.strptime(date, '%Y-%m-%d')
                    except ValueError:
                        print(f"        ⚠️ 无效日期: {date}")
                        continue
                    
                    day_events = day_data.get('events', [])
                    concepts = day_data.get('concept', [])
                    
                    for i, event_data in enumerate(day_events):
                        if isinstance(event_data, list) and len(event_data) > 0:
                            try:
                                title = event_data[0] if event_data[0] else ""
                                title_hash = hashlib.md5(title.encode('utf-8')).hexdigest()[:8]
                                concept_info = concepts[i] if i < len(concepts) else []
                                
                                event = StandardizedEvent(
                                    platform="tonghuashun",
                                    event_id=f"ths_{date.replace('-', '')}_{i}_{title_hash}",
                                    original_id=f"{date}_{i}",
                                    event_date=date,
                                    title=title,
                                    importance=3,
                                    country='中国',
                                    concepts=[{"code": c.get("code"), "name": c.get("name")} for c in concept_info] if concept_info else [],
                                    raw_data={"event": event_data, "concept": concept_info}
                                )
                                events.append(event)
                            except Exception as e:
                                print(f"        ❌ 事件处理失败: {e}")
                                continue
                
                return events
            else:
                return []
        except Exception as e:
            print(f"同花顺API请求失败: {e}")
            return []
    
    # 英为财情辅助方法
    def _request_investing_single_day(self, date: str) -> List[StandardizedEvent]:
        """请求英为财情单天数据"""
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
                
                return self._parse_investing_html_simple(html_content, date)
            else:
                return []
        except Exception as e:
            return []
    
    def _parse_investing_html_simple(self, html_content: str, date: str) -> List[StandardizedEvent]:
        """解析英为财情HTML数据"""
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
                    
                    # 只要当天的数据
                    if event_date == date:
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
                            importance=event_data.get('importance', 1),
                            country=event_data.get('country'),
                            raw_data=event_data
                        )
                        events.append(event)
        except Exception as e:
            print(f"英为财情HTML解析失败: {e}")
        
        return events
    
    def _extract_investing_event_enhanced(self, row) -> Dict[str, Any]:
        """增强版英为财情事件数据提取"""
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
    
    # 东方财富辅助方法
    def _extract_date_fixed(self, date_str: str) -> str:
        """修复版日期提取"""
        if not date_str:
            return ""
        try:
            if ' ' in date_str:
                return date_str.split(' ')[0]
            return date_str
        except Exception as e:
            return ""
    
    def _extract_time_fixed(self, datetime_str: str) -> str:
        """修复版时间提取"""
        if not datetime_str:
            return None
        try:
            if ' ' in datetime_str:
                parts = datetime_str.split(' ')
                if len(parts) > 1 and parts[1] != "00:00:00":
                    return parts[1]
        except:
            return None
    
    def _process_eastmoney_xsap(self, xsap_data: list, start_date: str, end_date: str) -> List[StandardizedEvent]:
        """处理休市安排数据"""
        events = []
        for item in xsap_data:
            if not item:
                continue
            item_date = self._extract_date_fixed(item.get('SDATE'))
            if item_date and start_date <= item_date <= end_date:
                event = StandardizedEvent(
                    platform="eastmoney",
                    event_id=f"em_xsap_{item_date.replace('-', '')}_{hash(item.get('HOLIDAY', ''))}",
                    original_id=f"xsap_{item.get('MKT', '')}_{item_date}",
                    event_date=item_date,
                    event_time=self._extract_time_fixed(item.get('SDATE')),
                    event_datetime=item.get('SDATE'),
                    title=f"{item.get('MKT', '')} - {item.get('HOLIDAY', '')}",
                    category='休市安排',
                    importance=2,
                    country='中国',
                    raw_data=item
                )
                events.append(event)
        return events
    
    def _process_eastmoney_xgsg(self, xgsg_data: list, start_date: str, end_date: str) -> List[StandardizedEvent]:
        """处理新股申购数据"""
        events = []
        for item in xgsg_data:
            if not item:
                continue
            item_date = self._extract_date_fixed(item.get('APPLY_DATE'))
            if item_date and start_date <= item_date <= end_date:
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
        return events
    
    def _process_eastmoney_tfpxx(self, tfpxx_data: list, start_date: str, end_date: str) -> List[StandardizedEvent]:
        """处理停复牌信息 - 修复版"""
        events = []
        
        print(f"      🔍 处理停复牌信息: 输入 {len(tfpxx_data)} 条")
        
        for tfpxx_item in tfpxx_data:
            if not tfpxx_item:
                continue
            
            # 数据结构：tfpxx_item包含Date和Data字段
            item_date = self._extract_date_fixed(tfpxx_item.get('Date'))
            data_list = tfpxx_item.get('Data', [])
            
            print(f"      📅 日期: {item_date}, 停复牌股票: {len(data_list)} 只")
            
            if item_date and start_date <= item_date <= end_date:
                for i, stock_data in enumerate(data_list):
                    if not stock_data:
                        continue
                    
                    stock_code = stock_data.get('Scode', '')
                    stock_name = stock_data.get('Sname', '')
                    
                    event = StandardizedEvent(
                        platform="eastmoney",
                        event_id=f"em_tfpxx_{stock_code}_{item_date.replace('-', '')}_{i}",
                        original_id=stock_code,
                        event_date=item_date,
                        title=f"{stock_name}停复牌",
                        content=f"股票代码: {stock_code}, 股票名称: {stock_name}",
                        category='停复牌信息',
                        importance=2,
                        country='中国',
                        stocks=[stock_code] if stock_code else [],
                        raw_data={
                            'stock_code': stock_code,
                            'stock_name': stock_name,
                            'date': item_date
                        }
                    )
                    events.append(event)
                
                print(f"      ✅ {item_date} 创建了 {len(data_list)} 个停复牌事件")
        
        print(f"      📊 停复牌信息处理完成: {len(events)} 个有效事件")
        return events

    

    def _process_eastmoney_hsgg(self, hsgg_data: list, start_date: str, end_date: str) -> List[StandardizedEvent]:
        """处理A股公告 - 修复版"""
        events = []
        
        print(f"      🔍 处理A股公告数据: 输入 {len(hsgg_data)} 条")
        
        for hsgg_item in hsgg_data:
            if not hsgg_item:
                continue
            
            # 新的数据结构：hsgg_item包含Date和Data字段
            item_date = self._extract_date_fixed(hsgg_item.get('Date'))
            total_count = hsgg_item.get('TotalCount', 0)
            data_list = hsgg_item.get('Data', [])
            
            print(f"      📅 日期: {item_date}, 总数: {total_count}, 当日数据: {len(data_list)} 条")
            
            if item_date and start_date <= item_date <= end_date:
                # 处理当日的所有股票公告
                for i, stock_data in enumerate(data_list):
                    if not stock_data:
                        continue
                    
                    stock_code = stock_data.get('Scode', '')
                    stock_name = stock_data.get('Sname', '')
                    
                    event = StandardizedEvent(
                        platform="eastmoney",
                        event_id=f"em_hsgg_{stock_code}_{item_date.replace('-', '')}_{i}",
                        original_id=stock_code,
                        event_date=item_date,
                        title=f"{stock_name}发布公告",
                        content=f"股票代码: {stock_code}, 股票名称: {stock_name}",
                        category='A股公告',
                        importance=3,
                        country='中国',
                        stocks=[stock_code] if stock_code else [],
                        raw_data={
                            'stock_code': stock_code,
                            'stock_name': stock_name,
                            'date': item_date,
                            'total_count': total_count
                        }
                    )
                    events.append(event)
                
                print(f"      ✅ {item_date} 创建了 {len(data_list)} 个A股公告事件")
            else:
                print(f"      ❌ 日期不在范围内: {item_date}")
        
        print(f"      📊 A股公告处理完成: {len(events)} 个有效事件")
        return events

    
    
    def _process_eastmoney_nbjb(self, nbjb_data: list, start_date: str, end_date: str) -> List[StandardizedEvent]:
        """处理年报季报"""
        events = []
        for item in nbjb_data:
            if not item:
                continue
            item_date = self._extract_date_fixed(item.get('REPORT_DATE'))
            if item_date and start_date <= item_date <= end_date:
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
        return events
    
    def _process_eastmoney_jjsj(self, jjsj_data: list, start_date: str, end_date: str) -> List[StandardizedEvent]:
        """处理经济数据"""
        events = []
        for item in jjsj_data:
            if not item:
                continue
            item_date = self._extract_date_fixed(item.get('Date'))
            if item_date and start_date <= item_date <= end_date:
                data_items = item.get('Data') or []
                for data_item in data_items:
                    if not data_item:
                        continue
                    event = StandardizedEvent(
                        platform="eastmoney",
                        event_id=f"em_jjsj_{item.get('Date', '').replace('-', '').replace(' ', '').replace(':', '')}_{hash(data_item.get('Name', ''))}",
                        original_id=f"{item.get('Date')}_{data_item.get('Name')}",
                        event_date=item_date,
                        event_time=self._extract_time_fixed(item.get('Date')),
                        event_datetime=item.get('Date'),
                        title=data_item.get('Name', ''),
                        category='经济数据',
                        importance=4,
                        country=item.get('City', ''),
                        raw_data=data_item
                    )
                    events.append(event)
        return events
        
    def _process_eastmoney_hyhy(self, hyhy_data: list, start_date: str, end_date: str) -> List[StandardizedEvent]:
        """处理行业会议 - 修复版"""
        events = []
        
        print(f"      🔍 处理行业会议: 输入 {len(hyhy_data)} 条")
        
        for item in hyhy_data:
            if not item:
                continue
            
            start_event_date = self._extract_date_fixed(item.get('START_DATE'))
            end_event_date = self._extract_date_fixed(item.get('END_DATE'))
            
            print(f"      📅 会议: {item.get('FE_NAME', '')} ({start_event_date} ~ {end_event_date})")
            
            # 检查会议是否在查询范围内（会议开始或结束日期在范围内）
            if start_event_date and (
                (start_date <= start_event_date <= end_date) or 
                (end_event_date and start_date <= end_event_date <= end_date) or
                (start_event_date <= start_date and end_event_date and end_event_date >= end_date)
            ):
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
                    raw_data={
                        'fe_code': item.get('FE_CODE'),
                        'fe_name': item.get('FE_NAME'),
                        'start_date': start_event_date,
                        'end_date': end_event_date,
                        'fe_type': item.get('FE_TYPE'),
                        'sponsor': item.get('SPONSOR_NAME'),
                        'city': item.get('CITY'),
                        'content': item.get('CONTENT')
                    }
                )
                events.append(event)
                print(f"      ✅ 创建会议事件: {event.title}")
        
        print(f"      📊 行业会议处理完成: {len(events)} 个有效事件")
        return events

    
    def _process_eastmoney_gddh(self, gddh_data: list, start_date: str, end_date: str) -> List[StandardizedEvent]:
        """处理股东大会"""
        events = []
        for item in gddh_data:
            if not item:
                continue
            item_date = self._extract_date_fixed(item.get('MEETING_DATE'))
            if item_date and start_date <= item_date <= end_date:
                event = StandardizedEvent(
                    platform="eastmoney",
                    event_id=f"em_gddh_{item.get('SECURITY_CODE', '')}_{item_date.replace('-', '')}",
                    original_id=item.get('SECURITY_CODE', ''),
                    event_date=item_date,
                    event_time=self._extract_time_fixed(item.get('MEETING_DATE')),
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
        return events

# ============================================================================
# 数据存储管理
# ============================================================================

class DataStorage:
    """数据存储管理"""
    
    def __init__(self):
        self.base_path = "./data"
        self.current_path = os.path.join(self.base_path, "active", "current")
        self.previous_path = os.path.join(self.base_path, "active", "previous")
        self.archived_path = os.path.join(self.base_path, "archived")
        self._ensure_directories()
    
    def _ensure_directories(self):
        """确保目录存在"""
        os.makedirs(self.current_path, exist_ok=True)
        os.makedirs(self.previous_path, exist_ok=True)
    
    def save_platform_data(self, platform: str, events: List[StandardizedEvent]):
        """保存平台数据为txt文件"""
        file_path = os.path.join(self.current_path, f"{platform}.txt")
        
        data = {
            "platform": platform,
            "total_events": len(events),
            "data_status": "ACTIVE",
            "date_type": "FUTURE",
            "last_updated": datetime.now().isoformat(),
            "immutable": False,
            "events": [event.to_dict() for event in events]
        }
        
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(json.dumps(data, ensure_ascii=False, indent=2))
        
        print(f"💾 {platform} 数据已保存到 {file_path}")

    def save_all_data(self, all_data: Dict[str, List[StandardizedEvent]]):
        """保存所有平台数据"""
        for platform, events in all_data.items():
            self.save_platform_data(platform, events)
        
        # 生成汇总信息
        self._generate_summary(all_data)
    
    def _generate_summary(self, all_data: Dict[str, List[StandardizedEvent]]):
        """生成数据汇总信息"""
        summary = {
            "collection_type": "ACTIVE",
            "collection_time": datetime.now().isoformat(),
            "platforms": {},
            "total_events": 0,
            "date_range": {
                "start": None,
                "end": None
            }
        }
        
        all_dates = []
        
        for platform, events in all_data.items():
            platform_dates = [event.event_date for event in events if event.event_date]
            all_dates.extend(platform_dates)
            
            summary["platforms"][platform] = {
                "event_count": len(events),
                "date_range": {
                    "start": min(platform_dates) if platform_dates else None,
                    "end": max(platform_dates) if platform_dates else None
                }
            }
            summary["total_events"] += len(events)
        
        if all_dates:
            summary["date_range"]["start"] = min(all_dates)
            summary["date_range"]["end"] = max(all_dates)
        
        # 保存汇总信息为txt文件
        summary_path = os.path.join(self.current_path, "metadata.txt")
        with open(summary_path, 'w', encoding='utf-8') as f:
            f.write(json.dumps(summary, ensure_ascii=False, indent=2))
        
        print(f"📊 数据汇总信息已保存到 {summary_path}")

# ============================================================================
# 事件变更检测引擎
# ============================================================================

class ChangeDetectionEngine:
    """事件变更检测引擎"""
    
    def __init__(self):
        self.base_path = "./data"
        self.current_path = os.path.join(self.base_path, "active", "current")
        self.previous_path = os.path.join(self.base_path, "active", "previous")
    
    def detect_all_changes_with_new_data(self, new_data: Dict[str, List[StandardizedEvent]]) -> Dict[str, Dict[str, List[StandardizedEvent]]]:
        """使用新采集的数据进行变更检测"""
        platforms = ['cls', 'jiuyangongshe', 'tonghuashun', 'investing', 'eastmoney']
        all_changes = {}
        
        print("🔍 开始检测事件变更...")
        print("=" * 50)
        
        total_new = 0
        total_updated = 0
        total_cancelled = 0
        
        for platform in platforms:
            print(f"\n📊 检测 {platform} 平台变更...")
            
            try:
                # 加载previous数据
                previous_events = load_platform_data(platform, self.previous_path)
                
                # 使用新采集的数据作为current
                current_events = new_data.get(platform, [])
                
                # 检测变更
                changes = self._detect_platform_changes(platform, previous_events, current_events)
                all_changes[platform] = changes
                
                # 统计
                new_count = len(changes['new_events'])
                updated_count = len(changes['updated_events'])
                cancelled_count = len(changes['cancelled_events'])
                
                total_new += new_count
                total_updated += updated_count
                total_cancelled += cancelled_count
                
                print(f"   新增: {new_count}, 更新: {updated_count}, 取消: {cancelled_count}")
                
                # 如果有变更，标记新数据中的事件
                if new_count > 0 or updated_count > 0:
                    self._mark_changes_in_new_data(platform, new_data[platform], changes)
                
            except Exception as e:
                print(f"❌ {platform} 变更检测失败: {e}")
                all_changes[platform] = {
                    'new_events': [],
                    'updated_events': [],
                    'cancelled_events': []
                }
        
        # 生成变更报告
        self._generate_change_report(all_changes, total_new, total_updated, total_cancelled)
        
        return all_changes
    
    def _detect_platform_changes(self, platform: str, previous_events: List[StandardizedEvent], 
                                current_events: List[StandardizedEvent]) -> Dict[str, List[StandardizedEvent]]:
        """检测单个平台的变更"""
        
        # 构建事件映射
        prev_map = {self._generate_event_key(event): event for event in previous_events}
        curr_map = {self._generate_event_key(event): event for event in current_events}
        
        new_events = []
        updated_events = []
        cancelled_events = []
        
        today = datetime.now().strftime('%Y-%m-%d')
        
        # 检测新增和更新事件
        for key, curr_event in curr_map.items():
            if curr_event.event_date >= today:
                if key not in prev_map:
                    # 新增事件
                    new_events.append(curr_event)
                else:
                    # 检查是否有内容更新
                    prev_event = prev_map[key]
                    if self._has_content_changed(prev_event, curr_event):
                        updated_events.append(curr_event)
        
        # 检测取消事件
        for key, prev_event in prev_map.items():
            if prev_event.event_date >= today and key not in curr_map:
                cancelled_events.append(prev_event)
        
        return {
            'new_events': new_events,
            'updated_events': updated_events,
            'cancelled_events': cancelled_events
        }
    
    def _mark_changes_in_new_data(self, platform: str, current_events: List[StandardizedEvent], 
                                 changes: Dict[str, List[StandardizedEvent]]):
        """在新数据中标记变更事件"""
        today = datetime.now().strftime('%Y-%m-%d')
        
        # 标记新增事件
        new_event_keys = {self._generate_event_key(event) for event in changes['new_events']}
        
        # 标记更新事件
        updated_event_keys = {self._generate_event_key(event) for event in changes['updated_events']}
        
        for event in current_events:
            event_key = self._generate_event_key(event)
            
            if event_key in new_event_keys:
                event.is_new = True
                event.discovery_date = today
            elif event_key in updated_event_keys:
                event.discovery_date = today
    
    def _generate_event_key(self, event: StandardizedEvent) -> str:
        """生成事件唯一标识"""
        if event.platform == "cls":
            return f"{event.original_id}_{event.event_date}_{event.category}"
        elif event.platform == "jiuyangongshe":
            return f"{event.original_id}_{event.event_date}"
        elif event.platform == "tonghuashun":
            return f"{event.event_date}_{hashlib.md5(event.title.encode()).hexdigest()[:8]}"
        elif event.platform == "investing":
            return f"{event.raw_data.get('event_attr_id', '')}_{event.event_date}_{event.event_time}"
        elif event.platform == "eastmoney":
            return f"{event.original_id}_{event.event_date}"
        else:
            return f"{event.event_id}"
    
    def _has_content_changed(self, prev_event: StandardizedEvent, curr_event: StandardizedEvent) -> bool:
        """检测内容是否变更"""
        return (
            prev_event.title != curr_event.title or
            prev_event.event_datetime != curr_event.event_datetime or
            prev_event.importance != curr_event.importance or
            prev_event.content != curr_event.content or
            prev_event.country != curr_event.country
        )
    
    def _generate_change_report(self, all_changes: Dict[str, Dict[str, List[StandardizedEvent]]], 
                              total_new: int, total_updated: int, total_cancelled: int):
        """生成变更报告"""
        today = datetime.now().strftime('%Y-%m-%d')
        
        report = {
            "detection_time": datetime.now().isoformat(),
            "summary": {
                "total_new": total_new,
                "total_updated": total_updated,
                "total_cancelled": total_cancelled
            },
            "platforms": {}
        }
        
        # 收集所有新增事件用于排序
        all_new_events = []
        
        for platform, changes in all_changes.items():
            new_count = len(changes['new_events'])
            updated_count = len(changes['updated_events'])
            cancelled_count = len(changes['cancelled_events'])
            
            report["platforms"][platform] = {
                "new_events": new_count,
                "updated_events": updated_count,
                "cancelled_events": cancelled_count,
                "sample_new_titles": [event.title[:50] for event in changes['new_events'][:3]]
            }
            
            all_new_events.extend(changes['new_events'])
        
        # 按重要性排序新增事件
        all_new_events.sort(key=lambda x: x.importance or 0, reverse=True)
        report["top_new_events"] = [
            {
                "platform": event.platform,
                "date": event.event_date,
                "title": event.title,
                "importance": event.importance,
                "country": event.country
            }
            for event in all_new_events[:10]
        ]
        
        # 保存变更报告
        report_path = os.path.join(self.current_path, f"change_report_{today}.txt")
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(json.dumps(report, ensure_ascii=False, indent=2))
        
        print(f"\n📋 变更报告已保存到 {report_path}")
        
        # 打印统计
        print(f"\n📈 今日变更统计:")
        print(f"   新增事件: {total_new} 个")
        print(f"   更新事件: {total_updated} 个")
        print(f"   取消事件: {total_cancelled} 个")

# ============================================================================
# 数据生命周期管理
# ============================================================================

class DataLifecycleManager:
    """数据生命周期管理器"""
    
    def __init__(self):
        self.base_path = "./data"
        self.current_path = os.path.join(self.base_path, "active", "current")
        self.previous_path = os.path.join(self.base_path, "active", "previous")
        self.archived_path = os.path.join(self.base_path, "archived")
    
    def archive_specific_date_data(self, target_date: str):
        """归档指定日期的数据"""
        print(f"📦 归档 {target_date} 的数据...")
        
        target_dt = datetime.strptime(target_date, '%Y-%m-%d')
        year, month = target_dt.year, target_dt.month
        
        platforms = ['cls', 'jiuyangongshe', 'tonghuashun', 'investing', 'eastmoney']
        archived_count = 0
        
        for platform in platforms:
            # 从current中提取目标日期的数据
            current_events = load_platform_data(platform, self.current_path)
            target_date_events = [
                event for event in current_events 
                if event.event_date == target_date
            ]
            
            if target_date_events:
                # 标记为已归档
                for event in target_date_events:
                    event.data_status = "ARCHIVED"
                
                # 保存到归档目录
                self._append_to_archive(platform, year, month, target_date_events)
                archived_count += len(target_date_events)
                
                print(f"   📦 {platform}: {len(target_date_events)} 个事件已归档")
        
        print(f"✅ {target_date} 共归档 {archived_count} 个事件")
    
    def rotate_future_data_only(self, archive_date: str):
        """精确轮转：只移动未来数据到previous"""
        print(f"🔄 轮转未来数据（排除 {archive_date}）...")
        
        # 清空并重建previous目录
        if os.path.exists(self.previous_path):
            shutil.rmtree(self.previous_path)
        os.makedirs(self.previous_path, exist_ok=True)
        
        platforms = ['cls', 'jiuyangongshe', 'tonghuashun', 'investing', 'eastmoney']
        
        for platform in platforms:
            current_events = load_platform_data(platform, self.current_path)
            
            # 筛选：只要archive_date之后的数据
            future_events = [
                event for event in current_events 
                if event.event_date > archive_date
            ]
            
            print(f"   📋 {platform}: {len(future_events)} 个未来事件移至previous")
            
            # 保存到previous
            if future_events:
                self._save_platform_data_to_path(platform, future_events, self.previous_path)
    
    def _save_platform_data_to_path(self, platform: str, events: List[StandardizedEvent], target_path: str):
        """保存平台数据到指定路径"""
        file_path = os.path.join(target_path, f"{platform}.txt")
        
        data = {
            "platform": platform,
            "total_events": len(events),
            "data_status": "ACTIVE",
            "date_type": "FUTURE",
            "last_updated": datetime.now().isoformat(),
            "immutable": False,
            "events": [event.to_dict() for event in events]
        }
        
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(json.dumps(data, ensure_ascii=False, indent=2))
    
    def _append_to_archive(self, platform: str, year: int, month: int, events: List[StandardizedEvent]):
        """保存数据到归档目录"""
        month_path = os.path.join(self.archived_path, str(year), f"{month:02d}月")
        os.makedirs(month_path, exist_ok=True)
        
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
        
        # 构建数据结构
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
        
        # 保存为txt文件
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(json.dumps(data, ensure_ascii=False, indent=2))

# ============================================================================
# 日常任务调度器
# ============================================================================

class DailyTaskScheduler:
    """日常任务调度器"""
    
    def __init__(self):
        self.collector = FutureDataCollector()
        self.storage = DataStorage()
        self.change_detector = ChangeDetectionEngine()
        self.lifecycle_manager = DataLifecycleManager()
    
    def run_first_time(self):
        """首次运行：只采集当天及未来数据"""
        today = datetime.now().strftime('%Y-%m-%d')
        
        print(f"🚀 首次运行模式：采集 {today} 至各平台最远日期")
        print("=" * 60)
        
        # 检查是否真的是首次运行
        if not self._is_first_run():
            print("❌ 检测到已有活跃数据，请使用 --daily 模式")
            return False
        
        try:
            # 采集未来数据（包含今天）
            future_data = self.collector.collect_all_future_data()
            self.storage.save_all_data(future_data)
            
            # 创建首次运行标记
            self._create_first_run_marker()
            
            print("✅ 首次运行完成，明天开始使用 --daily 模式")
            return True
            
        except Exception as e:
            print(f"❌ 首次运行失败: {e}")
            return False
    
    def run_daily_update(self):
        """执行每日更新流程"""
        today = datetime.now().strftime('%Y-%m-%d')
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        print(f"🔄 日常更新模式：处理 {yesterday} 归档，采集 {today} 至最远日期")
        print("=" * 60)
        
        try:
            # 第1步：归档昨天的数据
            print(f"\n📦 第1步：归档 {yesterday} 的数据")
            self.lifecycle_manager.archive_specific_date_data(yesterday)
            
            # 第2步：数据轮转（只轮转未来数据）
            print(f"\n🔄 第2步：轮转未来数据到 previous")
            self.lifecycle_manager.rotate_future_data_only(yesterday)
            
            # 第3步：采集新的未来数据
            print(f"\n📡 第3步：采集 {today} 至最远日期的数据")
            future_data = self.collector.collect_all_future_data()
            
            # 第4步：检测事件变更
            print(f"\n🔍 第4步：检测事件变更")
            changes = self.change_detector.detect_all_changes_with_new_data(future_data)
            
            # 第5步：保存新数据到current
            print(f"\n💾 第5步：保存新数据")
            self.storage.save_all_data(future_data)
            
            print("\n✅ 日常更新流程完成")
            return True
            
        except Exception as e:
            print(f"❌ 日常更新流程失败: {e}")
            return False
    
    def _is_first_run(self):
        """检查是否为首次运行 - 简化版"""
        if not os.path.exists(self.storage.current_path):
            return True
        
        # 检查关键平台数据文件
        required_platforms = ['cls', 'jiuyangongshe', 'tonghuashun']  # 至少需要这3个
        valid_files = 0
        
        for platform in required_platforms:
            file_path = os.path.join(self.storage.current_path, f"{platform}.txt")
            if os.path.exists(file_path) and os.path.getsize(file_path) > 100:  # 文件存在且不为空
                valid_files += 1
        
        return valid_files < 2  # 至少需要2个有效文件

    
    def _create_first_run_marker(self):
        """创建首次运行标记"""
        marker_path = os.path.join(self.storage.current_path, "first_run_marker.txt")
        marker_data = {
            "first_run_date": datetime.now().strftime('%Y-%m-%d'),
            "first_run_time": datetime.now().isoformat(),
            "status": "completed"
        }
        
        with open(marker_path, 'w', encoding='utf-8') as f:
            f.write(json.dumps(marker_data, ensure_ascii=False, indent=2))

# ============================================================================
# 查询功能
# ============================================================================

def get_new_events_by_date(discovery_date: str = None) -> List[StandardizedEvent]:
    """查询指定日期的新增事件"""
    if discovery_date is None:
        discovery_date = datetime.now().strftime('%Y-%m-%d')
    
    all_new_events = []
    platforms = ['cls', 'jiuyangongshe', 'tonghuashun', 'investing', 'eastmoney']
    
    current_path = "./data/active/current"
    
    for platform in platforms:
        events = load_platform_data(platform, current_path)
        new_events = [
            event for event in events 
            if event.is_new and event.discovery_date == discovery_date
        ]
        all_new_events.extend(new_events)
    
    return all_new_events

def get_events_by_date(date: str) -> List[StandardizedEvent]:
    """获取指定日期的所有事件"""
    all_events = []
    platforms = ['cls', 'jiuyangongshe', 'tonghuashun', 'investing', 'eastmoney']
    
    current_path = "./data/active/current"
    
    for platform in platforms:
        events = load_platform_data(platform, current_path)
        date_events = [event for event in events if event.event_date == date]
        all_events.extend(date_events)
    
    return all_events

def get_events_by_date_range(start_date: str, end_date: str) -> List[StandardizedEvent]:
    """获取指定日期范围的所有事件"""
    all_events = []
    platforms = ['cls', 'jiuyangongshe', 'tonghuashun', 'investing', 'eastmoney']
    
    current_path = "./data/active/current"
    
    for platform in platforms:
        events = load_platform_data(platform, current_path)
        range_events = [
            event for event in events 
            if event.event_date and start_date <= event.event_date <= end_date
        ]
        all_events.extend(range_events)
    
    # 按日期和时间排序
    all_events.sort(key=lambda x: (x.event_date, x.event_time or "00:00:00"))
    return all_events

def get_events_by_platform(platform: str) -> List[StandardizedEvent]:
    """按平台获取事件"""
    current_path = "./data/active/current"
    return load_platform_data(platform, current_path)


def print_events_summary(events: List[StandardizedEvent]):
    """打印事件汇总信息（详细版，全部输出）"""
    if not events:
        print("没有找到事件")
        return
    
    print(f"共找到 {len(events)} 个事件:")
    print("=" * 120)
    
    # 输出所有事件
    for i, event in enumerate(events, 1):
        # 新增标识
        new_mark = "🆕 " if event.is_new else "   "
        
        # 重要性显示
        importance_str = "★" * (event.importance or 1) if event.importance else "☆"
        
        # 平台标识
        platform_display = {
            'cls': '财联社',
            'jiuyangongshe': '韭研公社',        
            'tonghuashun': '同花顺',
            'investing': '英为财情',
            'eastmoney': '东方财富'
        }.get(event.platform, event.platform)
        
        # 标题行
        title_line = f"{new_mark}{i:3d}. [{platform_display:8}] {event.event_date}"
        if event.event_time:
            title_line += f" {event.event_time}"
        title_line += f" | {importance_str}"
        
        print(title_line)
        print(f"      📋 {event.title}")
        
        # 显示数值信息（如果有）
        if event.content and any(keyword in event.content for keyword in ['实际值', '预测值', '前值']):
            print(f"      📊 {event.content}")
        elif event.content and len(event.content) <= 100:
            print(f"      💬 {event.content}")
        
        # 显示分类和国家
        info_parts = []
        if event.category:
            info_parts.append(f"类别: {event.category}")
        if event.country:
            info_parts.append(f"国家: {event.country}")
        if event.city:
            info_parts.append(f"城市: {event.city}")
        
        if info_parts:
            print(f"      🏷️  {' | '.join(info_parts)}")
        
        # 显示相关股票（如果有）
        if event.stocks and len(event.stocks) > 0:
            stocks_str = ', '.join(event.stocks[:5])  # 最多显示5个股票
            if len(event.stocks) > 5:
                stocks_str += f" 等{len(event.stocks)}只股票"
            print(f"      📈 相关股票: {stocks_str}")
        
        # 显示主题概念（如果有）
        if event.themes and len(event.themes) > 0:
            themes_str = ', '.join(event.themes[:3])  # 最多显示3个主题
            if len(event.themes) > 3:
                themes_str += f" 等{len(event.themes)}个主题"
            print(f"      🎯 相关主题: {themes_str}")
        
        # 显示概念股（如果有）
        if event.concepts and len(event.concepts) > 0:
            concepts_str = ', '.join([c.get('name', '') for c in event.concepts[:3] if c.get('name')])
            if len(event.concepts) > 3:
                concepts_str += f" 等{len(event.concepts)}个概念"
            if concepts_str:
                print(f"      💡 相关概念: {concepts_str}")
        
        # 新增事件显示发现日期
        if event.is_new and event.discovery_date:
            print(f"      🔍 发现日期: {event.discovery_date}")
        
        print()  # 空行分隔
    
    # 统计信息
    print("\n" + "=" * 60)
    print("📊 统计信息:")
    
    # 按平台统计
    platform_stats = {}
    new_count = 0
    for event in events:
        platform_name = {
            'cls': '财联社',
            'jiuyangongshe': '韭研公社', 
            'tonghuashun': '同花顺',
            'investing': '英为财情',
            'eastmoney': '东方财富'
        }.get(event.platform, event.platform)
        
        platform_stats[platform_name] = platform_stats.get(platform_name, 0) + 1
        if event.is_new:
            new_count += 1
    
    print("📈 按平台分布:")
    for platform, count in sorted(platform_stats.items()):
        print(f"   {platform}: {count} 个")
    
    # 按类别统计
    category_stats = {}
    for event in events:
        if event.category:
            category_stats[event.category] = category_stats.get(event.category, 0) + 1
    
    if category_stats:
        print("\n🏷️  按类别分布:")
        for category, count in sorted(category_stats.items(), key=lambda x: x[1], reverse=True):
            print(f"   {category}: {count} 个")
    
    # 按重要性统计
    importance_stats = {}
    for event in events:
        importance = event.importance or 1
        importance_stats[importance] = importance_stats.get(importance, 0) + 1
    
    if importance_stats:
        print("\n⭐ 按重要性分布:")
        for importance in sorted(importance_stats.keys(), reverse=True):
            stars = "★" * importance
            count = importance_stats[importance]
            print(f"   {stars} ({importance}星): {count} 个")
    
    if new_count > 0:
        print(f"\n🆕 新增事件: {new_count} 个")
    
    print(f"\n✅ 已显示全部 {len(events)} 个事件")

def show_system_status():
    """显示系统状态"""
    print("\n📊 系统状态:")
    print("-" * 40)
    
    # 检查活跃数据
    current_path = "./data/active/current"
    if os.path.exists(current_path):
        txt_files = [f for f in os.listdir(current_path) if f.endswith('.txt') and f not in ['metadata.txt', 'first_run_marker.txt']]
        print(f"活跃数据平台: {len(txt_files)} 个")
        
        total_active_events = 0
        total_new_events = 0
        
        for txt_file in txt_files:
            platform = txt_file.replace('.txt', '')
            events = load_platform_data(platform, current_path)
            new_events = [e for e in events if e.is_new]
            
            platform_name = {
                'cls': '财联社',
                'jiuyangongshe': '韭研公社',
                'tonghuashun': '同花顺',
                'investing': '英为财情',
                'eastmoney': '东方财富'
            }.get(platform, platform)
            
            print(f"  {platform_name}: {len(events)} 个事件 (新增: {len(new_events)})")
            total_active_events += len(events)
            total_new_events += len(new_events)
        
        print(f"\n活跃数据总计: {total_active_events} 个事件")
        if total_new_events > 0:
            print(f"新增事件总计: {total_new_events} 个")
    
    # 检查历史数据
    archived_path = "./data/archived"
    if os.path.exists(archived_path):
        total_archived_events = 0
        archived_months = 0
        archived_files = 0
        
        for year_dir in os.listdir(archived_path):
            if year_dir.isdigit():
                year_path = os.path.join(archived_path, year_dir)
                for month_dir in os.listdir(year_path):
                    if month_dir.endswith('月'):
                        archived_months += 1
                        month_path = os.path.join(year_path, month_dir)
                        txt_files = [f for f in os.listdir(month_path) if f.endswith('.txt')]
                        archived_files += len(txt_files)
                        
                        # 统计历史事件数量
                        for txt_file in txt_files:
                            platform = txt_file.replace('.txt', '')
                            try:
                                events = load_platform_data(platform, month_path)
                                total_archived_events += len(events)
                            except:
                                pass
        
        print(f"\n历史数据: {archived_months} 个月, {archived_files} 个文件")
        print(f"历史事件总计: {total_archived_events} 个")
    
    # 检查比较基准
    previous_path = "./data/active/previous"
    if os.path.exists(previous_path):
        txt_files = [f for f in os.listdir(previous_path) if f.endswith('.txt') and f != 'metadata.txt']
        
        total_previous_events = 0
        for txt_file in txt_files:
            platform = txt_file.replace('.txt', '')
            try:
                events = load_platform_data(platform, previous_path)
                total_previous_events += len(events)
            except:
                pass
        
        print(f"比较基准: {len(txt_files)} 个平台, {total_previous_events} 个事件")
    
    # 检查数据日期范围
    print(f"\n📅 数据日期范围:")
    try:
        all_events = []
        for platform in ['cls', 'jiuyangongshe', 'tonghuashun', 'investing', 'eastmoney']:
            events = load_platform_data(platform, current_path)
            all_events.extend(events)
        
        if all_events:
            dates = [event.event_date for event in all_events if event.event_date]
            if dates:
                min_date = min(dates)
                max_date = max(dates)
                print(f"活跃数据范围: {min_date} 至 {max_date}")
    except:
        pass


# ============================================================================
# 程序入口
# ============================================================================
if __name__ == "__main__":
    import sys
    
    # 检查依赖
    try:
        import requests
        from bs4 import BeautifulSoup
    except ImportError as e:
        print(f"❌ 缺少依赖包: {e}")
        print("请运行: pip install requests beautifulsoup4")
        sys.exit(1)
    
    print("🔄 投资日历日常数据管理系统")
    print("=" * 50)
    
    # 新增：GitHub Actions环境支持
    if os.getenv('GITHUB_ACTIONS'):
        print("🤖 GitHub Actions环境，执行日常更新")
        os.makedirs("./data/active", exist_ok=True)
        scheduler = DailyTaskScheduler()
        success = scheduler.run_daily_update()
        sys.exit(0 if success else 1)
    
    # 原有的命令行参数处理代码保持不变
    if len(sys.argv) > 1:
        if sys.argv[1] == "--first-run":
            # 首次运行模式
            scheduler = DailyTaskScheduler()
            success = scheduler.run_first_time()
            if success:
                print("✅ 首次运行完成")
            else:
                sys.exit(1)
        
        elif sys.argv[1] == "--collect":
            # 只采集未来数据
            collector = FutureDataCollector()
            storage = DataStorage()
            future_data = collector.collect_all_future_data()
            storage.save_all_data(future_data)
            print("✅ 数据采集完成")
            
        elif sys.argv[1] == "--detect":
            # 只执行变更检测
            collector = FutureDataCollector()
            detector = ChangeDetectionEngine()
            future_data = collector.collect_all_future_data()
            detector.detect_all_changes_with_new_data(future_data)
            print("✅ 变更检测完成")
            
        elif sys.argv[1] == "--daily":
            # 执行完整日常更新
            scheduler = DailyTaskScheduler()
            success = scheduler.run_daily_update()
            if success:
                print("✅ 日常更新完成")
            else:
                sys.exit(1)
            
        elif sys.argv[1] == "--status":
            # 显示系统状态
            show_system_status()
            
        elif sys.argv[1] == "--new":
            # 查询新增事件
            discovery_date = sys.argv[2] if len(sys.argv) > 2 else datetime.now().strftime('%Y-%m-%d')
            try:
                datetime.strptime(discovery_date, '%Y-%m-%d')
                new_events = get_new_events_by_date(discovery_date)
                print(f"{discovery_date} 新增事件:")
                print_events_summary(new_events)
            except ValueError:
                print("❌ 日期格式错误，请使用 YYYY-MM-DD 格式")
                sys.exit(1)
            
        elif sys.argv[1] == "--today":
            # 查询今日事件
            today = datetime.now().strftime('%Y-%m-%d')
            events = get_events_by_date(today)
            print(f"今日 ({today}) 事件:")
            print_events_summary(events)
            
        elif sys.argv[1] == "--date":
            # 查询指定日期事件
            if len(sys.argv) > 2:
                query_date = sys.argv[2]
                try:
                    datetime.strptime(query_date, '%Y-%m-%d')
                    events = get_events_by_date(query_date)
                    print(f"{query_date} 的事件:")
                    print_events_summary(events)
                except ValueError:
                    print("❌ 日期格式错误，请使用 YYYY-MM-DD 格式")
                    sys.exit(1)
            else:
                print("❌ 请指定查询日期")
                print("用法: python daily_calendar.py --date YYYY-MM-DD")
                sys.exit(1)
        
        elif sys.argv[1] == "--range":
            # 查询日期范围事件
            if len(sys.argv) > 3:
                start_date = sys.argv[2]
                end_date = sys.argv[3]
                try:
                    datetime.strptime(start_date, '%Y-%m-%d')
                    datetime.strptime(end_date, '%Y-%m-%d')
                    
                    if start_date <= end_date:
                        events = get_events_by_date_range(start_date, end_date)
                        print(f"{start_date} 至 {end_date} 的事件:")
                        print_events_summary(events)
                    else:
                        print("❌ 开始日期不能晚于结束日期")
                        sys.exit(1)
                except ValueError:
                    print("❌ 日期格式错误，请使用 YYYY-MM-DD 格式")
                    sys.exit(1)
            else:
                print("❌ 请指定开始和结束日期")
                print("用法: python daily_calendar.py --range YYYY-MM-DD YYYY-MM-DD")
                sys.exit(1)
        
        elif sys.argv[1] == "--platform":
            # 按平台查询事件
            if len(sys.argv) > 2:
                platform = sys.argv[2]
                valid_platforms = ['cls', 'jiuyangongshe', 'tonghuashun', 'investing', 'eastmoney']
                
                if platform in valid_platforms:
                    events = get_events_by_platform(platform)
                    platform_name = {
                        'cls': '财联社',
                        'jiuyangongshe': '韭研公社',
                        'tonghuashun': '同花顺',
                        'investing': '英为财情',
                        'eastmoney': '东方财富'
                    }[platform]
                    print(f"{platform_name} 的事件:")
                    print_events_summary(events)
                else:
                    print(f"❌ 无效的平台名称: {platform}")
                    print(f"可用平台: {', '.join(valid_platforms)}")
                    sys.exit(1)
            else:
                print("❌ 请指定平台名称")
                print("用法: python daily_calendar.py --platform [cls|jiuyangongshe|tonghuashun|investing|eastmoney]")
                sys.exit(1)
        
        elif sys.argv[1] == "--help" or sys.argv[1] == "-h":
            # 显示帮助信息
            print("投资日历日常数据管理系统 - 使用说明")
            print("=" * 50)
            print("运行模式:")
            print("  python daily_calendar.py --first-run     # 首次运行模式")
            print("  python daily_calendar.py --daily         # 日常更新模式")
            print("  python daily_calendar.py --collect       # 只采集数据")
            print("  python daily_calendar.py --detect        # 只检测变更")
            print()
            print("查询功能:")
            print("  python daily_calendar.py --today         # 查询今日事件")
            print("  python daily_calendar.py --new [日期]     # 查询新增事件")
            print("  python daily_calendar.py --date 日期      # 查询指定日期")
            print("  python daily_calendar.py --range 开始 结束 # 查询日期范围")
            print("  python daily_calendar.py --platform 平台  # 按平台查询")
            print("  python daily_calendar.py --status        # 系统状态")
            print()
            print("参数说明:")
            print("  日期格式: YYYY-MM-DD (如: 2025-01-15)")
            print("  平台名称: cls, jiuyangongshe, tonghuashun, investing, eastmoney")
            print()
            print("示例:")
            print("  python daily_calendar.py --new 2025-01-15")
            print("  python daily_calendar.py --range 2025-01-15 2025-01-17")
            print("  python daily_calendar.py --platform investing")
            
        else:
            print("❌ 未知参数，使用 --help 查看使用说明")
            print("常用命令:")
            print("  python daily_calendar.py --first-run    # 首次运行")
            print("  python daily_calendar.py --daily        # 日常更新")
            print("  python daily_calendar.py --today        # 查询今日事件")
            print("  python daily_calendar.py --help         # 完整帮助")
            sys.exit(1)
    else:
        # 默认：自动判断运行模式
        scheduler = DailyTaskScheduler()
        if scheduler._is_first_run():
            print("🔍 检测到首次运行，执行首次采集...")
            success = scheduler.run_first_time()
        else:
            print("🔄 执行日常更新...")
            success = scheduler.run_daily_update()
        
        if not success:
            sys.exit(1)
        
        print("✅ 程序执行完成")


            
