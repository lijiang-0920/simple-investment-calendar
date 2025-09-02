#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
生成静态网页数据
将采集的数据转换为前端可用的JSON文件
"""

import json
import os
from datetime import datetime, timedelta
from typing import List, Dict, Any

def load_platform_data(platform: str, data_path: str) -> List[Dict]:
    """加载平台数据"""
    file_path = os.path.join(data_path, f"{platform}.txt")
    
    if not os.path.exists(file_path):
        return []
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.loads(f.read())
        return data.get('events', [])
    except Exception as e:
        print(f"加载 {platform} 数据失败: {e}")
        return []

def get_all_events_by_date(target_date: str) -> List[Dict]:
    """获取指定日期的所有事件"""
    all_events = []
    platforms = ['cls', 'jiuyangongshe', 'tonghuashun', 'investing', 'eastmoney']
    
    # 判断是历史数据还是活跃数据
    today = datetime.now().strftime('%Y-%m-%d')
    
    if target_date < today:
        # 历史数据：先尝试从archived目录读取
        target_dt = datetime.strptime(target_date, '%Y-%m-%d')
        year, month = target_dt.year, target_dt.month
        archived_path = f"./data/archived/{year}/{month:02d}月"
        
        # 检查archived目录是否存在
        if os.path.exists(archived_path):
            # 有历史数据，从archived读取
            for platform in platforms:
                events = load_platform_data(platform, archived_path)
                date_events = [e for e in events if e.get('event_date') == target_date]
                all_events.extend(date_events)
        else:
            # 没有历史数据，跳过历史日期
            return []
    else:
        # 活跃数据：从current目录读取
        current_path = "./data/active/current"
        if os.path.exists(current_path):
            for platform in platforms:
                events = load_platform_data(platform, current_path)
                date_events = [e for e in events if e.get('event_date') == target_date]
                all_events.extend(date_events)
        else:
            return []
    
    return all_events

def get_date_range() -> Dict[str, str]:
    """获取数据日期范围"""
    current_path = "./data/active/current"
    platforms = ['cls', 'jiuyangongshe', 'tonghuashun', 'investing', 'eastmoney']
    
    all_dates = []
    
    # 尝试从活跃数据获取日期
    if os.path.exists(current_path):
        for platform in platforms:
            events = load_platform_data(platform, current_path)
            dates = [e.get('event_date') for e in events if e.get('event_date')]
            all_dates.extend(dates)
    
    # 尝试从历史数据获取日期范围
    archived_path = "./data/archived"
    historical_dates = []
    
    if os.path.exists(archived_path):
        for year_dir in os.listdir(archived_path):
            if year_dir.isdigit():
                year_path = os.path.join(archived_path, year_dir)
                for month_dir in os.listdir(year_path):
                    if month_dir.endswith('月'):
                        month_path = os.path.join(year_path, month_dir)
                        for platform in platforms:
                            events = load_platform_data(platform, month_path)
                            dates = [e.get('event_date') for e in events if e.get('event_date')]
                            historical_dates.extend(dates)
    
    # 合并所有日期
    all_dates.extend(historical_dates)
    
    if all_dates:
        min_date = min(all_dates)
        max_date = max(all_dates)
    else:
        # 如果没有数据，使用默认范围
        today = datetime.now().strftime('%Y-%m-%d')
        min_date = today
        max_date = today
        print("   ⚠️ 未找到任何数据，使用默认日期范围")
    
    return {"start": min_date, "end": max_date}

def generate_daily_json(date: str, events: List[Dict]) -> Dict:
    """生成单日JSON数据"""
    # 统计信息
    total_events = len(events)
    new_events = len([e for e in events if e.get('is_new', False)])
    
    platform_stats = {}
    for event in events:
        platform = event.get('platform', 'unknown')
        platform_stats[platform] = platform_stats.get(platform, 0) + 1
    
    return {
        "date": date,
        "total_events": total_events,
        "new_events": new_events,
        "platforms": platform_stats,
        "events": events
    }

def generate_metadata() -> Dict:
    """生成元数据"""
    date_range = get_date_range()
    
    return {
        "platforms": [
            {"id": "cls", "name": "财联社"},
            {"id": "jiuyangongshe", "name": "韭研公社"},
            {"id": "tonghuashun", "name": "同花顺"},
            {"id": "investing", "name": "英为财情"},
            {"id": "eastmoney", "name": "东方财富"}
        ],
        "date_range": date_range,
        "last_updated": datetime.now().isoformat()
    }

def main():
    """主函数"""
    print("🔄 开始生成静态网页数据...")
    
    # 确保输出目录存在
    os.makedirs("./docs/data/events", exist_ok=True)
    
    # 检查数据目录是否存在
    if not os.path.exists("./data"):
        print("❌ 数据目录不存在，请先运行数据采集脚本")
        return
    
    # 获取日期范围
    date_range = get_date_range()
    
    if not date_range["start"]:
        print("❌ 未找到任何数据")
        return
    
    start_date = datetime.strptime(date_range["start"], '%Y-%m-%d')
    end_date = datetime.strptime(date_range["end"], '%Y-%m-%d')
    
    print(f"📅 数据日期范围: {date_range['start']} 至 {date_range['end']}")
    
    # 生成每日数据文件
    current_date = start_date
    total_files = 0
    total_events = 0
    
    while current_date <= end_date:
        date_str = current_date.strftime('%Y-%m-%d')
        
        try:
            events = get_all_events_by_date(date_str)
            
            if events:  # 只生成有事件的日期文件
                daily_data = generate_daily_json(date_str, events)
                
                # 保存日期文件
                file_path = f"./docs/data/events/{date_str}.json"
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(daily_data, f, ensure_ascii=False, indent=2)
                
                total_files += 1
                total_events += len(events)
                
                if total_files % 10 == 0:
                    print(f"   已生成 {total_files} 个日期文件...")
        
        except Exception as e:
            print(f"   ❌ {date_str} 处理失败: {e}")
        
        current_date += timedelta(days=1)
    
    # 生成元数据文件
    metadata = generate_metadata()
    with open("./docs/data/metadata.json", 'w', encoding='utf-8') as f:
        json.dump(metadata, f, ensure_ascii=False, indent=2)
    
    # 生成最新数据摘要
    today = datetime.now().strftime('%Y-%m-%d')
    try:
        today_events = get_all_events_by_date(today)
        latest_data = {
            "date": today,
            "total_events": len(today_events),
            "new_events": len([e for e in today_events if e.get('is_new', False)]),
            "last_updated": datetime.now().isoformat()
        }
        with open("./docs/data/latest.json", 'w', encoding='utf-8') as f:
            json.dump(latest_data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"   ⚠️ 生成最新摘要失败: {e}")
        # 创建空的latest.json
        empty_latest = {
            "date": today,
            "total_events": 0,
            "new_events": 0,
            "last_updated": datetime.now().isoformat()
        }
        with open("./docs/data/latest.json", 'w', encoding='utf-8') as f:
            json.dump(empty_latest, f, ensure_ascii=False, indent=2)
    
    print(f"✅ 静态数据生成完成！")
    print(f"   📁 生成了 {total_files} 个日期文件")
    print(f"   📊 总计 {total_events} 个事件")
    print(f"   📊 元数据文件: metadata.json")
    print(f"   🔄 最新摘要: latest.json")

if __name__ == "__main__":
    main()
