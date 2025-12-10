import redis
import asyncio
import csv
import os
import time
import math
import datetime
import requests
from bilibili_api import video, Credential, sync
# 尝试导入核心解码器
try:
    import dm_pb2
except ImportError:
    print("缺少 dm_pb2.py 文件！")
    exit(1)

# ================= 配置 =================
REDIS_HOST = 'localhost'
QUEUE_NAME = 'bilibili_tasks'

# 你的 Cookie (必须填！)
SESSDATA = ""
BILI_JCT = ""
BUVID3 = ""
# =========================================

r = redis.Redis(host=REDIS_HOST, port=6379, db=0, decode_responses=True)

def decode_protobuf(binary_data):
    """
    核心解析逻辑：参考 danMaKuApi.py
    将二进制流转换为结构化数据列表
    """
    try:
        danmaku_seg = dm_pb2.DmSegMobileReply()
        danmaku_seg.ParseFromString(binary_data)
        
        res = []
        for elem in danmaku_seg.elems:
            # 字段映射参考源码 src/api/danMaKuApi.py
            res.append({
                'dmid': str(elem.id),
                'video_time': round(elem.progress / 1000.0, 3), # 源码中 progress 单位为ms
                'text': elem.content,
                'send_date': datetime.datetime.fromtimestamp(elem.ctime).strftime('%Y-%m-%d %H:%M:%S'),
                'uid': elem.midHash
            })
        return res
    except Exception as e:
        print(f"      ⚠️ Protobuf 解析失败: {e}")
        return None

def download_and_parse(url, params, save_csv_writer, seen_ids, raw_save_path=None):
    """
    通用下载函数：支持原生请求 + Protobuf解析 + CSV写入 + Bin备份
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Cookie": f"SESSDATA={SESSDATA}"
    }
    
    try:
        # 发送请求
        resp = requests.get(url, params=params, headers=headers, timeout=10)
        
        if resp.status_code == 200 and len(resp.content) > 0:
            # 1. 尝试解析
            parsed_data = decode_protobuf(resp.content)
            
            if parsed_data:
                count = 0
                for row in parsed_data:
                    if row['dmid'] not in seen_ids:
                        row['source'] = 'api'
                        save_csv_writer.writerow(row)
                        seen_ids.add(row['dmid'])
                        count += 1
                return True, count
            else:
                # 2. 解析失败，保存原始 .bin 文件兜底
                if raw_save_path:
                    with open(raw_save_path, "wb") as f:
                        f.write(resp.content)
                    print(f"      💾 解析失败，已保存原始数据: {raw_save_path}")
                return False, 0
    except Exception as e:
        print(f"      ❌ 请求异常: {e}")
    return False, 0

async def process_video(bvid):
    print(f"🤖 [Worker] 处理任务: {bvid} ...")
    
    output_csv = f"danmaku_{bvid}.csv"
    raw_dir = f"raw_data_{bvid}"
    if not os.path.exists(raw_dir): os.makedirs(raw_dir)

    cred = Credential(sessdata=SESSDATA, bili_jct=BILI_JCT, buvid3=BUVID3)
    v = video.Video(bvid=bvid, credential=cred)

    # 初始化 CSV
    headers = ['dmid', 'video_time', 'text', 'send_date', 'uid', 'source']
    seen_dmids = set()
    
    # 断点续传：读取已有 ID
    if os.path.exists(output_csv):
        try:
            with open(output_csv, 'r', encoding='utf-8-sig') as f:
                for row in csv.DictReader(f): seen_dmids.add(row['dmid'])
        except: pass

    f = open(output_csv, 'a+', encoding='utf-8-sig', newline='')
    writer = csv.DictWriter(f, fieldnames=headers)
    if os.path.getsize(output_csv) == 0: writer.writeheader()

    try:
        info = await v.get_info()
        title = info['title']
        duration = info['duration']
        cid = info['cid']
        pub_date = datetime.date.fromtimestamp(info['ctime'])
        
        # 算法：参考源码 utils/yearDaysUtils.py 计算分包逻辑
        total_segments = math.ceil(duration / 360.0)
        print(f"   📺 视频: {title} (CID={cid}) | 分包数: {total_segments}")

        total_new = 0

        # === 阶段一：分包扫描 (API: web/seg.so) ===
        print(f"   🚀 [阶段一] 分包扫描...")
        for i in range(1, total_segments + 1):
            url = "https://api.bilibili.com/x/v2/dm/web/seg.so"
            params = {"type": 1, "oid": cid, "segment_index": i}
            
            # 下载并解析
            success, count = download_and_parse(url, params, writer, seen_dmids, 
                                              raw_save_path=os.path.join(raw_dir, f"seg_{i}.bin"))
            if success:
                print(f"      ✅ 分包 {i}: +{count} 条")
                total_new += count
            else:
                print(f"      ⚠️ 分包 {i} 失败或无数据")
            
            time.sleep(0.8)

        # === 阶段二：历史回溯 (API: web/history/seg.so) ===
        print(f"   🚀 [阶段二] 历史回溯...")
        # 1. 获取有弹幕的日期
        today = datetime.date.today()
        target_months = []
        for k in range(12): # 查最近1年
            d = today - datetime.timedelta(days=30*k)
            if d < pub_date and d.month != pub_date.month: break
            target_months.append(d.strftime("%Y-%m"))
        
        # 获取索引
        valid_dates = []
        for m in sorted(list(set(target_months))):
            try:
                # 使用 requests 直接查索引，避开库版本问题
                idx_url = "https://api.bilibili.com/x/v2/dm/history/index"
                idx_resp = requests.get(idx_url, params={"type":1, "oid":cid, "month":m}, 
                                      headers={"Cookie": f"SESSDATA={SESSDATA}"})
                if idx_resp.json()['code'] == 0 and idx_resp.json()['data']:
                    valid_dates.extend(idx_resp.json()['data'])
            except: pass
            time.sleep(0.5)

        print(f"      📅 发现 {len(valid_dates)} 个历史日期")
        
        # 2. 下载历史弹幕
        for date_str in valid_dates:
            url = "https://api.bilibili.com/x/v2/dm/web/history/seg.so"
            params = {"type": 1, "oid": cid, "date": date_str}
            
            success, count = download_and_parse(url, params, writer, seen_dmids,
                                              raw_save_path=os.path.join(raw_dir, f"hist_{date_str}.bin"))
            if count > 0:
                print(f"      ⬇️ {date_str}: +{count} 条")
                total_new += count
            time.sleep(1.2) # 历史接口限速

        print(f"✅ [Worker] {bvid} 结束。总入库: {total_new} 条")

    except Exception as e:
        print(f"❌ 任务出错: {e}")

    f.close()

async def main_loop():
    print("🚀 分布式爬虫 Worker Pro 启动...")
    while True:
        task = r.brpop(QUEUE_NAME, timeout=30)
        if task: await process_video(task[1])

if __name__ == '__main__':
    try: loop = asyncio.get_event_loop()
    except: loop = asyncio.new_event_loop(); asyncio.set_event_loop(loop)
    sync(main_loop())