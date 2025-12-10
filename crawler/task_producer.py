import redis
import traceback
import time

# 连接 Redis (假设 Redis 在服务器本机，如果在其他机器请改 IP)
# 这里的 host='localhost' 表示连接本地 Redis
def push_tasks():
    try:
        r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        
        # 测试连接
        if not r.ping():
            print("❌ Redis连接失败")
            return
            
        print("✅ Redis连接成功")
        
        target_bvids = [
             "BV1ZY4y187fA",
            "BV1oL4y1c7MB",
            "BV1xa411J7vQ",
            "BV1US4y1B7oe",
            "BV1US4y1z7wA",
            "BV1a94y1m7zu",
            "BV16g411Q7Np",
            "BV1HW4y167YW",
            "BV1Nv4y137zp",
            "BV1Zt4y1875w",
            "BV1eB4y1H7Bw",
            "BV1af4y1o74V",
            "BV1o84y1q7Js",
            "BV1ue411G7gE",
            "BV1TD4y1t7fk",
            "BV1wK411U7jE",
            "BV1pg411q7FN",
            "BV14G4y157E5",
            "BV15G411T79k",
            "BV1fv4y1R7Rv",
            "BV1sV4y1A73M",
            "BV1f44y1X7ey",
            "BV1Dx4y137TZ",
            "BV1vd4y1j7rp",
            "BV19M411b74D",
            "BV1rc411d7zz",
            "BV1Nu4y1N7N2",
            "BV1Rj411j76h",
            "BV1du4y1F7zi",
            "BV1Fu4y1u7B8",
            "BV1p94y1N7rZ",
            "BV1pw411V7yK",
            "BV1xK411b7fS",
            "BV1Xe411m7jP",
            "BV1de41127hi",
            "BV1FN4y1W7Rb",
            "BV1h5411k7Wz"
        ]
        
        # 清空旧队列 (可选)
        r.delete('bilibili_tasks')
        
        print(f"正在将 {len(target_bvids)} 个任务推送到 Redis 队列...")
        
        total_pushed = 0
        for bvid in target_bvids:
            # 从左边推入队列
            result = r.lpush('bilibili_tasks', bvid)
            if result > 0:
                print(f"✅ 已入列: {bvid} (返回结果: {result})")
                total_pushed += 1
                
                # 立即验证推入结果
                immediate_length = r.llen('bilibili_tasks')
                immediate_content = r.lrange('bilibili_tasks', 0, -1)
                print(f"   立即验证 - 长度: {immediate_length}, 内容: {immediate_content}")
            else:
                print(f"❌ 入列失败: {bvid}")
        
        # 等待一小段时间后再次验证
        print("等待1秒后再次验证...")
        time.sleep(1)
        
        # 验证队列内容
        queue_length = r.llen('bilibili_tasks')
        queue_content = r.lrange('bilibili_tasks', 0, -1)
        
        print(f"\n📊 任务发布完成！")
        print(f"📤 成功推送任务数: {total_pushed}")
        print(f"📊 队列实际长度: {queue_length}")
        print(f"📋 队列内容: {queue_content}")
        
        # 检查所有键
        all_keys = r.keys('*')
        print(f"🔑 所有Redis键: {all_keys}")
        
    except Exception as e:
        print(f"❌ 发生错误: {e}")
        traceback.print_exc()

if __name__ == '__main__':
    push_tasks()