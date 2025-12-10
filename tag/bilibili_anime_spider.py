import requests
import pandas as pd
import time
import random

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

def get_anime_tags_by_season_id(season_id):
    """
    【第二级】根据 season_id 查询番剧详情，提取 Tags
    """
    url = "https://api.bilibili.com/pgc/view/web/season"
    params = {"season_id": season_id}
    
    try:
        # 这里不需要 Cookie 也能查到公开的 Tag
        resp = requests.get(url, params=params, headers=headers)
        if resp.status_code == 200:
            data = resp.json()
            # 提取 styles 字段
            if data['code'] == 0 and 'result' in data:
                styles = data['result'].get('styles', [])
                if styles:
                    # styles 是一个列表，如 ["热血", "奇幻"]
                    # 我们把它拼接成字符串 "热血,奇幻"
                    return ",".join(styles)
    except Exception as e:
        print(f"  ! 获取 Tag 失败 (season_id={season_id}): {e}")
    
    return "无"

def get_bilibili_anime(page=1):
    # 1. 目标 API URL
    # 这里使用了我们刚刚在 F12 里分析到的接口地址
    url = "https://api.bilibili.com/pgc/season/index/result"

    # 2. 参数 (Parameters)
    # 这些参数对应 URL 问号后面的内容
    # https://api.bilibili.com/pgc/season/index/result?season_version=-1&
    # spoken_language_type=-1&area=-1&is_finish=-1&copyright=-1&
    # season_status=-1&season_month=-1&year=-1&style_id=-1&order=3&
    # st=1&sort=0&page=2&season_type=1&pagesize=20&type=1
    params = {
        "season_version": -1,
        "spoken_language_type": -1,
        "area": -1,
        "is_finish": -1,
        "copyright": -1,
        "season_status": -1,
        "season_month": -1,
        "year": -1,
        "style_id": -1,
        "order": 3,       # 3 代表按追番人数排序
        "st": 1,
        "sort": 0,
        "page": page,     # 当前页码
        "season_type": 1,
        "pagesize": 20,   # 每页数量
        "type": 1
    }

    # 3. 请求头 (Headers) - 伪装成浏览器
    # 这是最基本的反爬虫措施，必须加上
    # headers = {
    #     "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    # }

    try:
        # 发送 GET 请求
        response = requests.get(url, params=params, headers=headers)
        
        # 检查请求是否成功 (状态码 200)
        if response.status_code == 200:
            data_json = response.json() # B 站返回的是 JSON 格式
            
            # 4. 提取数据
            # 根据 JSON 结构找到 list 列表
            if 'data' in data_json and 'list' in data_json['data']:
                anime_list = data_json['data']['list']
                
                results = []
                for anime in anime_list:
                    s_id = anime.get("season_id")
                    if s_id:
                        time.sleep(0.5) 
                        tags = get_anime_tags_by_season_id(s_id)

                    # 提取我们需要的信息
                    info = {
                        "标题": anime.get("title"),
                        "标签": tags,
                        "追番人数": anime.get("order"),
                        "评分": anime.get("score"),
                        "剧集状态": anime.get("is_finish") # 1是完结，0是连载
                    }
                    results.append(info)
                    print(f"抓取成功: {info['标题']}")
                
                return results
            else:
                print("未找到数据列表")
                return []
        else:
            print(f"请求失败，状态码: {response.status_code}")
            return []
            
    except Exception as e:
        print(f"发生错误: {e}")
        return []

# 主程序
if __name__ == "__main__":
    all_anime_data = []
    
    for p in range(1, 3):
        print(f"--- 正在爬取第 {p} 页 ---")
        page_data = get_bilibili_anime(page=p)
        all_anime_data.extend(page_data)
        
        # 礼貌爬取：每爬一页暂停 1-3 秒，防止被封 IP
        time.sleep(random.uniform(1, 3))

    # 5. 保存数据
    if all_anime_data:
        df = pd.DataFrame(all_anime_data)
        # 保存为 CSV 文件
        df.to_csv("bilibili_anime_data.csv", index=False, encoding="utf-8-sig")
        print("\n爬取完成！数据已保存为 bilibili_anime_data.csv")