import sys
import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, floor, count, sum as spark_sum, udf, percent_rank, input_file_name, regexp_extract, collect_list, struct
from pyspark.sql.types import DoubleType, StringType
from pyspark.sql.window import Window

# ================= 配置区域 =================
# 输入路径：匹配所有分集的 CSV 文件
INPUT_PATH = "danmaku_*.csv"  # 确保这里能匹配到所有37集的文件
# 输出文件
OUTPUT_JSON = "all_episodes_energy.json"
# 时间窗口 (10秒)
WINDOW_SIZE = 10 
# ===========================================

def main():
    spark = SparkSession.builder \
        .appName("SpyFamilyBatchEnergy") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()
    
    # 1. 读取所有文件，并保留文件名作为"集数ID"
    # input_file_name() 会获取完整的 hdfs://.../danmaku_BVxxx.csv 路径
    raw_df = spark.read.csv(INPUT_PATH, header=True, inferSchema=True) \
        .withColumn("filepath", input_file_name())
    
    # 从文件名中提取 BVID (假设文件名格式为 danmaku_BVxxxx.csv)
    # 正则提取 BV 号
    df = raw_df.withColumn("bvid", regexp_extract(col("filepath"), r"(BV[a-zA-Z0-9]+)", 1)) \
               .filter(col("video_time").cast("double").isNotNull())

    # 2. 专属高能情感词典 (基于你的 CSV 统计数据定制)
    def get_weighted_score(text):
        if not text: return 1.0
        t = text.lower()
        
        # [Tier 1] 核心梗 (权重 x5.0) - 基于 CSV 频率Top词
        god_tier = ['优雅', 'elegance', '哇库', '哇酷', 'waku', '瓜神', '世界名画', '名场面']
        # [Tier 2] 角色与剧情 (权重 x3.0)
        high_tier = ['次子', '昏爹', '父亲', '约尔', '太太', '荆棘公主', '邦德', '上岸', '读心', '花生', '吃花生', '手雷', '母亲', '妈妈', '阿尼亚']
        # [Tier 3] 通用情绪 (权重 x2.0)
        mid_tier = ['高能', '泪目', '起立', '卧槽', '牛逼', 'awsl', '封神', '致敬', '完结', '撒花', '好帅', '可爱']
        # [Tier 4] 噪音降权 (权重 x0.5)
        noise_tier = ['哈哈', 'hhh', 'www', '233', '打卡', '第一', '热乎']

        for w in god_tier:
            if w in t: return 5.0
        for w in high_tier:
            if w in t: return 3.0
        for w in mid_tier:
            if w in t: return 2.0
        for w in noise_tier:
            if w in t: return 0.5
            
        return 1.0

    score_udf = udf(get_weighted_score, DoubleType())

    # 3. 计算基础得分
    # 增加 "bvid" 分组维度
    window_df = df.withColumn("raw_score", score_udf(col("text"))) \
                  .withColumn("time_bucket", (floor(col("video_time") / WINDOW_SIZE) * WINDOW_SIZE).cast("int")) \
                  .groupBy("bvid", "time_bucket") \
                  .agg(
                      count("dmid").alias("density"),
                      spark_sum("raw_score").alias("sentiment_score")
                  )

    # 4. 综合评分
    final_df = window_df.withColumn("energy", col("sentiment_score") * 0.37 + col("density") * 0.63)

    # 5. 【关键】按集数分组计算排名
    # 我们需要在"每一集内部"找出 Top 10% 的时刻，而不是跟别的集比
    w = Window.partitionBy("bvid").orderBy("energy")
    
    ranked_df = final_df.withColumn("rank_pct", percent_rank().over(w)) \
                        .withColumn("is_highlight", col("rank_pct") >= 0.9) # Top 10%

    # 6. 结构化输出
    # 我们需要把数据转换成前端好用的格式：
    # [ { "bvid": "BV1xx...", "timeline": [ { "time": 0, "value": 10 }, ... ] }, ... ]
    
    # 先按时间排序
    sorted_df = ranked_df.orderBy("bvid", "time_bucket")
    
    # 聚合每一集的数据
    output_structure = sorted_df.groupBy("bvid") \
        .agg(collect_list(struct(
            col("time_bucket").alias("time"),
            col("energy").alias("value"),
            col("is_highlight").alias("high")
        )).alias("timeline"))

    # 7. 导出 JSON
    print("🚀 正在聚合 37 集数据并导出...")
    results = output_structure.collect()
    
    final_json = []
    for row in results:
        final_json.append({
            "bvid": row["bvid"],
            # 简单的集数映射逻辑，实际可以用字典映射 BVID -> 第几集
            "title": f"视频 {row['bvid']}", 
            "timeline": [
                {
                    "time": item.time,
                    "value": round(item.value, 2),
                    "is_high": item.high
                } for item in row.timeline
            ]
        })

    with open(OUTPUT_JSON, "w", encoding='utf-8') as f:
        json.dump(final_json, f, ensure_ascii=False)

    print(f"✅ 批处理完成！已生成 {len(final_json)} 集的高能数据。")
    spark.stop()

if __name__ == "__main__":
    main()