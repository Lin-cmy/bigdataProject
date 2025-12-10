# -*- coding: utf-8 -*-
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, udf, count, desc
from pyspark.sql.types import ArrayType, StringType

# ================= 配置区域 =================
# 1. 输入数据路径 (HDFS路径)
# 请确保你已经把爬下来的csv上传到了这里，支持通配符 *.csv
INPUT_PATH = "/root/home/p1/family/*.csv" 

# 2. 输出路径 (HDFS路径)
# 结果将保存到这里
OUTPUT_PATH = "/root/home/p1/output/high_energy_words"
# ===========================================

def main():
    # 初始化 Spark
    # 初始化 Spark
    spark = SparkSession.builder \
        .appName("DanmakuPreprocessing") \
        .getOrCreate()
    
    # 打印日志
    print(f"🚀 [预处理] 开始读取数据: {INPUT_PATH}")

    try:
        # 读取 CSV 文件
        # header=True 表示第一行是表头
        # inferSchema=True 表示自动推断字段类型
        df = spark.read.csv(INPUT_PATH, header=True, inferSchema=True)
        
        # 打印一下数据结构，确认没读错
        df.printSchema()
    except Exception as e:
        print(f"❌ 读取失败，请检查HDFS路径。错误: {e}")
        return

    # 定义分词函数 (运行在每一个 Executor 上)
    def seg_text(text):
        import jieba
        if not text: 
            return []
        
        # 自定义停用词 (过滤掉没用的水词)
        stop_words = {
            '的', '了', '是', '在', '我', '有', '和', '就', '不', '人', '都', '一', '一个', '上', '也', '很', '到', '说', '要', '去', '你', '会', '着', '没有', '看', '好', '自己', '这',
            '啊', '吧', '呀', '嘛', '呢', '哈', '哦', '嗯'
        }
        
        # 分词
        words = jieba.cut(text)
        
        # 过滤逻辑：
        # 1. 不在停用词表中
        # 2. 长度大于1 (单个字通常很难代表高能情绪，除非是"草")
        # 3. 不是纯数字
        return [w for w in words if w not in stop_words and len(w) > 1 and not w.isnumeric()]

    # 注册 UDF (User Defined Function)
    seg_udf = udf(seg_text, ArrayType(StringType()))

    print("🔍 [预处理] 正在进行分词与清洗...")
    
    # 核心处理流程
    # 1. 过滤空弹幕
    # 2. 对 'text' 列进行分词，生成新列 'words'
    # 3. explode 将一行列表 [A, B] 炸开成两行 A, B (方便统计)
    words_df = df.filter(col("text").isNotNull()) \
                 .withColumn("words", seg_udf(col("text"))) \
                 .select(explode(col("words")).alias("word"))
    
    # 统计词频
    print("📊 [预处理] 正在统计词频...")
    word_counts = words_df.groupBy("word") \
                          .agg(count("word").alias("frequency")) \
                          .orderBy(desc("frequency")) \
                          .limit(500) # 只取前500个高频词，太多了没意义

    # 展示前 20 个结果到控制台
    print("🏆 Top 20 高能候选词:")
    word_counts.show(20, truncate=False)
    
    # 保存结果到 HDFS
    # 使用 coalesce(1) 将结果合并为一个文件，方便查看
    word_counts.coalesce(1).write.mode("overwrite").csv(OUTPUT_PATH, header=True)
    
    print(f"✅ 预处理完成！结果已保存至 HDFS: {OUTPUT_PATH}")
    spark.stop()

if __name__ == "__main__":
    main()