import os
import sys

# 删除指向旧版 Spark 的环境变量
if 'SPARK_HOME' in os.environ:
    del os.environ['SPARK_HOME']
if 'PYTHONPATH' in os.environ:
    del os.environ['PYTHONPATH']

sys.path = [p for p in sys.path if '/usr/local/spark' not in p]

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, explode, col, desc
from pyspark.sql.types import ArrayType, StringType
import itertools

spark = SparkSession.builder \
    .appName("SpyFamilyGraphAnalysisCluster") \
    .master("local[*]") \
    .config("spark.executor.memory", "1g") \
    .config("spark.executor.cores", "1") \
    .config("spark.driver.memory", "1g") \
    .getOrCreate()

def extract_entities_safe(content):
    if not content: return []
    content_str = str(content)
    found_roles = set()
    
    # mapping = {
    #     "瓜神": "阿尼亚", "阿尼亚": "阿尼亚", "安妮亚": "阿尼亚", "花生": "阿尼亚",
    #     "黄昏": "黄昏", "劳埃德": "黄昏", "父亲": "黄昏", "罗伊德": "黄昏",
    #     "约尔": "约尔", "荆棘公主": "约尔", "约儿": "约尔", "太太": "约尔", "睡美人": "约尔", "妈妈": "约尔",
    #     "次子": "次子", "达米安": "次子", 
    #     "尤里": "尤里", "弟弟": "尤里",
    #     "邦德": "邦德", "狗": "邦德", "狗狗": "邦德",
    #     "贝威": "贝威", "蓬蓬头": "贝威"
    # }

    mapping = {
        "韩立": "韩立", 
        "二愣子": "韩立",    
        "韩跑跑": "韩立",    
        "跑跑": "韩立",      
        "韩老魔": "韩立",    
        "韩天尊": "韩立",    
        "韩师弟": "韩立",    
        "小韩": "韩立",
        
        "厉飞雨": "厉飞雨",
        "厉师兄": "厉飞雨",

        "南宫婉": "南宫婉",
        "婉儿": "南宫婉",
        "南宫": "南宫婉",
        "师娘": "南宫婉", 

        "陈巧倩": "陈巧倩",
        "陈师姐": "陈巧倩",
        "巧倩": "陈巧倩",
        
        "董萱儿": "董萱儿",
        "红拂弟子": "董萱儿",
        
        "墨彩环": "墨彩环",
        "彩环": "墨彩环",
        
        "紫灵": "紫灵仙子",
        "紫灵仙子": "紫灵仙子",
        "汪凝": "紫灵仙子",
        
        "元瑶": "元瑶",
        
        "银月": "银月",

        "墨居仁": "墨大夫",
        "墨大夫": "墨大夫",
        "墨老": "墨大夫",
        
        "王蝉": "王蝉",     
        "少门主": "王蝉",     
        
        "大衍神君": "大衍神君",
        "大衍": "大衍神君",
        "老鬼": "大衍神君", 
        
        "曲魂": "曲魂", 
        "张铁": "曲魂",
        
        "雷万鹤": "雷万鹤",
        "雷师伯": "雷万鹤",
        
        "令狐老祖": "令狐老祖",
        
        "文思月": "文思月",
        
        "掌天瓶": "小绿瓶",
        "小绿瓶": "小绿瓶",
        "瓶子": "小绿瓶"
    }

    for alias, standard_name in mapping.items():
        if alias in content_str:
            found_roles.add(standard_name)
    return list(found_roles)

extract_udf = udf(extract_entities_safe, ArrayType(StringType()))

# 格式通常是：hdfs://namenode_host:port/path
# 假设 NameNode 是 master，默认端口是 9000
hdfs_root = "hdfs://master:9000"
input_path = hdfs_root + "/fanRen/input/danmaku_*.csv"

output_path_edges = hdfs_root + "/fanRen/output_edges"
output_path_nodes = hdfs_root + "/fanRen/output_nodes"

print(f"正在读取 HDFS 数据: {input_path}")

try:
    df = spark.read.csv(input_path, header=True, multiLine=True, escape='"', quote='"')
    
    if "text" not in df.columns:
        print(f"警告：列名未找到 text，现有列: {df.columns}")

    df_processed = df.withColumn("entities", extract_udf(col("text")))
    df_filtered = df_processed.filter(col("entities").isNotNull())
    
    df_filtered.cache()

    print("正在统计节点热度...")
    nodes_df = df_filtered.select(explode(col("entities")).alias("Id")) \
                          .groupBy("Id").count() \
                          .withColumnRenamed("count", "Size") \
                          .orderBy(desc("Size"))
    
    nodes_df.show(20)

    print("正在构建人物关系网...")
    rdd_entities = df_filtered.select("entities").rdd.map(lambda x: x[0])

    def map_to_pairs(roles):
        if not roles or len(roles) < 2: return []
        sorted_roles = sorted(roles)
        pairs = itertools.combinations(sorted_roles, 2)
        return [(p, 1) for p in pairs]

    edges_rdd = rdd_entities.flatMap(map_to_pairs).reduceByKey(lambda a, b: a + b)

    if not edges_rdd.isEmpty():
        edges_df = edges_rdd.map(lambda x: (x[0][0], x[0][1], x[1])) \
                            .toDF(["Source", "Target", "Weight"]) \
                            .orderBy(desc("Weight"))

        edges_df.show(10)

        print("正在保存结果到 HDFS...")
        edges_df.coalesce(1).write.csv(output_path_edges, header=True, mode="overwrite")
        nodes_df.coalesce(1).write.csv(output_path_nodes, header=True, mode="overwrite")
        print(f"任务完成！结果保存在 HDFS: {output_path_edges}")
    else:
        print("没有发现关系。")

except Exception as e:
    print(f"发生错误: {e}")

finally:
    spark.stop()