import os
import platform
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker  # 添加导入
import seaborn as sns  # 添加导入
from wordcloud import WordCloud  # 添加导入
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, regexp_replace, explode, desc, avg, when, regexp_extract, size
from pyspark.ml.fpm import FPGrowth

# ==========================================
# 0. 环境与字体设置 (解决Matplotlib中文乱码)
# ==========================================
system_name = platform.system()
if system_name == "Windows":
    plt.rcParams['font.sans-serif'] = ['SimHei']
elif system_name == "Darwin":  # Mac OS
    plt.rcParams['font.sans-serif'] = ['Arial Unicode MS'] 
else:
    plt.rcParams['font.sans-serif'] = ['WenQuanYi Micro Hei']
plt.rcParams['axes.unicode_minus'] = False

# ==========================================
# 1. 初始化 Spark (单机模式)
# ==========================================
# local[*] 表示使用本地所有CPU核心
spark = SparkSession.builder \
    .appName("BilibiliAnimeFPGrowthAnalysis") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN") # 减少控制台干扰信息

print(">>> Spark 环境初始化完成")

# ==========================================
# 2. 数据加载与预处理 (ETL)
# ==========================================
# 假设文件名为 data.csv
file_path = "data.csv" 

# 读取CSV
df = spark.read.csv(file_path, header=True, inferSchema=True)

print(">>> 原始数据预览:")
df.show(3)

# 数据清洗
# 1. 提取数字并转换为 float（处理“万”单位）
df_clean = df.withColumn("follows", 
    regexp_extract(col("追番人数"), r"(\d+\.?\d*)", 1).cast("float") * 
    when(col("追番人数").contains("万"), 10000).otherwise(1)) \
    .withColumn("tags_arr", split(col("标签"), ",")) \
    .withColumn("rating", col("评分").cast("float")) \
    .filter(col("tags_arr").isNotNull() & (col("标签") != "无") & (~col("追番人数").contains("想看")) & (~col("追番人数").contains("追剧")) & (size(col("tags_arr")) > 0))  # 排除空标签数组

print(">>> 清洗后数据预览:")
df_clean.select("标题", "tags_arr", "follows", "rating").show(3, truncate=False)

# ==========================================
# 3. 核心算法：FP-Growth 关联规则挖掘
# ==========================================
print(">>> 开始运行 FP-Growth 算法...")

# minSupport: 最小支持度 (例如 0.05 表示标签组合至少要在 5% 的番剧中出现)
# minConfidence: 最小置信度 (例如 0.3 表示规则的可信度至少 30%)
fp_growth = FPGrowth(itemsCol="tags_arr", minSupport=0.05, minConfidence=0.3)
model = fp_growth.fit(df_clean)

# 获取频繁项集 (Frequent Itemsets)
freq_itemsets = model.freqItemsets.sort(desc("freq"))

# 获取关联规则 (Association Rules)
assoc_rules = model.associationRules.sort(desc("lift"))

print(">>> 频繁项集 Top 5:")
freq_itemsets.show(5)

print(">>> 强关联规则 Top 10 (按提升度 Lift 排序):")
assoc_rules.show(10)

# ==========================================
# 4. 结果可视化
# ==========================================
print(">>> 开始生成可视化图表...")

# 将 Spark DataFrame 转换为 Pandas DataFrame 以便绘图
# 注意：在大数据场景下，通常只把聚合后的结果转为 Pandas，不要转整个原始表
pdf_rules = assoc_rules.limit(20).toPandas()
pdf_freq = freq_itemsets.limit(15).toPandas()

# 图表 1: 热门标签词频 (Bar Chart)
plt.figure(figsize=(12, 6))
# 处理 items 列，它是一个列表，转为字符串方便显示
pdf_freq['items_str'] = pdf_freq['items'].apply(lambda x: ','.join(x))
plt.bar(pdf_freq['items_str'], pdf_freq['freq'], color='skyblue', edgecolor='black')
plt.xlabel('标签组合', fontsize=14)
plt.ylabel('出现频次', fontsize=14)
plt.title('B站番剧热门标签组合 Top 15', fontsize=16, fontweight='bold')
plt.xticks(rotation=45, fontsize=12)
plt.yticks(fontsize=12)
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.tight_layout()
plt.savefig('top_tags.png', dpi=300, bbox_inches='tight')
print("   已保存: top_tags.png")
plt.show()  # 弹出窗口显示图表

# 图表 2: 关联规则散点图 (Scatter Plot)
# X轴: 支持度, Y轴: 置信度, 点大小: 提升度
plt.figure(figsize=(12, 8))  # 增大图表尺寸
# 处理 antecedent (前项) 和 consequent (后项) 用于标签显示
pdf_rules['rule_name'] = pdf_rules['antecedent'].apply(lambda x: list(x)[0] if len(x)>0 else '') + " -> " + \
                         pdf_rules['consequent'].apply(lambda x: list(x)[0] if len(x)>0 else '')

scatter = plt.scatter(pdf_rules['support'], 
                      pdf_rules['confidence'], 
                      s=pdf_rules['lift']*100, # 放大提升度以便观察
                      c=pdf_rules['lift'], 
                      cmap='viridis', 
                      alpha=0.6, edgecolors='black')

plt.colorbar(scatter, label='提升度 (Lift)')
plt.xticks(fontsize=12)
plt.yticks(fontsize=12)
plt.grid(True, linestyle='--', alpha=0.5)

# 静态标注规则名称（仅Top 5，偏移避免重叠）
for i in range(min(5, len(pdf_rules))):
    x, y = pdf_rules['support'][i], pdf_rules['confidence'][i]
    plt.annotate(pdf_rules['rule_name'][i], (x, y), fontsize=10, ha='left', va='bottom', 
                 xytext=(5, 5), textcoords='offset points', 
                 bbox=dict(boxstyle='round,pad=0.3', facecolor='white', alpha=0.8))

plt.tight_layout()
plt.subplots_adjust(bottom=0.15, left=0.15)  # 增加边距避免重叠
plt.savefig('association_rules.png', dpi=300, bbox_inches='tight')
print("   已保存: association_rules.png")
plt.show()  # 弹出窗口显示图表

# 新增图表 3: 提升度分布 (直方图)
plt.figure(figsize=(10, 6))
plt.hist(pdf_rules['lift'], bins=10, color='lightcoral', edgecolor='black', alpha=0.7)
plt.xlabel('提升度 (Lift)', fontsize=14)
plt.ylabel('频次', fontsize=14)
plt.title('关联规则提升度分布', fontsize=16, fontweight='bold')
plt.xticks(fontsize=12)
plt.yticks(fontsize=12)
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.tight_layout()
plt.savefig('lift_distribution.png', dpi=300, bbox_inches='tight')
print("   已保存: lift_distribution.png")
plt.show()

# 新增图表 4: 标签关联热力图 (基于频繁项集Top 10)
# 简化：计算Top 10标签的共现矩阵（支持度）
top_tags = pdf_freq['items'].explode().value_counts().head(10).index.tolist()
co_occurrence = pd.DataFrame(0, index=top_tags, columns=top_tags)
for items in pdf_freq['items']:
    for i in range(len(items)):
        for j in range(i+1, len(items)):
            if items[i] in top_tags and items[j] in top_tags:
                co_occurrence.loc[items[i], items[j]] += 1
                co_occurrence.loc[items[j], items[i]] += 1

plt.figure(figsize=(10, 8))
sns.heatmap(co_occurrence, annot=True, cmap='Blues', cbar=True, square=True, linewidths=0.5)
plt.xlabel('标签', fontsize=14)
plt.ylabel('标签', fontsize=14)
plt.title('Top 10标签共现热力图 (共现频次)', fontsize=16, fontweight='bold')
plt.xticks(rotation=45, fontsize=12)
plt.yticks(rotation=0, fontsize=12)
plt.tight_layout()
plt.savefig('tag_cooccurrence_heatmap.png', dpi=300, bbox_inches='tight')
print("   已保存: tag_cooccurrence_heatmap.png")
plt.show()

# 新增图表 5: 规则网络图 (使用NetworkX)
try:
    import networkx as nx
    plt.figure(figsize=(10, 8))  # 调整尺寸
    G = nx.DiGraph()
    for _, row in pdf_rules.iterrows():
        ant = list(row['antecedent'])[0] if len(row['antecedent'])>0 else ''
        cons = list(row['consequent'])[0] if len(row['consequent'])>0 else ''
        if ant and cons:
            G.add_edge(ant, cons, weight=row['lift'])
    
    pos = nx.circular_layout(G)  # 用circular_layout让节点更紧凑
    edges = G.edges()
    weights = [G[u][v]['weight'] for u, v in edges]
    
    nx.draw(G, pos, with_labels=True, node_color='lightblue', node_size=2500, font_size=18, font_weight='bold', 
            edge_color='gray', width=[w/2 for w in weights], arrows=True, arrowsize=20)
    plt.title('关联规则网络图 (边粗细表示提升度)', fontsize=16, fontweight='bold')
    plt.tight_layout()
    plt.savefig('rule_network.png', dpi=300, bbox_inches='tight')
    print("   已保存: rule_network.png")
    plt.show()
except ImportError:
    print("   网络图需安装networkx: pip install networkx")

# ==========================================
# 5. 额外分析：高分番剧的标签偏好
# ==========================================
print(">>> 进行高分番剧特征分析...")

# 筛选 9.5 分以上的番剧
high_score_df = df_clean.filter(col("rating") >= 9.5)
# 炸开标签数组，统计单个标签出现次数
top_high_score_tags = high_score_df.select(explode("tags_arr").alias("tag")) \
    .groupBy("tag").count().orderBy(desc("count")).limit(10).toPandas()

plt.figure(figsize=(8, 8))
plt.pie(top_high_score_tags['count'], labels=top_high_score_tags['tag'], autopct='%1.1f%%', startangle=140, textprops={'fontsize': 24}, colors=plt.cm.Paired.colors)
plt.title('9.5分以上高分番剧的标签分布', fontsize=16, fontweight='bold')
plt.tight_layout()
plt.savefig('high_score_tags_pie.png', dpi=300, bbox_inches='tight')
print("   已保存: high_score_tags_pie.png")
plt.show()  # 弹出窗口显示图表

print(">>> FP-Growth 分析完成！")
spark.stop()