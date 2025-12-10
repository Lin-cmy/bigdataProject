import os
import platform
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker  # 添加导入
import seaborn as sns  # 添加导入
from wordcloud import WordCloud  # 添加导入
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, regexp_replace, explode, desc, avg, when, regexp_extract, size

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
    .appName("BilibiliAnimeDataVisualization") \
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
# 新增：初始数据可视化 (使用Matplotlib)
# ==========================================
print(">>> 生成初始数据可视化...")

# 将预处理数据转为Pandas（仅用于可视化，小数据集适用）
pdf = df_clean.select("follows", "rating", "tags_arr").toPandas()

# 过滤评分缺失值
pdf = pdf.dropna(subset=['rating'])

# 1. 追番人数分布 (直方图)
plt.figure(figsize=(10, 6))
plt.hist(pdf['follows'], bins=10, color='skyblue', edgecolor='black')
plt.xlabel('追番人数')
plt.ylabel('频次')
plt.title('追番人数分布')
plt.tight_layout()
plt.savefig('follows_distribution.png')
print("   已保存: follows_distribution.png")
plt.show()  # 弹出窗口显示图表

# 2. 评分分布 (直方图)
plt.figure(figsize=(10, 6))
plt.hist(pdf['rating'], bins=10, color='lightgreen', edgecolor='black')
plt.xlabel('评分')
plt.ylabel('频次')
plt.title('评分分布')
plt.tight_layout()
plt.savefig('rating_distribution.png')
print("   已保存: rating_distribution.png")
plt.show()  # 弹出窗口显示图表

# 3. 标签频率 (饼图)
tags_flat = pdf['tags_arr'].explode().value_counts().head(10)
plt.figure(figsize=(8, 8))
plt.pie(tags_flat.values, labels=tags_flat.index, autopct='%1.1f%%', startangle=140, textprops={'fontsize': 24})
plt.title('热门标签Top10')
plt.tight_layout()
plt.savefig('top_tags_pie.png')
print("   已保存: top_tags_pie.png")
plt.show()  # 弹出窗口显示图表

# 4. 追番人数 vs. 评分散点图 (采样以提高性能)
pdf_sample = pdf.sample(n=min(1000, len(pdf)), random_state=42)
plt.figure(figsize=(10, 6))
plt.scatter(pdf_sample['follows'], pdf_sample['rating'], alpha=0.6, color='orange')
plt.xscale('log')  # 设置对数尺度
plt.xlabel('追番人数')
plt.ylabel('评分')
plt.title('追番人数 vs. 评分')
plt.grid(True, linestyle='--', alpha=0.5)
# 自定义横轴ticks和标签
plt.xticks([1000, 10000, 100000,1000000], ['1千', '1万', '10万','100万'])
plt.tight_layout()
plt.savefig('follows_vs_rating.png')
print("   已保存: follows_vs_rating.png")
plt.show()  # 弹出窗口显示图表

# 5. 标签与评分的箱线图
plt.figure(figsize=(12, 8))
# 展开标签，合并到DataFrame
pdf_exploded = pdf.explode('tags_arr')
top_tags = pdf_exploded['tags_arr'].value_counts().head(10).index
pdf_top_tags = pdf_exploded[pdf_exploded['tags_arr'].isin(top_tags)]
sns.boxplot(x='tags_arr', y='rating', data=pdf_top_tags)
plt.xlabel('标签')
plt.ylabel('评分')
plt.title('热门标签与评分的箱线图')
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig('tags_rating_boxplot.png')
print("   已保存: tags_rating_boxplot.png")
plt.show()

# 6. 追番人数与标签的热力图
plt.figure(figsize=(12, 8))
# 计算每个标签的平均追番人数
avg_follows = pdf_exploded.groupby('tags_arr')['follows'].mean().sort_values(ascending=False).head(10)
sns.heatmap(avg_follows.to_frame().T, annot=True, cmap='Blues', cbar=True)
plt.xlabel('标签')
plt.ylabel('平均追番人数')
plt.title('追番人数与标签的热力图')
plt.tight_layout()
plt.savefig('follows_tags_heatmap.png')
print("   已保存: follows_tags_heatmap.png")
plt.show()

# 7. 标签词云
plt.figure(figsize=(10, 6))
wordcloud = WordCloud(width=800, height=400, background_color='white', font_path='simhei.ttf' if system_name == "Windows" else None).generate_from_frequencies(tags_flat)
plt.imshow(wordcloud, interpolation='bilinear')
plt.axis('off')
plt.title('标签词云')
plt.tight_layout()
plt.savefig('tags_wordcloud.png')
print("   已保存: tags_wordcloud.png")
plt.show()

# 8. 相关性矩阵
plt.figure(figsize=(8, 6))
corr_matrix = pdf[['follows', 'rating']].corr()
sns.heatmap(corr_matrix, annot=True, cmap='coolwarm', cbar=True)
plt.title('相关性矩阵')
plt.tight_layout()
plt.savefig('correlation_matrix.png')
print("   已保存: correlation_matrix.png")
plt.show()

# 9. 散点图矩阵
plt.figure(figsize=(10, 10))
pd.plotting.scatter_matrix(pdf[['follows', 'rating']], alpha=0.6, diagonal='hist')
plt.suptitle('散点图矩阵')
plt.tight_layout()
plt.savefig('scatter_matrix.png')
print("   已保存: scatter_matrix.png")
plt.show()

print(">>> 原始数据可视化完成！")
spark.stop()