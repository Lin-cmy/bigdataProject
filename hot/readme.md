# 🔥 B站视频高能时刻检测
[![Spark](https://img.shields.io/badge/Spark-Streaming-orange)](https://spark.apache.org/)
[![Algorithm](https://img.shields.io/badge/Algorithm-Multi--Modal%20Fusion-blue)]()
[![Visualization](https://img.shields.io/badge/Visualization-ECharts-green)](https://echarts.apache.org/)

  本模块旨在解决“在缺乏用户拖拽后台日志的情况下，如何精准定位视频精彩片段”这一核心难题。我们提出了一种结合弹幕语义与时序密度的融合算法，实现了对官方“高能进度条”的高精度拟合。

---

## 🧐 调研与背景 (Research & Motivation)

### 1. 业界主流方案
B站官方及 YouTube 的“高能进度条”主要依赖用户行为日志：
* 核心特征：用户拖拽、回看、留存率。
* 优势：最直接反映用户兴趣，准确率极高。
* 局限：作为第三方开发者，数据黑盒是我们无法逾越的壁垒。

### 2. 传统基线方案
大多数开源爬虫分析项目仅采用“弹幕密度统计法”：
* **方法**：统计单位时间内的弹幕数量。
* **缺陷**：
    * 信噪比低：容易被片头/片尾的“打卡”、“第一”等刷屏水词误导，产生伪高能。
    * 长尾失效：在冷门番剧或日常剧情中，由于弹幕基数少，导致整集无波峰，出现大量漏检。

### 3. 我们的思路 
为了在数据受限条件下实现同等效果，我们采用“内容理解替代行为统计”的策略：
> 虽然不知道用户在哪儿回看，但我们知道用户在哪儿喊“卧槽”和“泪目”。

---

## 💡 算法设计与实现 (Methodology)

我们提出了一种算法，将高能检测转化为一个时间序列评分问题。

### 1. 核心数学模型 
![image-20251210220017288](C:\Users\shijun\AppData\Roaming\Typora\typora-user-images\image-20251210220017288.png)

| 符号      | 特征维度     | 实现逻辑                                                     |
| :-------- | :----------- | :----------------------------------------------------------- |
| Density   | **相对热度** | 消除不同集数的基础流量差异，使用 `当前密度 / 全集均值` 计算。 |
| Sentiment | **内容情感** | 基于 Spark 词频统计辅助人工不成构建领域专属词典。            |

### 2. 设计合理性分析
  引入领域词典权重，有效压制了高频低义的水军弹幕，解决了Baseline的信噪比问题。

---

## 📂 文件结构 (File Structure)

```text
hot/
├── preprocess_high_energy.py   # [预处理] 数据清洗与时间窗口切分
├── spark_energy.py             # [核心算法] 实现多维融合评分与自适应阈值计算
├── visualize.html              # [可视化] 复刻 B 站高能进度条的大屏页面
├── all_episodes_energy.json    # [结果] 计算完成的高能时刻数据
└── README.md                   # 本文档
```

------

## 🛠️ 快速开始 (Usage)

### 1. 提交 Spark 计算任务

使用 Spark 分布式引擎进行全量数据计算：

```
# 提交任务到 Yarn 集群
spark-submit \
  --master yarn \
  --deploy-mode client \
  hot/spark_energy.py
```

*输出：生成 `all_episodes_energy.json` 结果文件。*

### 2. 启动可视化大屏

```
cd hot
python -m http.server 8000
```

访问浏览器 `http://localhost:8000/visualize.html` 即可查看沉浸式的高能波形图。