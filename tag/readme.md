# 🏷️ B站番剧标签关联挖掘
[![Python](https://img.shields.io/badge/Python-3.8%2B-blue)](https://www.python.org/)
[![Algorithm](https://img.shields.io/badge/Algorithm-FP--Growth-green)](https://en.wikipedia.org/wiki/Association_rule_learning#FP-growth_algorithm)
[![Visualization](https://img.shields.io/badge/Visualization-Pyecharts-red)](https://pyecharts.org/)

  本模块旨在挖掘 Bilibili 番剧标签背后的深层联系。通过爬取番剧索引页的标签数据，利用 FP-Growth 关联规则算法，发现不同题材标签（如“热血”、“恋爱”、“校园”）之间的共现规律，并生成交互式的桑基图 (Sankey Diagram) 和 热力图，为内容推荐与题材分析提供数据支持。

---

## ⚡ 核心功能 (Key Features)

1.  **标签数据采集 (Tag Crawler)**
    * `bilibili_anime_spider.py`: 针对 B 站番剧索引接口的轻量级爬虫。
    * 自动遍历多页索引，提取每部番剧的 Tag 列表、播放量、追番数等元数据。
    * 数据持久化保存为 `bilibili_anime_tags.csv`。

2.  **关联规则挖掘 (Association Mining)**
    * `fp_growth_analysis.py`: 实现 **FP-Growth** 算法。
    * **频繁项集挖掘**：找出同时出现频率较高的标签组合（如 `{'战斗', '奇幻'}`）。
    * **关联规则生成**：计算置信度（Confidence）和提升度（Lift），导出强关联规则（如 `日常 -> 搞笑`）。

3.  **数据可视化 (Visualization)**
    * `data_visualization.py`: 基于 Pyecharts 生成动态图表。
    * **标签词云**：直观展示热门题材。
    * **关联桑基图**：清晰呈现标签之间的流动与强关联关系。

---

## 📂 文件结构 (File Structure)

```text
tag/
├── bilibili_anime_spider.py  # [爬虫] 抓取番剧标签数据
├── fp_growth_analysis.py     # [算法] FP-Growth 关联分析核心逻辑
├── data_visualization.py     # [展示] 生成 HTML 可视化图表
├── main.py                   # [入口] 一键启动脚本
└── README.md                 # 本文档
```

------

## 🛠️ 快速开始 (Quick Start)

### 1. 安装依赖

确保已安装所需的 Python 库：

```
pip install pandas requests pyecharts mlxtend
```

### 2. 运行系统

直接运行 `main.py` 即可按顺序执行“爬取 -> 分析 -> 可视化”全流程：

```
python main.py
```

### 3. 查看结果

运行结束后，目录下会生成以下文件：

- `bilibili_anime_tags.csv`: 原始标签数据
- `frequent_itemsets.csv`: 挖掘出的频繁项集
- `association_rules.csv`: 挖掘出的关联规则
- `tag_cloud.html`: 标签词云图
- `tag_sankey.html`: 标签关联桑基图

## 