# 🕷️ 分布式弹幕采集系统

[![Python](https://img.shields.io/badge/Python-3.8%2B-blue)](https://www.python.org/)
[![Redis](https://img.shields.io/badge/Redis-Queue-red)](https://redis.io/)
[![Protobuf](https://img.shields.io/badge/Protocol-Buffers-green)](https://developers.google.com/protocol-buffers)

  本模块实现了针对 Bilibili 视频弹幕的毫秒级抓取。不同于传统的 XML 接口爬虫，本项目通过逆向破解 B 站底层的 Protobuf 二进制协议，配合分布式 Producer-Consumer 架构，能够突破单次请求 3000 条的限制，实现全量弹幕的高效采集。

---

## ⚡ 核心特性 (Key Features)

1.  **硬核逆向**
    * 直接解析 B 站移动端/Web 端底层的 `Seg.so` 二进制流接口。
    * 核心定义文件：`dm_pb2.py` (由 `dm.proto` 编译而来)。
2.  **分布式架构 (Distributed Architecture)**
    * **Master-Slave 模式**：支持多台机器同时作为 Worker 节点运行。
    * **Redis 调度**：使用 Redis List (`bilibili_tasks`) 实现任务分发与状态管理，支持断点续传。
    * **高并发**：Producer 负责极速发单，Worker 负责耗时下载与解析。
3.  **全量采集策略 (Full Data Strategy)**
    * **智能分包**：自动根据视频时长计算分包（Segment）数量，遍历所有 6 分钟切片。
    * **历史回溯**：支持调用历史弹幕接口，抓取过去一年的全量弹幕数据。

---

## 📂 文件说明 (File Structure)

```text
crawler/
├── task_producer.py    # [Master] 任务生产者：将目标 BVID 推送至 Redis 队列
├── spider_worker.py    # [Slave]  爬虫工作节点：消费任务、下载、解析 Protobuf、存 CSV
├── dm_pb2.py           # [Core]   Protobuf 序列化协议文件 (逆向产物)
├── requirements.txt    # 依赖库清单
└── README.md           # 本文档
```

------

## 🛠️ 快速开始 (Quick Start)

### 1. 环境准备

确保已安装 Redis 服务，并安装 Python 依赖：

```
pip install -r requirements.txt
# 主要依赖: redis, protobuf, requests, bilibili-api-python
```

### 2. 配置参数 (Configuration)

打开 `spider_worker.py`，配置你的 B 站 Cookie（用于获取历史弹幕权限）：

```
# spider_worker.py
SESSDATA = "你的_SESSDATA"
BILI_JCT = "你的_BILI_JCT"
BUVID3   = "你的_BUVID3"
REDIS_HOST = "localhost" # Redis 地址
```

### 3. 启动系统 (Running)

**Step 1: 启动任务生产者 (Producer)** 编辑 `task_producer.py` 中的 `targets` 列表，填入你想爬取的番剧 BVID，然后运行：

```
python task_producer.py
# 输出: 🔥 正在推送 3 个任务... ✅ 任务发布完毕！
```

**Step 2: 启动分布式 Worker (Consumer)** 你可以在单机开启多个终端，或在多台服务器上同时运行此命令，构建分布式集群：

```
# 建议使用 nohup 后台运行
nohup python -u spider_worker.py > spider.log 2>&1 &
```

------

## 📚 参考与致谢 (Acknowledgments)

本项目在开发过程中参考了以下优秀的开源项目与文档，在此表示感谢：

* https://github.com/HengXin666/BiLiBiLi_DanMu_Crawling
