# import pandas as pd
# from pyecharts import options as opts
# from pyecharts.charts import Graph

# # nodes.csv: Id,Size
# # edges.csv: Source,Target,Weight

# nodes_file = "output_nodes/result_nodes.csv"
# edges_file = "output_edges/result_edges.csv"

# df_nodes = pd.read_csv(nodes_file)
# df_edges = pd.read_csv(edges_file)

# nodes = []
# max_size = df_nodes["Size"].max()
# min_size = df_nodes["Size"].min()

# dynamic_threshold = df_nodes["Size"].quantile(0.75)

# for _, row in df_nodes.iterrows():
#     normalized_size = 10 + (row["Size"] - min_size) / (max_size - min_size) * 40
    
    # nodes.append({
    #     "name": row["Id"],
    #     "symbolSize": normalized_size,
    #     "draggable": True,
    #     "value": row["Size"],
    #     # "category": 0 if row["Size"] > 1000 else 1 
    #     "category": 0 if row["Size"] > dynamic_threshold else 1 
    # })

# links = []
# for _, row in df_edges.iterrows():
#     links.append({
#         "source": row["Source"],
#         "target": row["Target"],
#         "value": row["Weight"],
#         "lineStyle": {"width": 1 + (row["Weight"] / 10)} 
#     })

# c = (
#     Graph(init_opts=opts.InitOpts(width="1000px", height="800px"))
#     .add(
#         "",
#         nodes,
#         links,
#         repulsion=80000,
#         edge_label=opts.LabelOpts(
#             is_show=True, position="middle", formatter="{c}"
#         ),
#         categories=[{"name": "核心人物"}, {"name": "次要人物"}],
#         layout="force",
#     )
#     .set_global_opts(
#         title_opts=opts.TitleOpts(title="凡人修仙传-人物共现关系图谱"),
#         legend_opts=opts.LegendOpts(is_show=True)
#     )
#     .render("fanRen_relation.html")
# )

# print("图谱已生成：fanRen_relation.html，请在浏览器打开查看。")

import pandas as pd
import numpy as np  # 需要导入 numpy 进行对数运算
from pyecharts import options as opts
from pyecharts.charts import Graph

nodes_file = "output_nodes/result_nodes.csv"
edges_file = "output_edges/result_edges.csv"

df_nodes = pd.read_csv(nodes_file)
df_edges = pd.read_csv(edges_file)

# 节点大小优化
# 使用 Log (对数) 缩放，平滑巨大差异
df_nodes["log_size"] = np.log1p(df_nodes["Size"])

max_log = df_nodes["log_size"].max()
min_log = df_nodes["log_size"].min()

top_nodes = df_nodes.nlargest(5, "Size")["Id"].tolist()

nodes = []
for _, row in df_nodes.iterrows():
    # 映射到 15px ~ 60px 之间
    symbol_size = 15 + (row["log_size"] - min_log) / (max_log - min_log) * 45
    
    nodes.append({
        "name": row["Id"],
        "symbolSize": symbol_size,
        "draggable": True,
        "value": row["Size"],
        "category": 0 if row["Id"] in top_nodes else 1,
        "label": {
            "show": True,
            # 只有比较大的节点才一直显示名字，小的鼠标悬停才显示，防止文字重叠
            "fontSize": 12 if symbol_size > 30 else 0 
        }
    })

# 边粗细优化
links = []
max_weight = df_edges["Weight"].max()

for _, row in df_edges.iterrows():
    # 将权重映射到 1px ~ 8px 之间
    normalized_weight = (row["Weight"] / max_weight) ** 0.5 
    line_width = 1 + normalized_weight * 7
    
    links.append({
        "source": row["Source"],
        "target": row["Target"],
        "value": row["Weight"],
        "lineStyle": {
            "width": line_width, 
            "curveness": 0.1,
            "opacity": 0.7
        }
    })

c = (
    Graph(init_opts=opts.InitOpts(width="1200px", height="800px", page_title="凡人修仙传人物图谱"))
    .add(
        "",
        nodes,
        links,
        repulsion=5000,
        gravity=0.05,
        edge_length=[50, 400],
        
        categories=[{"name": "核心人物"}, {"name": "重要人物"}],
        
        layout="force",
        is_roam=True, 
        is_focusnode=True,
        
        edge_label=opts.LabelOpts(
            is_show=False,
            position="middle", 
            formatter="{c}"
        ),
        linestyle_opts=opts.LineStyleOpts(color="source", curve=0.1),
    )
    .set_global_opts(
        title_opts=opts.TitleOpts(title="凡人修仙传-人物共现关系图谱"),
        legend_opts=opts.LegendOpts(orient="vertical", pos_left="2%", pos_top="20%"),
        tooltip_opts=opts.TooltipOpts(formatter="{b}: {c}")
    )
    .render("fanRen_relation.html")
)

print("优化后的图谱已生成：fanRen_relation.html")