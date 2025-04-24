import re
import matplotlib.pyplot as plt
import networkx as nx

from klara.core.tree_rewriter import AstBuilder
from klara.core.cfg import Cfg

from cascade.frontend.ast_visitors.simplify_returns import simplify_returns


color_map_map = {0: 'b', 1:'g', 2:'r', 3:'c', 4:'m', 5:'y', 6:'k', -1:'pink'}


def plot_graph_with_color(G: nx.DiGraph, grey_background: bool=True):
    fig, ax = plt.subplots()
    pos = nx.nx_agraph.graphviz_layout(G, prog="dot")
    labels = {}
    color_map = []
    for block, label_data in G.nodes(data=True):
        labels[block] = block.block_num
        color_map.append(color_map_map[block.color])
        
    # plt.figure(next(counter),figsize=figzise) 
    nx.draw(G, pos, labels=labels, node_color=color_map, with_labels=True)
    if grey_background:
        fig.set_facecolor('darkgrey')


def plot_dataflow_graph(G: nx.DiGraph, grey_background: bool = True):
    fig, ax = plt.subplots()
    pos = nx.nx_agraph.graphviz_layout(G, prog="dot")
    labels = {}
    for block, label_data in G.nodes(data=True):
        
        label = '{' + ','.join(str(s.block_num) for s in block.statement_list) +'}'
        labels[block] = label
        
    nx.draw(G, pos, labels=labels, 
            # node_size=[len(labels[v]) * 200 for v in G.nodes()],  
            node_shape="s",  node_color="none", 
            bbox=dict(facecolor="skyblue", edgecolor='black', boxstyle='round,pad=0.2'),
            with_labels=True)
    if grey_background:
        fig.set_facecolor('darkgrey')


def to_camel_case(name):
    return re.sub(r'(?<!^)(?=[A-Z])', '_', name).lower()
