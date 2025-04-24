from cascade.frontend.ast_visitors.replace_name import ReplaceSelfWithState
from cascade.frontend.ast_visitors.simplify_returns import simplify_returns
from klara.core.tree_rewriter import AstBuilder
from klara.core.cfg import Cfg
from klara.core import nodes

def setup_cfg(code: str, preprocess=True) -> tuple[Cfg, nodes.Module]:
    as_tree = AstBuilder().string_build(code)
    cfg = Cfg(as_tree)
    cfg.convert_to_ssa()
    if preprocess:
        ReplaceSelfWithState.replace(as_tree)
        simplify_returns(as_tree)
    # TODO: do this in preprocessing
    return cfg, as_tree