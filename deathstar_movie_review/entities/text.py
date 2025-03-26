from typing import Any
from cascade.dataflow.dataflow import DataFlow, Edge, InvokeMethod, OpNode, StatelessOpNode
from cascade.dataflow.operator import StatelessOperator
from deathstar_movie_review.entities.compose_review import ComposeReview

class Text():
    @staticmethod
    def upload_text_2(review: ComposeReview, text: str):
        review.upload_text(text)


###### COMPILED FUNCTIONS ######

def upload_text_2_compiled_0(variable_map: dict[str, Any]):
    pass

text_op = StatelessOperator(
    {
        "upload_text_2": upload_text_2_compiled_0
    },
    None
)

df = DataFlow("upload_text")
n0 = StatelessOpNode(text_op, InvokeMethod("upload_text_2"))
n1 = OpNode(ComposeReview, InvokeMethod("upload_text"), read_key_from="review")
df.add_edge(Edge(n0, n1))
df.entry = n0
text_op.dataflow = df