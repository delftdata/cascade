from typing import Any
import uuid
from cascade.dataflow.dataflow import DataFlow, InvokeMethod, OpNode, StatelessOpNode
from cascade.dataflow.operator import StatelessOperator
from deathstar_movie_review.entities.compose_review import ComposeReview


class UniqueId():
    @staticmethod
    def upload_unique_id_2(review: ComposeReview):
        review_id = uuid.uuid1().int >> 64
        review.upload_unique_id(review_id)



###### COMPILED FUNCTIONS ######

def upload_unique_compiled_0(variable_map: dict[str, Any]):
    variable_map["review_id"] = uuid.uuid1().int >> 64

unique_id_op = StatelessOperator(
    {
        "upload_unique": upload_unique_compiled_0,
    },
    None
)

df = DataFlow("upload_unique_id")
n0 = StatelessOpNode(unique_id_op, InvokeMethod("upload_unique"))
n1 = OpNode(ComposeReview, InvokeMethod("upload_unique_id"), read_key_from="review")
df.entry = n0
unique_id_op.dataflow = df
