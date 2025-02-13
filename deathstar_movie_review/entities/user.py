from typing import Any
from deathstar_movie_review.entities.compose_review import ComposeReview
from src.cascade.dataflow.dataflow import DataFlow, Edge, InvokeMethod, OpNode
from src.cascade.dataflow.operator import StatefulOperator


class User:
    def __init__(self, username: str, user_data: dict):
        self.username = username
        self.user_data = user_data

    def upload_user(self, review: ComposeReview):
        review.upload_user_id(self.user_data["userId"])


def upload_user_compiled_0(variable_map: dict[str, Any], state: User) -> Any:
    variable_map["user_id"] = state.user_data["userId"]

user_op = StatefulOperator(
    User,
    {
        "upload_user_compiled_0": upload_user_compiled_0,
    },
    {}
)

def upload_df():
    df = DataFlow("user_upload_user")
    n0 = OpNode(User, InvokeMethod("upload_user_compiled_0"), read_key_from="username")
    n1 = OpNode(ComposeReview, InvokeMethod("upload_user_id"), read_key_from="review")

    df.add_edge(Edge(n0, n1))
    df.entry = n0
    return df

user_op.dataflows["upload_user"] = upload_df()