from typing import Any
import uuid

from cascade.dataflow.dataflow import CollectNode, CollectTarget, DataFlow, Edge, InvokeMethod, OpNode, StatelessOpNode
from cascade.dataflow.operator import StatelessOperator
from deathstar_movie_review.entities.compose_review import ComposeReview
from deathstar_movie_review.entities.movie import MovieId
from deathstar_movie_review.entities.user import User


# unique_id is stateless 
class UniqueId():
    @staticmethod
    def upload_unique_id_2(review: ComposeReview):
        review_id = uuid.uuid1().int >> 64
        review.upload_unique_id(review_id)

# text is stateless
class Text():
    @staticmethod
    def upload_text_2(review: ComposeReview, text: str):
        review.upload_text(text)

CHAR_LIMIT = 50

# frontend is made stateless
class Frontend():
    @staticmethod
    def compose(review: ComposeReview, user: User, title: MovieId, rating: int, text: str):

        # dead node elimination will remove "returning back" to the original function
        # 
        # cascade could theoritically allow for more advanced analysis,
        # that would enable all these to run in parallel. However, this is only
        # possible because
        #   1. the individual functions don't depend on each other
        #   2. the ordering of side-effects does not matter 
        UniqueId.upload_unique_id_2(review)
        user.upload_user(review)
        title.upload_movie(review, rating)

        text = text[:CHAR_LIMIT] # an operation like this could be reorderd for better efficiency!
        Text.upload_text_2(review, text)

###### COMPILED FUNCTIONS ######

### UPLOAD UNIQUE ###

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

### TEXT ###

text_op = StatelessOperator(
    {},
    None
)

df = DataFlow("upload_text")
n0 = OpNode(ComposeReview, InvokeMethod("upload_text"), read_key_from="review")
df.entry = n0
text_op.dataflow = df

### FRONTEND ###

def compose_compiled_0(variable_map: dict[str, Any]):
    pass


frontend_op = StatelessOperator(
    {
        "empty": compose_compiled_0,
    },
    None
)

def frontend_df_serial():
    # This dataflow calls many other dataflows. 
    # It could be more useful to have a "Dataflow" node
    df = DataFlow("compose")
    n0 = StatelessOpNode(frontend_op, InvokeMethod("empty"))

    # Upload Unique DF
    n1_a = StatelessOpNode(unique_id_op, InvokeMethod("upload_unique"))
    n1_b = OpNode(ComposeReview, InvokeMethod("upload_unique_id"), read_key_from="review")

    n2 = StatelessOpNode(frontend_op, InvokeMethod("empty"))

    # Upload User DF
    n3_a = OpNode(User, InvokeMethod("upload_user_compiled_0"), read_key_from="user")
    n3_b = OpNode(ComposeReview, InvokeMethod("upload_user_id"), read_key_from="review")

    n4 = StatelessOpNode(frontend_op, InvokeMethod("empty"))

    # Upload Movie DF
    n5_a = OpNode(MovieId, InvokeMethod("upload_movie_cond"), read_key_from="title", is_conditional=True)
    n5_b = OpNode(ComposeReview, InvokeMethod("upload_movie_id"), read_key_from="review")
    n5_c = OpNode(ComposeReview, InvokeMethod("upload_rating"), read_key_from="review")

    n6 = StatelessOpNode(frontend_op, InvokeMethod("empty"))

    # Upload Text DF
    n7 = OpNode(ComposeReview, InvokeMethod("upload_text"), read_key_from="review")

    n8 = StatelessOpNode(frontend_op, InvokeMethod("empty"))

    df.add_edge(Edge(n0, n1_a))
    df.add_edge(Edge(n1_a, n1_b))
    df.add_edge(Edge(n1_b, n2))

    df.add_edge(Edge(n2, n3_a))
    df.add_edge(Edge(n3_a, n3_b))
    df.add_edge(Edge(n3_b, n4))

    df.add_edge(Edge(n4, n5_a))
    df.add_edge(Edge(n5_a, n5_b, if_conditional=True))
    df.add_edge(Edge(n5_a, n5_c, if_conditional=False))
    df.add_edge(Edge(n5_b, n6))
    df.add_edge(Edge(n5_c, n6))

    df.add_edge(Edge(n6, n7))
    df.add_edge(Edge(n7, n8))

    df.entry = n0
    return df

def frontend_df_parallel():
    # This dataflow calls many other dataflows. 
    # It could be more useful to have a "Dataflow" node
    df = DataFlow("compose")
    # n0 = StatelessOpNode(frontend_op, InvokeMethod("empty"))
    ct = CollectNode(assign_result_to="results", read_results_from="dummy")

    # Upload Unique DF
    n1_a = StatelessOpNode(unique_id_op, InvokeMethod("upload_unique"))
    n1_b = OpNode(ComposeReview, InvokeMethod("upload_unique_id"), read_key_from="review", collect_target=CollectTarget(ct, 4, 0))


    # Upload User DF
    n3_a = OpNode(User, InvokeMethod("upload_user_compiled_0"), read_key_from="user")
    n3_b = OpNode(ComposeReview, InvokeMethod("upload_user_id"), read_key_from="review", collect_target=CollectTarget(ct, 4, 1))


    # Upload Movie DF
    n5_a = OpNode(MovieId, InvokeMethod("upload_movie_cond"), read_key_from="title", is_conditional=True)
    n5_b = OpNode(ComposeReview, InvokeMethod("upload_movie_id"), read_key_from="review", collect_target=CollectTarget(ct, 4, 2))
    n5_c = OpNode(ComposeReview, InvokeMethod("upload_rating"), read_key_from="review", collect_target=CollectTarget(ct, 4, 2))


    # Upload Text DF
    n7 = OpNode(ComposeReview, InvokeMethod("upload_text"), read_key_from="review",collect_target=CollectTarget(ct, 4, 3))


    # df.add_edge(Edge(n0, n1_a))
    df.add_edge(Edge(n1_a, n1_b))
    df.add_edge(Edge(n1_b, ct))

    # df.add_edge(Edge(n0, n3_a))
    df.add_edge(Edge(n3_a, n3_b))
    df.add_edge(Edge(n3_b, ct))

    # df.add_edge(Edge(n0, n5_a))
    df.add_edge(Edge(n5_a, n5_b, if_conditional=True))
    df.add_edge(Edge(n5_a, n5_c, if_conditional=False))
    df.add_edge(Edge(n5_b, ct))
    df.add_edge(Edge(n5_c, ct))

    # df.add_edge(Edge(n0, n7))
    df.add_edge(Edge(n7, ct))

    df.entry = [n1_a, n3_a, n5_a, n7]
    return df

frontend_op.dataflow = frontend_df_parallel()
    
