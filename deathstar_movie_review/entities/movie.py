from typing import Any
from cascade.dataflow.dataflow import DataFlow, Edge, InvokeMethod, OpNode
from cascade.dataflow.operator import StatefulOperator
from deathstar_movie_review.entities.compose_review import ComposeReview
from deathstar_movie_review.entities.user import User


class MovieId:
    # key: 'title'
    def __init__(self, title: str,  movie_id: str):
        self.title = title
        self.movie_id = movie_id

    def upload_movie(self, review: ComposeReview, rating: int):
        if self.movie_id is not None:
            review.upload_movie_id(self.movie_id)
        else:
            review.upload_rating(rating)


def upload_movie_compiled_cond_0(variable_map: dict[str, Any], state: MovieId) -> Any:
    variable_map["movie_id"] = state.movie_id # SSA
    return variable_map["movie_id"] is not None

movie_id_op = StatefulOperator(
    MovieId,
    {
        "upload_movie_cond": upload_movie_compiled_cond_0
    },
    {}
)

def upload_movie_df():
    df = DataFlow("movieId_upload_movie")
    n0 = OpNode(MovieId, InvokeMethod("upload_movie_cond"), read_key_from="title", is_conditional=True)
    n1 = OpNode(ComposeReview, InvokeMethod("upload_movie_id"), read_key_from="review")
    n2 = OpNode(ComposeReview, InvokeMethod("upload_rating"), read_key_from="review")

    df.add_edge(Edge(n0, n1, if_conditional=True)) 
    df.add_edge(Edge(n0, n2, if_conditional=False)) 
    df.entry = n0
    return df

movie_id_op.dataflows["upload_movie"] = upload_movie_df()



### Other movie-related operators

# key: movie_id

class Plot:
    def __init__(self, movie_id: str, plot: str):
        self.movie_id = movie_id
        self.plot = plot

class MovieInfo:
    def __init__(self, movie_id: str, info: dict):
        self.movie_id = movie_id
        self.info = info

movie_info_op = StatefulOperator(
    MovieInfo,
    {},
    {}
)

plot_op = StatefulOperator(
    Plot,
    {},
    {}
)