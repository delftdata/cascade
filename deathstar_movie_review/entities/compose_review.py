from typing import Any

from cascade.dataflow.operator import StatefulOperator


class ComposeReview:
    def __init__(self, req_id: str, *args): # *args is a temporary hack to allow for creation of composereview on the fly
        self.req_id = req_id
        self.review_data = {}

    def upload_unique_id(self, review_id: int):
        self.review_data["review_id"] = review_id

    # could use the User class instead?
    def upload_user_id(self, user_id: str):
        self.review_data["userId"] = user_id
    
    def upload_movie_id(self, movie_id: str):
        self.review_data["movieId"] = movie_id

    def upload_rating(self, rating: int):
        self.review_data["rating"] = rating

    def upload_text(self, text: str):
        self.review_data["text"] = text

    def get_data(self):
        return self.review_data

def upload_unique_id_compiled(variable_map: dict[str, Any], state: ComposeReview) -> Any:
    state.review_data["review_id"] = variable_map["review_id"]

def upload_user_id_compiled(variable_map: dict[str, Any], state: ComposeReview) -> Any:
    state.review_data["userId"] = variable_map["user_id"]

def upload_movie_id_compiled(variable_map: dict[str, Any], state: ComposeReview) -> Any:
    state.review_data["movieId"] = variable_map["movie_id"]

def upload_rating_compiled(variable_map: dict[str, Any], state: ComposeReview) -> Any:
    state.review_data["rating"] = variable_map["rating"]

def upload_text_compiled(variable_map: dict[str, Any], state: ComposeReview) -> Any:
    state.review_data["text"] = variable_map["text"]

def get_data_compiled(variable_map: dict[str, Any], state: ComposeReview) -> Any:
    return state.review_data

compose_review_op = StatefulOperator(
    ComposeReview,
    {
        "upload_unique_id": upload_unique_id_compiled,
        "upload_user_id": upload_user_id_compiled,
        "upload_movie_id": upload_movie_id_compiled,
        "upload_rating": upload_rating_compiled,
        "upload_text": upload_text_compiled,
        "get_data": get_data_compiled,
    },
    {}
)