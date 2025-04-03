import uuid
from cascade import cascade

@cascade
class ComposeReview:
    def __init__(self, req_id: str, **kwargs): # **args is a temporary hack to allow for creation of composereview on the fly
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
        x = self.review_data
        return x

@cascade
class User:
    def __init__(self, username: str, user_data: dict):
        self.username = username
        self.user_data = user_data

    def upload_user(self, review: ComposeReview):
        user_id = self.user_data["userId"]
        review.upload_user_id(user_id)

@cascade
class MovieId:
    # key: 'title'
    def __init__(self, title: str,  movie_id: str):
        self.title = title
        self.movie_id = movie_id

    def upload_movie(self, review: ComposeReview, rating: int):
        # if self.movie_id is not None:
        #     review.upload_movie_id(self.movie_id)
        # else:
        #     review.upload_rating(rating)
        movie_id = self.movie_id
        review.upload_movie_id(movie_id)

@cascade
class Frontend():
    @staticmethod
    def compose(review: ComposeReview, user: User, title: MovieId, rating: int, text: str):
        UniqueId.upload_unique_id_2(review)
        user.upload_user(review)
        title.upload_movie(review, rating)
        # text = text[:CHAR_LIMIT] # an operation like this could be reorderd for better efficiency!
        Text.upload_text_2(review, text)
        
@cascade
class UniqueId():
    @staticmethod
    def upload_unique_id_2(review: ComposeReview):
        # TODO: support external libraries
        # review_id = uuid.uuid1().int >> 64
        review_id = 424242
        review.upload_unique_id(review_id)

@cascade
class Text():
    @staticmethod
    def upload_text_2(review: ComposeReview, text: str):
        review.upload_text(text)
    

@cascade
class Plot:
    def __init__(self, movie_id: str, plot: str):
        self.movie_id = movie_id
        self.plot = plot

@cascade
class MovieInfo:
    def __init__(self, movie_id: str, info: dict):
        self.movie_id = movie_id
        self.info = info