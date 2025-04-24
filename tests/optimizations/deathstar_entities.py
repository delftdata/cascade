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
class MovieId:
    # key: 'title'
    def __init__(self, title: str,  movie_id: str):
        self.title = title
        self.movie_id = movie_id

    def upload_movie_prefetch(self, review: ComposeReview, rating: int):
        cond = rating is not None
        movie_id = self.movie_id
        review.upload_rating(rating)
        review.upload_movie_id(movie_id)
        return cond
        