import csv
import os

from dotenv import load_dotenv
import praw
from praw.models import Comment, MoreComments
from prawcore.exceptions import Forbidden
from tqdm import tqdm


# TODO verbosity
# TODO multiprocessing

class CommentScraper:

    def __init__(self):
        self.reddit = None
        self._authenticate()

        self.posts = None
        self.comments = []
        self.limit = None
        self.subreddit = None


    def _authenticate(self):
        """
        Authenticate credentials passed in .env. To get credentials follow the steps at:
        https://praw.readthedocs.io/en/latest/getting_started/quick_start.html#read-only-reddit-instances
        """
        print("Authenticating credentials...")
        
        try:
            load_dotenv()
            
            self.reddit = praw.Reddit(client_id=os.environ["client_id"],
                                      client_secret=os.environ["client_secret"],
                                      user_agent=os.environ["user_agent"])
            print("Authentication success.")
        except (KeyError):
            print("Authentication failed.")
            print("Could not find .env file containing a client_id, client_secret, and user_agent.")


    def _get_top_posts(self, subreddit, time_filter='year', limit=999):
        """
        params:
            - subreddit (string): name of subreddit without 'r/'.
            - time_filter (string): time fiter for top posts, must
                be one of: 'all', 'day', 'month', 'week', 'year', 'hour'.
            - limit (int): amount of top posts to scrape comments from. Max 999.
        """
        # Save subreddit name and limit
        self.limit = limit
        self.subreddit = subreddit

        # Get subreddit instance of passed subreddit name
        subreddit = self.reddit.subreddit(subreddit)

        # Check if subreddit is quarantined. If this is the case
        # opt in to scrape quarantined data
        try:
            subreddit.quarantine
        except Forbidden:
            subreddit.quaran.opt_in()

        # Use passed filters to retrieve the top posts of the passed subreddit
        top_posts_generator = subreddit.top(time_filter=time_filter, limit=limit)

        # Keep posts as generator object as we only need to iterate
        # once over all posts
        self.posts = top_posts_generator


    def _retrieve_post_comments(self, upvote_threshold=10):
        """
        Iterate over the posts attribute and store each comment in the comments attribute.

        params:
            - upvote_threshold (int): minimum amount of upvotes a comment must have to be saved.
        """
        for post in tqdm(self.posts, desc=f"Scraping post comments for {self.subreddit}", total=self.limit):
            # Get list of Comment and MoreComments objects
            comment_list = post.comments.list()

            # Because comment list is a list containing both Comment
            # and MoreComment objects we need to use a while loop
            # so we can add comments from the MoreComments object
            # back into the list so they can be processed.
            while comment_list:
                # Pop first object from comment list
                obj = comment_list.pop()

                # Check if popped object is an instance of the praw Comment class
                if isinstance(obj, Comment):
                    # Get comment score from Comment object
                    score = obj.score

                    # Only save comment text if score is higher than threshold
                    if score > upvote_threshold:
                        # Get comment text from Comment object
                        comment_text = obj.body

                        # Store comments in list
                        self.comments.append(comment_text)

                # Check if popped object is an instance of the praw MoreComments class
                elif isinstance(obj, MoreComments):
                    # MoreComments object can give a list containing
                    # more Comment and MoreComments objects or a CommentForest
                    # object. This is the easy way of handling both cases.
                    try:
                        obj_list = obj.comments().list()
                    except AttributeError:
                        obj_list = obj.comments()

                    # Extend this list to comment list so they will be
                    # processed later during iteration.
                    comment_list.extend(obj_list)
                else:
                    raise Exception("Unexpected object found in comment list."
                                    f"Found object of type {type(obj)}")


    def _save_comments(self, path=None):
        """
        Save all commments stored in comments attribute to a csv file.

        params:
            - path (string): if None save file as {subreddit_name}.csv, otherwise as given path.
        """
        # Use subreddit name as filename if no path is passed
        if path is None:
            path = f"{self.subreddit}.csv"

        # Create directory passed in path.
        try:
            # Grab directory and filename from passed path
            fdir, _ = os.path.split(path)
            # Only make folder is there was a folder passed in path
            if fdir:
                os.makedirs(fdir)
        except (FileExistsError):
            pass 

        # Save as csv using built-in lib, prevents extra pandas dependency
        with open(path, 'w', encoding='UTF-8') as csv_file:
            writer = csv.writer(csv_file, quoting=csv.QUOTE_ALL)
            writer.writerow(self.comments)


    def scrape_subreddit(self, subreddit, time_filter='year',
                         limit=999, upvote_threshold=5, path=None):
        """
        Wrapper method for retrieving top posts, extracting comments, and saving comments to disk

        params:
            - subreddit (string): name of subreddit without 'r/'.
            - time_filter (string): time fiter for top posts, must
                be one of: 'all', 'day', 'month', 'week', 'year', 'hour'.
            - limit (int): amount of top posts to scrape comments from. Max 999.
            - upvote_threshold (int): minimum amount of upvotes a comment must have to be saved.
             - path (string): if None save file as {subreddit_name}.csv, otherwise as given path.
        """
        if path is not None and os.path.exists(path):
            raise FileExistsError("Passed path already exists.")

        self._get_top_posts(subreddit, time_filter=time_filter, limit=limit)

        self._retrieve_post_comments(upvote_threshold=upvote_threshold)

        self._save_comments(path=path)


if __name__ == '__main__':
    cs = CommentScraper()

    cs.scrape_subreddit('python', path='data/python.csv', time_filter='year', limit=100)
