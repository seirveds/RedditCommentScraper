import csv
from datetime import datetime
from multiprocessing import Pool, cpu_count
import os
import re

from dotenv import load_dotenv
import praw
from praw.models import Comment, MoreComments
from prawcore.exceptions import Forbidden, Redirect
from tqdm import tqdm


# TODO verbosity
# TODO multiprocessing

class CommentScraper:

    def __init__(self):
        self.reddit = None
        self._authenticate()

        self.posts = None
        # Every comment stored in a dict containing post_title, comment text, author, and upvotes as values
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


    def _get_posts(self, subreddit, time_filter, limit, sort_by):
        """
        params:
            - subreddit (string): name of subreddit without 'r/'.
            - time_filter (string): time fiter for top posts, must
                be one of: 'all', 'day', 'month', 'week', 'year', 'hour'.
            - limit (int): amount of top posts to scrape comments from. Max 999.
            - sort_by (string): gets posts sorted by "top", "new", or "hot"
        """

        assert sort_by in ["top", "new", "hot"]
        assert time_filter in ["all", "day", "month", "week", "year", "hour"]

        # Save subreddit name and limit
        self.limit = limit
        self.subreddit = subreddit

        # Get subreddit instance of passed subreddit name
        subreddit = self.reddit.subreddit(subreddit)

        # Try accessing a random attribute to check if the passed subreddit actually exists or not
        try:
            subreddit.fullname
        except Redirect:
            raise Exception(f"Passed subreddit '{subreddit}' does not exist. ")

        # Check if subreddit is quarantined. If this is the case
        # opt in to scrape quarantined data
        try:
            subreddit.quarantine
        except Forbidden:
            subreddit.quaran.opt_in()

        # Use passed filters to retrieve the top posts of the passed subreddit
        if sort_by == "top":
            posts_generator = subreddit.top(time_filter=time_filter, limit=limit)
        elif sort_by == "new":
            posts_generator = subreddit.new(limit=limit)
        else:
            posts_generator = subreddit.hot(limit=limit)

        # Keep posts as generator object as we only need to iterate
        # once over all posts
        self.posts = posts_generator


    def _retrieve_post_comments(self, upvote_threshold, strip_comments, use_multiprocessing):
        """
        Iterate over the posts attribute and store each comment in the comments attribute.

        params:
            - upvote_threshold (int): minimum amount of upvotes a comment must have to be saved.
            - strip_comments (boolean): remove newlines and markdown syntax from comments.
            - use_multiprocessing (boolean): use multiple process threads to speed up data collection
        """
        if use_multiprocessing:
            # Need an iterable containing arguments we want to pass to the function.
            # IMPORTANT: arguments must be in the order the function wants
            func_arguments_iterable = [(post, upvote_threshold, strip_comments) for post in self.posts]
            # Initialize multiprocessing pool using all available threads - 2 to prevent slowing down whole system
            with Pool(cpu_count() - 2) as p:
                p.starmap(self._retrieve_post_comment, func_arguments_iterable)
        else:
            for post in tqdm(self.posts, desc=f"Scraping post comments for {self.subreddit}", total=self.limit):
                self._retrieve_post_comment(post, upvote_threshold=upvote_threshold, strip_comments=strip_comments)


    def _retrieve_post_comment(self, post, upvote_threshold, strip_comments):
        """
        Retrieve all comments and extra post metadata for a single post and store in self.comments.
        params:
            - post (praw.models.reddit.submission.Submission): post object
            - upvote_threshold (int): minimum amount of upvotes a comment must have to be saved.
            - strip_comments (boolean): remove newlines and markdown syntax from comments.
        """
        # Get list of Comment and MoreComments objects
        comment_list = post.comments.list()

        # If there are no comments still save the post title to log we have seen the post
        if not comment_list:
            self.comments.append({
                "post": post.title,
                "author": None,
                "comment": None,
                "upvotes": None
            })
        else:
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
                        # Get comment text, clean if strip_comments is set to True
                        comment_text = obj.body
                        if strip_comments:
                            comment_text = self.strip_newlines(obj.body)

                        # Store comments + metadata in list
                        self.comments.append({
                            "post": post.title,
                            "author": obj.author,
                            "comment": comment_text,
                            "upvotes": score
                        })

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
            path = f"{self.subreddit}_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"

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
        with open(path, 'w', encoding='UTF-8', newline="") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=self.comments[0].keys(), quoting=csv.QUOTE_ALL)
            writer.writeheader()
            for comment_dict in self.comments:
                writer.writerow(comment_dict)


    def scrape_subreddit(self, subreddit, time_filter='year', limit=999, upvote_threshold=5,
                         sort_by="top", path=None, strip_comments=True, use_multiprocessing=True):
        """
        Wrapper method for retrieving top posts, extracting comments, and saving comments to disk

        params:
            - subreddit (string): name of subreddit without 'r/'.
            - time_filter (string): time fiter for top posts, must
                be one of: 'all', 'day', 'month', 'week', 'year', 'hour'.
            - limit (int): amount of top posts to scrape comments from. Max 999.
            - upvote_threshold (int): minimum amount of upvotes a comment must have to be saved.
            - sort_by (string): gets posts sorted by "top", "new", or "hot"
            - path (string): if None save file as {subreddit_name}.csv, otherwise as given path.
            - strip_comments (boolean): remove newlines and markdown syntax from comments.
            - use_multiprocessing (boolean): use multiple process threads to speed up data collection
        """
        if path is not None and os.path.exists(path):
            raise FileExistsError("Passed path already exists.")

        self._get_posts(subreddit, time_filter=time_filter, limit=limit, sort_by=sort_by)

        self._retrieve_post_comments(upvote_threshold=upvote_threshold, strip_comments=strip_comments,
                                     use_multiprocessing=use_multiprocessing)

        self._save_comments(path=path)

    @staticmethod
    def strip_newlines(comment):
        """
        Remove newlines and duplicate whitespaces from comment. 
        
        params:
            - comment (string): comment to process.
        """

        # Matches one or two newlines
        comment = re.sub(r"\n[\n]?", " ", comment)
        # Matches one or more whitespaces
        comment = re.sub(r"\s+", r" ", comment)

        return comment



if __name__ == '__main__':
    cs = CommentScraper()

    cs.scrape_subreddit('python', path='data/python.csv', time_filter='year', limit=100)
