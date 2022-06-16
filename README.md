# RedditCommentScraper
Use praw to scrape comments from a subreddit

## Prerequisites

- Dependencies from ```requirements.txt```
- Reddit account
- Registered an app (personal script) at https://www.reddit.com/prefs/apps
- Saved following 3 tokens in a .env file in root of project:
```
client_id=""      # Shown under registered app name
client_secret=""  # Shown after clicking 'edit'
user_agent=""     # App name
```

## Usage

```python
from CommentScraper import CommentScraper

# Automatically authenticates using credentials supplied in .env file
scraper = CommentScraper()

scraper.scrape_subreddit(
    subreddit="python",       # Subreddit name without r/
    time_filter="week",       # Possible options: 'all', 'day', 'month', 'week', 'year', 'hour'
    limit=100,                # Limit scraping to 100 posts
    upvote_threshold=5,       # Only save comments with >5 upvotes
    sort_by="top",            # Possible options: 'top', 'new', 'hot'
    path="comments.csv"       # Path to file where comment data is saved
    strip_comments=True       # Remove newlines and double whitespaces from comments
    use_multiprocessing=True  # Use multiple threads to process data faster
)
```

### TODO
- Update README
- ~~Implement multiprocessing~~
- Verbosity for multiprocessing
- ~~Remove empty lines from csv output~~
