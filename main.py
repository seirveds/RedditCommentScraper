from CommentScraper import CommentScraper

if __name__ == '__main__':
    scraper = CommentScraper()
    scraper.scrape_subreddit('tomorrow', limit=2)
