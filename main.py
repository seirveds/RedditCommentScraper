from CommentScraper import CommentScraper

if __name__ == '__main__':
    scraper = CommentScraper()
    scraper.scrape_subreddit('weirddalle', sort_by="new", use_multiprocessing=True)
