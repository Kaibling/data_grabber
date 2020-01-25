import logging
import os

#from urllib.request import urlopen, Request

from requests import Session
import praw
import dblayer
from tqdm import tqdm

logger = logging.getLogger('grabber')
logger.setLevel(logging.INFO)

class Reddit_bot:

    def __init__(self, reddit_config):
        session = Session()
        self.reddit = praw.Reddit(client_id=reddit_config['client_id'],
                            client_secret=reddit_config['client_secret'],
                            password=reddit_config['password'],
                            requestor_kwargs={'session': session},  # pass Session
                            user_agent='testscript by /u/'+reddit_config['username'],
                            username=reddit_config['username'])
        logger.info("Wrapper completed")
    
    def add_DB_connector(self,conn):
        self.conn = conn

    def grab_subreddit(self,subreddit_name,post_amount):
        logger.debug(subreddit_name)
        subreddit = self.reddit.subreddit(subreddit_name)
        cnt = 0

        with tqdm(total=post_amount,desc="Grab top of "+subreddit_name) as pbar:
            for submission in subreddit.top(limit=post_amount):
                if "jpg" in submission.url:
                    cnt = cnt +1 
                    dblayer.add_post(self.conn,(submission.id,submission.created_utc,submission.url,subreddit_name))
                    pbar.update(1)

        with tqdm(total=post_amount,desc="Grab new of "+subreddit_name) as pbar:
            for submission in subreddit.new(limit=post_amount):
                if "jpg" in submission.url:
                    cnt = cnt +1 
                    dblayer.add_post(self.conn,(submission.id,submission.created_utc,submission.url,subreddit_name))
                    pbar.update(1)

        logger.info("Grab " + subreddit.display_name + " with " + str(cnt) + " entries")

        #if cnt != 0:
        #    elapsed_time = time.time() - start_time
        #    pretty_time(elapsed_time)
        #    time_per_post = elapsed_time / cnt
        #    logger.debug("Time per post {} ".format(pretty_time(time_per_post)))
        #get_pic_status(conn)