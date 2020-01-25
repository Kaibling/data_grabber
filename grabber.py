import logging
import time
import dblayer
import train_data
import utils
import grab_utils
import shutil

# Init logger
logger = logging.getLogger('grabber')
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - [%(threadName)s] - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)
# End Init logger

logger.info('Init wrapper ...')
reddit_config = utils.config_file_parser()['reddit'][0]
reddit_bot = grab_utils.Reddit_bot(reddit_config)

conn = dblayer.init_database()
reddit_bot.add_DB_connector(conn)


generate_data = True


if generate_data is False:
    subreddit_names = ["cats","catpics","catpictures","DogPics","dogpictures"]
    post_amount = 700
    
    for subreddit_name in subreddit_names:
        reddit_bot.grab_subreddit(subreddit_name,post_amount)


if generate_data:
    tags = dict()
    #tags["dog"] = ["DogPics",]
    tags["cats"] = ["catpics",]

    #tags = dict()
    #tags["dog"] = ["DogPics","dogpictures"]
    #tags["cats"] = ["cats","catpics","catpictures"]

    train_data.generate_training_data(conn,tags, "./train_1")


conn.close()