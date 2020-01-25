import logging
import os
import grab_utils
from os import listdir
from os.path import isfile, join
import shutil
import cv2
import numpy as np
import sys
import time
import utils
from urllib.request import urlopen, Request
import urllib
from tqdm import tqdm
import multiprocessing

logger = logging.getLogger('grabber')
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def generate_training_data(conn,tags, file_path):

    #set paths
    train_dir = file_path + "/train"
    val_dir = file_path + "/validation"
    download_metric = dict()

    #create folder
    try:
        os.mkdir(file_path)
        logger.debug("Directory " + file_path + " Created ") 
        os.mkdir(train_dir)
        logger.debug("Directory " + train_dir +  " Created ")
        os.mkdir(val_dir)
        logger.debug("Directory " + val_dir +  " Created ") 
    except FileExistsError as e:
        logger.info("Folder already exists: {}".format(e))

    logger.info("All Folders for {} existing".format(file_path))

    #create subfolder with pictures
    for key in tags:
        class_name = key
        class_dir = train_dir + "/" + class_name
        class_val_dir = val_dir + "/" + class_name

        try:
            os.mkdir(class_val_dir)
            logger.debug("Directory " + val_dir +  " Created ") 
        except FileExistsError as e:
            logger.debug("Folder already exists: {}".format(e))
        
        
        #for every class
        for db_tag in tags[class_name]:
            #download images into "train/<class_name>" folder
            logger.debug("Starting downloading files for {} ...".format(db_tag))
            download_subreddit_images(conn,db_tag,class_name,class_dir)
            logger.debug("Files download for {} finished".format(db_tag))

            #copy 30% of images into the validation folder
            file_list = [f for f in listdir(class_dir) if isfile(join(class_dir, f))]
            train_images = int(len(file_list) * 0.8)
            download_metric[class_name] = dict()
            download_metric[class_name][db_tag] =dict()
            download_metric[class_name][db_tag]['traindata'] = train_images
            
            logger.debug("{} trainings files for {}".format(train_images,db_tag))
            val_cnt = 0
            for image in file_list[train_images:]:
                from_dir = class_dir + "/" + image
                to_dir = val_dir + "/" + class_name + "/" + image
                os.rename(from_dir,to_dir)
                val_cnt += 1
            download_metric[class_name][db_tag]['validationdata'] = val_cnt


    # zip the file 
    shutil.make_archive(file_path, 'zip', file_path)
    logger.info("Trainings dada saved under {}.zip".format(file_path))
    logger.info(download_metric)

def download_thread(queue,class_name,file_path):
   
    while not queue.empty():
        msg =  queue.get()
        url = msg[1]
        postid = msg[0]
        file_name = file_path + "/" + class_name + "_" + postid + ".jpg"
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.3"}
        logger.debug("Download {} from thread {} ".format(url,os.getpid()))
        try:
            req = Request(url=url, headers=headers) 
            resp = urlopen(req).read()
        except urllib.error.HTTPError as e:
            logger.debug("Download of file {} forbidden. skip.{}".format(file_name,e))
        except:
            logger.debug("Download Unexpected error: {}".format(sys.exc_info()[0]))

        try:
            img_array = np.asarray(bytearray(resp), dtype="uint8")
            img = cv2.imdecode(img_array, cv2.IMREAD_COLOR)
            img_resize = cv2.resize(img, dsize=(150, 150), interpolation=cv2.INTER_CUBIC)
            cv2.imwrite(file_name, img_resize)
        except TypeError as e:
            logger.debug("Typeerror: {}".format(e))
        except:
            logger.debug("Unexpected error: {}".format(sys.exc_info()[0]))
        logger.debug("Download {} from thread {} ".format(url,os.getpid()))
        

def download_subreddit_images(conn,subreddit,class_name,file_path):

    try:
        os.mkdir(file_path)
        logger.debug("Download directory " + file_path +  " Created ") 
    except FileExistsError:
        logger.debug("Directory " + file_path +  " already exists")

    cur = conn.cursor()
    cur.execute("SELECT postid,url FROM reddit_posts where subreddit = ?",(subreddit,))
    rows = cur.fetchall()

    #check, if files already exists
    file_list = [f for f in listdir(file_path) if isfile(join(file_path, f))]
    #get already downloaded files
    existing_posts = [f.split(".")[:1][0].split("_")[-1] for f in file_list]
    #get missing files
    missing_posts = [postid for postid in rows if postid[0] not in existing_posts]
    logger.info(" {} images missing existing".format(len(missing_posts)))

    logger.debug("download image: found {} images in db for {}".format(len(rows),subreddit))

    logger.info("Starting Downloading {}".format(class_name))

    concurrent_threads = 20

    m = multiprocessing.Manager()
    list_queue = m.Queue()
    for row in missing_posts:
        list_queue.put(row)
    logger.info("Queue for {} set up with {} Entries".format(class_name,len(rows)))
    start_time = time.time()
    pool = multiprocessing.Pool(processes=concurrent_threads)
    for i in range(concurrent_threads):
        pool.apply_async(download_thread, args= (list_queue,class_name,file_path))
    pool.close()
    pool.join()
    elapsed_time = time.time() - start_time
    time_per_post = elapsed_time / len(rows)
    logger.info("Time {} ".format(utils.pretty_time(elapsed_time)))
    logger.info("Time per post {} ".format(utils.pretty_time(time_per_post)))

    logger.info("Finish {} ".format(class_name))
