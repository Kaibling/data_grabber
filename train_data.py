import logging
import os
import grab_utils
from os import listdir
from os.path import isfile, join
import shutil
import asyncio
import cv2
import numpy as np
import aiohttp
import sys
import time
import utils


import PIL
import io
from PIL import Image

logger = logging.getLogger('grabber')
logger.setLevel(logging.INFO)
#ch = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
#ch.setFormatter(formatter)
#logger.addHandler(ch)

def generate_training_data(conn,tags, file_path):

  train_dir = file_path + "/train"
  val_dir = file_path + "/validation"

  os.mkdir(file_path)
  logger.debug("Directory " + file_path + " Created ") 
  os.mkdir(train_dir)
  logger.debug("Directory " + train_dir +  " Created ")
  os.mkdir(val_dir)
  logger.debug("Directory " + val_dir +  " Created ") 
  logger.info("All Folders for {} created".format(file_path))


  for key in tags:
    class_name = key
    class_dir = train_dir + "/" + class_name
    class_val_dir = val_dir + "/" + class_name
    os.mkdir(class_val_dir)

    for db_tag in tags[class_name]:
      logger.debug("Starting downloading files for {} ...".format(db_tag))
      download_subreddit_images(conn,db_tag,class_name,class_dir)
      logger.debug("Files download for {} finished".format(db_tag))

    file_list = [f for f in listdir(class_dir) if isfile(join(class_dir, f))]
    train_images = int(len(file_list) * 0.6)
    logger.debug("{} trainings files for {}".format(train_images,db_tag))

    for image in file_list[train_images:]:
      from_dir = class_dir + "/" + image
      to_dir = val_dir + "/" + class_name + "/" + image
      os.rename(from_dir,to_dir)

  shutil.make_archive(file_path, 'zip', file_path)
  logger.info("Trainings dada saved under {}.zip".format(file_path))


#def download_resize_image(url,filename):
#  headers = {"User-Agent": "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.3"}
#  try:
#    req = urllib.request.Request(url=url, headers=headers) 
#    resp = urllib.request.urlopen(req)
#  except urllib.error.HTTPError as e:
#    logger.debug("Download of file {} forbidden. skip.".format(filename,e))
#    return
#  except e:
#    logger.debug("Download file {} failed. gneral errror {}".format(filename,e))
#    return
#  try:
#    img_array = np.asarray(bytearray(resp.read()), dtype="uint8")
#    img = cv2.imdecode(img_array, cv2.IMREAD_COLOR)
#    img_resize = cv2.resize(img, dsize=(150, 150), interpolation=cv2.INTER_CUBIC)
#    cv2.imwrite(filename, img_resize)
#    logger.debug(url  + " downloaded " + filename)
#  except e:
#    logger.debug("cv2 error {} ".format(e))
#    #logger.debug("{} not a proper image or not resizable".format(filename))
#    return

async def download_thread(queue,class_name,file_path,worker_name,session):
  while not queue.empty():
        msg = await queue.get()
        if msg is None:
            break
        url = msg[1]
        postid = msg[0]
        file_name = file_path + "/" + class_name + "_" + postid + ".jpg"
        try:
            async with session.get(url) as response:
                if response.status != 200:
                    response.raise_for_status()
                logger.info("url: {}, {}".format(url,worker_name))
                response_read = await response.read()
        except aiohttp.client_exceptions.ClientConnectorCertificateError as e:
            logger.info("cert error {}".format(e))
        except aiohttp.client_exceptions.ClientResponseError as e:
            logger.info("File not found {}".format(e))
        
        try:
            img_array = np.asarray(bytearray(response_read), dtype="uint8")
            img = cv2.imdecode(img_array, cv2.IMREAD_COLOR)
            img_resize = cv2.resize(img, dsize=(150, 150), interpolation=cv2.INTER_CUBIC)
            cv2.imwrite(file_name, img_resize)
        except:
            logger.info("Unexpected error: {}".format(sys.exc_info()[0]))
        queue.task_done()

    #pbar.update(1)

def download_subreddit_images(conn,subreddit,class_name,file_path):
  try:
    os.mkdir(file_path)
    logger.debug("Download directory " + file_path +  " Created ") 
  except FileExistsError:
    logger.debug("Directory " + file_path +  " already exists")

  cur = conn.cursor()
  cur.execute("SELECT postid,url FROM reddit_posts where subreddit = ?",(subreddit,))
  rows = cur.fetchall()
  logger.debug("download image: found {} images in db for {}".format(len(rows),subreddit))


  
  logger.info("Starting ThreadPoolExecutor")
  start_time = time.time()
  asyncio.run(start_download(rows,class_name,file_path))
  elapsed_time = time.time() - start_time
  #time_per_post = elapsed_time / cnt
  logger.debug("Time {} ".format(utils.pretty_time(elapsed_time)))
  #logger.debug("Time per post {} ".format(pretty_time(time_per_post)))
  #with tqdm(total=len(rows),desc="Download "+subreddit) as pbar:
  #  for row in rows:
  #    url = row[1]
  #    postid = row[0]
  #    file_name = file_path + "/" + class_name + "_" + postid + ".jpg"
  #    #todo: thread and fix errors during download
  #    download_resize_image(url,file_name)
  #    logger.debug(postid + " downloaded ")
  #    pbar.update(1)

async def start_download(rows,class_name,file_path): 
    concurrent_threads = 10
    list_queue = asyncio.Queue()
    cnt = 0
    for row in rows:
        cnt += 1
        #a = list(row) + [class_name] + [file_path]
        list_queue.put_nowait(row)
    logger.info("Queue for {} set up with {} Entries".format(class_name,cnt))

    async with aiohttp.ClientSession() as session:
            tasks = []
            for i in range(concurrent_threads):
                worker_name = "worker " + str(i) 
                task = asyncio.create_task(download_thread(list_queue,class_name,file_path,worker_name,session))
                tasks.append(task)
            await asyncio.gather(*tasks)