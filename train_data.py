import logging
import os
import grab_utils
from os import listdir
from os.path import isfile, join
import shutil
import urllib
import concurrent.futures
from multiprocessing import Queue


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


def download_resize_image(url,filename):
  headers = {"User-Agent": "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.3"}
  try:
    req = Request(url=url, headers=headers) 
    resp = urlopen(req)
  except urllib.error.HTTPError as e:
    logger.debug("Download of file {} forbidden. skip.".format(filename,e))
    return
  except:
    logger.debug("Download file {} failed. gneral errror".format(filename))
    return
  try:
    img_array = np.asarray(bytearray(resp.read()), dtype="uint8")
    img = cv2.imdecode(img_array, cv2.IMREAD_COLOR)
    img_resize = cv2.resize(img, dsize=(150, 150), interpolation=cv2.INTER_CUBIC)
    cv2.imwrite(filename, img_resize)
  except:
    logger.debug("{} not a proper image or not resizable".format(filename))
    return

def download_thread(queue):
  while not queue.empty():
    work = queue.get()
    url = work[1]
    postid = work[0]
    class_name = work[2]
    file_path = work[3]

    file_name = file_path + "/" + class_name + "_" + postid + ".jpg"
    download_resize_image(url,file_name)
    logger.debug(postid + " downloaded ")
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

  list_queue = Queue(maxsize=0)
  cnt = 0
  for row in rows:
    cnt += 1
    a = list(row) + [class_name] + [file_path]
    list_queue.put(a)

  logger.info("Queue for {} set up with {} Entries".format(subreddit,cnt))

  logger.info("Starting ThreadPoolExecutor")
  with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
    executor.map(download_thread, list_queue)
    #for a in range(5):
  logger.info("ThreadPoolExecutor finished")
  #list_queue.close()

  #with tqdm(total=len(rows),desc="Download "+subreddit) as pbar:
  #  for row in rows:
  #    url = row[1]
  #    postid = row[0]
  #    file_name = file_path + "/" + class_name + "_" + postid + ".jpg"
  #    #todo: thread and fix errors during download
  #    download_resize_image(url,file_name)
  #    logger.debug(postid + " downloaded ")
  #    pbar.update(1)