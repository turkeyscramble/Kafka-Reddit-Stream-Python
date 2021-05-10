from pushshift_py import PushshiftAPI
import time
import datetime as dt
import pandas as pd
from time import sleep, time
import threading
import logging
from kafka import KafkaProducer
from flask import Flask, Response, request
from json import dumps, loads
from pprint import pprint

#Fields: https://api.pushshift.io/reddit/search/comment/?subreddit=MachineLearning&size=2
#Example of using a get to request json comment_data = api._get(f"https://api.pushshift.io/reddit/comment/search?ids={id}")

# instantiate an app object from Flask. Takes in the name of the script file ie producer.py
#constants
app = Flask(__name__)
api = PushshiftAPI()

#Logging for Debug
format = "%(asctime)s: %(message)s"
logging.basicConfig(format=format, level=logging.INFO, datefmt="%H:%M:%S")

#CONFIGS
start_epoch = int(dt.datetime(2021, 2, 28).timestamp())
subreddits = ['MachineLearning']
#number of submissions to look through
submission_limit = 100000
#number of comments to look through
comment_limit = 1000
#seconds to wait before retrieving data
batch_time = 0

#Make  submission_author='AutoModerator' and comment_author='!AutoModerator to get all comments of people who replied to an automoderator's post
submission_author = ['!AutoModerator']
comment_author = ['!AutoModerator']

submission_filter = []

comment_filter = []
#most recent to oldest, or oldest to most recent (stream new?)
sort="desc"

def get_comment_forest(subreddit):
    submission_gen = api.search_submissions(after=start_epoch, subreddit=subreddit, sort=sort, limit=submission_limit, filter=['subreddit', 'id', 'num_comments', 'created_utc', 'author', 'title'], author=submission_author)

    #TODO: Why is this repeatedly printing the last submission when descending? And why does this not keep streaming while ascending, it stops? EDIT: PROBLEM COULD BE CAUSED BY MULTIPLE THREADS
    for submission in submission_gen:
        #A submission counts as a comment, and a bot could have commented. We want to return all with more than 2 comments
        if submission.num_comments > 2:
            #link_id=submission.id is the same as using `comment_ids = _get_submission_comment_ids(submission.id)`
            submission_comments = api.search_comments(link_id=submission.id, limit=comment_limit, sort=sort, filter=['created_utc','author', 'id', 'score', 'body'], author=comment_author)
            data = []
            #This is fixed with above if "submissions.num_comments > 2.
            s = time()
            #setup kafka
            producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

            for comment in submission_comments:
                print(submission.subreddit, submission.id, dt.datetime.fromtimestamp(submission.created_utc), dt.datetime.fromtimestamp(comment.created_utc), submission.author, submission.title, submission.num_comments, comment.author, comment.id, comment.score, comment.body)
                producer.send(subreddit, submission.subreddit+" "+submission.id)

            e = time()
            #print for loop of append
            print("For Loop Execution Time:")
            print(e - s)

            #TODO: kafka config
            #producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
            #for row in df:
            #    producer.send(subreddit,row)
            #    producer.flush()
            #    print("Hello")
            #print("Done")



            #print dataframe results
            #df = pd.DataFrame(data, columns=['submission_subreddit', 'submission_id', 'sub_created_utc', 'comment_created_utc', 'submission_author', 'submission_title', 'num_comments', 'comment_author', 'comment_id', 'comment_Score', 'comment'])
            #df['comment_created_utc'] = pd.to_datetime(df['comment_created_utc'], unit = 's')
            #df['sub_created_utc'] = pd.to_datetime(df['sub_created_utc'], unit='s')
            #print(df.to_string(index=False, justify='left'))


def get_comments(subreddit):
    #asc makes sure we get a constant stream of new comments
    comment_gen = api.search_comments(after=start_epoch, subreddit=subreddit,sort=sort, limit=comment_limit)
    data = []
    for comment in comment_gen:
        data.append([comment.created_utc, comment.subreddit, comment.author, comment.score, comment.body])
    df = pd.DataFrame(data,columns=['created_utc','subreddit','author', 'score', 'body'])
    # UTC is 4 hours behind Pacific Time!
    df['created_utc'] = pd.to_datetime(df['created_utc'], unit = 's')
    print(df.to_string(index=False, justify='left'))
    print(len(df))
    sleep(batch_time)

#Is this truly multithreaded?
@app.route('/', methods=['POST', 'GET'])
def multithread():
    threads = []
    for subreddit in subreddits:
        #args=("target-argument",) otherwise the get_Data function executes before the thread starts..
        threads.append(threading.Thread(target=get_comment_forest, args=(subreddit,)))
    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    print()
    logging.info("Main    : all done")
    # Need to specifcy explicit response for Flask
    return 'OK', 200

if __name__ == '__main__':
    #When we build our consumer, this will refresh
    app.run(port=5000, threaded=True, debug=True)