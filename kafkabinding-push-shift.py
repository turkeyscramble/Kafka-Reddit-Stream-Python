from pushshift_py import PushshiftAPI
from datetime import datetime
import pandas as pd
from time import sleep, time
import threading
import logging
from kafka import KafkaProducer
from json import dumps
from pprint import pprint
from multiprocessing import Process

#Fields: https://api.pushshift.io/reddit/search/comment/?subreddit=MachineLearning&size=2
#Example of using a get to request json comment_data = api._get(f"https://api.pushshift.io/reddit/comment/search?ids={id}")

api = PushshiftAPI()

#Logging for Debug
format = "%(asctime)s: %(message)s"
logging.basicConfig(format=format, level=logging.INFO, datefmt="%H:%M:%S")

#CONFIGS
#SubReddits which are names of the our topics
subreddits = ['stocks', 'MachineLearning', 'AskReddit']
#Time of when we want to start streaming from. Use as a limit for sort="desc" or a start for sort="asc"
start_epoch = int(datetime(2021, 2, 28).timestamp())
#When this is ascending, can we garuntee generator waits for new comments before ending?
sort="desc"
#number of submissions to look through
submission_limit = 10000
#number of comments to look through
comment_limit = 1000
#seconds to wait before retrieving submission comments
submission_batch_time = 0

#Make  submission_author='AutoModerator' and comment_author='!AutoModerator to get all comments of people who replied to an automoderator's post
submission_author = ['!AutoModerator']
comment_author = ['!AutoModerator']

submission_filter = ['subreddit', 'id', 'num_comments', 'created_utc', 'author', 'title']
comment_filter = ['created_utc','author', 'id', 'score', 'body']

# noinspection PyStatementEffect
def get_comment_forest(subreddit):
    submission_gen = api.search_submissions(after=start_epoch, subreddit=subreddit, sort=sort, limit=submission_limit, filter=submission_filter, author=submission_author)

    #The lambda just means it will expect a value x, and that value will be json encoded
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: dumps(x).encode('utf-8'))

    #TODO: Why is this repeatedly printing the last submission when descending? And why does this not keep streaming while ascending, it stops? EDIT: PROBLEM COULD BE CAUSED BY MULTIPLE THREADS
    for submission in submission_gen:
        #A submission counts as a comment, and a bot could have commented. We want to return all with more than 2 comments
        if submission.num_comments > 2:
            #link_id=submission.id is the same as using `comment_ids = _get_submission_comment_ids(submission.id)`
            submission_comments = api.search_comments(link_id=submission.id, limit=comment_limit, sort=sort, filter=comment_filter, author=comment_author)

            #Most expensive op, still don't know why. Takes most time. Generator/ Rate limiting?s
            s = time()
            thread = threading.Thread(target=threadedComments(submission, submission_comments, producer))
            thread.start()
            thread.join()
            e = time()
            #print for loop of append
            print(f"Accumulated Comments from: ${submission.subreddit} ${submission.title} Timelapse:")
            print(e - s)

            sleep(submission_batch_time)

def threadedComments(submission, submission_comments, producer):
    # If I thread this, would that make it faster. Nope
    for comment in submission_comments:
        submission_timestamp = datetime.fromtimestamp(submission.created_utc).strftime("%y-%m-%d, %H:%M:%S")
        comment_timestamp = datetime.fromtimestamp(comment.created_utc).strftime("%y-%m-%d, %H:%M:%S")
        # Time issue is not here
        data = (submission.subreddit, submission.id, submission_timestamp, comment_timestamp,
                submission.author, submission.title, submission.num_comments,
                comment.author, comment.id, comment.score, comment.body)
        # What is dumps(data).encode('utf-8') doing exactly?
        # Is it better to send these individually, or add them to a dataframe, then send the dataframe?
        producer.send(submission.subreddit, value=data)
        print(data)


def multithread():
    processes = []
    for subreddit in subreddits:
        #args=("target-argument",) otherwise the get_Data function executes before the thread starts..
        processes.append(Process(target=get_comment_forest, args=(subreddit,)))
    for process in processes:
        process.start()

    for process in processes:
        process.join()

    print()
    logging.info("Main    : all done")
    # Need to specifcy explicit response for Flask
    return 'OK', 200

if __name__ == '__main__':
    #When we build our consumer, this will refresh
    multithread()