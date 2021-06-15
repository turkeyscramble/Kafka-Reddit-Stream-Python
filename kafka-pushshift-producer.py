from pushshift_py import PushshiftAPI
from datetime import datetime
#Measure our most time expensive operation, the comment generator. time allows us to convert timestamp to UTC
from time import sleep, time
import logging
#kafka sends our data to my other program for further analytics
from kafka import KafkaProducer
#To jsonify our python object
from json import dumps
#Each subreddit data collect will execute on a separate cpu core. If more subreddits than cores, the next most available core will take the process
from multiprocessing import Process
#Each subreddit's comments data collect will execute on different threads
import threading
#convert to dataframe for writing fields to file for testin
import pandas as pd

#Fields: https://api.pushshift.io/reddit/search/comment/?subreddit=MachineLearning&size=2
#Example of using a get to request json comment_data = api._get(f"https://api.pushshift.io/reddit/comment/search?ids={id}")

api = PushshiftAPI()

#Logging for Debug
format = "%(asctime)s: %(message)s"
logging.basicConfig(format=format, level=logging.INFO, datefmt="%H:%M:%S")
#TODO: CONFIGS AND PARAMETERS
#SubReddits which are names of the our topics
subreddits = ['AskReddit', 'MachineLearning', 'WallStreetBets']
#query for submissions and comments
submission_query=['']
comment_query=['']
#aggregate data
submission_agg_data=['']
comment_agg_data=['']
#number of comments lower bound. A submission counts as a comment, and a bot could have commented. We want to return all with more than 2 comments to start
comment_minimum = ['>2']
#number of submissions to look through
submission_limit = 100000
#number of comments to look through
comment_limit = 100
#When this is ascending, can we guarantee generator waits for new comments before ending?
sort=['asc']
#seconds to wait before retrieving submission comments
submission_batch_time = 0
#Time of when we want to start streaming from. Use as a limit for sort="desc" or a start for sort="asc"
start_epoch = int(datetime(2021, 2, 28).timestamp())

#Make submission_author='AutoModerator' and comment_author='!AutoModerator to get all comments of people who replied to an automoderator's post
submission_author = ['!AutoModerator']
comment_author = ['!AutoModerator']

#Fields we want to use from submissions and comments
submission_filter = ['subreddit', 'id', 'num_comments', 'created_utc', 'author', 'title', 'body']
comment_filter = ['created_utc','author', 'id', 'score', 'body']

# noinspection PyStatementEffect
def get_comment_forest(subreddit):
    submission_gen = api.search_submissions(q=submission_query, aggs=submission_agg_data, after=start_epoch, subreddit=subreddit, sort=sort, size=submission_limit, filter=submission_filter, author=submission_author, num_comments=comment_minimum)

    #The lambda just means it will expect a value x, and that value will be json encoded
    producer = KafkaProducer(bootstrap_servers=['127.0.1.1:9092'], value_serializer=lambda x: dumps(x).encode('utf-8'))

    #TODO: Why is this repeatedly printing the last submission when descending? And why does this not keep streaming while ascending, it stops? EDIT: PROBLEM COULD BE CAUSED BY MULTIPLE THREADS
    for submission in submission_gen:
        #link_id=submission.id is the same as using `comment_ids = _get_submission_comment_ids(submission.id)`
        submission_comments = api.search_comments(q=comment_query, aggs=comment_agg_data, link_id=submission.id, size=comment_limit, sort=sort, filter=comment_filter, author=comment_author)

        #MWhy so much time? Can I improve by changing rate limit?
        s = time()
        thread = threading.Thread(target=threaded_comments(submission, submission_comments, producer))
        thread.start()
        thread.join()
        e = time()
        #print for loop of append
        print(f"Accumulated {submission.num_comments} comments from: {submission.subreddit} {submission.title} Timelapse (if no data, no query match):")
        print(e - s)
        print("\n")

        sleep(submission_batch_time)

def threaded_comments(submission, submission_comments, producer):
    # If I thread this, would that make it faster. Nope
    for comment in submission_comments:
        submission_timestamp = datetime.fromtimestamp(submission.created_utc).strftime("%y-%m-%d, %H:%M:%S")
        comment_timestamp = datetime.fromtimestamp(comment.created_utc).strftime("%y-%m-%d, %H:%M:%S")
        # Time issue is not here

        data = (submission.subreddit, submission.id, submission_timestamp, comment_timestamp,
                submission.author, submission.title, submission.num_comments,
                comment.author, comment.id, comment.score, comment.body)
        print(data)
        # What is dumps(data).encode('utf-8') doing exactly?
        # Is it better to send these individually, or add them to a dataframe, then send the dataframe?
        producer.send(submission.subreddit, value=data)
        write_to_file(data)



def write_to_file(data):
    df = pd.DataFrame(data)
    with open("querydata.txt", "a") as out_file:
        out_file.write(" ".join(map(str,df))+"\n")

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

if __name__ == '__main__':
    #When we build our consumer, this will refresh
    multithread()
