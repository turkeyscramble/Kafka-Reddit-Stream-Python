from time import sleep
from json import dumps, loads
from typing import List, Any
import threading
import logging

from kafka import KafkaProducer
from flask import Flask, Response, request
import pandas as pd
import praw

# client_id: jkjKtKcZDNM42g
# secret: wG7PDFb4ba_70_ryNuWcPIl5LzEbmg
# name / user_agent: Data Stream

# instantiate an app object from Flask. Takes in the name of the script file ie producer.py
app = Flask(__name__)

#Is this truly multithreaded?
@app.route('/', methods=['POST', 'GET'])
def multithread():
    format = "%(asctime)s: %(message)s"
    logging.basicConfig(format=format, level=logging.INFO, datefmt="%H:%M:%S")

    #Apparently, you need args=("target-argument",) otherwise the get_Data function executes before the thread starts..
    streamML = threading.Thread(target=get_data, args=('MachineLearning',))
    streamRE = threading.Thread(target=get_data, args=('RenewableEnergy',))

    streamML.start()
    streamRE.start()

    streamML.join()
    streamRE.join()
    logging.info("Main    : all done")

    # Need to specifcy explicit response for Flask
    return 'OK', 200

# Why is this not infinitely running?
def get_data(subreddit_name):
    reddit = praw.Reddit(client_id='jkjKtKcZDNM42g', client_secret='wG7PDFb4ba_70_ryNuWcPIl5LzEbmg',
                         user_agent='Data Stream')


    #Streams the most recent data, but does not go back far in time.. Use pushshift
    submissions = reddit.subreddit(subreddit_name).stream.submissions()

    for submission in submissions:
        data = []
        # So we don't get "AttributeError: 'MoreComments' object has no attribute 'body'"
        submission.comments.replace_more(limit=0)
        for comment in submission.comments:
            # Otherwise an exception is thrown when creating the DataFrame
            if comment.author is None:
                comment.author = "Null"
            # Append information about the submission and comment (Submission: Non-Idempotent, Comment: Idempotent)
            data.append(
                [submission.subreddit.display_name, submission.title, submission.created, submission.num_comments,
                 comment.author.name, comment.created_utc, comment.score, comment.body])

        data = pd.DataFrame(data,
                            columns=['subreddit', 'title', 'score', 'num_comments', 'comment_author', 'created_utc',
                                     'upvotes', 'comment_body'])
        print(submission.subreddit.display_name + " " + data.comment_author + " " + data.comment_body)



if __name__ == '__main__':
    #When we build our consumer, this will refresh
    app.run(port=5000, threaded=True, debug=True)