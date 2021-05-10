from time import sleep
from json import dumps, loads
from kafka.producer import KafkaProducer
from flask import Flask, Response, request
import praw

#instantiate an app object from Flask. Takes in the name of the script file ie producer.py
app = Flask(__name__)

#app.route = decorator: decorates a view functon
#This makes the below function accessible from anything external to this app
#when this app is accessed at http://domainname/ a user will do a POST or GET?
#methods keyword arg takes a lst of strings as a value
@app.route('/', methods=['POST', 'GET'])
def get_data():
    config = loads(request.data.decode('utf-8'))
    data = {}

    r = praw.Reddit(client_id='jkjKtKcZDNM42g', client_secret='wG7PDFb4ba_70_ryNuWcPIl5LzEbmg',
                         user_agent='Data Stream')

    submissions = r.subreddit('GetMotivated').hot(limit=config['limit'])

    producer = KafkaProducer(bootstrap_servers = ['localhost:9092'],
                             value_serializer = lambda x : dumps(x).encode('utf-8')
    )

    del config['subreddit']
    del config['limit']

    if config['comments']:
        for submission in submissions:
            config['comments'] = True
            data[submission.id] = {}
            temp = ""
            for comment in submission.comments.list():
                try:
                    temp += comment.body
                    temp += '\n'
                except:
                    continue
            data[submission.id]['comments'] = temp

            config['comments'] = False
            for key in config:
                if config[key]:
                    if key == 'author':
                        data[submission.id][key] = vars(submission)[key].name
                    else:
                        data[submission.id][key] = vars(submission)[key]
            producer.send('redditStream', value = data[submission.id]) #redditStream -> name of created topic
            print(data[submission.id])

    else:
        for submission in submissions:
            if submission.id not in data:
                data[submission.id] = {}

            for key in config:
                if config[key]:
                    if key == 'author':
                        data[submission.id][key] = vars(submission)[key].name
                    else:
                        data[submission.id][key] = vars(submission)[key]
            producer.send('redditStream', value = data[submission.id]) #redditStream -> name of created topic
            print(data[submission.id])

    return 'OK', 200

if __name__ == '__main__':
    app.run(port=5000, debug=True)
