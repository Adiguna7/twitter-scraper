from gevent import monkey
monkey.patch_all()

from manager import Manager
from urllib.parse import urlparse
from time import sleep
from refactored.scweet import scrape
from refactored.user import get_user_information
from shutil import rmtree

import greenstalk
import functools
import csv
import datetime
import json
import logging
import gevent
import os

logger = logging.getLogger(__name__)

BEANSTALK_URL = 'tcp://localhost:11300'
JOB_TIMEOUT = int(datetime.timedelta(minutes=1).total_seconds())
MAX_RETRY_COUNT = 3
RETRY_DELAY_IN_SECONDS = 5
TWEET_INTERVAL = 7

manager = Manager()
username_list = []
final_users_info = {}

@manager.command
def run(concurrency=4):
    try:
        greenlets = [
            gevent.spawn(_retry(_run_pre_process))
            for i in range(0, concurrency)
        ]
        gevent.joinall(greenlets)
    except BaseException as e:
        logger.exception(str(e))

@manager.command
def seed():
    global username_list
    if not username_list:
        username_list = _get_username_list()

    url_parts = urlparse(BEANSTALK_URL)

    with greenstalk.Client(
        address=(url_parts.hostname, url_parts.port)
    ) as client:
        for index, user in enumerate(username_list):
            print(f"putting {user} into queue")
            client.put(json.dumps({
                'type': 'scrape',
                'user_number': index,
            }), ttr=JOB_TIMEOUT)
    
    print(f'===== timestamp start {datetime.datetime.now().timestamp()} =====')


def find_followers_count_and_latest_tweet(user: str):
    time_start = datetime.datetime.now()
    tweet = scrape(
        since=(datetime.datetime.now() - datetime.timedelta(days=TWEET_INTERVAL)).strftime('%Y-%m-%d'),
        from_account=user,
        until=datetime.datetime.now().strftime('%Y-%m-%d'),
        display_type="Latest",
        interval=TWEET_INTERVAL,
        save_dir='temp'
    )
    latest_tweet = tweet['Tweet URL'].iloc[0] if len(tweet) > 0 else None
    time_checkpoint = datetime.datetime.now()
    print(f"===== time taken for latest tweet: {time_checkpoint - time_start} =====")

    user_info = get_user_information([user])
    followers_count = user_info[user][1] if user_info else None
    print(f"===== time taken for followers count: {datetime.datetime.now() - time_checkpoint} =====")
    
    return followers_count, latest_tweet

def _retry(fn):
    @functools.wraps(fn)
    def wrapped():
        while True:
            try:
                fn()
            except BaseException as e:
                logger.exception(str(e))
                sleep(5000)

    return wrapped


def _run_pre_process():
    url_parts = urlparse(BEANSTALK_URL)

    with greenstalk.Client(
        address=(url_parts.hostname, url_parts.port)
    ) as client:
        while True:
            try:
                job = client.reserve()
                if client.stats_job(job).get('reserves') > MAX_RETRY_COUNT:
                    logger.warning(
                        'Job %s with body %s exceeded MAX_RETRY_COUNT (%d)',
                        job.id,
                        job.body,
                        MAX_RETRY_COUNT
                    )
                    client.bury(job)

                job_body = json.loads(job.body)
                print('job_body', job_body)
                user_number = job_body.get('user_number')
                job_type = job_body.get('type')


                if job_type == 'scrape':
                    _run_scrape(user_number)
                elif job_type == 'post_process':
                    _run_post_process()
                else:
                    logger.warning(
                        'Job %s with body %s has unknown type %s',
                        job.id,
                        job.body,
                        job_type
                    )
                    client.bury(job)

                client.delete(job)
            except Exception as e:
                logger.warning(
                    'Exception occurred when processing job %s with body %s',
                    job.id,
                    job.body
                )
                logger.exception(str(e), extra={
                    'jobId': job.id,
                    'jobBody': job.body
                })

                client.release(job, delay=RETRY_DELAY_IN_SECONDS)

def _run_scrape(user_number):
    try:
        global username_list
        if len(username_list) == 0:
            username_list = _get_username_list()

        username = username_list[user_number]
        followers_count, latest_tweet = find_followers_count_and_latest_tweet(username)
        final_users_info[username] = {
            'followers_count': followers_count,
            'latest_tweet': latest_tweet
        }

        if user_number + 1 >= len(username_list):
            url_parts = urlparse(BEANSTALK_URL)
            with greenstalk.Client(
                address=(url_parts.hostname, url_parts.port)
            ) as client:
                client.put(json.dumps({
                    'type': 'post_process'
                }), ttr=JOB_TIMEOUT)
    except BaseException as e:
        logger.exception(str(e), extra={
            'username': username,
            'userNumber': user_number
        })

def _run_post_process():
    with open('final_users_info.json', 'w') as f:
        json.dump(final_users_info, f)

    if os.path.exists('temp'):
        rmtree('temp')
    
    print(f"===== end timestamp {datetime.datetime.now().timestamp()} =====")

def _get_username_list():
    with open('twitter-leaks-10000.csv', 'r') as f:
        reader = csv.reader(f)
        raw_users = list(reader)

    return [user[3] for user in raw_users[1:52]]


if __name__ == '__main__':
    manager.main()