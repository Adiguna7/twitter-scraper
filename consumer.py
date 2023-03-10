import greenstalk
import json
import datetime
import gevent

from refactored.scweet import scrape
from refactored.user import get_user_information

TWEET_INTERVAL = 7

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


def process_job():
    final_users_info = {}
    with greenstalk.Client(('127.0.0.1', 11300)) as client:
        while True:
            job = client.reserve()
            job_body = json.loads(job.body)
            user = job_body.get('username')
        
            followers_count, latest_tweet = find_followers_count_and_latest_tweet(user)
            final_users_info[user] = {
                'followers_count': followers_count,
                'latest_tweet': latest_tweet
            }
            client.delete(job)

if __name__ == '__main__':
    concurrency = 4
    try:
        greenlets = [
            gevent.spawn(process_job)
            for i in range(0, concurrency)
        ]
        print(greenlets)
        gevent.joinall(greenlets)
    except BaseException as e:
        logger.exception(str(e))