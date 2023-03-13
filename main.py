from refactored.scweet import scrape
from refactored.user import get_user_information
from shutil import rmtree

import datetime
import csv
import json
import os

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


with open('twitter-leaks-10000.csv', 'r') as f:
    reader = csv.reader(f)
    raw_users = list(reader)

users = [user[3] for user in raw_users[1:52]]

final_users_info = {}

for user in users:
    time_start = datetime.datetime.now()
    followers_count, latest_tweet = find_followers_count_and_latest_tweet(user)
    final_users_info[user] = {
        'followers_count': followers_count,
        'latest_tweet': latest_tweet
    }

with open('final_users_info.json', 'w') as f:
    json.dump(final_users_info, f)


if os.path.exists('temp'):
    rmtree('temp')