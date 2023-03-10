import greenstalk
import csv
import json

with open('twitter-leaks-10000.csv', 'r') as f:
    reader = csv.reader(f)
    raw_users = list(reader)

users = [user[3] for user in raw_users[1:11]]
with greenstalk.Client(
    ('127.0.0.1', 11300)
) as client:
    for user in users:
        print(f"putting {user} into queue")
        client.put(json.dumps({
            'username': user
        }))