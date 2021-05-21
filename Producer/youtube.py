from apiclient.discovery import build
import json

file = open("../ApiCredentials.json",)
api_keys = json.load(file)

youtube = build('youtube', 'v3', developerKey=api_keys["youtube_key"])
type(youtube)
req = youtube.search().list(part='snippet', q='news', type='video',maxResults=50)
res = req.execute()
count = 0
for item in res['items']:
    print(item['snippet'])
    print("\n")
