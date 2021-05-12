from apiclient.discovery import build

api_key = "AIzaSyCa0DW-hyOehNJI1RWRv9LkvBDH9pSJ9R4"

youtube = build('youtube', 'v3', developerKey=api_key)
type(youtube)
req = youtube.search().list(part='snippet', q='news', type='video',maxResults=100)
res = req.execute()
count = 0
for item in res['items']:
    print(item['snippet'])
    print("\n")
    count+=1

print(count)
