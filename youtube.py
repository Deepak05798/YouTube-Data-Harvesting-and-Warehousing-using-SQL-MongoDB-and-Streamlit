#importing the necessary libraries
#importing the necessary libraries
import pandas as pd
import plotly.express as px
import streamlit as st
from streamlit_option_menu import option_menu
import mysql.connector as sql
import pymongo
from googleapiclient.discovery import build
from PIL import Image



# Bridging a connection with MongoDB Atlas and Creating a new database(youtube_data)
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client['youtube']

# BUILDING CONNECTION WITH YOUTUBE API
api_key = "AIzaSyCM__tAGFuN8Gw0FxqusIoZtqYHLa7uLFQ"
youtube = build('youtube','v3',developerKey=api_key)

# Streamlit app title and input
st.title('YouTube Data Harvesting')
st.subheader('Channel Data')
channel_ids = st.text_input("Enter the channel ID")
st.button('get data')

# FUNCTION TO GET CHANNEL DETAILS
def get_channel_details(channel_id):
    ch_data = []
    response = youtube.channels().list(part = 'snippet,contentDetails,statistics',
                                     id= channel_id).execute()

    for i in range(len(response['items'])):
        data = dict(Channel_id = channel_id[i],
                    Channel_name = response['items'][i]['snippet']['title'],
                    Playlist_id = response['items'][i]['contentDetails']['relatedPlaylists']['uploads'],
                    Subscribers = response['items'][i]['statistics']['subscriberCount'],
                    Views = response['items'][i]['statistics']['viewCount'],
                    Total_videos = response['items'][i]['statistics']['videoCount'],
                    Description = response['items'][i]['snippet']['description'],
                    Country = response['items'][i]['snippet'].get('country')
                    )
        ch_data.append(data)
    return ch_data


# FUNCTION TO GET VIDEO IDS
def get_channel_videos(channel_id):
    video_ids = []
    # get Uploads playlist id
    res = youtube.channels().list(id=channel_id, 
                                  part='contentDetails').execute()
    playlist_id = res['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None
    
    while len(video_ids) < 10:  # Limit to 10 videos
        res = youtube.playlistItems().list(playlistId=playlist_id, 
                                           part='snippet', 
                                           maxResults=10 - len(video_ids),  # Adjust the maxResults
                                           pageToken=next_page_token).execute()
        
        for i in range(len(res['items'])):
            video_ids.append(res['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = res.get('nextPageToken')
        
        if next_page_token is None:
            break
    return video_ids

# FUNCTION TO GET VIDEO DETAILS
def get_video_details(v_ids):
    video_stats = []
    
    for i in range(0, len(v_ids), 10):
        response = youtube.videos().list(
                    part="snippet,contentDetails,statistics",
                    id=','.join(v_ids[:10])).execute()
        for video in response['items']:
            video_details = dict(Channel_name = video['snippet']['channelTitle'],
                                Channel_id = video['snippet']['channelId'],
                                Video_id = video['id'],
                                Title = video['snippet']['title'],
                                Tags = video['snippet'].get('tags'),
                                Thumbnail = video['snippet']['thumbnails']['default']['url'],
                                Description = video['snippet']['description'],
                                Published_date = video['snippet']['publishedAt'],
                                Duration = video['contentDetails']['duration'],
                                Views = video['statistics']['viewCount'],
                                Likes = video['statistics'].get('likeCount'),
                                Comments = video['statistics'].get('commentCount'),
                                Favorite_count = video['statistics']['favoriteCount'],
                                Definition = video['contentDetails']['definition'],
                                Caption_status = video['contentDetails']['caption']
                               )
            video_stats.append(video_details)
    return video_stats


# FUNCTION TO GET COMMENT DETAILS
def get_comments_details(v_id):
    comment_data = []
    try:
        next_page_token = None
        comment_count = 0
        while comment_count < 2:  # Limit to 3 comments
            response = youtube.commentThreads().list(part="snippet,replies",
                                                    videoId=v_id,
                                                    maxResults=20,
                                                    pageToken=next_page_token).execute()
            for cmt in response['items']:
                data = dict(Comment_id = cmt['id'],
                            Video_id = cmt['snippet']['videoId'],
                            Comment_text = cmt['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_author = cmt['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_posted_date = cmt['snippet']['topLevelComment']['snippet']['publishedAt'],
                            Like_count = cmt['snippet']['topLevelComment']['snippet']['likeCount'],
                            Reply_count = cmt['snippet']['totalReplyCount']
                           )
                comment_data.append(data)
                comment_count += 1
                if comment_count >=2 :
                    break
            next_page_token = response.get('nextPageToken')
            if next_page_token is None:
                break
    except:
        pass
    return comment_data

# Display data from MongoDB and SQL in Streamlit
if st.button('Fetch Data'):
    st.write ('Fetched data')
    # Fetch data from MongoDB
    channel_id = "UC_HZY9d5wJ-MEiuq6vhx8hg"  # Replace with the actual channel ID
    channel_data = get_channel_details(channel_id)
    channel_collection = db['youtube data']
    channel_collection.insert_many(channel_data)

mysql_connection = sql.connector.connect(
    host='127.0.0.1',
    user='root',
    password='12345',
    database='youtube'
)
mysql_cursor = mysql_connection.cursor()

# Retrieve data from MongoDB
mongo_data = channel_collection.find_one({})

if mongo_data:
    # Assuming the structure of the MongoDB data is as follows:
    # {
    #     'channel_details': [...],
    #     'video_details': [...],
    #     'comments_in_videos': [...]
    # }

    # Extract data
    channel_details = mongo_data.get('channel_details', [])
    video_details = mongo_data.get('video_details', [])
    comments_in_videos = mongo_data.get('comments_in_videos', [])

    # Insert channel details into MySQL
    for channel in channel_details:
        mysql_cursor.execute(
            "INSERT INTO channel_details (channel_name, description, total_views, subscribers, total_videos, playlist_id) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            (
                channel['Channel Name'],
                channel['Description'],
                channel['Total Views'],
                channel['Subscribers'],
                channel['Total Videos'],
                channel['playlist_id']
            )
        )

    # Insert video details into MySQL
    for video in video_details:
        mysql_cursor.execute(
            "INSERT INTO video_details (video_id, channel_title, channel_id, title, description, published_at, view_count, like_count, comment_count, duration, definition, caption) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (
                video['video_id'],
                video['channelTitle'],
                video['channelId'],
                video['title'],
                video['description'],
                video['publishedAt'],
                video['viewCount'],
                video['likeCount'],
                video['commentCount'],
                video['duration'],
                video['definition'],
                video['caption']
            )
        )

    # Insert comments into MySQL
    for comment in comments_in_videos:
        mysql_cursor.execute(
            "INSERT INTO video_comments (video_id, comment_id, comment, author_name, published) "
            "VALUES (%s, %s, %s, %s, %s)",
            (
                comment['video_Id'],
                comment['comments_id'],
                comment['comment'],
                comment['author_name'],
                comment['published']
            )
        )

    # Commit changes and close connections
    mysql_connection.commit()
    mysql_connection.close()
    client.close()

    print("Data migrated from MongoDB to MySQL successfully.")
else:
    print("No data found in MongoDB to migrate.")
