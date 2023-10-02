import pandas as pd
import plotly.express as px
import streamlit as st
from streamlit_option_menu import option_menu
import mysql.connector
import pymongo
from googleapiclient.discovery import build
from PIL import Image
from datetime import datetime
import logging


# SETTING PAGE CONFIGURATIONS
icon = Image.open("C:\\Users\\deepa\\OneDrive\\Desktop\\images (1).jpeg")
st.set_page_config(page_title= "Youtube Data Harvesting and Warehousing | By Deepak Harikrishnan",
                   page_icon= icon,
                   layout= "wide",
                   initial_sidebar_state= "expanded",
                   menu_items={'About': """# This app is created by *Deepak Harikrishnan!*"""})

# CREATING OPTION MENU
with st.sidebar:
    selected = option_menu(None, ["Home","Extract & Transform","View"], 
                           icons=["house-door-fill","play button","eye"],
                           default_index=0,
                           orientation="vertical",
                           styles={"nav-link": {"font-size": "30px", "text-align": "centre", "margin": "0px", 
                                                "--hover-color": "#C80101"},
                                   "icon": {"font-size": "20px"},
                                   "container" : {"max-width": "2000px"},
                                   "nav-link-selected": {"background-color": "#C80101"}})

# Bridging a connection with MongoDB Atlas and Creating a new database(youtube_data)
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client.youtube_data

# Create a connection to the MySQL database
mydb = mysql.connector.connect(
    host="127.0.0.1",
    user="root",
    password="12345",
    database="youtube"
)

# Create a cursor to execute SQL commands
mycursor = mydb.cursor()



# BUILDING CONNECTION WITH YOUTUBE API
api_key = "AIzaSyDTbZ9lfrMrk-nHsiA8cReFwz9QZxQol_g"
youtube = build('youtube','v3',developerKey=api_key)

logging.basicConfig(level=logging.INFO) 
# FUNCTION TO GET CHANNEL DETAILS
def get_channel_details(channel_id):
    ch_data = []
    response = youtube.channels().list(part='snippet,contentDetails,statistics', id=channel_id).execute()

    for i in range(len(response['items'])):
        data = dict(Channel_id=channel_id[i],
                    Channel_name=response['items'][i]['snippet']['title'],
                    Playlist_id=response['items'][i]['contentDetails']['relatedPlaylists']['uploads'],
                    Subscribers=response['items'][i]['statistics']['subscriberCount'],
                    Views=response['items'][i]['statistics']['viewCount'],
                    Total_videos=response['items'][i]['statistics']['videoCount'],
                    Description=response['items'][i]['snippet']['description'],
                    Country=response['items'][i]['snippet'].get('country')
                    )
        ch_data.append(data)
    return ch_data

# FUNCTION TO GET VIDEO IDS
def get_channel_videos(channel_id):
    video_ids = []
    # get Uploads playlist id
    res = youtube.channels().list(id=channel_id, part='contentDetails').execute()
    playlist_id = res['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None

    while True:
        res = youtube.playlistItems().list(playlistId=playlist_id, part='snippet', maxResults=50, pageToken=next_page_token).execute()

        for i in range(len(res['items'])):
            video_ids.append(res['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = res.get('nextPageToken')

        if next_page_token is None:
            break
        logging.info(f"Retrieved {len(video_ids)} video IDs. Next Page Token: {next_page_token}")

    return video_ids


# FUNCTION TO GET VIDEO DETAILS
def get_video_details(video_ids):
    video_stats = []

    for i in range(0, len(video_ids), 50):
        response = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=','.join(video_ids[i:i+50])
        ).execute()

        for video in response['items']:
            # Convert ISO 8601 date to MySQL datetime format
            published_date_iso = video['snippet']['publishedAt']
            published_date_mysql = datetime.strptime(published_date_iso, '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')

            video_details = {
                'Channel_name': video['snippet']['channelTitle'],
                'Channel_id': video['snippet']['channelId'],
                'Video_id': video['id'],
                'Title': video['snippet']['title'],
                'Thumbnail': video['snippet']['thumbnails']['default']['url'],
                'Description': video['snippet']['description'],
                'Published_date': published_date_mysql,
                'Duration': video['contentDetails']['duration'],
                'Views': video['statistics']['viewCount'],
                'Likes': video['statistics'].get('likeCount'),
                'Comments': video['statistics'].get('commentCount'),
                'Favorite_count': video['statistics']['favoriteCount'],
                'Definition': video['contentDetails']['definition'],
                'Caption_status': video['contentDetails']['caption']
            }
            video_stats.append(video_details)
            
        logging.info(f"Retrieved details for {len(video_stats)} videos. Remaining video IDs: {len(video_ids) - (i + 50)}")

    return video_stats




# FUNCTION TO GET COMMENT DETAILS
def get_comments_details(v_id):
    comment_data = []
    try:
        next_page_token = None
        comment_count = 0
        while comment_count < 5:  # Limit to 5 comments
            response = youtube.commentThreads().list(part="snippet,replies",
                                                     videoId=v_id,
                                                     maxResults=50,
                                                     pageToken=next_page_token).execute()
            for cmt in response['items']:
                data = dict(Comment_id=cmt['id'],
                            Video_id=cmt['snippet']['videoId'],
                            Comment_text=cmt['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_author=cmt['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_posted_date=cmt['snippet']['topLevelComment']['snippet']['publishedAt'],
                            Like_count=cmt['snippet']['topLevelComment']['snippet']['likeCount'],
                            Reply_count=cmt['snippet']['totalReplyCount']
                            )
                comment_data.append(data)
                comment_count += 1
                if comment_count >= 5:
                    break
            next_page_token = response.get('nextPageToken')
            if next_page_token is None or comment_count >= 5:
                break
    except Exception as e:
        print(f"An error occurred while fetching comments: {e}")
    return comment_data

from datetime import datetime

# Example input datetime string
def format_datetime_for_mysql(input_datetime_str):
    parsed_datetime = datetime.strptime(input_datetime_str, '%Y-%m-%dT%H:%M:%SZ')
    formatted_datetime_str = parsed_datetime.strftime('%Y-%m-%d %H:%M:%S')
    return formatted_datetime_str

# FUNCTION TO GET CHANNEL NAMES FROM MONGODB
def channel_names():   
    ch_name = []
    for i in db.channel_details.find():
        ch_name.append(i['Channel_name'])
    return ch_name


# HOME PAGE
if selected == "Home":
    # Title Image
    st.image("C:\\Users\\deepa\\OneDrive\\Desktop\\images.jpeg")
    col1,col2 = st.columns(2,gap= 'medium')
    col1.markdown("## :blue[Domain] : Social Media")
    col1.markdown("## :blue[Technologies used] : Python,MongoDB, Youtube Data API, MySql, Streamlit")
    col1.markdown("## :blue[Overview] : Retrieving the Youtube channels data from the Google API, storing it in a MongoDB as data lake, migrating and transforming data into a SQL database,then querying the data and displaying it in the Streamlit app.")
    col2.markdown("#   ")
    col2.markdown("#   ")
    col2.markdown("#   ")
    col2.image("C:\\Users\\deepa\\OneDrive\\Desktop\\free-youtube-logo-icon-2431-thumb.png")






# EXTRACT AND TRANSFORM PAGE
if selected == "Extract & Transform":
    tab1, tab2 = st.tabs(["$\huge  EXTRACT $", "$\huge TRANSFORM $"])
    
    # EXTRACT TAB
    with tab1:
        st.write("### Enter YouTube Channel_ID below:")
        ch_id = st.text_input("Hint: Goto channel's home page > Right click > View page source > Find channel_id").split(',')

        if ch_id and st.button("Extract Data"):
            logging.info(f"Extracting data for channel IDs: {ch_id}")
            ch_details = get_channel_details(ch_id)
            st.write(f'#### Extracted data from :green["{ch_details[0]["Channel_name"]}"] channel')
            st.table(ch_details)

        if st.button("Upload to MongoDB"):
            with st.spinner('Please Wait for it...'):
                logging.info("Uploading data to MongoDB")
                ch_details = get_channel_details(ch_id)
                v_ids = get_channel_videos(ch_id)
                vid_details = get_video_details(v_ids)

                def comments():
                    com_d = []
                    for i in v_ids:
                        com_d += get_comments_details(i)
                    return com_d
                comm_details = comments()

                collections1 = db.channel_details
                collections1.insert_many(ch_details)

                collections2 = db.video_details
                collections2.insert_many(vid_details)

                collections3 = db.comments_details
                collections3.insert_many(comm_details)
                
                logging.info("Upload to MongoDB successful")

            st.success("Upload to MongoDB successful !!")
      
    # TRANSFORM TAB
    with tab2:     
        st.markdown("#   ")
        st.markdown("### Select a channel to begin Transformation to SQL")
        
        ch_names = channel_names()
        user_inp = st.selectbox("Select channel", options=ch_names)
        
def insert_into_channel_details():
    collections = db.channel_details
    query = """INSERT INTO channel_details VALUES(%s,%s,%s,%s,%s,%s,%s,%s)"""

    for i in collections.find({"Channel_name": user_inp}, {'_id': 0}):
        mycursor.execute(query, tuple(i.values()))
        mydb.commit()

def insert_into_video_details():
            collections1 = db.video_details
            query1 = """INSERT INTO video_details (
                         Channel_name, Channel_id, Video_id, Title, Thumbnail, Description, Published_date,
                         Duration, Views, Likes, Comments, Favorite_count, Definition, Caption_status
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

            for item in collections1.find({"Channel_name": user_inp}, {"_id": 0}):
                values = (
                    item.get("Channel_name", ""),
                    item.get("Channel_id", ""),
                    item.get("Video_id", ""),
                    item.get("Title", ""),
                    item.get("Thumbnail", ""),
                    item.get("Description", ""),
                    item.get("Published_date", ""),
                    item.get("Duration", ""),
                    item.get("Views", 0),  # Default value for Views if missing
                    item.get("Likes", 0),  # Default value for Likes if missing
                    item.get("Comments", 0),  # Default value for Comments if missing
                    item.get("Favorite_count", 0),  # Default value for Favorite_count if missing
                    item.get("Definition", ""),
                    item.get("Caption_status", "")
                )

                mycursor.execute(query1, values)
                mydb.commit()

def insert_into_comments():
    collections1 = db.video_details
    collections2 = db.comments_details
    query2 = """INSERT INTO comments (
                 Comment_id, Video_id, Comment_text, Comment_author, Comment_posted_date,
                 Like_count, Reply_count
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)"""

    for vid in collections1.find({"Channel_name": user_inp}, {'_id': 0}):
        for i in collections2.find({'Video_id': vid['Video_id']}, {'_id': 0}):
            # Format the datetime field before inserting
            i['Comment_posted_date'] = format_datetime_for_mysql(i['Comment_posted_date'])
            t = tuple(i.values())
            mycursor.execute(query2, t)
            mydb.commit()

if st.button('process'):
    try:
        # Insert data into channel_details table
        insert_into_channel_details()
        st.success("Transformation of channel_details to MySQL Successful !!")
    except mysql.connector.Error as err:
        print(f"MySQL Error (channel_details): {err}")
        st.error("An error occurred while transforming channel_details to MySQL.")
    except Exception as e:
        print(f"An error occurred (channel_details): {e}")
        st.error("An error occurred while transforming channel_details to MySQL.")

    try:
        # Insert video details
        insert_into_video_details()
        st.success("Transformation of videos_details to MySQL Successful !!")
    except mysql.connector.Error as err:
        print(f"MySQL Error (video details): {err}")
        st.error(f"MySQL Error (video details): {err}")

    try:
        # Insert comment details
        insert_into_comments()
        st.success("Transformation of comments_details to MySQL Successful !!")
    except mysql.connector.Error as err:
        print(f"MySQL Error (comments_details): {err}")
        st.error(f"MySQL Error (comments_details): {err}")

# VIEW PAGE
if selected == "View":
    
    st.write("## :orange[Select any question to get Insights]")
    questions = st.selectbox('Questions',
    ['1. What are the names of all the videos and their corresponding channels?',
    '2. Which channels have the most number of videos, and how many videos do they have?',
    '3. What are the top 10 most viewed videos and their respective channels?',
    '4. How many comments were made on each video, and what are their corresponding video names?',
    '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
    '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
    '7. What is the total number of views for each channel, and what are their corresponding channel names?',
    '8. What are the names of all the channels that have published videos in the year 2022?',
    '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
    '10. Which videos have the highest number of comments, and what are their corresponding channel names?'])
    
    if questions == '1. What are the names of all the videos and their corresponding channels?':
        mycursor.execute("""SELECT title AS Video_Title, channel_name AS Channel_Name
                            FROM video_details
                            ORDER BY channel_name""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        
    elif questions == '2. Which channels have the most number of videos, and how many videos do they have?':
        mycursor.execute("""SELECT channel_name AS Channel_Name, total_videos AS Total_Videos
                            FROM channel_details
                            ORDER BY total_videos DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Number of videos in each channel :]")
        #st.bar_chart(df,x= mycursor.column_names[0],y= mycursor.column_names[1])
        fig = px.bar(df,
                     x=mycursor.column_names[0],
                     y=mycursor.column_names[1],
                     orientation='v',
                     color=mycursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
        
    elif questions == '3. What are the top 10 most viewed videos and their respective channels?':
        mycursor.execute("""SELECT channel_name AS Channel_Name, title AS Video_Title, views AS Views 
                            FROM video_details
                            ORDER BY views DESC
                            LIMIT 10""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Top 10 most viewed videos :]")
        fig = px.bar(df,
                     x=mycursor.column_names[2],
                     y=mycursor.column_names[1],
                     orientation='h',
                     color=mycursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
        
    elif questions == '4. How many comments were made on each video, and what are their corresponding video names?':
        mycursor.execute("""SELECT a.video_id AS Video_id, a.title AS Video_Title, b.Total_Comments
                            FROM video_details AS a
                            LEFT JOIN (SELECT video_id,COUNT(comment_id) AS Total_Comments
                            FROM comments GROUP BY video_id) AS b
                            ON a.video_id = b.video_id
                            ORDER BY b.Total_Comments DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
          
    elif questions == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name AS Channel_Name,title AS Title,likes AS Likes_Count 
                            FROM video_details
                            ORDER BY likes DESC
                            LIMIT 10""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Top 10 most liked videos :]")
        fig = px.bar(df,
                     x=mycursor.column_names[2],
                     y=mycursor.column_names[1],
                     orientation='h',
                     color=mycursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
        
    elif questions == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        mycursor.execute("""SELECT title AS Title, likes AS Likes_Count
                            FROM video_details
                            ORDER BY likes DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
         
    elif questions == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name AS Channel_Name, views AS Views
                            FROM channel_details
                            ORDER BY views DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Channels vs Views :]")
        fig = px.bar(df,
                     x=mycursor.column_names[0],
                     y=mycursor.column_names[1],
                     orientation='v',
                     color=mycursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
        
    elif questions == '8. What are the names of all the channels that have published videos in the year 2022?':
        mycursor.execute("""SELECT channel_name AS Channel_Name
                            FROM video_details
                            WHERE published_date LIKE '2022%'
                            GROUP BY channel_name
                            ORDER BY channel_name""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        
    elif questions == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name AS Channel_Name,
                            AVG(duration)/60 AS "Average_Video_Duration (mins)"
                            FROM video_details
                            GROUP BY channel_name
                            ORDER BY AVG(duration)/60 DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Avg video duration for channels :]")
        fig = px.bar(df,
                     x=mycursor.column_names[0],
                     y=mycursor.column_names[1],
                     orientation='v',
                     color=mycursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
        
    elif questions == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name AS Channel_Name,video_id AS Video_ID,comments AS Comments
                            FROM video_details
                            ORDER BY comments DESC
                            LIMIT 10""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Videos with most comments :]")
        fig = px.bar(df,
                     x=mycursor.column_names[1],
                     y=mycursor.column_names[2],
                     orientation='v',
                     color=mycursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)

        
        
    