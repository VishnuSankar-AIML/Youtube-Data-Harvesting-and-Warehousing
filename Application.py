import streamlit as st
from streamlit_option_menu import option_menu
from PIL import Image
from googleapiclient.discovery import build
from googleapiclient import errors
import pymongo
import mysql.connector as sq
import pandas as pd
import re
import plotly.express as pl

api_key = 'Your API Key'  # API Key need to be provided here inside quotes

# service object created to interact with YouTube API
youtube = build('youtube',
                'v3',
                developerKey=api_key)

# MongoClient object created to interact with a MongoDB database.
myclient = pymongo.MongoClient("connection string")  # Add the connection string available in MongoDB Atlas
db = myclient['Capstone_Project-1']

# Connection object created to interact with MySQL Database
sql_connect = sq.connect(user="root",
                         password="<password>",  # Add the password of MySQL Workbench
                         database="project1_youtube")

# cursor object created to execute SQL queries and fetch results from MySQL Database
cursor_object = sql_connect.cursor()

# Setting up Streamlit Page and adding name to browser tab
icon = Image.open("Youtube Logo.png")
st.set_page_config(
    page_title=" YouTube Data Harvesting and Warehousing",
    page_icon=icon,

    layout="wide",
    initial_sidebar_state="auto",
    menu_items={
        'About': '''This streamlit application was developed by J.Vishnu Sankar.
        Contact e-mail:
        vishnusankaraiml@gmail.com'''
    }
)

# Setting up sidebar in streamlit page with required options
with st.sidebar:
    selected = option_menu("Main Menu",
                           ['Application Details', "View Details & Upload to MongoDB", "Migrate to MySQL Database",
                            "Analysis using SQL", "Data Visualization"],
                           icons=['app', 'cloud-upload', "database", "filetype-sql", "bar-chart-line"],
                           menu_icon="menu-up",
                           orientation="vertical")

# Setting up the option 'Application Details' in streamlit page
if selected == 'Application Details':
    st.title(":red[You]Tube:rainbow[Data Harvesting & Warehousing]")
    st.markdown(
        '''This **application** extracts data of youtube channels via youtube API, stores extracted data in
         MongoDB Atlas,Fetches data from MongoDB Atlas and stores in MySQL database in structured format.''')
    st.markdown('The data stored in MySQL database is used for **Analysis** and **Visualization** of various results.')
    st.markdown('**View Details & Upload to MongoDB** :')
    st.text('''Provide channel ID in input field.Clicking the 'View Details' button will display overview of  
youtube channel.
Clicking 'Upload to MongoDB' will extract channel overview,Videos,Playlists,Comments with 
Replies and store in MongoDB Atlas.''')
    st.markdown('**Migrate to MySQL Database** :')
    st.text('''The channel names of those youtube channels stored in MongoDB Atlas are available in dropdown.
Clicking  'Migrate' button will fetch data of selected youtube channel from MongoDB Atlas and 
stores in MySQL database.''')
    st.markdown('**Analysis using SQL** :')
    st.text('Analysis results are displayed using MySQL queries for 10 questions selected from dropdown.')
    st.markdown('**Data Visualization**')
    st.text('Statistical analysis of youtube channels is visually represented in Charts.')


# Function to retrieve channel overview details of a channel from YouTube
def get_channel_data(youtube, channel_id):
    channel_details = {}
    request = youtube.channels().list(id=channel_id,
                                      part='snippet,statistics')
    response = request.execute()
    for i in response['items']:
        channel_details = {
            'Thumbnail': i['snippet']['thumbnails']['default']['url'],
            'Channel id': i['id'],
            'Channel Name': i['snippet']['title'],
            'Description': i['snippet']['description'],
            'Subscriber_Count': i['statistics']['subscriberCount'],
            'Total_Views': i['statistics']['viewCount'],
            'Created_Date': i['snippet']['publishedAt'][:10],
            'Created_Time': i['snippet']['publishedAt'][11:19],
            'Total_Videos': i['statistics']['videoCount']

        }
    return channel_details


# Function to retrieve playlist details of a channel from YouTube
def get_playlist_ids(youtube, channel_id):
    playlistid_details = []
    request = youtube.playlists().list(channelId=channel_id,
                                       part='snippet,contentDetails',
                                       maxResults=50)
    response = request.execute()
    for i in response['items']:
        playlist_detail = {
            'Playlist id': i['id'],
            'Title': i['snippet']['title'],
            'Description': i['snippet']['description'],
            'Video Count': i['contentDetails']['itemCount'],
            'Created Date': i['snippet']['publishedAt'][:10],
            'Created Time': i['snippet']['publishedAt'][11:19],
            'Channel id': i['snippet']['channelId']

        }
        playlistid_details.append(playlist_detail)
    next_page_token = response.get('nextPageToken')

    while next_page_token is not None:
        request = youtube.playlists().list(channelId=channel_id, part='snippet,contentDetails',
                                           maxResults=50, pageToken=next_page_token)
        response = request.execute()
        for i in response['items']:
            playlist_detail = {
                'Playlist id': i['id'],
                'Title': i['snippet']['title'],
                'Description': i['snippet']['description'],
                'Video Count': i['contentDetails']['itemCount'],
                'Created Date': i['snippet']['publishedAt'][:10],
                'Created Time': i['snippet']['publishedAt'][11:19],
                'Channel id': i['snippet']['channelID']

            }
            playlistid_details.append(playlist_detail)
        next_page_token = response.get('nextPageToken')
        if next_page_token is None:
            break
    return playlistid_details


# Function to retrieve Video IDs of a YouTube channel from YouTube
def get_video_id(youtube, channel_id):
    request = youtube.channels().list(id=channel_id, part='contentDetails')
    response = request.execute()

    upload_playlistid = response['items'][0]['contentDetails']['relatedPlaylists'][
        'uploads']  # Get playslist ID of uploaded videos
    next_page_token = None
    video_ids = []
    while True:
        try:
            request = youtube.playlistItems().list(playlistId=upload_playlistid, part='contentDetails',
                                                   maxResults=50, pageToken=next_page_token)
            response = request.execute()
            for i in response['items']:
                videoid = i['contentDetails']['videoId']
                video_ids.append(videoid)
            next_page_token = response.get('nextPageToken')
            if next_page_token is None:
                break
        except 'videoNotFound':
            continue
    return video_ids


# Function to convert time duration from ISO 8601 format to HH:MM:SS format
def duration_convert(x=None):
    pattern = r'PT(\d+H)?(\d+M)?(\d+S)?'
    find = re.match(pattern, x)
    if find:
        hours, minutes, seconds = find.groups()
        hours = int(hours[:-1]) if hours else 0
        minutes = int(minutes[:-1]) if minutes else 0
        seconds = int(seconds[:-1]) if seconds else 0
        return "{:02d}:{:02d}:{:02d}".format(hours, minutes, seconds)
    else:
        return "{:02d}:{:02d}:{:02d}".format(0, 0, 0)


# Function to retrieve video details of all video IDS from YouTube
def video_details(youtube, video_ids=None):
    list_video_details = []
    for video in video_ids:
        request = youtube.videos().list(part='snippet,statistics,contentDetails',
                                        id=video)
        response = request.execute()

        for i in response['items']:
            try:

                details = {
                    'VideoID': i['id'],
                    'title': i['snippet']['title'],
                    'Upload Date': i['snippet']['publishedAt'][:10],
                    'Upload Time': i['snippet']['publishedAt'][11:19],
                    'Description': i['snippet']['description'],
                    'Duration': duration_convert(i['contentDetails']['duration']),
                    'Definition': i['contentDetails']['definition'],
                    'Caption': i['contentDetails']['caption'],
                    'View Count': i['statistics']['viewCount'],
                    'Likes': i['statistics']['likeCount'],
                    'Comments Count': i['statistics']['commentCount'],
                    'Channel id': i['snippet']['channelId']

                }
                list_video_details.append(details)
            except KeyError:
                continue

    return list_video_details


# Function to retrieve comments and replies for all video IDs from YouTube
def get_comment(youtube, video_id=None):
    list_comment_details = []
    for video in video_id:
        next_page_token = None
        while True:
            try:
                request = youtube.commentThreads().list(videoId=video, part='snippet,replies',
                                                        maxResults=100, pageToken=next_page_token)
                response = request.execute()
                for i in response['items']:
                    det = {
                        'videoId': i['snippet']['videoId'],
                        'comment id': i['id'],
                        'comment': i['snippet']['topLevelComment']['snippet']['textDisplay'],
                        'Comment Date': i['snippet']['topLevelComment']['snippet']['publishedAt'][:10],
                        'Comment Time': i['snippet']['topLevelComment']['snippet']['publishedAt'][11:19],
                        'author': i['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        'Channel id': i['snippet']['channelId']
                    }
                    list_comment_details.append(det)

                    if 'replies' in i.keys():
                        for reply in i['replies']['comments']:
                            det = {
                                'comment id': reply['snippet']['parentId'],
                                'Replies': reply['snippet']['textDisplay'],
                                'Reply Date': reply['snippet']['publishedAt'][:10],
                                'Reply Time': reply['snippet']['publishedAt'][11:19],
                                'Reply Author': reply['snippet']['authorDisplayName'],
                                'ChannelId': reply['snippet']['channelId']
                            }
                            list_comment_details.append(det)

                next_page_token = response.get('nextPageToken')
                if next_page_token is None:
                    break
            except errors.HttpError:
                det = {
                    'videoId': video,
                    'comment id': 'Null',
                    'comment': 'Comments disabled',
                    'Comment Date': 00 - 00 - 00,
                    'Comment Time': 'NULL',
                    'author': 'Null',
                    'Channel id': i['snippet']['channelId']
                }
                list_comment_details.append(det)
                break

    return list_comment_details


# Setting up option 'View Details & Upload to MongoDB' in streamlit page
if selected == 'View Details & Upload to MongoDB':

    st.markdown("**Enter the channel ID of youtube channel in below box**")
    channel_id = st.text_input("channel ID")

    if st.button("View details"):

        with st.spinner('Extraction in progress...'):

            try:

                extracted_details = get_channel_data(youtube, channel_id=channel_id)
                st.write('**:green[Channel Thumbnail]** :')
                st.image(extracted_details['Thumbnail'])
                st.write('**:green[Channel Name]** :', extracted_details['Channel Name'])
                st.write('**:green[Description]** :', extracted_details['Description'])
                st.write('**:green[Total_Videos]** :', extracted_details['Total_Videos'])
                st.write('**:green[Subscriber Count]** :', extracted_details['Subscriber_Count'])
                st.write('**:green[Total Views]** :', extracted_details['Total_Views'])
                st.write('**:green[Created Date]** :', extracted_details['Created_Date'])
                st.write('**:green[Created Time]** :', extracted_details['Created_Time'])

            except KeyError:

                st.error("Invalid channelID.Enter valid input ID")

    count = 0  # Initializing variable count to use as index for creation and dropping of collection
    d = []  # list to store documents retrieved with channel id
    channelID_list = []  # list to store channel IDs available in MongoDB Atlas

    if st.button("Upload to MongoDB"):

        try:

            col = db['channel']
            names = db.list_collection_names()

            if len(names) == 0:

                with st.spinner('Upload in progress...'):

                    channel_details = get_channel_data(youtube, channel_id=channel_id)
                    playlist_details = get_playlist_ids(youtube, channel_id=channel_id)
                    videos_detail = video_details(youtube, video_ids=get_video_id(youtube, channel_id))
                    comment_details = get_comment(youtube, video_id=get_video_id(youtube, channel_id))
                    col[count].insert_one(channel_details)
                    col[count].insert_many(playlist_details)
                    col[count].insert_many(videos_detail)
                    col[count].insert_many(comment_details)
                    col[count].rename(channel_details['Channel Name'])
                st.success('Channel Details,Playlists,Videos,Comments,Replies uploaded successfully.')

            else:

                for i in range(len(names)):

                    col = db[names[i]]
                    x = col.find({}, {'_id': 0, 'Channel id': 1}).limit(1)
                    for y in x:
                        d.append(y)

                for document in d:
                    channelID_list.append(*list(document.values()))

                if channel_id not in channelID_list:

                    with st.spinner('Upload in progress...'):

                        count += 1
                        channel_details = get_channel_data(youtube, channel_id=channel_id)
                        playlist_details = get_playlist_ids(youtube, channel_id=channel_id)
                        videos_detail = video_details(youtube, video_ids=get_video_id(youtube, channel_id))
                        comment_details = get_comment(youtube, video_id=get_video_id(youtube, channel_id))
                        col[count].insert_one(channel_details)
                        col[count].insert_many(playlist_details)
                        col[count].insert_many(videos_detail)
                        col[count].insert_many(comment_details)
                        col[count].rename(channel_details['Channel Name'])
                    st.success('Channel Details,Playlists,Videos,Comments,Replies uploaded successfully.')

                else:

                    st.info("Channel details already available in MongoDB.Re-uploading again")
                    with st.spinner('ReUpload in progress...'):

                        channel_details = get_channel_data(youtube, channel_id=channel_id)
                        db[channel_details['Channel Name']].drop()
                        videos_detail = video_details(youtube, video_ids=get_video_id(youtube, channel_id))
                        playlist_details = get_playlist_ids(youtube, channel_id=channel_id)
                        comment_details = get_comment(youtube, video_id=get_video_id(youtube, channel_id))
                        col[count].insert_one(channel_details)
                        col[count].insert_many(playlist_details)
                        col[count].insert_many(videos_detail)
                        col[count].insert_many(comment_details)
                        col[count].rename(channel_details['Channel Name'])

                    st.success('Channel Details,Playlists,Videos,Comments,Replies reuploaded successfully.')

        except KeyError:

            st.error("Invalid channelID.Enter valid input ID")


# Function to create required tables in MySQL Database
def create_tables_sql():
    cursor_object.execute('''CREATE TABLE IF NOT EXISTS Channel_Details(Thumbnail LONGTEXT,Channel_id VARCHAR(30),
                             Channel_Name MEDIUMTEXT, Description LONGTEXT,Subscriber_Count BIGINT,Total_Views BIGINT,
                             Created_Date DATE,Created_Time TIME,Total_Videos BIGINT)''')

    cursor_object.execute('''CREATE TABLE IF NOT EXISTS Playlist_Ids(Playlist_id VARCHAR(40),Title MEDIUMTEXT,
                             Description LONGTEXT, Video_count BIGINT,Created_Date DATE,Created_Time TIME,
                             Channel_id VARCHAR(30))''')

    cursor_object.execute('''CREATE TABLE IF NOT EXISTS Videos(VideoID VARCHAR(20),title MEDIUMTEXT,Upload_Date DATE, 
                             Upload_Time TIME,Description LONGTEXT,Duration VARCHAR(14),Definition VARCHAR(6),
                             Caption VARCHAR(5),View_Count BIGINT,Likes BIGINT,Comments_Count BIGINT,
                             Channel_id VARCHAR(30))''')

    cursor_object.execute('''CREATE TABLE IF NOT EXISTS Comments(VideoID VARCHAR(20),Comment_id VARCHAR(40),
                             comment LONGTEXT,Comment_Date DATE,Comment_Time TIME,author LONGTEXT,
                             Channel_id VARCHAR(30))''')

    cursor_object.execute('''CREATE TABLE IF NOT EXISTS Replies(Comment_id VARCHAR(40),Replies LONGTEXT,
                             Reply_Date DATE,Reply_Time TIME,Reply_author LONGTEXT,Channel_id VARCHAR(30))''')


# Function to fetch channel overview details of a channel from MongoDB Atlas and store in table 'Channel_Details' in MySQL Database
def table_channel_sql(y='None'):
    col = db[y]
    x = col.find({"Channel Name": {"$exists": True}}, {'_id': 0})
    for i in x:
        tuple_values = tuple(i.values())
        qry_stmt = '''INSERT INTO Channel_Details VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cursor_object.execute(qry_stmt, tuple_values)
        sql_connect.commit()


# Function to fetch playlist details of a channel from MongoDB Atlas and store in table 'Playlist_Ids' in MySQL Database
def table_playlist_sql(y='None'):
    col = db[y]
    x = col.find({"Playlist id": {"$exists": True}}, {'_id': 0})

    for i in x:
        tuple_values = tuple(i.values())
        qry_stmt = '''INSERT INTO Playlist_Ids VALUES(%s,%s,%s,%s,%s,%s,%s)'''
        cursor_object.execute(qry_stmt, tuple_values)
        sql_connect.commit()


# Function to fetch video details of a channel from MongoDB Atlas and store in table 'Videos' in MySQL Database
def table_video_sql(y=None):
    col = db[y]
    x = col.find({"Upload Date": {"$exists": True}}, {'_id': 0})
    for i in x:
        tuple_values = tuple(i.values())
        qry_stmt = '''INSERT INTO Videos VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cursor_object.execute(qry_stmt, tuple_values)
        sql_connect.commit()


# Function to fetch comment details of a channel from MongoDB Atlas and store in table 'Comments' in MySQL Database
def table_comment_sql(y=None):
    cursor_object.execute('''SET SESSION sql_mode='' ''')
    col = db[y]
    x = col.find({"author": {"$exists": True}}, {'_id': 0})
    for i in x:
        tuple_values = tuple(i.values())
        qry_stmt = '''INSERT INTO Comments VALUES(%s,%s,%s,%s,%s,%s,%s)'''
        cursor_object.execute(qry_stmt, tuple_values)
        sql_connect.commit()
    cursor_object.execute(
        ''' SET SESSION sql_mode='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' ''')


# Function to fetch reply details of all videosa of channel from MongoDB Atlas and store in table 'Replies' in MySQL Database
def table_replies_sql(y=None):
    col = db[y]
    x = col.find({"Replies": {"$exists": True}}, {'_id': 0})
    for i in x:
        tuple_values = tuple(i.values())
        qry_stmt = '''INSERT INTO Replies VALUES(%s,%s,%s,%s,%s,%s)'''
        cursor_object.execute(qry_stmt, tuple_values)
        sql_connect.commit()


# Function to fetch channelID of channel in MySQL Database
def fetch_channel_id(y=None):
    c = []
    c.append(y)
    query_stmt = ''' SELECT Channel_id FROM channel_details WHERE Channel_Name = %s '''
    cursor_object.execute(query_stmt, c)
    fetched_channel_id = cursor_object.fetchall()
    list_of_fetched_channelid = []
    for i in fetched_channel_id:
        list_of_fetched_channelid.append(*(list(i)))

    return delete_rows_sql(list_of_fetched_channelid)


# Function to delete details of a channel in all tables using channelID in MySQL Database
def delete_rows_sql(x=None):
    y = tuple(x)
    query_stmt = '''DELETE FROM channel_details WHERE Channel_id = %s '''
    cursor_object.execute(query_stmt, x)
    sql_connect.commit()
    query_stmt = '''DELETE FROM playlist_ids WHERE Channel_id = %s '''
    cursor_object.execute(query_stmt, x)
    sql_connect.commit()
    query_stmt = '''DELETE FROM videos WHERE Channel_id = %s '''
    cursor_object.execute(query_stmt, x)
    sql_connect.commit()
    query_stmt = '''DELETE FROM comments WHERE Channel_id = %s '''
    cursor_object.execute(query_stmt, x)
    sql_connect.commit()
    query_stmt = '''DELETE FROM replies WHERE Channel_id = %s '''
    cursor_object.execute(query_stmt, x)
    sql_connect.commit()


# Function to fetch channel names of channels available in MySQL Database
def display_channel_name():
    cursor_object.execute('''SELECT Channel_Name FROM channel_details''')
    fetched_channelnames = cursor_object.fetchall()
    list_fetched_channelnames = []
    for i in fetched_channelnames:
        list_fetched_channelnames.append(*(list(i)))

    return list_fetched_channelnames


# Setting up option 'Migrate to MySQL Database' in streamlit page
if selected == 'Migrate to MySQL Database':

    create_tables_sql()
    names = db.list_collection_names()
    selected_channel = st.selectbox('Select a channel', options=names)
    x = display_channel_name()

    if st.button('Migrate'):

        if selected_channel in x:

            with st.spinner('Re-Migration in progress...'):

                fetch_channel_id(selected_channel)
                table_channel_sql(selected_channel)
                table_playlist_sql(selected_channel)
                table_video_sql(selected_channel)
                table_comment_sql(selected_channel)
                table_replies_sql(selected_channel)

            st.success('Channel Details,Playlists,Videos,Comments,Replies remigrated successfully to MySQL')
            x = display_channel_name()

        else:

            with st.spinner('''Migration in progress...'''):

                table_channel_sql(selected_channel)
                table_playlist_sql(selected_channel)
                table_video_sql(selected_channel)
                table_comment_sql(selected_channel)
                table_replies_sql(selected_channel)

            st.success('Channel Details,Playlists,Videos,Comments,Replies migrated successfully to MySQL')
            x = display_channel_name()

    st.write('List of channels already migrated to MySQL :', len(x))
    st.write(x)


# Function to execute Select query for 1st SQL Question
def sql_question1():
    cursor_object.execute('''SELECT v.title AS Video_Title,ch.Channel_Name 
                             FROM videos v 
                             LEFT JOIN channel_details ch 
                             ON ch.Channel_id = v.Channel_id 
                             order by Channel_Name''')

    result = cursor_object.fetchall()
    df = pd.DataFrame(result, columns=['Video Title', 'Channel Name'])
    serial_number = [i for i in range(1, len(result) + 1)]
    df['S.No'] = serial_number
    st.dataframe(df.set_index('S.No'))


# Function to execute Select query for 2nd SQL Question
def sql_question2():
    cursor_object.execute(''' SELECT Channel_Name,Total_Videos FROM channel_details 
                     order by Total_Videos DESC ''')

    result = cursor_object.fetchall()
    df = pd.DataFrame(result, columns=['Channel Name', 'Total Videos'])
    serial_number = [i for i in range(1, len(result) + 1)]
    df['S.No'] = serial_number
    return df


# Function to execute Select query for 3rd SQL Question
def sql_question3():
    cursor_object.execute(''' SELECT Videos.title AS Video_Title,format(Videos.View_Count,0) AS Total_Views,
                              channel_details.Channel_Name 
                              FROM videos 
                              LEFT JOIN channel_details 
                              ON channel_details.Channel_id = videos.Channel_id 
                              order by View_Count Desc 
                              limit 10''')

    result = cursor_object.fetchall()
    df = pd.DataFrame(result, columns=['Video Title', 'Total Views', 'Channel Name'])
    serial_number = [i for i in range(1, len(result) + 1)]
    df['S.No'] = serial_number
    return df


# Function to execute Select query for 4th SQL Question
def sql_question4():
    cursor_object.execute(''' SELECT title AS Video_Title,Comments_Count AS Total_Comments  
                             FROM videos ''')

    result = cursor_object.fetchall()
    df = pd.DataFrame(result, columns=['Video Title', 'Total Comments'])
    serial_number = [i for i in range(1, len(result) + 1)]
    df['S.No'] = serial_number
    st.dataframe(df.set_index('S.No'))


# Function to execute Select query for 5th SQL Question
def sql_question5():
    cursor_object.execute(''' SELECT v.title AS Video_Title,format(v.Likes,0) AS Likes,ch.Channel_Name  
                              FROM videos v 
                              LEFT JOIN channel_details ch 
                              ON v.channel_id=ch.channel_id 
                              WHERE Likes=
                              (SELECT MAX(Likes) FROM videos v1 WHERE v.channel_id=v1.channel_id GROUP BY channel_id) 
                              order by v.Likes Desc''')

    result = cursor_object.fetchall()
    df = pd.DataFrame(result, columns=['Video Title', 'Likes', 'Channel Name'])
    serial_number = [i for i in range(1, len(result) + 1)]
    df['S.No'] = serial_number
    st.dataframe(df.set_index('S.No'))


# Function to execute Select query for 6th SQL Question
def sql_question6():
    cursor_object.execute(''' SELECT title AS Video_Title,format(Likes,0) AS Likes FROM Videos ''')

    result = cursor_object.fetchall()
    df = pd.DataFrame(result, columns=['Video Title', 'Likes'])
    serial_number = [i for i in range(1, len(result) + 1)]
    df['S.No'] = serial_number
    st.dataframe(df.set_index('S.No'))


# Function to execute Select query for 7th SQL Question
def sql_question7():
    cursor_object.execute(''' SELECT Channel_Name,format(Total_Views,0) AS Total_Views 
                              FROM channel_details 
                              order by Total_Views desc ''')

    result = cursor_object.fetchall()
    df = pd.DataFrame(result, columns=['Channel Name', 'Total Views'])
    serial_number = [i for i in range(1, len(result) + 1)]
    df['S.No'] = serial_number
    return df


# Function to execute Select query for 8th SQL Question
def sql_question8():
    cursor_object.execute(''' SELECT DISTINCT ch.Channel_Name  
                              FROM channel_details ch 
                              JOIN videos 
                              ON videos.Channel_id=ch.Channel_id 
                              WHERE year(videos.Upload_Date) = 2022 ''')

    result = cursor_object.fetchall()
    df = pd.DataFrame(result, columns=['Channel Name'])
    serial_number = [i for i in range(1, len(result) + 1)]
    df['S.No'] = serial_number
    st.dataframe(df.set_index('S.No'))


# Function to execute Select query for 9th SQL Question
def sql_question9():
    cursor_object.execute(''' SELECT ch.channel_Name,format(AVG(format((TIME_TO_SEC(v.Duration) / 60),0)),0) 
                              AS Average_Duration_Minutes 
                              FROM Videos v 
                              Left join channel_details ch 
                              ON v.Channel_id=ch.Channel_id 
                              GROUP BY v.channel_id,ch.channel_name 
                              ORDER BY Average_Duration_Minutes ''')

    result = cursor_object.fetchall()
    df = pd.DataFrame(result, columns=['Channel Name', 'Average Duration(Mins)'])
    serial_number = [i for i in range(1, len(result) + 1)]
    df['S.No'] = serial_number
    st.dataframe(df.set_index('S.No'))


# Function to execute Select query for 10th SQL Question
def sql_question10():
    cursor_object.execute(''' SELECT v.title AS Video_Title,v.Comments_Count,ch.Channel_Name 
                              FROM videos v 
                              LEFT JOIN channel_details ch 
                              on v.channel_id=ch.channel_id 
                              WHERE Comments_Count = (SELECT max(Comments_Count) FROM Videos v1 
                                                      where v.channel_id=v1.channel_id group by Channel_id) 
                              order by v.Comments_Count Desc ''')

    result = cursor_object.fetchall()
    df = pd.DataFrame(result, columns=['Video Title', 'Comments Count', 'Channel Name'])
    serial_number = [i for i in range(1, len(result) + 1)]
    df['S.No'] = serial_number
    st.dataframe(df.set_index('S.No'))


# Setting up option 'Analysis using SQL' in streamlit page
if selected == 'Analysis using SQL':
    Questions = [
        'Select a question',
        '1. What are the names of all the videos and their corresponding channels?',
        '2. Which channels have the most number of videos, and how many videos do they have?',
        '3. What are the top 10 most viewed videos and their respective channels?',
        '''4. How many comments were made on each video, and what are their corresponding 
video names?''',
        '''5. Which videos have the highest number of likes, and what are their corresponding 
channel names?''',
        '''6. What is the total number of likes for each video, and what are  their corresponding 
video names?''',
        '''7. What is the total number of views for each channel, and what are their corresponding
channel names?''',
        '''8. What are the names of all the channels that have published videos in the year
2022?''',
        '''9. What is the average duration of all videos in each channel, and what are their corresponding 
channel names?''',
        '''10. Which videos have the highest number of comments,
and what are their corresponding channel names?'''
    ]

    Selected_Question = st.selectbox('View Results for your selected Questions', options=Questions)

    if Selected_Question == '1. What are the names of all the videos and their corresponding channels?':
        sql_question1()
    if Selected_Question == '2. Which channels have the most number of videos, and how many videos do they have?':
        st.dataframe(sql_question2().set_index('S.No'))
    if Selected_Question == '3. What are the top 10 most viewed videos and their respective channels?':
        st.dataframe(sql_question3().set_index('S.No'))
    if Selected_Question == '''4. How many comments were made on each video, and what are their corresponding 
video names?''':
        sql_question4()
    if Selected_Question == '''5. Which videos have the highest number of likes, and what are their corresponding 
channel names?''':
        sql_question5()
    if Selected_Question == '''6. What is the total number of likes for each video, and what are  their corresponding 
video names?''':
        sql_question6()
    if Selected_Question == '''7. What is the total number of views for each channel, and what are their corresponding
channel names?''':
        st.dataframe(sql_question7().set_index('S.No'))
    if Selected_Question == '''8. What are the names of all the channels that have published videos in the year
2022?''':
        sql_question8()
    if Selected_Question == '''9. What is the average duration of all videos in each channel, and what are their corresponding 
channel names?''':
        sql_question9()
    if Selected_Question == '''10. Which videos have the highest number of comments,
and what are their corresponding channel names?''':
        sql_question10()

# Setting up option 'Data Visualization' in streamlit page
if selected == "Data Visualization":
    x = [
        'Select to view analysis',
        '1.Channels with highest number of videos',
        '2.Channels with Top 10 viewed videos',
        '3.Channels with Views',
        '4.Channels with Subscriber Count',
        '5.Year wise Performance of each Channel'
    ]
    Option = st.selectbox('View statistical analysis of each channels', x)

    if Option == '1.Channels with highest number of videos':
        def plot_total_videos(df=sql_question2()):
            fig = pl.bar(df, x='Channel Name', y='Total Videos', color='Channel Name', text='Total Videos')
            st.plotly_chart(fig, use_container_width=True)


        plot_total_videos()

    if Option == '2.Channels with Top 10 viewed videos':
        def plot_highviewed_videos(df=sql_question3()):
            fig = pl.bar(df, x='Video Title', y='Total Views', color='Channel Name', text='Total Views')
            st.plotly_chart(fig, use_container_width=True)


        plot_highviewed_videos()

    if Option == '3.Channels with Views':
        def plot_highviewed_channels(df=sql_question7()):
            fig = pl.bar(df, y='Channel Name', x='Total Views', color='Channel Name', text='Total Views',
                         orientation='h')
            st.plotly_chart(fig)


        plot_highviewed_channels()

    if Option == '4.Channels with Subscriber Count':
        def plot_subscriber_channels():
            cursor_object.execute(''' SELECT Channel_Name,format(Subscriber_Count,0) 
                                         FROM channel_details 
                                         order by Subscriber_Count Desc''')

            result = cursor_object.fetchall()
            df = pd.DataFrame(result, columns=['Channel Name', 'Subscriber Count'])
            fig = pl.bar(df, y='Channel Name', x='Subscriber Count', color='Channel Name', text='Subscriber Count',
                         orientation='h')
            st.plotly_chart(fig)


        plot_subscriber_channels()

    if Option == '5.Year wise Performance of each Channel':
        def year_wise_statistics():
            cursor_object.execute('''SELECT distinct year(v.Upload_Date) AS years,count(v.VideoID) AS Total_Videos,
                                     format(Sum(v.Likes),0) AS Likes,format(SUM(v.View_Count),0) As Total_Views,
                                     ch.channel_name 
                                     FROM Videos v 
                                     LEFT JOIN channel_details ch 
                                     ON v.channel_id=ch.Channel_id 
                                     GROUP BY Channel_Name,years''')

            result = cursor_object.fetchall()

            df = pd.DataFrame(result, columns=['Years', 'Total Videos', 'Total Likes', 'Total Views', 'Channel Name'])

            fig = pl.line(df, x='Years', y='Total Videos', markers=True, color='Channel Name',
                          title='Year-wise Total Videos Uploaded')
            st.plotly_chart(fig)

            fig1 = pl.line(df, x='Years', y='Total Views', markers=True, color='Channel Name',
                           title='Total Views of year-wise Uploaded Videos')
            st.plotly_chart(fig1)

            fig2 = pl.line(df, x='Years', y='Total Likes', markers=True, color='Channel Name',
                           title='Total Likes of year-wise Uploaded Videos')
            st.plotly_chart(fig2)


        year_wise_statistics()
