# Youtube-Data-Harvesting-and-Warehousing
## Introduction 

This project is about building a streamlit application used for extracting data of youtube channels via youtube API, storing extracted data in MongoDB Atlas,Fetch data from MongoDB Atlas and store in MySQL database in structured format.The data stored in MySQL database is used for Data Analysis and Visualization of various results.

## Domain 

Social Media 

## Technologies used 

Python 3.11.5 

MySQL 8.0.34 

MongoDB Atlas 

Streamlit 

Youtube API 

## Features of Application 

### Retrieve details from YouTube 

The channel ID of a youtube channel need to be provided as input. The details of channel such as channel overview,playlists,videos,comments with replies will be retrieved from YouTube using youtube API.  

By clicking ‘View details’ button in application,the overview details of channel such as Channel Thumbnail, Channel Name, Description, Total_Videos,Subscriber Count, Total Views, Created Date and time of channel are displayed. 

### Upload to MongoDB Atlas 

By clicking ‘Upload toMongoDB’ button in application, the retrieved details are stored in MongoDB Atlas which is a cloud-based NoSQL database that stores data in JSON documents. 

Every channel is stored as separate collection in MongoDB Atlas. 

When a youtube channel is already available in MongoDB Atlas and that youtube channel details are again being stored in MongoDB Atlas, the existing details of that channel are deleted first and latest extracted channel details will be stored. 

This ensures that the latest channel details retrieved from youtube is stored in MongoDB Atlas and no duplicate collections are stored. 

### Migrate to MySQL Database 

The list of channels available in MongoDB Atlas can be selected in appliation.By clicking ‘Migrate’ button, the selected channel details are fetched from MongoDB Atlas and stored in MySQL Database in 5 tables Channel_Details, Playlist_Ids, Videos, Comments, Replies. 

 

When a youtube channel detail which is already stored in MySQL database is being migrated from MongoDB Atlas,the existing details of that channel is deleted from all 5 tables first and then latest details are stored. 

 

This is to ensure no duplicate details of same youtube channel is stored in MySQL database. 

 

### Analysis using SQL 

 

By using details of channels stored in MySQL database, the results are derived using MySQL queries for 10 questions about the youtube channels. The results are displayed in streamlit application in form of tables. 

 

### Data Visualization 

 

By using details of channels stored in MySQL database, various statistical results about the channels are derived using MySQL queries and charts are created using plotly and displayed in streamlit page 

 

## Requirements 

google-api-python-client ==2.102.0 

streamlit==1.27.2 

streamlit-option-menu==0.3.6 

pillow ==10.0.1 

pymongo==4.5.0 

pandas==2.1.1 

mysql-connector-python==8.1.0 

plotly==5.17.0 

regex==2023.10.3 

## Application Demo

URL : [https://www.youtube.com/watch?v=AWEVN4JGF0Q](https://www.youtube.com/watch?v=AWEVN4JGF0Q)

## Contact

E-mail : [vishnusankaraiml@gmail.com](vishnusankaraiml@gmail.com)

Linkedin : [https://www.linkedin.com/in/j-vishnusankar](https://www.linkedin.com/in/j-vishnusankar)





 

 

 

 

 

 
