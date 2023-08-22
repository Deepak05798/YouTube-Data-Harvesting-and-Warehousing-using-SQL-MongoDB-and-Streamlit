#YouTube Data Harvesting and Warehousing using SQL, MongoDB and Streamlit.
Introduction This Streamlit application that allows users to access and analyze data from multiple YouTube channels like Channel name, subscribers, total video count, playlist ID, video ID, likes, dislikes, comments of each video using Google API.The retrieved datas are stored in Mongodb database as a data lake,then it migrates from data lake to SQL database as tables.This application also allow user to search and retrieve data from the SQL database using different search options, including joining tables to get channel details.

Approach:

Set up a Streamlit app: Streamlit is a great choice for building data visualization and analysis tools quickly and easily whereas I used Streamlit to create a simple UI where users can enter a YouTube channel ID, view the channel details, and select channels to migrate to the data warehouse.

Connect to the YouTube API: YouTube API helps to retrieve channel and video data. You can use the Google API client library for Python to make requests to the API.

Store data in a MongoDB data lake: Once the data is retrieved from the YouTube API, it will store in a MongoDB data lake. MongoDB is a great choice for a data lake because it can handle unstructured and semi-structured data easily.

Migrate data to a SQL data warehouse: After collected the multiple channels data, it will migrate in to a SQL data warehouse. You can use a SQL database such as MySQL or PostgreSQL for this.

Query the SQL data warehouse: SQL queries can be used to join the tables in the SQL data warehouse and retrieve data for specific channels based on user input. You can use a Python SQL library such as SQLAlchemy to interact with the SQL database.

Display data in the Streamlit app: Finally,the retrieved data was displayed in the Streamlit app. You can use Streamlit's data visualization features to create charts and graphs to help users analyze the data.

Summary: Overall, this approach involves building a simple UI with Streamlit, retrieving data from the YouTube API, storing it in a MongoDB data lake, migrating it to a SQL data warehouse, querying the data warehouse with SQL, and displaying the data in the Streamlit app.
