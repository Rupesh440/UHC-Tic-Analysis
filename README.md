An ETL of Transparency in Coverage data extracted from the Tic website of UHC.
The file Parallel_Extraction.py is used to extract the first 1000 json files from the website paralelly and downloaded to a folder called downloaded_files parallely. 
Parallel processing is considered here for scalable data handling. This solution can is created keeping all the best practices in mind.
The file PysparkTransform_and_SnowflakeLoad.py is used to process the nested JSON file and write the tables to snowflake database through pyspark.
Inside the testing files there are files that I have experinmented on.

