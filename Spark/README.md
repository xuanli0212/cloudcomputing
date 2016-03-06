Spark - Count Listening Frequencies of Artists

##PROJECT NAME##

##AUTHOR##

##INTRODUCTION##


#HOW TO BUILD#

%1.	Submit local data files into HDFS.%
  Step1: First put the user-artist.dat file into VM local file through Fetch
su - 
cd /
cd usr/local/hadoopData  
mkdir Project2Data

Use Fetch to put file into Project2Data directory


Step2: Create working directory in HDFS for our input data

su - hadoop
hdfs dfs -mkdir /user/project2_input

Step3: Move the local data into HDFS

hdfs dfs -put /usr/local/hadoopData/Project2Data/user_artists.dat /user/project2_input



2.	Run Spark using Scala Spark-shell

Step1. open Scala spark-shell
su – hadoop
spark-shell


