
###Cloud Computing Codes 
*Author: Xuan Li*

##File Name 	
   NGramJob.java
##Options required
   NGramJob <Input File Address> < Output File Address> <N : 1 ,2 ,3,…….>

##Introduction
This NGramJob.java is for n-gram mapreduce program that designed for hadoop platform.
In Mapper function,we tokenize each word and generate n-grams. We follow the below assumptions:
     * 1. All the words are consecutive alphabetic English words. Other languages will not be proceed.
     * 2. All numbers and special characters (\u00A0\t\r\n\f~`!@#$%^&*()[{]}/?=+\\|-_'\",<.>;:
) are eliminated.
     * 3. Upper case and lower case are not differentiated.
     * 4. No words are separated by two lines. 

##How to build
**1. First complie NGramJob.java **
-       `javac -classpath ``${HADOOP_INSTALL}/bin/hadoop classpath` `NGramJob.java`
 
**2. Create ngramjob.jar **
- `jar cf ngramjob.jar NGramJob*.class`	
 
**3. Run the map-reduce program **
- `hadoop jar ngramjob.jar NGramJob <INPUT file address> <OUTPUT file address>  <Number of Ngram>`

**4. View output **
- `hdfs dfs -cat /user/Ngram_twice_output/part*`
 

 

 

