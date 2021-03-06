# N-Gram-Model-with-MapReduce-for-Auto-Complete
Build N-Gram language model for autocomplete on website

Raw Data:

Wiki articles are collected by a 5-thread web crawler, and sit in ./data

Source Code:

This program include 2 MapReduce jobs: NGramBuild and ModelCreate
The first job builds n-gram library from wiki articles, and the second job create the language model from n-gram library.

DataBase:

The model is stores into MySQL database "langMode.ngram", the schema is decribed in ./schema.ini

--------------------------------------------------
How to run Hadoop MapReduce:

 1. Upload input files into hadoop file system:
    
    $ hdfs fs -mkdir /input
    
    $ hdfs fs -put data/* /input/
    
 2. Generate jar file from your java mapReduce program
    
    $ hadoop com.sun.tools.javac.Main *.java
    
    $ jar cf autoComp.jar *.class
 
 3. Run MapReduce on Hadoop
  
    $ hadoop jar autoComp.jar Driver /input /library 4 10 10
    
    Explanation of Arguments:
    
        input : raw data; library : N Gram library
        
        4 : the value for N in N Gram
        
        10 : threshold to filter out infrequent phrase
        
        10 : only record the top 10 followers

 4. Check the result from MapReduce
 
    $ hdfs fs -cat /library/part-r-00000
