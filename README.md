# N-Gram-Model-with-MapReduce-for-Auto-Complete
Build N-Gram language model for autocomplete on website

How to run Hadoop MapReduce:

 1. Upload input files into hadoop file system:
    
    $ hdfs dfs -mkdir input/
    
    $ hdfs dfs -put *.txt /input/
    
 2. Generate jar file from your java mapReduce program
    
    $ hadoop com.sun.tools.javac.Main *.java
    
    $ jar cf myprog.jar *.class
 
 3. Run MapReduce on Hadoop
 
    $ hadoop jar myprog.jar [main class] input/ output/
 
 4. Check the result from MapReduce
 
    $ hdfs dfs -cat /output/part-r-00000
