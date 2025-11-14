# F. Raimbault
# 2025/10/21
# Count and save the word occurrences of text files without considering too frequent words.

import subprocess # for using CLI commands
import re # for filtering the words

WORD_RE = re.compile(r"[a-z][a-z]+")

from pyspark import SparkContext

# Local PySpark is not installed with AWS libraries.

# To be able to read or write files on S3 from your LOCAL PC you need to launch it this way:
# spark-submit --master local[4] --packages org.apache.hadoop:hadoop-aws:3.3.1 word_count.py

# on an AWS cluster launch it directly with :
# spark-submit --master yarn word_count.py

def main():

    #create a SparkContext
    sc = SparkContext(appName='Spark Word Count')
    
    # define the log level to WARN (only ERROR and LOG messages are printed) instead of INFO by default
    sc.setLogLevel("WARN")
    
    #Step 1:  build a RDD from text files    
    # to read one file on S3:
    lines = sc.textFile('s3a://ubs-datasets/gutenberg/Aldous_Huxley___Crome_Yellow.txt') 
    # or to read the file locally after having download it
    # lines = sc.textFile('Aldous_Huxley___Crome_Yellow.txt')
    # or to read the whole dataset on S3:
    #lines = sc.textFile('s3a://ubs-datasets/gutenberg/*.txt') 
   
    #Step 2:  lower lines
    lowered_lines = lines.map(lambda line:line.lower())
    
    #for word in lowered_lines.take(10): print(word) # display first 10 lines (client mode needed)

    #Step 3:  split lines into words
    words= lowered_lines.flatMap(lambda line: WORD_RE.findall(line)) 
    
    #for word in words.take(10): print(word) # display first 10 words
   
    #Step 4: remove stop words (too frequent words as "the" "as" "I"...)
    stop_words= sc.textFile('s3a://ubs-datasets/stop-words/stop-words-english4.txt')
    filtered_words= words.subtract(stop_words);
    
    #for word in filtered_words.take(10): print(word) # display first 10 filtered words
    
    #Step 5:  build a RDD of (word,1)
    pairs = filtered_words.map(lambda word:(word,1))
    
    #for pair in pairs.take(10): print(pair) # display first 10 (pairs word,1) 
    
    #Step 6:  count words
    counts= pairs.reduceByKey(lambda a,b: a+b)
    
    #for count in counts.take(10): print(count) # display first 10 pairs (word,count) 
                                        
    #Step 7:  save the result in a text file as lines of "('word',count)" under S3 or HDFS or locally
    #         in any case one file per partition of the RDD will be created under the name part-00000, part-00001, part-00002...
    
    # 7.1: save into S3 under s3://ubs-homes/erasmus/yourname/output-wc-spark
    #      output directory should not exist, 
    #      remove it with: aws s3 rm --recursive s3://ubs-cde/home/yourname/output-wc-spark
    #      or programmatically with:
    #      subprocess.call(["aws","s3","rm","--recursive","s3://ubs-homes/erasmus/yourname/output-wc-spark/"])
    #      and then:
    #      counts.saveAsTextFile('s3a://ubs-homes/erasmus/yourname/output-wc-spark') 

    # 7.2: or save it on HDFS under ~/output-wc-spark
    #      output directory should not exist, remove it with: hdfs dfs -rmr output-wc-spark
    #      or programmatically with:
    #      subprocess.call(["hdfs","dfs","-rmr","output-wc-spark"])
    #      and then :
    #      counts.saveAsTextFile('output-wc-spark') 
    
    # 7.3: or save it on your LOCAL filesystem under /tmp/output-wc-spark
    #      output directory should not exist, remove it with: rm -rf /tmp/output-wc-spark
    #      or programmatically with:
    subprocess.call(["rm","-rf","./output-wc-spark"])
    #      and then :
    counts.saveAsTextFile('output-wc-spark')
    #  
                                                                # On the whole dataset:
    print('Lines count= ',lowered_lines.count())            # Lines count=           22 866 480
    print('Total words count= ',words.count())              # Total words count=    198 776 132
    print('Filtered words count= ',filtered_words.count())  # Filtered words count=  77 289 846
    print('Unique words count= ',counts.count())            # Unique words count=       380 057

    # Release the SparkContext
    sc.stop()

if __name__ == '__main__':
    main()

    
