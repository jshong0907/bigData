#### Wordcount Example

1. load inputfile from HDFS and store outputfile to HDFS
2. filter noise 'Tokyo' and count the number of Tokyo by using accumulator
3. sort count in ascending order
4. output should be small letter
5. print the number of tokyo word in stdout
6. do cluster setting and then run this kind of command 
>> ./bin/spark-submit wc_practice.py spark://yun-1:7077 hdfs://yun-1:9000//input/input_air hdfs://yun-1:9000//output/output
7. zip files (code file and output folder) and upload it to KLAS

