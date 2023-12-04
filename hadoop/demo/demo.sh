hadoop jar ~/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar -files /home/xinshuo3/hadoop_test/mapper1.py,/home/xinshuo3/hadoop_test/reducer1.py -mapper /home/xinshuo3/hadoop_test/mapper1.py -reducer /home/xinshuo3/hadoop_test/reducer1.py -input data/Traffic_Signal_Intersections.csv -output ./$1/ -cmdenv interconn=$4
hadoop fs -getmerge $1 $3
hdfs dfs -put $3 data
hadoop jar ~/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar -files /home/xinshuo3/hadoop_test/mapper2.py,/home/xinshuo3/hadoop_test/reducer2.py -mapper /home/xinshuo3/hadoop_test/mapper2.py -reducer /home/xinshuo3/hadoop_test/reducer2.py -input data/$3 -output ./$2/ 
hadoop fs -getmerge $2 final
rm $3
hadoop fs -rm data/$3