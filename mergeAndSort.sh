#Pleasese include your program to merge the output of mapreduce jobs and output
#top 100 ngrams to stdout.
#e.g.
#python mergeAndSort.py
#java MergeAndSort
#
#please also put the source code in code/
hadoop fs -get /~wordcount/output /home/hadoop/p41task1/example/
python files.py
sort -nrk 2 -t$'\t' output100 > output200
python mergeAndSort.py

