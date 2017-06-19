import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;


import java.io.*;
import java.util.*;

public class WordCount {

	public static class WordCountMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        	
           String a,b,word,num;
           String[] list,list1;
           int c,j=0;
           
        	b = value.toString();
            list = b.split("\t");
            a=list[0];
            c=Integer.parseInt(list[1]);
            list1=a.split(" ");
            if(list1.length <=1){
            	return;
            }		
            word="";
            for(int k=0;k<(list1.length)-1;k++)
            {
            	word=word+list1[k]+" ";
            }
            word = word.trim();	
            num=(list1[(list1.length)-1]).trim()+" "+c;
            context.write(new Text(word),new Text(num));
        }
    }

	public static class WordCountReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {
        
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	double val10,val20;
        	String[] list;
            String word;
            int sum=0,num;
            HashMap <String, Integer> map1 = new HashMap<String, Integer>();
            for (Text value : values) {
                String b = value.toString();
                list=b.split(" ");
                int num1 = b.lastIndexOf(" ");
                word = b.substring(0,num1);
                num = Integer.parseInt(list[(list.length)-1]);
                sum=sum+num;
                map1.put(word, num);
            }
            ArrayList<Map.Entry<String, Integer>> sorted = new ArrayList<Map.Entry<String, Integer>> (map1.entrySet());


            Collections.sort(sorted,
                new Comparator <Map.Entry<String,Integer>>() {
                    public int compare(Map.Entry<String,Integer> val, Map.Entry<String,Integer>val1) {
                        int result = (Integer) val1.getValue() - (Integer) val.getValue();
                        return result;
                    }
                });
            
            String ten = "";
            int length1=(sorted.size()>5)?5:sorted.size();
            Put new1;

            for(int p=0;p<length1;p++){
                ten = sorted.get(p).getKey();
                val10 = sorted.get(p).getValue();
                val20 = val10/sum;
                new1 = new Put(Bytes.toBytes(key.toString()));
                new1.add(Bytes.toBytes("Probability"),Bytes.toBytes(ten),Bytes.toBytes("" + val20));
                context.write(new ImmutableBytesWritable(key.getBytes()), new1);
            }

            
        }

    }

    
    
    public static void main(String[] args) throws Exception {
    	
    	Configuration conf = HBaseConfiguration.create();
    	conf.set("hbase.zookeeper.quorum", "172.31.23.16");
    	conf.set("hbase.zookeeper.property.clientport", "2181");
		Job job = new Job(conf, "predict");
		job.setJarByClass(WordCount.class);

		TableMapReduceUtil.initTableReducerJob("predict",WordCountReducer.class,job);

		job.setNumReduceTasks(1);
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
