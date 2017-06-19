import java.io.IOException;
import org.apache.commons.lang3.StringEscapeUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;


import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import java.io.StringReader;

import java.util.regex.*;


public class WordCount {

	public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		private Text word=new Text();
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException{
			
			String w = value.toString(),b,f;
			boolean g1,g2;
			int x=0,y=0,z=0,y1;
			w=StringEscapeUtils.unescapeHtml3(w);
			w=StringEscapeUtils.unescapeHtml3(w);
			w=w.replaceAll("&"," ");
			Pattern pattern = Pattern.compile("<text");
		    Matcher matcher = pattern.matcher(w);
		    if(matcher.find())
		    {x=matcher.start();}
		    pattern = Pattern.compile(">");
		    matcher = pattern.matcher(w.substring(x));
		    if(matcher.find())
		    {y=matcher.start();}
		    
		    pattern = Pattern.compile("</text>");
		    matcher = pattern.matcher(w);
		    if(matcher.find())
		    {z=matcher.start();}
		    if(x>0 && y>0 && z>0)
		    {
		    w=w.substring(x+y, z);
		    
            b=w.toLowerCase();
            b=b.replaceAll("(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]", " ");
            b=b.replaceAll("<ref>"," ");
            b=b.replaceAll("</ref>"," ");
            b=b.replaceAll("<ref name"," ");
            b=b.replaceAll("[^A-Za-z' ]"," ");
            b=b.replaceAll("[^a-z ]'[^a-z ]"," ");
            b=b.replaceAll(" '"," ");
            b=b.replaceAll(" '"," ");
            b=b.replaceAll("' "," ");
            b=b.replaceAll("' "," ");
            b = b.trim().replaceAll(" +", " ");
            String element[] = b.split(" ");
			
			for (int a = 0; a < element.length ; a++) {
				StringBuilder g = new StringBuilder();
				int flag=0;
				for (int c = 0; c < 5 && c+a < element.length; c++) {
					if (flag==0) {
						flag=1;
					} else {
						g.append(" ");
					}
					g.append(element[c+a]);
					word.set(g.toString());
					context.write(word, new IntWritable(1));
				}
			}
		}}
	}

	public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

		public void reduce(Text key, Iterable<IntWritable> values, Context context) 
				throws IOException, InterruptedException{
			int sum = 0;
			for (IntWritable val : values) {
				sum  += val.get();
			}
			if(sum>2)
			{context.write(key, new IntWritable(sum));}
		}
	}

	public static void main (String[] args) throws Exception{
		Configuration conf = new Configuration();
		
		Job job = new Job(conf, "word count");
		job.setJarByClass(WordCount.class);

		job.setMapperClass(WordCountMapper.class);
		job.setCombinerClass(WordCountReducer.class);
		job.setReducerClass(WordCountReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setNumReduceTasks(1);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}


