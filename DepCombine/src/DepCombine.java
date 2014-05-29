import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.GenericOptionsParser;

public class DepCombine {

	public static class DCMapper extends Mapper<Object, Text, Text, Text> {
		private Text line = new Text("");

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			line = value;
			context.write(line, new Text(""));
		}
	}

	public static class DCReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			context.write(key, new Text(""));
		}
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		// 这句话很关键
		conf.set("fs.default.name", "hdfs://localhost:9000");
		// conf.set("mapred.job.tracker", "localhost:9001");
		// 下面的参数路径设置为hadoop dfs目录下的路径,查看方式 bin/hadoop fs -ls /input
		String[] ioArgs = new String[] { "/depcombine/depin",
				"/depcombine/depout" };
		// 下面的参数路径设置为当前workspace目录下的路径
		// String[] ioArgs = new String[] { "input", "output" };
		String[] otherArgs = new GenericOptionsParser(conf, ioArgs)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: Deduplication <in> <out>");
			System.exit(2);
		}

		Job job = new Job(conf, "Data Deduplication");
		job.setJarByClass(DepCombine.class);
		// 设置Map、Combine和Reduce处理类
		job.setMapperClass(DCMapper.class);
		job.setCombinerClass(DCReducer.class);
		job.setReducerClass(DCReducer.class);
		// 设置输出类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		// 设置输入和输出目录
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
