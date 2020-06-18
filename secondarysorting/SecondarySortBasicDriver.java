


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SecondarySortBasicDriver extends Configured implements Tool {

  @Override
	public int run(String[] args) throws Exception {

		if (args.length != 2) {
			System.out
					.printf("Two parameters are required for SecondarySortBasicDriver- <input dir> <output dir>\n",getClass().getSimpleName());
			return -1;
		}

		//Job job = new Job(getConf());
		Job job = Job.getInstance(getConf(),"Row count combined input format");
		job.setJobName("Secondary sort example");

		job.setJarByClass(SecondarySortBasicDriver.class);
		job.getConfiguration().setStrings("mapreduce.reduce.shuffle.memory.limit.percent","0.2");
		
		//FileInputFormat.setInputDirRecursive(job,true);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setInputFormatClass(CombineTextInputFormat.class);
		CombineTextInputFormat.setMaxInputSplitSize(job, 128000000);
		//Path HdpPath = new Path(args[0]);
		//Path clouderaPath = new Path(args[1]);
		//Path outputPath = new Path(args[2]);
		//MultipleInputs.addInputPath(job, clouderaPath, TextInputFormat.class);
		//MultipleInputs.addInputPath(job, HdpPath, TextInputFormat.class);
		//FileOutputFormat.setOutputPath(job,outputPath);
		
		job.setMapperClass(SecondarySortBasicMapper.class);
		job.setMapOutputKeyClass(CompositeKeyWritable.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setPartitionerClass(SecondarySortBasicPartitioner.class);
		job.setSortComparatorClass(SecondarySortBasicCompKeySortComparator.class);
		job.setGroupingComparatorClass(SecondarySortBasicGroupingComparator.class);
		job.setReducerClass(SecondarySortBasicReducer.class);
		job.setOutputKeyClass(CompositeKeyWritable.class);
		job.setOutputValueClass(NullWritable.class);

		job.setNumReduceTasks(100);

		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(),
				new SecondarySortBasicDriver(), args );
		System.exit(exitCode);
	}
}