import java.io.IOException;
import java.util.StringTokenizer;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Partitioner;

class WordPair implements Writable, WritableComparable<WordPair>{
	private String key;
	private String docName;
	
	public WordPair() {}
	public WordPair(String key, String docName) {
		this.key = key;
		this.docName = docName;
	}
	
	@Override
	public int compareTo(WordPair pair) {
		int keyCompare = this.key.compareTo(pair.key);
		if(keyCompare == 0) {
			int docNameCompare = this.docName.compareTo(pair.docName);
			return docNameCompare;
		}
		else
			return keyCompare;
	}
	
	public String getKey() {
		return key;
	}
	
	public String getDocName() {
		return docName;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		key = in.readUTF();
		docName = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(key);;
		out.writeUTF(docName);
	}
	
	@Override
	public String toString() {
		return key + "\t" + docName;
	}
}

// class NKeyPartitioner extends Partitioner<WordPair, IntWritable>{
// 	@Override
// 	public int getPartition(WordPair key, IntWritable value, int numPartitions) {
// 		return Math.abs(key.getKey().hashCode() % numPartitions);
// 	}
	
// }


public class InvertedIndex {
	public static enum COUNTERS {
		WORD_COUNT,
		MAPPER_COUNT
	}
	public static class IIMapper extends Mapper<Object, Text, WordPair, IntWritable>{
		private String fileName;
		
		@Override
		public void setup(Context ctx){
			fileName = ((FileSplit)ctx.getInputSplit()).getPath().getName();
			ctx.getCounter(COUNTERS.MAPPER_COUNT).increment(1);
		}
		
		@Override
		public void map(Object offset, Text line, Context output) throws IOException, InterruptedException{
			StringTokenizer tokenizer = new StringTokenizer(line.toString().replaceAll("[^\\x00-\\x7F]", ""), " \t\r\n\r\f\",.:;?![]'*/-()&#");
			while(tokenizer.hasMoreTokens()){
				String word = tokenizer.nextToken();
				output.getCounter(COUNTERS.WORD_COUNT).increment(1);
				output.write(new WordPair(word, fileName), new IntWritable(1));
			}
		}
	}
	
	public static class IIReducer extends Reducer<WordPair, IntWritable , Text, IntWritable>{
		@Override
		public void reduce(WordPair pair, Iterable<IntWritable> list, Context output) throws IOException, InterruptedException {
			int count = 0;
			for (IntWritable item: list)
				count++;
			output.write(new Text(pair.toString()), new IntWritable(count));
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Inverted Index Job");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setJarByClass(InvertedIndex.class);
		// job.setPartitionerClass(NKeyPartitioner.class);
		
		job.setMapperClass(IIMapper.class);		
		job.setMapOutputKeyClass(WordPair.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setReducerClass(IIReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		
		job.waitForCompletion(true);
		
		Counters counters = job.getCounters();
		System.out.println(counters.findCounter(COUNTERS.WORD_COUNT).getDisplayName() + ": " + counters.findCounter(COUNTERS.WORD_COUNT).getValue());
		System.out.println(counters.findCounter(COUNTERS.MAPPER_COUNT).getDisplayName() + ": " + counters.findCounter(COUNTERS.MAPPER_COUNT).getValue());
		
		Path deletePath = new Path(args[1] + "/_SUCCESS");
		
		FileSystem fs = deletePath.getFileSystem(conf);
		
		fs.delete(deletePath, false);
		Path srcPath = new Path(args[1]);
		Path desPath = new Path("gs://dataproc-staging-us-west1-818742901208-uyi6ac2g/II.txt");
		boolean copySuccess = FileUtil.copyMerge(fs, srcPath, fs, desPath, false, conf, null);
		if(copySuccess)
			System.out.println("Files Merge Successful.");
		else
			System.out.println("Files Merge Failed.");
	}

}
