import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;

public class Driver {
	public static void main(String args[])
			throws ClassNotFoundException, IOException, InterruptedException {
		
		Configuration confBuild = new Configuration();
		confBuild.set("textinputformat.record.delimiter", ".");
		confBuild.set("noGram", args[2]);
		
		Job jobBuild = Job.getInstance(confBuild, "NGram-Build");
		jobBuild.setJarByClass(Driver.class);
		
		jobBuild.setMapperClass(NGramBuild.NGramBuildMapper.class);
		jobBuild.setReducerClass(NGramBuild.NGramBuildReducer.class);
		
		jobBuild.setOutputKeyClass(Text.class);
		jobBuild.setOutputValueClass(IntWritable.class);
		
		jobBuild.setInputFormatClass(TextInputFormat.class);
		jobBuild.setOutputFormatClass(TextOutputFormat.class);
		
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);

		FileSystem hdfsBuild = FileSystem.get(URI.create("hdfs://hadoop-master:9000"), confBuild);
		if (hdfsBuild.exists(outputPath))
        	hdfsBuild.delete(outputPath, true);
        
		TextInputFormat.setInputPaths(jobBuild, inputPath);
		TextOutputFormat.setOutputPath(jobBuild, outputPath);
		
		jobBuild.waitForCompletion(true);
		
		Configuration confCreate = new Configuration();
		confCreate.set("threshold", args[3]);
		confCreate.set("topK", args[4]);
		
		DBConfiguration.configureDB(confCreate, 
				"com.mysql.jdbc.Driver",
				"jdbc:mysql://52.15.48.17:3306/langModel", // DB name
				"root",	// user name
				"bigdata"); // password
		
		Job jobCreate = Job.getInstance(confCreate, "Model-Create");
		jobCreate.setJarByClass(Driver.class);
		
		jobCreate.addArchiveToClassPath(new Path("/mysql/mysql-connector-java-5.1.39-bin.jar"));

		jobCreate.setMapperClass(ModelCreate.ModelCreateMapper.class);
		jobCreate.setReducerClass(ModelCreate.ModelCreateReducer.class);
		
		jobCreate.setMapOutputKeyClass(Text.class);
		jobCreate.setMapOutputValueClass(Text.class);
		
		jobCreate.setOutputKeyClass(Text.class);
		jobCreate.setOutputValueClass(NullWritable.class);
		
		jobCreate.setInputFormatClass(TextInputFormat.class);
		jobCreate.setOutputFormatClass(DBOutputFormat.class);
		
		TextInputFormat.setInputPaths(jobCreate, outputPath);
		DBOutputFormat.setOutput(
			     jobCreate,
			     "ngram",    // output table name
			     new String[] { "starting_phrase", "following_word", "count" }   //table columns
			     );

		jobCreate.waitForCompletion(true);
	}
}
