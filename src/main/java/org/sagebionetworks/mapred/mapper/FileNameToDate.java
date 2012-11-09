package org.sagebionetworks.mapred.mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FileNameToDate
	extends Mapper<LongWritable, Text, Text, Text> {

	public Pattern s3Pattern = Pattern.compile("s3://[.\\w]+/(\\w+/\\w)/[-\\w]/([-\\w.]+)");
	public Pattern filePattern = Pattern.compile("[-\\w]+\\.(\\d{4}-\\d{2}-\\d{2})-\\d{2}-\\d{2}\\.gz");

	public void map(LongWritable key, Text value, Context context) 
		throws IOException, InterruptedException {
		String line = value.toString();

		Matcher s3Matcher = s3Pattern.matcher(line);
		Matcher fileMatcher = filePattern.matcher(s3Matcher.group(2));

		StringBuilder keyBuilder = new StringBuilder();
		keyBuilder.append(s3Matcher.group(1));
		keyBuilder.append("-");
		keyBuilder.append(fileMatcher.group());

		context.write(new Text(keyBuilder.toString()), new Text(s3Matcher.group()));
	}
}
