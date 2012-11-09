package org.sagebionetworks.mapred.mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FileNameToDate
	extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
		String line = value.toString();
		String[] split = line.split("/");

		String outKey = makeKey(split);
		context.write(new Text(outKey), value);
	}

	String makeKey(String[] split) {
		StringBuilder keyBuilder = new StringBuilder();
		// Stack type (prod, dev ...)
		keyBuilder.append(split[3]);
		keyBuilder.append("-");

		// Stack name (A, B, C ...)
		keyBuilder.append(split[4]);
		keyBuilder.append("-");

		String filename = split[6];
		String[] fileSplit = filename.split("\\.");

		// Log type (repo-activity, repo-slow-profile ...)
		keyBuilder.append(fileSplit[0]);
		keyBuilder.append("-");

		// Date, truncated to day
		String date = fileSplit[1];
		keyBuilder.append(date.substring(0, 10));

		return keyBuilder.toString();
	}
}
