package org.sagebionetworks.mapred.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FileNameToDate
	extends Mapper<LongWritable, Text, Text, Text> {

}
