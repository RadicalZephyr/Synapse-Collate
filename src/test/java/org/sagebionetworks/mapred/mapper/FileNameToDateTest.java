package org.sagebionetworks.mapred.mapper;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.regex.Matcher;

import org.junit.Test;

public class FileNameToDateTest {

	@Test
	public void testPatterns() throws IOException {

		BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(new File(
				"src/test/resource/listing.txt"))));
		String line;
		while ((line = reader.readLine()) != null) {
			Matcher s3Matcher = FileNameToDate.s3Pattern.matcher(line);
			assertTrue("Could not find an s3 match: " + line, s3Matcher.find());
		}

	}

}
