package org.sagebionetworks.mapred.mapper;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.junit.Test;

public class FileNameToDateTest {

	String[] testFiles = {"repo-activity-2012-09-11",
						  "repo-activity-2012-09-16",
						  "repo-slow-profile-2012-09-16"};

	@Test
	public void testPatterns() throws IOException {
		FileNameToDate fntd = new FileNameToDate();
		for (String key : testFiles) {

			BufferedReader reader = new BufferedReader(new InputStreamReader(
					new FileInputStream(new File("src/test/resource/"+key+".txt"))));
			String line;
			while ((line = reader.readLine()) != null) {
				assertEquals("prod-A-"+key, fntd.makeKey(line.split("/")));
			}
		}
	}

}
