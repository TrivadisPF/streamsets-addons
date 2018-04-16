package com.trivadis.streamsets.el.offset;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import com.streamsets.pipeline.api.ElDef;
import com.streamsets.pipeline.api.ElFunction;
import com.streamsets.pipeline.api.ElParam;

@ElDef
public class OffsetEL {

	private static final String OFFSET = "offset";
	private static final String OFFSET_FILE_NAME = "offset.dat";

	private static File getOffsetFile(String key) {
		String offsetDir = System.getenv("SDC_OFFSET_DIRECTORY");
		if (offsetDir == null) {
			throw new RuntimeException("SDC_OFFSET_DIRECTORY not set, necessary for offset:xxxx EL functions!");
		}
		
		String offsetFile = offsetDir + File.separator + key + File.separator + OFFSET_FILE_NAME;
		
		return new File(offsetFile);
	}
	
	@ElFunction(prefix = OFFSET, name = "set", description = "Returns true if this is a private IPv4 address.")
	public static void set(@ElParam("key") String key, @ElParam("offset") String offset) {
		
		File file = getOffsetFile(key);
		
		BufferedWriter writer = null;
		file.getParentFile().mkdirs();
		try {
			writer = new BufferedWriter(new FileWriter(file));
			writer.write(offset);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (writer != null) {
					writer.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	@ElFunction(prefix = OFFSET, name = "get", description = "Returns true if this is a private IPv4 address.")
	public static String get(@ElParam("key") String key, @ElParam("startValue") String startValue) {
		String returnValue = startValue;
		
		File file = getOffsetFile(key);
		if (file.exists()) {
			BufferedReader reader = null;
			try {
				reader = new BufferedReader(new FileReader(file));
				returnValue = reader.readLine();
			} catch (IOException e) {
				e.printStackTrace();
			}
		} 
		
		return returnValue;
	}


}