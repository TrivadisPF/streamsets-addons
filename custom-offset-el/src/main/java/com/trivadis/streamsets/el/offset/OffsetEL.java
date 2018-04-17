package com.trivadis.streamsets.el.offset;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;

import com.google.common.io.Files;
import com.streamsets.pipeline.api.ElDef;
import com.streamsets.pipeline.api.ElFunction;
import com.streamsets.pipeline.api.ElParam;

@ElDef
public class OffsetEL {

	private static final String OFFSET = "offset";
	private static final String OFFSET_FILE_NAME = "offset.dat";

	@ElFunction(prefix = OFFSET, name = "getOffsetFilename", description = "Returns the latest stored and commited offset. If the offset does not exists yet, then it will be set to the startValue and that value will be returned.")
	public static String getOffsetFilename(@ElParam("key") String key) {
		String offsetDir = System.getenv("SDC_OFFSET_DIRECTORY");
		if (offsetDir == null) {
			throw new RuntimeException("SDC_OFFSET_DIRECTORY not set, necessary for offset:xxxx EL functions!");
		}
	
		String offsetFileName = offsetDir + File.separator + key + File.separator + OFFSET_FILE_NAME;
		return offsetFileName;
	}

	@ElFunction(prefix = OFFSET, name = "get", description = "Returns the latest stored and commited offset. If the offset does not exists yet, then it will be set to the startValue and that value will be returned.")
	public static Long get(@ElParam("key") String key, @ElParam("startValue") Long startValue) {
		Long returnValue = getAndInc(key, startValue, 0);
		
		return returnValue;
	}
	
	@ElFunction(prefix = OFFSET, name = "getAndInc", description = "Returns the latest stored and commited offset, incremented by the incrementBy before returning it. If the offset does not exists yet, then it will be set to the startValue and that value will be returned.")
	public static Long getAndInc(@ElParam("key") String key, @ElParam("startValue") Long startValue, @ElParam("incrementBy") Integer incrementBy) {
		Long returnValue = startValue;
		
		File file = new File(getOffsetFilename(key));
		if (file.exists()) {
			BufferedReader reader = null;
			try {
				reader = new BufferedReader(new FileReader(file));
				returnValue = Long.valueOf(reader.readLine());
				returnValue += incrementBy;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		return returnValue;
	}

	@ElFunction(prefix = OFFSET, name = "prepare", description = "Sets an offset to the file inside SDC_OFFSET_DIRECTORY")
	public static void prepare(@ElParam("key") String key, @ElParam("offset") Long offset) {
		
		File file = new File(getOffsetFilename(key) + ".prep");
		
		BufferedWriter writer = null;
		file.getParentFile().mkdirs();
		try {
			writer = new BufferedWriter(new FileWriter(file));
			writer.write(String.valueOf(offset));
		} catch (FileNotFoundException e) {
			// ignore
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				if (writer != null) {
					writer.close();
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	

	@ElFunction(prefix = OFFSET, name = "commit", description = "Returns true if this is a private IPv4 address.")
	public static Long commit(@ElParam("key") String key) {
		Long returnValue = 0L;

		File from = new File(getOffsetFilename(key) + ".prep");
		File to = new File(getOffsetFilename(key));
		
		try {
			Files.copy(from, to);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		returnValue = get(key, 0L);
		return returnValue;
	}

}