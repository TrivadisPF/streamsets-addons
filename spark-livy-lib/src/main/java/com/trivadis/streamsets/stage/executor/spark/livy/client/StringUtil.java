package com.trivadis.streamsets.stage.executor.spark.livy.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;

public class StringUtil {
	/** 
	 * Converts the Stream into a string
	 * @param stream	the input stream	
	 * @param charset	the characterset to use
	 * @return			the string representation of the stream
	 * @throws IOException
	 */
    public static String getStreamAsString(InputStream stream, String charset) throws IOException {
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(stream, charset));
            StringWriter writer = new StringWriter();

            char[] chars = new char[256];
            int count = 0;
            while ((count = reader.read(chars)) > 0) {
                writer.write(chars, 0, count);
            }

            return writer.toString();
        } finally {
            if (stream != null) {
                stream.close();
            }
        }
    }
}
