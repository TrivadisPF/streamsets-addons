import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;

import javax.swing.plaf.FileChooserUI;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;

public class TestOffsetEL {
	
	public final static String SDC_OFFSET_DIR = "/tmp/offset";
	
	@Rule
    public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

	@Before
	public void tearDown() throws IOException {
		FileUtils.deleteDirectory(new File(SDC_OFFSET_DIR));
	}
	
	@Test
	public void testGetOffsetFromExistingFile() {
		environmentVariables.set("SDC_OFFSET_DIRECTORY", SDC_OFFSET_DIR);
		OffsetEL.set("test", "1");
		
		// Test
		String offset = OffsetEL.get("test", "0");
		
		assertTrue(offset.equals("1"));
	}

	@Test
	public void testGetOffsetFromNonExistingFile() {
		environmentVariables.set("SDC_OFFSET_DIRECTORY", SDC_OFFSET_DIR);
		
		String offset = OffsetEL.get("test", "0");
		
		assertTrue(offset.equals("0"));
	}

	@Test
	public void testSetOffset() {
		environmentVariables.set("SDC_OFFSET_DIRECTORY", SDC_OFFSET_DIR);
		
		//
		OffsetEL.set("test", "2");
				
		assertTrue(OffsetEL.get("test", "0").equals("2"));
	}
}
