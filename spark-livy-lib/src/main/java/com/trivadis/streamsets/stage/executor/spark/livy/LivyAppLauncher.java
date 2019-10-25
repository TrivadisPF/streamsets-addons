/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.trivadis.streamsets.stage.executor.spark.livy;

import static com.trivadis.streamsets.stage.executor.spark.Errors.SPARK_EXEC_03;
import static com.trivadis.streamsets.stage.executor.spark.Errors.SPARK_EXEC_09;
import static com.trivadis.streamsets.stage.executor.spark.Errors.SPARK_EXEC_10;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import org.apache.commons.lang3.StringUtils;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.lib.event.EventCreator;
import com.trivadis.streamsets.stage.executor.spark.AppLauncher;
import com.trivadis.streamsets.stage.executor.spark.ApplicationLaunchFailureException;
import com.trivadis.streamsets.stage.executor.spark.CredentialsConfigBean;
import com.trivadis.streamsets.stage.executor.spark.Errors;
import com.trivadis.streamsets.stage.executor.spark.SparkExecutorConfigBean;
import com.trivadis.streamsets.stage.executor.spark.livy.client.BatchJobParameters;
import com.trivadis.streamsets.stage.executor.spark.livy.client.LivyBatchClient;
import com.trivadis.streamsets.stage.executor.spark.livy.client.LivyException;
import com.trivadis.streamsets.stage.executor.spark.livy.client.Session;

public class LivyAppLauncher implements AppLauncher {

	private final String AZUREHDINSIGHT_LIVY_URI = ".azurehdinsight.net/livy";

	private static final String YARN = "yarn";
	private static final String PREFIX = "conf.yarnConfigBean.";
	private static final String JAVA_HOME_CONF = "conf.javaHome";
	private static final String SPARK_HOME_CONF = "conf.sparkHome";
	private static final String MEMORY_STRING_REGEX = "[0-9]+[gGmMkK]$";
	private static final String SPARK_GROUP = "SPARK";
	private static final String APPLICATION_GROUP = "APPLICATION";
	private CountDownLatch latch = new CountDownLatch(1);
	private long timeout;
	private SparkExecutorConfigBean configs;
	private LivyConfigBean livyConfigs;
	private CredentialsConfigBean credentialsConfigs;
	private Stage.Context context;
	
	public static final String APP_SUBMITTED_EVENT = "AppSubmittedEvent";
	public static final String APP_ID = "app-id";
	public static final EventCreator JOB_CREATED = new EventCreator.Builder(APP_SUBMITTED_EVENT, 1)
		      .withRequiredField(APP_ID)
		      .build();
	
	@Override
	public List<Stage.ConfigIssue> init(Stage.Context context, SparkExecutorConfigBean configs) {
		List<Stage.ConfigIssue> issues = new ArrayList<>();
		this.configs = configs;
		this.livyConfigs = configs.livyConfigBean;
		this.credentialsConfigs = configs.credentialsConfigBean;
		this.context = context;

//    Optional.ofNullable(livyConfigs.init(context, PREFIX)).ifPresent(issues::addAll);

		verifyKeyValueArgs(context, livyConfigs.env, PREFIX + "env", issues);

		verifyKeyValueArgs(context, livyConfigs.args, PREFIX + "args", issues);

		// verifyJavaAndSparkHome(context, issues);

		verifyMemoryStrings(context, issues);

		verifyAllFileProperties(context, issues);

		return issues;
	}

	private void verifyKeyValueArgs(Stage.Context context, Map<String, String> params, String config,
			List<Stage.ConfigIssue> issues) {

		params.forEach((String k, String v) -> {
			if (!StringUtils.isEmpty(k) && StringUtils.isEmpty(v)) {
				issues.add(context.createConfigIssue(SPARK_GROUP, config, SPARK_EXEC_09, k));
			}
			if (StringUtils.isEmpty(k) && !StringUtils.isEmpty(v)) {
				issues.add(context.createConfigIssue(SPARK_GROUP, config, SPARK_EXEC_10, v));
			}
		});
	}

	private void verifyAllFileProperties(Stage.Context context, List<Stage.ConfigIssue> issues) {

		verifyFileIsAccessible(livyConfigs.appResource, context, issues, APPLICATION_GROUP, PREFIX + "appResource");

		verifyFilesAreAccessible(livyConfigs.additionalJars, context, issues, APPLICATION_GROUP,
				PREFIX + "additionalJars");

		verifyFilesAreAccessible(livyConfigs.additionalFiles, context, issues, APPLICATION_GROUP,
				PREFIX + "additionalFiles");

		verifyFilesAreAccessible(livyConfigs.pyFiles, context, issues, APPLICATION_GROUP, PREFIX + "pyFiles");
	}

	private void verifyMemoryStrings(Stage.Context context, List<Stage.ConfigIssue> issues) {
		if (!isValidMemoryString(livyConfigs.driverMemory)) {
			issues.add(context.createConfigIssue(SPARK_GROUP, PREFIX + "driverMemory", SPARK_EXEC_03,
					livyConfigs.driverMemory));
		}

		if (!isValidMemoryString(livyConfigs.executorMemory)) {
			issues.add(context.createConfigIssue(SPARK_GROUP, PREFIX + "executorMemory", SPARK_EXEC_03,
					livyConfigs.executorMemory));
		}
	}

	/*
	 * private void verifyJavaAndSparkHome( Stage.Context context,
	 * List<Stage.ConfigIssue> issues ) {
	 * 
	 * if (StringUtils.isEmpty(configs.sparkHome)) { String sparkHomeEnv =
	 * System.getenv("SPARK_HOME"); if (StringUtils.isEmpty(sparkHomeEnv)) {
	 * issues.add(context.createConfigIssue(SPARK_GROUP, SPARK_HOME_CONF,
	 * SPARK_EXEC_01)); } else { verifyFileIsAccessible(sparkHomeEnv, context,
	 * issues, SPARK_GROUP, SPARK_HOME_CONF, SPARK_EXEC_08); } } else {
	 * verifyFileIsAccessible(configs.sparkHome, context, issues, SPARK_GROUP,
	 * SPARK_HOME_CONF); }
	 * 
	 * if (StringUtils.isEmpty(configs.javaHome)) { String javaHomeEnv =
	 * System.getenv("JAVA_HOME"); if (StringUtils.isEmpty(javaHomeEnv)) {
	 * issues.add(context.createConfigIssue(SPARK_GROUP, JAVA_HOME_CONF,
	 * SPARK_EXEC_02)); } else { verifyFileIsAccessible(javaHomeEnv, context,
	 * issues, SPARK_GROUP, JAVA_HOME_CONF, SPARK_EXEC_07); } } else {
	 * verifyFileIsAccessible(configs.javaHome, context, issues, SPARK_GROUP,
	 * JAVA_HOME_CONF); } }
	 */
	private boolean isValidMemoryString(String memoryString) {
		Optional<Boolean> valid = Optional.ofNullable(memoryString).map((String x) -> x.matches(MEMORY_STRING_REGEX));
		return valid.isPresent() && valid.get();
	}

	private void verifyFilesAreAccessible(List<String> files, Stage.Context context, List<Stage.ConfigIssue> issues,
			String configGroup, String config) {
		files.forEach((String file) -> verifyFileIsAccessible(file, context, issues, configGroup, config));
	}

	private void verifyFileIsAccessible(String file, Stage.Context context, List<Stage.ConfigIssue> issues,
			String configGroup, String config) {
//    verifyFileIsAccessible(file, context, issues, configGroup, config, SPARK_EXEC_04);
	}

	private void verifyFileIsAccessible(String file, Stage.Context context, List<Stage.ConfigIssue> issues,
			String configGroup, String config, Errors error) {
		File f = new File(file);
		if (!f.exists() || !f.canRead()) {
			issues.add(context.createConfigIssue(configGroup, config, error, file));
		}
	}

	@Override
  public Optional<String> launchApp(Record record) throws ApplicationLaunchFailureException {
	  
	  LivyBatchClient client = null;

	  String baseUri = "https://" + configs.livyEndpoint + AZUREHDINSIGHT_LIVY_URI;

	  try {
			client = new LivyBatchClient(baseUri, credentialsConfigs.user.get(), credentialsConfigs.password.get());
	  } catch (MalformedURLException e) {
			e.printStackTrace();
	  } catch (StageException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	  }
	  
	  BatchJobParameters param = new BatchJobParameters(livyConfigs.appResource, livyConfigs.mainClass);	
	  	  
	  Map<String,String> confMap = new HashMap<>();
	  param.conf = confMap;
	  
	  if (!StringUtils.isEmpty(livyConfigs.appName)) { 
		  confMap.put("spark.app.name", livyConfigs.appName);
	  }
	  
	  if (livyConfigs.dynamicAllocation) {
		  confMap.put("spark.dynamicAllocation.enabled", "true");
		  confMap.put("spark.shuffle.service.enabled", "true");
		  confMap.put("spark.dynamicAllocation.minExecutors", String.valueOf(livyConfigs.minExecutors));
		  confMap.put("spark.dynamicAllocation.maxExecutors", String.valueOf(livyConfigs.maxExecutors));
	  } else {
		  confMap.put("spark.dynamicAllocation.enabled", "false");
		  param.numExecutors = livyConfigs.numExecutors;
	  }

	  param.driverMemory = livyConfigs.driverMemory;
	  param.executorMemory = livyConfigs.executorMemory;

	  // For files, no need of removing empty strings, since we verify the file exists in init itself.
	  if (livyConfigs.additionalJars != null) {
		  confMap.put("spark.jars.packages", StringUtils.join(livyConfigs.additionalJars, ","));
	  }
	  if (livyConfigs.pyFiles != null) {
		  param.pyFiles = livyConfigs.pyFiles.toArray(new String[livyConfigs.pyFiles.size()]);
	  }
	  if (livyConfigs.pyFiles != null) {
		  param.files = livyConfigs.additionalFiles.toArray(new String[livyConfigs.additionalFiles.size()]);
	  }
	  
	  param.args = livyConfigs.appArgs.toArray(new String[livyConfigs.appArgs.size()]);

	  if (!StringUtils.isEmpty(livyConfigs.proxyUser)) {
		  param.proxyUser = livyConfigs.proxyUser;
	  }
	  	  
		try {
			//System.out.println(client.getActiveSessions());
			client.createJob(param);
			int status = Session.NOT_STARTED;
			while(true) {
				status = client.getSession().getState();
		    	if(status == Session.RUNNING) {
//		    		JOB_CREATED.create(context).
		    		System.out.println("Status: RUNNING");
					System.out.println(" AppId: " + client.getSession().getAppId());
					System.out.println(" AppInfo: " + client.getSession().getAppInfo());
					System.out.println(" Id: " + client.getSession().getId());
					System.out.println(" Log: " + client.getSession().getLog());
		    	}
		    	else if(status == Session.SUCCESS) {
		    		System.out.println("Status: SUCCESS");
		    		break;
		    	}
		    	else if(status == Session.ERROR) {
		    		System.out.println("Status: ERROR");
		    		System.out.println(client.getFullLog());
		    		throw new ApplicationLaunchFailureException(client.getFullLog());
		    	}
				else if(status == Session.STARTING) {
					System.out.println("Status: STARTING");
				}
		    	else {
		    		System.out.println("Status: None");
		    		break;
		    	}
				Thread.sleep(6000);
			}
		} catch (IOException e) {
			throw new ApplicationLaunchFailureException(e);
		} catch (LivyException e) {
			throw new ApplicationLaunchFailureException(e);
		} catch (InterruptedException e) {
			throw new ApplicationLaunchFailureException(e);
		}
	  
	  
	  return null;
  }

	/**
	 * If there is a RecordEL, then an arg could eval to empty string. This method
	 * returns args that are not null or empty.
	 */
	private String[] getNonEmptyArgs(List<String> appArgs) {
		List<String> nonEmpty = new ArrayList<>();
		appArgs.forEach((String val) -> {
			if (!StringUtils.isEmpty(val)) {
				nonEmpty.add(val);
			}
		});
		return nonEmpty.toArray(new String[nonEmpty.size()]);
	}

	private static void applyConfIfPresent(String config, Consumer<? super String> configFn) {
		// Empty is valid, since the user may have just created a new one by clicking
		// (+), but not
		// entered anything. So just don't pass it along.
		if (!StringUtils.isEmpty(config)) {
			configFn.accept(config);
		}
	}

	@SuppressWarnings("ReturnValueIgnored")
	private static void applyConfIfPresent(String configName, String configValue,
			BiFunction<String, String, ?> configFn) {
		// Both being empty is valid, since the user may have just created a new one by
		// clicking (+), but not
		// entered anything. So just don't pass it along.
		// Just one being empty is taken care of by the init method.
		if (!StringUtils.isEmpty(configName) && !StringUtils.isEmpty(configValue)) {
			configFn.apply(configName, configValue);
		}
	}

	@Override
	public boolean waitForCompletion() throws InterruptedException {
		if (livyConfigs.waitForCompletion) {
			if (timeout > 0) {
				return latch.await(timeout, TimeUnit.MILLISECONDS);
			} else {
				latch.await();
				return true;
			}
		}
		return true;
	}

	@Override
	public void close() {
		// No op
	}
/*
	private class AppListener implements SparkAppHandle.Listener {
		@Override
		public void stateChanged(SparkAppHandle handle) {
			if (appCompleted(handle)) {
				latch.countDown();
			}
		}

		@Override
		public void infoChanged(SparkAppHandle handle) { // NOSONAR
		}

		private boolean appCompleted(SparkAppHandle handle) {
			return handle.getState() == SparkAppHandle.State.FAILED
					|| handle.getState() == SparkAppHandle.State.FINISHED
					|| handle.getState() == SparkAppHandle.State.KILLED;
		}
	}
*/
}
