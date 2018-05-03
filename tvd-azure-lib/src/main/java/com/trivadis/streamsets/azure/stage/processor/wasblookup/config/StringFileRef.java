package com.trivadis.streamsets.azure.stage.processor.wasblookup.config;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Set;

import com.streamsets.pipeline.api.FileRef;
import com.streamsets.pipeline.api.ProtoConfigurableEntity.Context;

public class StringFileRef extends FileRef {
	private final String data;

	public StringFileRef(String data) {
		super(data.length());
		this.data = data;
	}

	private final Set<Class<? extends AutoCloseable>> supportedStreamClasses = Collections.singleton(InputStream.class);

	@Override
	public <T extends AutoCloseable> Set<Class<T>> getSupportedStreamClasses() {
		return (Set) supportedStreamClasses;
	}

	@Override
	public <T extends AutoCloseable> T createInputStream(Context context, Class<T> aClass) throws IOException {
		return (T) new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8.name()));
	}
}
