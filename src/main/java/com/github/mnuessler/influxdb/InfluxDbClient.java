package com.github.mnuessler.influxdb;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface InfluxDbClient {

	String CONTENT_TYPE = "application/influxdb-line";

	Charset CHARSET = StandardCharsets.UTF_8;

	void write(@Nonnull CharSequence payload, @Nonnull String database, @Nullable String retentionPolicy) throws IOException;

}
