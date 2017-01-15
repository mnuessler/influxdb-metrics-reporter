package com.github.mnuessler.metrics;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.github.mnuessler.influxdb.InfluxDbClient;
import com.github.mnuessler.influxdb.LineProtocolEncoder;

public class InfluxDbReporter extends ScheduledReporter {

	public static Builder forRegistry(@Nonnull final MetricRegistry registry) {
		return new Builder(registry);
	}

	public static class Builder {

		private final MetricRegistry registry;
		private MetricFilter filter = MetricFilter.ALL;
		private String name;
		private InfluxDbClient client;
		private String database;
		private String retentionPolicy = "default";
		private TimeUnit rateUnit = TimeUnit.SECONDS;
		private TimeUnit durationUnit = TimeUnit.MILLISECONDS;
		private final SortedMap<String, String> tags = new TreeMap<>();

		private Builder(@Nonnull final MetricRegistry registry) {
			this.registry = registry;
		}

		public Builder withName(@Nonnull final String name) {
			this.name = name;
			return this;
		}

		public Builder withFilter(@Nullable final MetricFilter filter) {
			if (filter != null) {
				this.filter = filter;
			}
			return this;
		}

		public Builder withInfluxDbClient(@Nonnull final InfluxDbClient client) {
			this.client = client;
			return this;
		}

		public Builder withDatabase(@Nonnull final String database) {
			this.database = database;
			return this;
		}

		public Builder withRetentionPolicy(@Nullable final String retentionPolicy) {
			if (retentionPolicy != null) {
				this.retentionPolicy = retentionPolicy;
			}
			return this;
		}

		public Builder withTag(@Nonnull final String key, @Nonnull final String value) {
			return withTags(Collections.singletonMap(key, value));
		}

		public Builder withTags(@Nonnull final Map<String, String> tags) {
			this.tags.putAll(tags);
			return this;
		}

		public InfluxDbReporter build() {
			return new InfluxDbReporter(registry, name, filter, rateUnit, durationUnit, database, retentionPolicy, client, tags);
		}
	}

	private static final Logger LOG = LoggerFactory.getLogger(InfluxDbReporter.class);

	private final LineProtocolEncoder encoder = new LineProtocolEncoder();

	private final MetricFilter filter;

	private final SortedMap<String, String> tags;

	private final String database;

	private final String retentionPolicy;

	private final InfluxDbClient client;

	private final AtomicInteger bufferInitialCapacity = new AtomicInteger(500);

	private InfluxDbReporter(MetricRegistry registry, String name, MetricFilter filter, TimeUnit rateUnit, TimeUnit durationUnit,
			String database, String retentionPolicy, InfluxDbClient client, SortedMap<String, String> tags) {
		super(registry, name, filter, rateUnit, durationUnit);
		this.filter = filter;
		this.database = database;
		this.retentionPolicy = retentionPolicy;
		this.client = client;
		this.tags = tags;
	}

	public void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters, SortedMap<String, Histogram> histograms,
			SortedMap<String, Meter> meters, SortedMap<String, Timer> timers) {
		long now = System.currentTimeMillis();
		int capacity = bufferInitialCapacity.get();
		StringBuilder buffer = new StringBuilder(capacity);

		appendGauges(buffer, gauges, now);
		appendCounters(buffer, counters, now);
		appendHistograms(buffer, histograms, now);
		appendMeters(buffer, meters, now);
		appendTimers(buffer, timers, now);

		bufferInitialCapacity.compareAndSet(capacity, Math.max(capacity, buffer.length()));
		LOG.trace("Payload: \n{}", buffer);
		try {
			client.write(buffer, database, retentionPolicy);
		} catch (Exception e) {
			LOG.info("Failed to send metrics to InfluxDB", e);
		}
	}

	private void appendGauges(StringBuilder buffer, SortedMap<String, Gauge> gauges, long timestamp) {
		for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
			String measurement = entry.getKey();
			Gauge gauge = entry.getValue();
			if (!filter.matches(measurement, gauge)) {
				continue;
			}

			Map<String, Object> fields = Collections.singletonMap("value", gauge.getValue());
			encoder.encodeInto(buffer, measurement, fields, tags, timestamp);
		}
	}

	private void appendCounters(StringBuilder buffer, SortedMap<String, Counter> counters, long timestamp) {
		for (Map.Entry<String, Counter> entry : counters.entrySet()) {
			String measurement = entry.getKey();
			Counter counter = entry.getValue();
			if (!filter.matches(measurement, counter)) {
				continue;
			}

			Map<String, ?> fields = Collections.singletonMap("count", counter.getCount());
			encoder.encodeInto(buffer, measurement, fields, tags, timestamp);
		}
	}

	private void appendHistograms(final StringBuilder buffer, final SortedMap<String, Histogram> histograms, final long timestamp) {
		for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
			String measurement = entry.getKey();
			Histogram histogram = entry.getValue();
			Snapshot snapshot = histogram.getSnapshot();
			if (!filter.matches(measurement, histogram)) {
				continue;
			}

			Map<String, Object> fields = new HashMap<>();
			fields.put("min", snapshot.getMin());
			fields.put("max", snapshot.getMax());
			fields.put("mean", snapshot.getMean());
			fields.put("median", snapshot.getMedian());
			fields.put("std-dev", snapshot.getStdDev());
			fields.put("count", histogram.getCount());
			fields.put("75-percentile", convertDuration(snapshot.get75thPercentile()));
			fields.put("95-percentile", snapshot.get95thPercentile());
			fields.put("98-percentile", snapshot.get98thPercentile());
			fields.put("99-percentile", snapshot.get99thPercentile());
			fields.put("999-percentile", snapshot.get999thPercentile());

			encoder.encodeInto(buffer, measurement, fields, tags, timestamp);
		}
	}

	private void appendMeters(StringBuilder buffer, SortedMap<String, Meter> meters, long timestamp) {
		for (Map.Entry<String, Meter> entry : meters.entrySet()) {
			String measurement = entry.getKey();
			Meter meter = entry.getValue();
			if (!filter.matches(measurement, meter)) {
				continue;
			}

			Map<String, Object> fields = new HashMap<>();
			fields.put("count", meter.getCount());
			fields.put("mean-rate", convertRate(meter.getMeanRate()));
			fields.put("1-min-rate", convertRate(meter.getOneMinuteRate()));
			fields.put("5-min-rate", convertRate(meter.getFiveMinuteRate()));
			fields.put("15-min-rate", convertRate(meter.getFifteenMinuteRate()));

			encoder.encodeInto(buffer, measurement, fields, tags, timestamp);
		}
	}

	private void appendTimers(StringBuilder buffer, SortedMap<String, Timer> timers, long timestamp) {
		for (Map.Entry<String, Timer> entry : timers.entrySet()) {
			String measurement = entry.getKey();
			Timer timer = entry.getValue();
			Snapshot snapshot = timer.getSnapshot();
			if (!filter.matches(measurement, timer)) {
				continue;
			}

			Map<String, Object> fields = new HashMap<>();
			fields.put("count", timer.getCount());
			fields.put("mean-rate", convertRate(timer.getMeanRate()));
			fields.put("1-min-rate", convertRate(timer.getOneMinuteRate()));
			fields.put("5-min-rate", convertRate(timer.getFiveMinuteRate()));
			fields.put("15-min-rate", convertRate(timer.getFifteenMinuteRate()));
			fields.put("75-percentile", snapshot.get75thPercentile());
			fields.put("95-percentile", snapshot.get95thPercentile());
			fields.put("98-percentile", snapshot.get98thPercentile());
			fields.put("99-percentile", snapshot.get99thPercentile());
			fields.put("999-percentile", snapshot.get999thPercentile());
			fields.put("max", snapshot.getMax());
			fields.put("mean", snapshot.getMean());
			fields.put("median", snapshot.getMedian());
			fields.put("min", snapshot.getMin());
			fields.put("std-dev", snapshot.getStdDev());

			encoder.encodeInto(buffer, measurement, fields, tags, timestamp);
		}
	}
}
