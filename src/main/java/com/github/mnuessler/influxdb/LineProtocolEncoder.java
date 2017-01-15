package com.github.mnuessler.influxdb;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.NumberFormat;
import java.util.Locale;
import java.util.Map;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LineProtocolEncoder {

	private static final Logger LOG = LoggerFactory.getLogger(LineProtocolEncoder.class);

	public String encode(@Nonnull final String measurement, @Nonnull final Map<String, ?> fields, @Nonnull final Map<String, String> tags,
			final long timestamp) {
		StringBuilder buffer = new StringBuilder();
		encodeInto(buffer, measurement, fields, tags, timestamp);
		return buffer.toString();
	}

	public void encodeInto(@Nonnull final StringBuilder buffer, @Nonnull final String measurement, @Nonnull final Map<String, ?> fields,
			@Nonnull final Map<String, String> tags, final long timestamp) {
		if (fields.isEmpty()) {
			LOG.debug("Skipping measurement '{}' because no field given", measurement);
			return;
		}
		if (containsInvalidValue(fields)) {
			LOG.debug("Skipping measurement '{}' because of an invalid field value. Fields: {}", measurement, fields);
			return;
		}

		appendEscapedMeasurement(buffer, measurement);

		// tags are optional
		if (!tags.isEmpty()) {
			buffer.append(',');
			for (Map.Entry<String, String> entry : tags.entrySet()) {
				appendEscapedKey(buffer, entry.getKey());
				buffer.append('=');
				appendEscapedKey(buffer, entry.getValue());
				buffer.append(',');
			}
			buffer.deleteCharAt(buffer.length() - 1);
		}
		buffer.append(' ');

		// fields
        NumberFormat nf = NumberFormat.getInstance(Locale.ENGLISH);
        for (Map.Entry<String, ?> entry : fields.entrySet()) {
			String fieldName = entry.getKey();
			Object fieldValue = entry.getValue();
			appendEscapedKey(buffer, fieldName);
			buffer.append('=');

			if (isFloatingPointNumber(fieldValue)) {
				buffer.append(nf.format(fieldValue));
			} else if (isIntegerNumber(fieldValue)) {
				buffer.append(fieldValue).append('i');
			} else if (isBoolean(fieldValue)) {
				// 'TRUE', 'FALSE', 'true', 'false', 't', 'f' are all valid for booleans
				//buffer.append((Boolean) fieldValue ? 't' : 'f');
                buffer.append(fieldValue);
			} else if (isString(fieldValue)) {
				appendEscapeFieldStringValue(buffer, fieldValue.toString());
			}

			buffer.append(',');
		}
		buffer.deleteCharAt(buffer.length() - 1);

		// time
		buffer.append(' ');
		buffer.append(timestamp);
		buffer.append('\n');
	}

	private static void appendEscapedMeasurement(StringBuilder buffer, String measurement) {
		for (char c : measurement.toCharArray()) {
			if (c == ',' || c == ' ') {
				buffer.append('\\');
			}
			buffer.append(c);
		}
	}

	private static void appendEscapeFieldStringValue(StringBuilder buffer, String value) {
		buffer.append('"');
		for (char c : value.toCharArray()) {
			if (c == '"') {
				buffer.append('\\');
			}
			buffer.append(c);
		}
		buffer.append('"');
	}

	private static void appendEscapedKey(StringBuilder buffer, String value) {
		for (char c : value.toCharArray()) {
			if (c == ',' || c == '=' || c == ' ') {
				buffer.append('\\');
			}
			buffer.append(c);
		}
	}

	private static boolean containsInvalidValue(Map<String, ?> fields) {
		for (Map.Entry<String, ?> entry : fields.entrySet()) {
			Object value = entry.getValue();
			if (!isSupportedType(value) || isInvalidNumber(value)) {
				return true;
			}
		}
		return false;
	}

	private static boolean isIntegerNumber(Object fieldValue) {
		return (fieldValue instanceof Integer) || (fieldValue instanceof Long || (fieldValue instanceof BigInteger));
	}

	private static boolean isFloatingPointNumber(Object fieldValue) {
		return (fieldValue instanceof Double) || (fieldValue instanceof Float) || (fieldValue instanceof BigDecimal);
	}

	private static boolean isBoolean(Object fieldValue) {
		return (fieldValue instanceof Boolean);
	}

	private static boolean isString(Object fieldValue) {
		return (fieldValue instanceof CharSequence);
	}

	private static boolean isSupportedType(Object fieldValue) {
		return isFloatingPointNumber(fieldValue) || isIntegerNumber(fieldValue) || isBoolean(fieldValue) || isString(fieldValue);
	}

	private static boolean isInvalidNumber(Object fieldValue) {
		if (fieldValue instanceof Double) {
			Double d = (Double) fieldValue;
			return d.isNaN() || d.isInfinite();
		}
		return false;
	}
}
