package com.github.mnuessler.influxdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.junit.Test;

public class LineProtocolEncoderTest {

    private static final long TIMESTAMP = 1484385081215L;

	private final LineProtocolEncoder encoder = new LineProtocolEncoder();

	@Test
	public void testEncodeSkipEmptyFields() throws Exception {
	    // Given
        Map<String, String> tags = Collections.singletonMap("host", "server01");
        Map<String, Double> fields = Collections.emptyMap();

        // When
        String line = encoder.encode("foo", fields, tags, TIMESTAMP);

        // Then
        assertThat(line).isEmpty();
    }

    @Test
    public void testEncodeSkipUnsupportedType() throws Exception {
        // Given
        Map<String, String> tags = Collections.singletonMap("host", "server01");
        Map<String, ?> fields = Collections.singletonMap("value", Collections.singletonList("elem1"));

        // When
        String line = encoder.encode("foo", fields, tags, TIMESTAMP);

        // Then
        assertThat(line).isEmpty();
    }

    @Test
    public void testEncodeSkipInvalidNumberNaN() throws Exception {
       testEncodeInvalidDouble(Double.NaN);
    }

    @Test
    public void testEncodeSkipInvalidNumberPositiveInfinity() throws Exception {
      testEncodeInvalidDouble(Double.POSITIVE_INFINITY);
    }

    @Test
    public void testEncodeSkipInvalidNumberNegativeInfinity() throws Exception {
        testEncodeInvalidDouble(Double.NEGATIVE_INFINITY);
    }

    @Test
    public void testEncodeIntegerFieldNoTags() throws Exception {
	    testSingleValueNoTags(Integer.MAX_VALUE, "%s %s=%si %s%n");
    }

    @Test
    public void testEncodeDoubleFieldNoTags() throws Exception {
	    // TODO check double number formatting in previous implementation
        testSingleValueNoTags(1.976, "%s %s=%s %s%n");
    }

    @Test
    public void testEncodeBooleanFieldTrueNoTags() throws Exception {
        testSingleValueNoTags(Boolean.TRUE, "%s %s=%s %s%n");
    }

    @Test
    public void testEncodeBooleanFieldFalseNoTags() throws Exception {
        testSingleValueNoTags(Boolean.FALSE, "%s %s=%s %s%n");
    }

    @Test
    public void testEncodeStringFieldNoTags() throws Exception {
        // Given
        String measurement = "foo";
        String fieldName = "value";
        String fieldValue = "bar";
        Map<String, String> tags = Collections.emptyMap();
        Map<String, ?> fields = Collections.singletonMap(fieldName, fieldValue);

        // When
        String line = encoder.encode("foo", fields, tags, TIMESTAMP);

        // Then
        String expected = String.format("%s %s=\"%s\" %s%n", measurement, fieldName, fieldValue, TIMESTAMP);
        assertThat(line).isEqualTo(expected);
	}

    @Test
    public void testEncodeStringFieldSingleTag() throws Exception {
        // Given
        String measurement = "foo";
        String tagKey = "host";
        String tagValue = "server01";
        String fieldName = "value";
        Double fieldValue = 0.1;
        Map<String, String> tags = Collections.singletonMap(tagKey, tagValue);
        Map<String, ?> fields = Collections.singletonMap(fieldName, fieldValue);

        // When
        String line = encoder.encode("foo", fields, tags, TIMESTAMP);

        // Then
        String expected = String.format("%s,%s=%s %s=%s %s%n", measurement, tagKey, tagValue, fieldName, fieldValue, TIMESTAMP);
        assertThat(line).isEqualTo(expected);
    }

    @Test
    public void testEncodeStringFieldMultipleTags() throws Exception {
        // Given
        String measurement = "foo";
        String tagKey1 = "host";
        String tagValue1 = "server01";
        String tagKey2 = "service";
        String tagValue2 = "monolith";
        String fieldName = "value";
        Integer fieldValue = 42;
        Map<String, String> tags = new TreeMap<>();
        tags.put(tagKey1, tagValue1);
        tags.put(tagKey2, tagValue2);
        Map<String, ?> fields = Collections.singletonMap(fieldName, fieldValue);

        // When
        String line = encoder.encode("foo", fields, tags, TIMESTAMP);

        // Then
		String expected = String.format(
				"%s,%s=%s,%s=%s %s=%si %s%n",
				measurement,
				tagKey1,
				tagValue1,
				tagKey2,
				tagValue2,
				fieldName,
				fieldValue,
				TIMESTAMP);
        assertThat(line).isEqualTo(expected);
    }

	@Test
    public void testEncodeEscapeMeasurement() {
        // Given
        String measurement = "a,b cdef";
        String fieldName = "foo";
        Integer fieldValue = 1;
        Map<String, String> tags = Collections.emptyMap();
        Map<String, ?> fields = Collections.singletonMap(fieldName, fieldValue);

        // When
        String line = encoder.encode(measurement, fields, tags, TIMESTAMP);

        // Then
        String escapedMeasurement = "a\\,b\\ cdef";
		String expectedLine = String.format("%s %s=%si %s%n", escapedMeasurement, fieldName, fieldValue, TIMESTAMP);
        assertThat(line).isEqualTo(expectedLine);
    }

    @Test
    public void testEncodeEscapeFieldName() {
        // Given
        String measurement = "foo";
        String fieldName = " value,bar=baz";
        Integer fieldValue = 1;
        Map<String, String> tags = Collections.emptyMap();
        Map<String, ?> fields = Collections.singletonMap(fieldName, fieldValue);

        // When
        String line = encoder.encode(measurement, fields, tags, TIMESTAMP);

        // Then
        String escapedFieldName = "\\ value\\,bar\\=baz";
        String expectedLine = String.format("%s %s=%si %s%n", measurement, escapedFieldName, fieldValue, TIMESTAMP);
        assertThat(line).isEqualTo(expectedLine);
    }

    @Test
    public void testEncodeEscapeStringFieldValue() {
        // Given
        String measurement = "weather";
        String fieldName = "temperature";
        String fieldValue = "too \"hot\",a=1";
        Map<String, String> tags = Collections.emptyMap();
        Map<String, ?> fields = Collections.singletonMap(fieldName, fieldValue);

        // When
        String line = encoder.encode(measurement, fields, tags, TIMESTAMP);

        // Then
        String escapedFieldValue = "too \\\"hot\\\",a=1";
        String expectedLine = String.format("%s %s=\"%s\" %s%n", measurement, fieldName, escapedFieldValue, TIMESTAMP);
        assertThat(line).isEqualTo(expectedLine);
    }

    @Test
    public void testEncodeEscapeTagKey() {
        // Given
        String measurement = "foo";
        String tagKey = "host foo=bar,baz";
        String tagValue = "server01";
        String fieldName = "value";
        Integer fieldValue = 1;
        Map<String, String> tags = Collections.singletonMap(tagKey, tagValue);
        Map<String, ?> fields = Collections.singletonMap(fieldName, fieldValue);

        // When
        String line = encoder.encode(measurement, fields, tags, TIMESTAMP);

        // Then
        String escapedTagKey = "host\\ foo\\=bar\\,baz";
		String expectedLine = String.format("%s,%s=%s %s=%si %s%n", measurement, escapedTagKey, tagValue, fieldName, fieldValue, TIMESTAMP);
        assertThat(line).isEqualTo(expectedLine);
    }

    @Test
    public void testEncodeEscapeTagValue() {
        // Given
        String measurement = "foo";
        String tagKey = "host";
        String tagValue = "server 1,bar=baz";
        String fieldName = "value";
        Integer fieldValue = 1;
        Map<String, String> tags = Collections.singletonMap(tagKey, tagValue);
        Map<String, ?> fields = Collections.singletonMap(fieldName, fieldValue);

        // When
        String line = encoder.encode(measurement, fields, tags, TIMESTAMP);

        // Then
        String escapedTagValue = "server\\ 1\\,bar\\=baz";
        String expectedLine = String.format("%s,%s=%s %s=%si %s%n", measurement, tagKey, escapedTagValue, fieldName, fieldValue, TIMESTAMP);
        assertThat(line).isEqualTo(expectedLine);
    }

    private void testEncodeInvalidDouble(Double value) {
        // Given
        long timestamp = System.currentTimeMillis();
        Map<String, String> tags = Collections.singletonMap("host", "server01");
        Map<String, Double> fields = Collections.singletonMap("value", value);

        // When
        String line = encoder.encode("foo", fields, tags, timestamp);

        // Then
        assertThat(line).isEmpty();
    }

    private void testSingleValueNoTags(Object fieldValue, String expectedFormat) {
        // Given
        String measurement = "foo";
        String fieldName = "value";
        Map<String, String> tags = Collections.emptyMap();
        Map<String, ?> fields = Collections.singletonMap(fieldName, fieldValue);

        // When
        String line = encoder.encode(measurement, fields, tags, TIMESTAMP);

        // Then
        String expected = String.format(expectedFormat, measurement, fieldName, fieldValue, TIMESTAMP);
        assertThat(line).isEqualTo(expected);
    }

}
