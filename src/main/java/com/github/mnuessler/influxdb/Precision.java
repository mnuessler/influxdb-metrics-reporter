package com.github.mnuessler.influxdb;

enum Precision {

	NANOSECONDS("n"), MICROSECONDS("u"), MILLISECONDS("ms"), SECONDS("s"), MINUTES("m"), HOURS("h");

	private final String unit;

	Precision(final String unit) {
		this.unit = unit;
	}

	String getUnit() {
        return unit;
    }
}
