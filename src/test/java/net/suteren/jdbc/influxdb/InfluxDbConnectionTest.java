package net.suteren.jdbc.influxdb;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class InfluxDbConnectionTest {

	@Test
	public void testNativeSql() {
		InfluxDbConnection c = new InfluxDbConnection("http://localhost:8086", "user", "password", "database", new InfluxDbDriver());
		String result = c.nativeSQL("SELECT * from measure");
		assertEquals("SELECT * from measure", result);
	}
}