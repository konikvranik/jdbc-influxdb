package net.suteren.jdbc.influxdb;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class InfluxDbConnectionTest {

	private InfluxDbConnection connection;

	@Before
	public void setUp() throws Exception {
		connection = new InfluxDbConnection("http://localhost:8086", "user", "password", "database", new InfluxDbDriver());
	}

	@Test public void testNativeSql() {
		assertEquals("SELECT * from measure", connection.nativeSQL("SELECT * from measure"));
		assertEquals("SELECT * from \"measure\"", connection.nativeSQL("SELECT * from \"measure\""));
		assertEquals("SELECT * from \\\"measure\\\"", connection.nativeSQL("SELECT * from \\\"measure\\\""));

		assertEquals("SELECT * from \\\"measure\\\"", connection.nativeSQL("SELECT * from \"\"measure\"\""));

		assertEquals("SELECT * FROM measure", connection.nativeSQL("SELECT * from test.measure"));
	}
}