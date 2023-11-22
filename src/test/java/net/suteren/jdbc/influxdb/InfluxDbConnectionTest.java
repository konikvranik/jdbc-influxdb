package net.suteren.jdbc.influxdb;

import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class InfluxDbConnectionTest {

	private InfluxDbConnection connection;

	@BeforeEach
	public void setUp() {
		connection = new InfluxDbConnection("http://localhost:8086", "user", "password", "database", new InfluxDbDriver());
	}

	public static Stream<Arguments> sqlSamples() {
		return Stream.of(
			Arguments.arguments("select * from measure", "select * from measure"),
			Arguments.arguments("select * from \"measure\"", "select * from \"measure\""),
			Arguments.arguments("select * from \\\"measure\\\"", "select * from \\\"measure\\\""),
			Arguments.arguments("select * from \\\"measure\\\"", "select * from \"\"measure\"\""),
			Arguments.arguments("select * from \"\\\"measure\\\"\"", "select * from \"\"\"measure\"\"\""),
			Arguments.arguments("select * from test.measure", "select * from test.measure"),
			Arguments.arguments("select * from jmeter.jmeter where timestamp > now() - \"1 day\"", "select * from jmeter.jmeter where timestamp > now() - \"1 day\""),
			Arguments.arguments("SELECT * FROM jmeter.jmeter\nwhere timestamp > now() - \"2 days\"",
				"select t.*\nfrom jmeter.jmeter t\nwhere timestamp > now() - \"2 days\""),
			Arguments.arguments("SELECT * FROM jmeter.jmeter", "select t.*\nfrom jmeter.jmeter t"),
			Arguments.arguments("SELECT * FROM \"_internal\".cq", "SELECT t.*\n  FROM \"_internal\".cq t"),
			Arguments.arguments("SELECT * FROM \"_internal\".\"measurement\".cq", "SELECT t.*\n  FROM \"_internal\".\"measurement\".cq t")
		);
	}

	@MethodSource("sqlSamples")
	@ParameterizedTest void testNativeSql(String expected, String actual) {
		assertEquals(expected, connection.nativeSQL(actual));
	}
}