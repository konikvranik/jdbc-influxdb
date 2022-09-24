import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.influxdb.InfluxDB;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.InfluxDBContainer;
import org.testcontainers.utility.DockerImageName;

import net.suteren.jdbc.influxdb.InfluxDbConnection;
import net.suteren.jdbc.influxdb.InfluxDbDriver;

import static org.junit.Assert.assertTrue;

public class DriverTest<SELF extends InfluxDBContainer<SELF>> {

	public static final String USERNAME = "admin";
	private static final String PASSWORD = "password";
	@Rule public InfluxDBContainer<SELF> influxDbContainer =
		new InfluxDBContainer<>(DockerImageName.parse("influxdb:1.8"));
	private InfluxDB influxDB;

	@Before
	public void setUp() throws Exception {
		influxDB = influxDbContainer.withUsername(USERNAME).withPassword(PASSWORD).getNewInfluxDB();
	}

	@After
	public void tearDown() throws Exception {
		influxDB.close();
	}

	@Test public void someTestMethod() throws SQLException {
		InfluxDbDriver influxDbDriver = new InfluxDbDriver();
		Properties properties = new Properties();
		properties.put("username", USERNAME);
		properties.put("password", PASSWORD);
		InfluxDbConnection conn =
			influxDbDriver.connect(String.format("jdbc:influxdb:%s?db=test", influxDbContainer.getUrl()), properties);
		assertTrue(conn.isValid(1));
		ResultSet r = conn.createStatement().executeQuery("select * from test");

	}
}
