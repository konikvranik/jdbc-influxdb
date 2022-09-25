import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Properties;

import org.influxdb.InfluxDB;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.InfluxDBContainer;
import org.testcontainers.utility.DockerImageName;

import net.suteren.jdbc.influxdb.InfluxDbConnection;
import net.suteren.jdbc.influxdb.InfluxDbDriver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class DriverTest<SELF extends InfluxDBContainer<SELF>> {

	public static final String USERNAME = "admin";
	private static final String PASSWORD = "password";
	public static final String DATABASE = "test";
	@Rule public InfluxDBContainer<SELF> influxDbContainer =
		new InfluxDBContainer<>(DockerImageName.parse("influxdb:1.8"));
	private InfluxDB influxDB;

	@Before
	public void setUp() {
		influxDB = influxDbContainer.withUsername(USERNAME).withPassword(PASSWORD).getNewInfluxDB();
		influxDB.query(new Query(String.format("CREATE DATABASE %s;", DATABASE)));
		influxDB.setDatabase(DATABASE);
		Point point = Point.measurement("measurement1").addField("field1", 13).tag("tag1", "tag1value").build();
		influxDB.write(point);
	}

	@After
	public void tearDown() {
		influxDB.close();
	}

	@Test public void someTestMethod() throws SQLException {
		InfluxDbDriver influxDbDriver = new InfluxDbDriver();
		Properties properties = new Properties();
		properties.put("username", USERNAME);
		properties.put("password", PASSWORD);
		properties.put("database", DATABASE);
		try (InfluxDbConnection conn =
			influxDbDriver.connect(String.format("jdbc:influxdb:%s?db=test", influxDbContainer.getUrl()), properties)) {
			assertTrue(conn.isValid(1));
			assertConnectionMetadata(conn.getMetaData());
			ResultSet r = conn.createStatement().executeQuery("show databases");
			assertResultMetadata(r.getMetaData());
			assertTrue(r.isBeforeFirst());
			assertTrue(r.first());
			assertTrue(r.isFirst());
			assertEquals("test", r.getString(1));
			assertEquals("test", r.getString("name"));
			assertEquals("test", r.getString("NAME"));
			assertFalse(r.getStatement().getMoreResults());
		}

	}

	private static void assertResultMetadata(ResultSetMetaData metaData) throws SQLException {
		assertEquals(1, metaData.getColumnCount());
		assertEquals("name", metaData.getColumnName(1));
		assertEquals("name", metaData.getColumnLabel(1));
		assertEquals("databases", metaData.getTableName(1));
	}

	private static void assertConnectionMetadata(DatabaseMetaData metaData) throws SQLException {
		ResultSet tables = metaData.getTables(null, null, null, null);
		while (tables.next()) {
			tables.getString(1);
			tables.getString(2);
			tables.getString(3);
			tables.getString(4);
		}
		assertFalse(tables.getStatement().getMoreResults());

		ResultSet columns = metaData.getColumns(null, null, null, null);
		while (columns.next()) {

			assertNull(columns.getString("TABLE_CAT"));
			assertNull(columns.getString("TABLE_SCHEM"));
			assertEquals("measurement1", columns.getString("TABLE_NAME"));
			assertEquals("field1", columns.getString("COLUMN_NAME"));
			assertEquals("integer", columns.getString("DATA_TYPE"));
			assertEquals("integer", columns.getString("TYPE_NAME"));
			assertNull(columns.getString("COLUMN_SIZE"));
			assertNull(columns.getString("BUFFER_LENGTH"));
			assertNull(columns.getString("DECIMAL_DIGITS"));
			assertNull(columns.getString("NUM_PREC_RADIX"));
			assertTrue(columns.getBoolean("NULLABLE"));
			assertNull(columns.getString("REMARKS"));
			assertNull(columns.getString("COLUMN_DEF"));
			assertNull(columns.getString("SQL_DATA_TYPE"));
			assertNull(columns.getString("SQL_DATETIME_SUB"));
			assertNull(columns.getString("CHAR_OCTET_LENGTH"));
			assertNull(columns.getString("ORDINAL_POSITION"));
			assertTrue(columns.getBoolean("IS_NULLABLE"));
			assertNull(columns.getString("SCOPE_CATALOG"));
			assertNull(columns.getString("SCOPE_SCHEMA"));
			assertEquals("measurement1", columns.getString("SCOPE_TABLE"));
			assertEquals("integer", columns.getString("SOURCE_DATA_TYPE"));
			assertNull(columns.getObject("IS_AUTOINCREMENT"));
			assertNull(columns.getObject("IS_GENERATEDCOLUMN"));

			columns.getString(1);
			columns.getString(2);
			columns.getString(3);
			columns.getString(4);
		}

	}
}
