import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Iterator;
import java.util.List;
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
import net.suteren.jdbc.influxdb.InfluxDbMetadata;
import net.suteren.jdbc.influxdb.resultset.proxy.AbstractProxyResultSet;
import net.suteren.jdbc.influxdb.resultset.proxy.GetTablesResultSet;

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
		influxDB.write(Point.measurement("measurement1").addField("field1", 13).tag("tag1", "tag1value").build());
		influxDB.write(Point.measurement("measurement1").addField("field2", 13).tag("tag2", "tag2value").build());
		influxDB.write(Point.measurement("measurement2").addField("field2", 13).tag("tag2", "tag2value").build());
		influxDB.write(Point.measurement("measurement2").addField("field3", 13).tag("tag3", "tag3value").build());
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
		String database = "test";
		String url = influxDbContainer.getUrl();
		try (InfluxDbConnection conn =
			influxDbDriver.connect(String.format("jdbc:influxdb:%s?db=%s", url, database), properties)) {
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

	private static void assertConnectionMetadata(InfluxDbMetadata metaData) throws SQLException {
		GetTablesResultSet tables = metaData.getTables(null, null, null, null);
		assertTrue(tables.isBeforeFirst());
		assertFalse(tables.isFirst());
		assertFalse(tables.isLast());
		assertFalse(tables.isAfterLast());
		Iterator<String> tableNames = List.of("measurement1", "measurement2").iterator();
		while (tables.next()) {
			assertTable(tables, tableNames.next());
		}
		assertFalse(tables.isBeforeFirst());
		assertFalse(tables.isFirst());
		assertFalse(tables.isLast());
		assertTrue(tables.isAfterLast());

		assertFalse(tables.getStatement().getMoreResults());

		tables = metaData.getTables(null, null, "%", null);
		tableNames = List.of("measurement1", "measurement2").iterator();
		while (tables.next()) {
			assertTable(tables, tableNames.next());
		}
		assertFalse(tables.getStatement().getMoreResults());

		AbstractProxyResultSet columns = metaData.getColumns(null, null, null, null);
		Iterator<String[]> expectations = List.of(
			new String[] { "measurement1", "field1", "integer" },
			new String[] { "measurement1", "field2", "integer" },
			new String[] { "measurement2", "field2", "integer" },
			new String[] { "measurement2", "field3", "integer" },
			new String[] { "measurement1", "tag1", "string" },
			new String[] { "measurement1", "tag2", "string" },
			new String[] { "measurement2", "tag2", "string" },
			new String[] { "measurement2", "tag3", "string" }
		).iterator();
		while (columns.next()) {
			String[] ex = expectations.next();
			assertField(columns, ex[0], ex[1], ex[2]);
		}

		columns = metaData.getColumns(null, null, "%", null);
		expectations = List.of(
			new String[] { "measurement1", "field1", "integer" },
			new String[] { "measurement1", "field2", "integer" },
			new String[] { "measurement2", "field2", "integer" },
			new String[] { "measurement2", "field3", "integer" },
			new String[] { "measurement1", "tag1", "string" },
			new String[] { "measurement1", "tag2", "string" },
			new String[] { "measurement2", "tag2", "string" },
			new String[] { "measurement2", "tag3", "string" }
		).iterator();
		while (columns.next()) {
			String[] ex = expectations.next();
			assertField(columns, ex[0], ex[1], ex[2]);
		}
		assertTrue(columns.isAfterLast());
		expectations = List.of(
			new String[] { "measurement2", "tag3", "string" },
			new String[] { "measurement2", "tag2", "string" },
			new String[] { "measurement1", "tag2", "string" },
			new String[] { "measurement1", "tag1", "string" },
			new String[] { "measurement2", "field3", "integer" },
			new String[] { "measurement2", "field2", "integer" },
			new String[] { "measurement1", "field2", "integer" },
			new String[] { "measurement1", "field1", "integer" }
		).iterator();
		while (columns.previous()) {
			String[] ex = expectations.next();
			assertField(columns, ex[0], ex[1], ex[2]);
		}

		columns = metaData.getColumns(null, null, "measurement1", null);
		expectations = List.of(
			new String[] { "measurement1", "field1", "integer" },
			new String[] { "measurement1", "field2", "integer" },
			new String[] { "measurement1", "tag1", "string" },
			new String[] { "measurement1", "tag2", "string" }
		).iterator();
		while (columns.next()) {
			String[] ex = expectations.next();
			assertField(columns, ex[0], ex[1], ex[2]);
		}

	}

	private static void assertTable(ResultSet tables, String tableName) throws SQLException {
		assertNull(tables.getString("TABLE_CAT"));
		assertNull(tables.getString("TABLE_SCHEM"));
		assertEquals(tableName, tables.getString("TABLE_NAME"));
		assertEquals("TABLE", tables.getString("TABLE_TYPE"));
		assertNull(tables.getString("REMARKS"));
		assertNull(tables.getString("TYPE_CAT"));
		assertNull(tables.getString("TYPE_SCHEM"));
		assertNull(tables.getString("TYPE_NAME"));
		assertNull(tables.getString("SELF_REFERENCING_COL_NAME"));
		assertNull(tables.getString("REF_GENERATION"));
	}

	private static void assertField(ResultSet columns, String measurementName, String fieldName, String type)
		throws SQLException {
		assertNull(columns.getString("TABLE_CAT"));
		assertNull(columns.getString("TABLE_SCHEM"));
		assertEquals(measurementName, columns.getString("TABLE_NAME"));
		assertEquals(fieldName, columns.getString("COLUMN_NAME"));
		assertEquals(Types.NUMERIC, columns.getInt("DATA_TYPE"));
		assertEquals(type, columns.getString("TYPE_NAME"));
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
		assertEquals(measurementName, columns.getString("SCOPE_TABLE"));
		assertEquals(Types.NUMERIC, columns.getInt("SOURCE_DATA_TYPE"));
		assertNull(columns.getObject("IS_AUTOINCREMENT"));
		assertNull(columns.getObject("IS_GENERATEDCOLUMN"));
	}
}
