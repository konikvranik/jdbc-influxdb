import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;

import org.influxdb.InfluxDB;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.InfluxDBContainer;
import org.testcontainers.utility.DockerImageName;

import net.suteren.jdbc.influxdb.InfluxDbConnection;
import net.suteren.jdbc.influxdb.InfluxDbDriver;
import net.suteren.jdbc.influxdb.resultset.proxy.AbstractProxyResultSet;
import net.suteren.jdbc.influxdb.resultset.proxy.GetCatalogResultSet;
import net.suteren.jdbc.influxdb.resultset.proxy.GetSchemaResultSet;
import net.suteren.jdbc.influxdb.resultset.proxy.GetTablesResultSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class DriverTest {

	public static final String USERNAME = "admin";
	private static final String PASSWORD = "password";
	public static final String DATABASE = "test";
	public static final String DATABASE1 = "test1";
	public static final List<String[]> EXPECTED_FIELDS = List.of(
		new String[] { "measurement1", "field1", "integer" },
		new String[] { "measurement1", "field2", "integer" },
		new String[] { "measurement2", "field2", "integer" },
		new String[] { "measurement2", "field3", "integer" },
		new String[] { "measurement1", "tag1", "string" },
		new String[] { "measurement1", "tag2", "string" },
		new String[] { "measurement2", "tag2", "string" },
		new String[] { "measurement2", "tag3", "string" }
	);
	@ClassRule public static InfluxDBContainer<?> influxDbContainer =
		new InfluxDBContainer<>(DockerImageName.parse("influxdb:1.8"));
	private static InfluxDB influxDB;

	@BeforeClass public static void setUp() {
		influxDB = influxDbContainer.withUsername(USERNAME).withPassword(PASSWORD).getNewInfluxDB();
		influxDB.query(new Query(String.format("CREATE DATABASE %s;", DATABASE)));
		influxDB.query(new Query(String.format("CREATE DATABASE %s;", DATABASE1)));
		influxDB.setDatabase(DATABASE);
		influxDB.write(Point.measurement("measurement1").addField("field1", 13).tag("tag1", "tag1value").build());
		influxDB.write(Point.measurement("measurement1").addField("field2", 13).tag("tag2", "tag2value").build());
		influxDB.write(Point.measurement("measurement2").addField("field2", 13).tag("tag2", "tag2value").build());
		influxDB.write(Point.measurement("measurement2").addField("field3", 13).tag("tag3", "tag3value").build());
	}

	@After public void tearDown() {
		influxDB.close();
	}

	@Test public void testCatalogs() throws SQLException {
		try (InfluxDbConnection conn = connectDb()) {
			Iterator<String> tableNames = List.of(DATABASE, DATABASE1).iterator();
			GetCatalogResultSet catalogs = conn.getMetaData().getCatalogs();
			assertBefore(catalogs);
			while (catalogs.next()) {
				assertEquals(tableNames.next(), catalogs.getString("TABLE_CAT"));
			}
			assertAfter(catalogs);
		}

	}

	@Test public void testSchemas() throws SQLException {
		try (InfluxDbConnection conn = connectDb()) {
			Iterator<String> tableNames = List.of(DATABASE, DATABASE1).iterator();
			GetSchemaResultSet schemas = conn.getMetaData().getSchemas();
			assertBefore(schemas);
			while (schemas.next()) {
				assertSchema(schemas, tableNames.next());
			}
			assertAfter(schemas);
		}
	}

	@Test public void testSchemasOfDb() throws SQLException {
		try (InfluxDbConnection conn = connectDb()) {
			Iterator<String> tableNames = List.of(DATABASE).iterator();
			GetSchemaResultSet schemas = conn.getMetaData().getSchemas(DATABASE, null);
			assertBefore(schemas);
			while (schemas.next()) {
				assertSchema(schemas, tableNames.next());
			}
			assertAfter(schemas);
		}
	}

	@Test public void testTables() throws SQLException {
		try (InfluxDbConnection conn = connectDb()) {
			Iterator<String> tableNames = List.of("measurement1", "measurement2").iterator();
			GetTablesResultSet tables = conn.getMetaData().getTables(null, null, null, null);
			assertBefore(tables);
			while (tables.next()) {
				assertTable(tables, tableNames.next());
			}
			assertAfter(tables);
		}
	}

	@Test public void testTablesWildcard() throws SQLException {
		try (InfluxDbConnection conn = connectDb()) {
			Iterator<String> tableNames = List.of("measurement1", "measurement2").iterator();
			GetTablesResultSet tables = conn.getMetaData().getTables(null, null, "%", null);
			while (tables.next()) {
				assertTable(tables, tableNames.next());
			}
			assertFalse(tables.getStatement().getMoreResults());
		}
	}

	@Test public void testColumns() throws SQLException {
		try (InfluxDbConnection conn = connectDb()) {
			ListIterator<String[]> expectations = EXPECTED_FIELDS.listIterator();
			AbstractProxyResultSet columns = conn.getMetaData().getColumns(null, null, null, null);
			assertBefore(columns);
			while (columns.next()) {
				String[] ex = expectations.next();
				assertField(columns, ex[0], ex[1], ex[2]);
			}
			assertAfter(columns);
		}

	}

	@Test public void testColumnsWithWildcardAndReverse() throws SQLException {
		try (InfluxDbConnection conn = connectDb()) {
			ListIterator<String[]> expectations = EXPECTED_FIELDS.listIterator();
			AbstractProxyResultSet columns = conn.getMetaData().getColumns(null, null, "%", null);
			assertBefore(columns);
			while (columns.next()) {
				String[] ex = expectations.next();
				assertField(columns, ex[0], ex[1], ex[2]);
			}
			assertAfter(columns);

			while (columns.previous()) {
				String[] ex = expectations.previous();
				assertField(columns, ex[0], ex[1], ex[2]);
			}
			assertBefore(columns);
		}
	}

	@Test public void testColumnsSingleTable() throws SQLException {
		try (InfluxDbConnection conn = connectDb()) {
			AbstractProxyResultSet columns = conn.getMetaData().getColumns(null, null, "measurement1", null);
			ListIterator<String[]> expectations =
				EXPECTED_FIELDS.stream().filter(l -> Objects.equals(l[0], "measurement1")).collect(Collectors.toList())
					.listIterator();
			assertBefore(columns);
			while (columns.next()) {
				String[] ex = expectations.next();
				assertField(columns, ex[0], ex[1], ex[2]);
			}
			assertAfter(columns);
		}

	}

	@Test public void testMetadata() throws SQLException {
		try (InfluxDbConnection conn = connectDb()) {
			ResultSet r = conn.createStatement().executeQuery("show databases");
			ResultSetMetaData metaData = r.getMetaData();
			assertEquals(1, metaData.getColumnCount());
			assertEquals("name", metaData.getColumnName(1));
			assertEquals("name", metaData.getColumnLabel(1));
			assertEquals("databases", metaData.getTableName(1));
		}
	}

	@Test public void someTestMethod() throws SQLException {
		try (InfluxDbConnection conn = connectDb()) {
			ResultSet r = conn.createStatement().executeQuery("select * from measurement1");
			assertBefore(r);
			r.next();
			assertFirst(r);
			assertEquals(13.0f, r.getFloat(2), .00001);
			assertEquals(13.0f, r.getFloat("field1"), .00001);
			assertEquals("tag1value", r.getString("tag1"));
			r.next();
			assertEquals(13.0f, r.getFloat(2), .00001);
			assertEquals(13.0f, r.getFloat("field2"), .00001);
			assertEquals("tag2value", r.getString("tag2"));
			r.next();
			assertAfter(r);
		}
	}

	private static void assertFirst(ResultSet catalogs) throws SQLException {
		assertFalse(catalogs.isBeforeFirst());
		assertTrue(catalogs.isFirst());
		assertFalse(catalogs.isLast());
		assertFalse(catalogs.isAfterLast());
	}

	private static void assertAfter(ResultSet resultset) throws SQLException {
		assertFalse(resultset.isBeforeFirst());
		assertFalse(resultset.isFirst());
		assertFalse(resultset.isLast());
		assertTrue(resultset.isAfterLast());
		assertFalse(resultset.getStatement().getMoreResults());
	}

	private static void assertBefore(ResultSet catalogs) throws SQLException {
		assertTrue(catalogs.isBeforeFirst());
		assertFalse(catalogs.isFirst());
		assertFalse(catalogs.isLast());
		assertFalse(catalogs.isAfterLast());
	}

	private static void assertSchema(GetSchemaResultSet schemas, String next) throws SQLException {
		assertEquals(next, schemas.getString("TABLE_CATALOG"));
		assertNull(schemas.getString("TABLE_SCHEM"));
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

	private InfluxDbConnection connectDb()
		throws SQLException {
		Properties properties = new Properties();
		properties.put("username", USERNAME);
		properties.put("password", PASSWORD);
		properties.put("database", DATABASE);
		String database = "test";
		String url = influxDbContainer.getUrl();
		InfluxDbConnection conn =
			new InfluxDbDriver().connect(String.format("jdbc:influxdb:%s?db=%s", url, database), properties);
		assertTrue(conn.isValid(1));
		return conn;
	}
}
