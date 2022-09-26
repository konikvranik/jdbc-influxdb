import java.sql.SQLException;
import java.util.Objects;
import java.util.Properties;

import org.influxdb.InfluxDB;
import org.influxdb.dto.Pong;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import lombok.extern.slf4j.Slf4j;
import net.suteren.jdbc.influxdb.InfluxDbConnection;
import net.suteren.jdbc.influxdb.InfluxDbDriver;
import net.suteren.jdbc.influxdb.statement.InfluxDbStatement;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Slf4j
public class MockedTest {

	@Mock InfluxDB influxDB;
	@Spy InfluxDbDriver influxDbDriver = new InfluxDbDriver();
	InfluxDbConnection connection;
	private AutoCloseable closeable;

	@Before public void setUp() throws SQLException {
		closeable = MockitoAnnotations.openMocks(this);
		when(influxDB.query(any(Query.class))).thenAnswer(invocation -> {
			log.info("Query: {}", invocation.getArgument(0, Query.class).getCommand());
			return new QueryResult();
		});
		Pong pong = new Pong();
		pong.setVersion("TEST");
		when(influxDB.ping()).thenReturn(pong);
		InfluxDbConnection connection1 =
			spy(new InfluxDbConnection("http://localhost:8086?db=test", "username", "password",
				"database", influxDbDriver));
		when(connection1.getClient()).thenReturn(influxDB);
		doReturn(connection1).when(influxDbDriver).connect(anyString(), any(Properties.class));
		connection = connection1;

	}

	@After public void tearDown() throws Exception {
		closeable.close();
	}

	@Test public void testSQL() throws SQLException {
		InfluxDbStatement statement = connection.createStatement();
		statement.execute("SELECT * FROM test");
		verify(influxDB, times(1)).query(any(Query.class));
		verify(influxDB).query(argThat(q -> Objects.equals(q.getCommand(), "SELECT * FROM test")));
		connection.close();
	}

	@Test public void testSqlCleanup() throws SQLException {
		InfluxDbStatement statement = connection.createStatement();
		statement.execute("SELECT * FROM test t where t.a>0");
		verify(influxDB, times(1)).query(any(Query.class));
		verify(influxDB).query(argThat(queryMatches("select * from test where a>0")));
		//verify(influxDB).query(eq(new Query("SELECT * FROM test")));
		connection.close();
	}

	@Test public void testKeepAlive() throws SQLException {
		InfluxDbStatement statement = connection.createStatement();
		statement.execute("SELECT 'keep alive'");
		verify(influxDB, times(1)).query(any(Query.class));
		verify(influxDB).query(argThat(q -> Objects.equals(q.getCommand(), "")));
		connection.close();
	}

	public static QueryMatcher queryMatches(String expected) {
		return new QueryMatcher(expected);
	}

	private static class QueryMatcher implements ArgumentMatcher<Query> {
		private final String expected;

		public QueryMatcher(String expected) {
			this.expected = expected;
		}

		@Override public boolean matches(Query argument) {
			assertEquals(expected, argument.getCommand());
			return true;
		}
	}
}
