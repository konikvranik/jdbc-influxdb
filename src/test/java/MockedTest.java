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
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import net.suteren.jdbc.influxdb.InfluxDbConnection;
import net.suteren.jdbc.influxdb.InfluxDbDriver;
import net.suteren.jdbc.influxdb.statement.InfluxDbStatement;

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

	@Before public void setUp() {
		closeable = MockitoAnnotations.openMocks(this);
		when(influxDB.query(any(Query.class))).thenAnswer(invocation -> {
			log.info("Query: {}", invocation.getArgument(0, Query.class).getCommand());
			return new QueryResult();
		});

		connection = getInfluxDbConnection(influxDbDriver);

	}

	@After public void release() throws Exception {
		closeable.close();
	}

	@Test public void testSQL() throws SQLException {
		InfluxDbStatement statement = connection.createStatement();
		statement.execute("SELECT * FROM test");
		verify(influxDB, times(1)).query(any(Query.class));
		verify(influxDB).query(argThat(q -> Objects.equals(q.getCommand(), "SELECT * FROM test")));
		connection.close();
	}

	@SneakyThrows private InfluxDbConnection getInfluxDbConnection(InfluxDbDriver driver) {
		mockPing(influxDB);
		InfluxDbConnection connection =
			spy(new InfluxDbConnection("http://localhost:8086?db=test", "username", "password",
				"database", driver));
		when(connection.getClient()).thenReturn(influxDB);
		doReturn(connection).when(driver).connect(anyString(), any(Properties.class));
		return connection;
	}

	private void mockPing(InfluxDB influxDB) {
		Pong pong = new Pong();
		pong.setVersion("TEST");
		when(influxDB.ping()).thenReturn(pong);
	}
}
