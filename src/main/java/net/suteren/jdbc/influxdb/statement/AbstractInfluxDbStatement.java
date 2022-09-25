package net.suteren.jdbc.influxdb.statement;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;

import org.influxdb.InfluxDB;
import org.jetbrains.annotations.NotNull;

import lombok.Getter;
import lombok.Setter;
import net.suteren.jdbc.influxdb.InfluxDbConnection;
import net.suteren.jdbc.influxdb.resultset.InfluxDbResultSet;

public abstract class AbstractInfluxDbStatement implements Statement {
	protected final InfluxDbConnection influxDbConnection;
	protected final InfluxDB client;
	protected SQLWarning error;
	protected InfluxDbResultSet resultSet;
	@Setter boolean escapeProcessing;
	@Getter @Setter int fetchDirection;
	@Getter @Setter int fetchSize;
	@Getter private boolean closed = false;
	@Getter @Setter private int queryTimeout;
	@Getter @Setter private int maxRows;
	@Getter @Setter private int maxFieldSize;
	@Getter @Setter private boolean poolable;
	@Getter private boolean closeOnCompletion;
	@Getter private int resultSetHoldability;

	public AbstractInfluxDbStatement(InfluxDbConnection influxDbConnection, InfluxDB client) {
		this.influxDbConnection = influxDbConnection;
		this.client = client;
	}

	@Override public void close() {
		getResultSet().close();
		closed = true;
	}

	@Override public void cancel() {

	}

	@Override public SQLWarning getWarnings() {
		return new SQLWarning(error);
	}

	@Override public void clearWarnings() {
		error = null;
	}

	@Override public void setCursorName(String name) {
		getResultSet().setCursorName(name);
	}

	@Override public InfluxDbResultSet getResultSet() {
		return resultSet;
	}

	@Override public int getUpdateCount() {
		return resultSet.getCurrentRows().size();
	}

	@Override public boolean getMoreResults() {
		return resultSet.getMoreResults();
	}

	@Override public int getResultSetConcurrency() {
		return ResultSet.CONCUR_READ_ONLY;
	}

	@Override public int getResultSetType() {
		return ResultSet.TYPE_SCROLL_INSENSITIVE;
	}

	@Override public Connection getConnection() {
		return influxDbConnection;
	}

	@Override public boolean getMoreResults(int current) {
		return getMoreResults();
	}

	@Override public ResultSet getGeneratedKeys() {
		return null;
	}

	@Override public void closeOnCompletion() {
		closeOnCompletion = true;
	}

	@Override public <T> T unwrap(Class<T> iface) {
		return null;
	}

	@Override public boolean isWrapperFor(Class<?> iface) {
		return false;
	}

	@Override public int executeUpdate(String sql, String[] columnNames) throws SQLException {
		return executeUpdate(sql, getColumnIndexes(columnNames));
	}

	@Override public boolean execute(String sql, String[] columnNames) throws SQLException {
		return execute(sql, getColumnIndexes(columnNames));
	}

	@NotNull private int[] getColumnIndexes(String[] columnNames) throws SQLException {
		int[] columnIndexes = new int[columnNames.length];
		for (int i = 0; i < columnNames.length; i++) {
			columnIndexes[i] = resultSet.findColumn(columnNames[i]);
		}
		return columnIndexes;
	}
}
