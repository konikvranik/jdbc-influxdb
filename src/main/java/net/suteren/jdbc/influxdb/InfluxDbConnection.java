package net.suteren.jdbc.influxdb;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Pong;

import lombok.Getter;
import net.suteren.jdbc.influxdb.statement.InfluxDbPreparedStatement;
import net.suteren.jdbc.influxdb.statement.InfluxDbStatement;

public class InfluxDbConnection implements Connection {

	private final InfluxDB influxDbClient;
	private final InfluxDbMetadata influxDbMetadata;
	private boolean isClosed;
	private final Logger log;
	private static final Pattern KEEP_ALIVE_SQL_PATTERN =
		Pattern.compile("\\s*SELECT\\s+['\"]keep\\s+alive['\"]\\s*.*", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
	private static final Pattern TABLE_ALIASES_SQL_PATTERN =
		Pattern.compile("\\s*SELECT\\s+(\\S+)\\s+FROM\\s+(\\S+)\\s+(?!where)(?:as\\s+)?((['\"]?)(\\S+)\\4)(.*)",
			Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

	private static final Pattern TABLE_SCHEMA_SQL_PATTERN =
		Pattern.compile("\\s*SELECT\\s+(\\S+)\\s+FROM\\s+(?:(?:(([\"']?)(\\S+)\\3)\\.)?(([\"']?)(\\S+)\\6)\\.)?(([\"']?)(\\S+)\\9(\\s.*)?)",
			Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

	private static final Pattern DEFAULT_SCHEMA_PATTERN =
		Pattern.compile("\\s*SELECT\\s.*(['\"]?)default\\1\\..*\\sFROM\\s+.+",
			Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
	@Getter private String catalog;
	@Getter private String schema;

	public InfluxDbConnection(String url, String username, String password, String database,
		InfluxDbDriver influxDbDriver) {
		influxDbClient =
			username == null ? InfluxDBFactory.connect(url) : InfluxDBFactory.connect(url, username, password);
		if (database != null) {
			getClient().setDatabase(database);
		}
		influxDbMetadata = new InfluxDbMetadata(url, username, influxDbDriver, this);
		log = influxDbDriver.getParentLogger();
	}

	@Override public InfluxDbStatement createStatement() {
		return new InfluxDbStatement(this, getClient());
	}

	@Override public PreparedStatement prepareStatement(String sql) {
		return new InfluxDbPreparedStatement(this, sql, getClient());
	}

	@Override public CallableStatement prepareCall(String sql) {
		return null;
	}

	@Override public String nativeSQL(String sql) {
		String finalSql = sql;
		log.fine(() -> String.format("NativeSQL: %s", finalSql));
		if (KEEP_ALIVE_SQL_PATTERN.matcher(sql).matches()) {
			return "";
		}
		Matcher matcher = TABLE_ALIASES_SQL_PATTERN.matcher(sql);
		if (matcher.matches()) {
			String alias = matcher.group(5);
			sql = matcher.replaceFirst("SELECT $1 FROM $2$6")
				.replaceAll(String.format("\\s+%s\\.", alias), " ")
				.replaceAll(String.format("\\s+\"%s\"\\.", alias), " ");
		}
		matcher = TABLE_SCHEMA_SQL_PATTERN.matcher(sql);
		if (matcher.matches()) {
			if (StringUtils.isNotBlank(matcher.group(4))) {
				if (StringUtils.isNotBlank(matcher.group(7))) {
					sql = matcher.replaceFirst("SELECT $1 FROM \"$4\".\"$7\".\"$10\"$11");
				} else {
					sql = matcher.replaceFirst("SELECT $1 FROM \"$4\".\"$10\"$11");
				}
			} else {
				sql = matcher.replaceFirst("SELECT $1 FROM \"$10\"$11");
			}
		}

		//		if (DEFAULT_SCHEMA_PATTERN.matcher(sql).matches()) {
		//			sql = sql.replaceAll("(\\s,)([\"']?)default\\2\\.", "$1");
		//		}
		sql = convertSqlQuoting(sql);
		return sql;
	}

	private String convertSqlQuoting(String sql) {
		char[] chars = sql.toCharArray();
		StringBuilder result = new StringBuilder();
		AtomicInteger counter = new AtomicInteger(0);
		AtomicBoolean order = new AtomicBoolean(false);
		AtomicBoolean escape = new AtomicBoolean(false);
		for (char ch : chars) {
			escape.set(false);
			if (ch == '"' && !escape.get()) {
				counter.incrementAndGet();
				escape.set(false);
			} else {
				handleQuotes(result, counter, order);
				result.append(ch);
				escape.set(ch == '\\');
			}
		}
		handleQuotes(result, counter, order);
		return result.toString();
	}

	private void handleQuotes(StringBuilder result, AtomicInteger counter, AtomicBoolean order) {
		if (counter.get() > 0) {
			boolean odd = counter.get() % 2 != 0;
			boolean b = order.getAndSet(!order.get());
			if (odd && !b) {
				result.append("\"");
			}
			result.append("\\\"".repeat(counter.get() / 2));
			if (odd && b) {
				result.append("\"");
			}
			counter.set(0);
		}
	}

	@Override public void setAutoCommit(boolean autoCommit) {
		if (!autoCommit) {
			throw new UnsupportedOperationException("Transactions are not supported. Autocommit must be true.");
		}
	}

	@Override public boolean getAutoCommit() {
		return true;
	}

	@Override public void commit() {
		throw new UnsupportedOperationException();
	}

	@Override public void rollback() {
		throw new UnsupportedOperationException("Transactions are not supported. Can not rollback.");
	}

	@Override public void close() {
		getClient().close();
		isClosed = true;
	}

	@Override public boolean isClosed() {
		return isClosed;
	}

	@Override public InfluxDbMetadata getMetaData() {
		return influxDbMetadata;
	}

	@Override public void setReadOnly(boolean readOnly) {
		if (!readOnly) {
			throw new UnsupportedOperationException("Currently only readonly access to the InfluxDB is supported.");
		}
	}

	@Override public boolean isReadOnly() {
		return true;
	}

	@Override public void setCatalog(String catalog) {
		if (catalog != null) {
			influxDbClient.setDatabase(catalog);
			this.catalog = catalog;
		}
	}

	@Override public void setTransactionIsolation(int level) {
		if (level != TRANSACTION_NONE) {
			throw new UnsupportedOperationException("Transactions are not supported. Can set transaction isolation.");
		}
	}

	@Override public int getTransactionIsolation() {
		return TRANSACTION_NONE;
	}

	@Override public SQLWarning getWarnings() {
		return null;
	}

	@Override public void clearWarnings() {
		throw new UnsupportedOperationException("Clear warning is not supported.");
	}

	@Override public Statement createStatement(int resultSetType, int resultSetConcurrency) {
		return new InfluxDbStatement(this, getClient());
	}

	@Override public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) {
		return new InfluxDbPreparedStatement(this, sql, getClient());
	}

	@Override public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) {
		return null;
	}

	@Override public Map<String, Class<?>> getTypeMap() {
		return new HashMap<>();
	}

	@Override public void setTypeMap(Map<String, Class<?>> map) {
		throw new UnsupportedOperationException("Type maps are not supported.");
	}

	@Override public void setHoldability(int holdability) {
		throw new UnsupportedOperationException("Result set holdability is not supported.");
	}

	@Override public int getHoldability() {
		return 0;
	}

	@Override public Savepoint setSavepoint() {
		return null;
	}

	@Override public Savepoint setSavepoint(String name) {
		return null;
	}

	@Override public void rollback(Savepoint savepoint) {
		throw new UnsupportedOperationException("Transactions are not supported. Can not rollback.");
	}

	@Override public void releaseSavepoint(Savepoint savepoint) {
		throw new UnsupportedOperationException("Transactions are not supported. Savepoint can not be released.");
	}

	@Override public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) {
		return new InfluxDbStatement(this, getClient());
	}

	@Override public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
		int resultSetHoldability) {
		return new InfluxDbPreparedStatement(this, sql, getClient());
	}

	@Override public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
		int resultSetHoldability) {
		return null;
	}

	@Override public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) {
		return new InfluxDbPreparedStatement(this, sql, getClient());
	}

	@Override public PreparedStatement prepareStatement(String sql, int[] columnIndexes) {
		return new InfluxDbPreparedStatement(this, sql, getClient());
	}

	@Override public PreparedStatement prepareStatement(String sql, String[] columnNames) {
		return new InfluxDbPreparedStatement(this, sql, getClient());
	}

	@Override public Clob createClob() {
		return null;
	}

	@Override public Blob createBlob() {
		return null;
	}

	@Override public NClob createNClob() {
		return null;
	}

	@Override public SQLXML createSQLXML() {
		return null;
	}

	@Override public boolean isValid(int timeout) {
		Pong pong = getClient().ping();
		return pong.isGood() && pong.getResponseTime() < timeout * 1000L;
	}

	@Override public void setClientInfo(String name, String value) {
		throw new UnsupportedOperationException("Client info is not supported.");
	}

	@Override public void setClientInfo(Properties properties) {
		throw new UnsupportedOperationException("Client info is not supported.");
	}

	@Override public String getClientInfo(String name) {
		return null;
	}

	@Override public Properties getClientInfo() {
		return new Properties();
	}

	@Override public Array createArrayOf(String typeName, Object[] elements) {
		return null;
	}

	@Override public Struct createStruct(String typeName, Object[] attributes) {
		return null;
	}

	@Override public void setSchema(String schema) {
		influxDbClient.setRetentionPolicy(schema);
		this.schema = schema;
	}

	@Override public void abort(Executor executor) {
		throw new UnsupportedOperationException("Aborting is not supported.");
	}

	@Override public void setNetworkTimeout(Executor executor, int milliseconds) {
		throw new UnsupportedOperationException("Network timeout is not supported.");
	}

	@Override public int getNetworkTimeout() {
		return 0;
	}

	@Override public <T> T unwrap(Class<T> iface) {
		return null;
	}

	@Override public boolean isWrapperFor(Class<?> iface) {
		return false;
	}

	public InfluxDB getClient() {
		return influxDbClient;
	}
}
