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
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Pong;

import net.suteren.jdbc.influxdb.statement.InfluxDbPreparedStatement;
import net.suteren.jdbc.influxdb.statement.InfluxDbStatement;

public class InfluxDbConnection implements Connection {

	private final InfluxDB influxDbClient;
	private final InfluxDbMetadata influxDbMetadata;
	private boolean isClosed;
	private final Logger log;
	private static final Pattern KEEP_ALIVE_SQL_PATTERN =
		Pattern.compile("^\\s*SELECT\\s+['\"]keep\\s+alive['\"]\\s*.*$", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
	private static final Pattern TABLE_ALIASES_SQL_PATTERN =
		Pattern.compile("^\\s*SELECT\\s+(\\S+)\\s+FROM\\s+(\\S+)\\s+(?:as\\s+)?(['\"]?)(\\S+)\\3(\\s.*)?$",
			Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

	private static final Pattern DEFAULT_SCHEMA_PATTERN =
		Pattern.compile("^\\s*SELECT\\s+(?:\"?default\"?\\.(\\S+)(?:\\s*,\\s*)?)+\\s+FROM\\s+.+$",
			Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

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
			String alias = matcher.group(4);
			sql = matcher.replaceFirst("select $1 from $2$5");
			sql = sql.replaceAll(String.format("\\s+%s\\.", alias), " ")
				.replaceAll(String.format("\\s+\"%s\"\\.", alias), " ");
		}
		if (DEFAULT_SCHEMA_PATTERN.matcher(sql).matches()) {
			sql = sql.replaceAll("\\s+\"?default\"?\\.", " ");
		}
		return sql;
	}

	@Override public void setAutoCommit(boolean autoCommit) {

	}

	@Override public boolean getAutoCommit() {
		return false;
	}

	@Override public void commit() {

	}

	@Override public void rollback() {

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

	}

	@Override public boolean isReadOnly() {
		return true;
	}

	@Override public void setCatalog(String catalog) {

	}

	@Override public String getCatalog() {
		return null;
	}

	@Override public void setTransactionIsolation(int level) {

	}

	@Override public int getTransactionIsolation() {
		return Connection.TRANSACTION_NONE;
	}

	@Override public SQLWarning getWarnings() {
		return null;
	}

	@Override public void clearWarnings() {

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
		return null;
	}

	@Override public void setTypeMap(Map<String, Class<?>> map) {

	}

	@Override public void setHoldability(int holdability) {

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

	}

	@Override public void releaseSavepoint(Savepoint savepoint) {

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

	}

	@Override public void setClientInfo(Properties properties) {

	}

	@Override public String getClientInfo(String name) {
		return null;
	}

	@Override public Properties getClientInfo() {
		return null;
	}

	@Override public Array createArrayOf(String typeName, Object[] elements) {
		return null;
	}

	@Override public Struct createStruct(String typeName, Object[] attributes) {
		return null;
	}

	@Override public void setSchema(String schema) {

	}

	@Override public String getSchema() {
		return null;
	}

	@Override public void abort(Executor executor) {

	}

	@Override public void setNetworkTimeout(Executor executor, int milliseconds) {

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
