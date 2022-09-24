package net.suteren.jdbc.influxdb;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
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

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;

public class InfluxDbConnection implements Connection {

	final InfluxDB influxDbClient;

	public InfluxDbConnection(String url) {
		influxDbClient = InfluxDBFactory.connect(url);
	}

	@Override public Statement createStatement() {
		return new InfluxDbStatement(this);
	}

	@Override public PreparedStatement prepareStatement(String sql) {
		return null;
	}

	@Override public CallableStatement prepareCall(String sql) {
		return null;
	}

	@Override public String nativeSQL(String sql) {
		return null;
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
		influxDbClient.close();
	}

	@Override public boolean isClosed() {
		return false;
	}

	@Override public DatabaseMetaData getMetaData() {
		return null;
	}

	@Override public void setReadOnly(boolean readOnly) {

	}

	@Override public boolean isReadOnly() {
		return false;
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
		return null;
	}

	@Override public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) {
		return null;
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
		return null;
	}

	@Override public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
		int resultSetHoldability) {
		return null;
	}

	@Override public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
		int resultSetHoldability) {
		return null;
	}

	@Override public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) {
		return null;
	}

	@Override public PreparedStatement prepareStatement(String sql, int[] columnIndexes) {
		return null;
	}

	@Override public PreparedStatement prepareStatement(String sql, String[] columnNames) {
		return null;
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
		return false;
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
}
