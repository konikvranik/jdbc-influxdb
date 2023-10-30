package net.suteren.jdbc.influxdb.statement;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

import org.influxdb.InfluxDB;

import net.suteren.jdbc.influxdb.InfluxDbConnection;

public class InfluxDbPreparedStatement extends InfluxDbStatement implements PreparedStatement {
	private final String sql;

	public InfluxDbPreparedStatement(InfluxDbConnection influxDbConnection, String sql, InfluxDB influxDbClient) {
		super(influxDbConnection, influxDbClient);
		this.sql = sql;
	}

	@Override public ResultSet executeQuery() throws SQLException {
		return executeQuery(sql);
	}

	@Override public int executeUpdate() {
		return 0;
	}

	@Override public void setNull(int parameterIndex, int sqlType) {
		throw new UnsupportedOperationException();
	}

	@Override public void setBoolean(int parameterIndex, boolean x) {
		throw new UnsupportedOperationException();
	}

	@Override public void setByte(int parameterIndex, byte x) {
		throw new UnsupportedOperationException();
	}

	@Override public void setShort(int parameterIndex, short x) {
		throw new UnsupportedOperationException();
	}

	@Override public void setInt(int parameterIndex, int x) {
		throw new UnsupportedOperationException();
	}

	@Override public void setLong(int parameterIndex, long x) {
		throw new UnsupportedOperationException();
	}

	@Override public void setFloat(int parameterIndex, float x) {
		throw new UnsupportedOperationException();
	}

	@Override public void setDouble(int parameterIndex, double x) {
		throw new UnsupportedOperationException();
	}

	@Override public void setBigDecimal(int parameterIndex, BigDecimal x) {
		throw new UnsupportedOperationException();
	}

	@Override public void setString(int parameterIndex, String x) {
		throw new UnsupportedOperationException();
	}

	@Override public void setBytes(int parameterIndex, byte[] x) {
		throw new UnsupportedOperationException();
	}

	@Override public void setDate(int parameterIndex, Date x) {
		throw new UnsupportedOperationException();
	}

	@Override public void setTime(int parameterIndex, Time x) {
		throw new UnsupportedOperationException();
	}

	@Override public void setTimestamp(int parameterIndex, Timestamp x) {
		throw new UnsupportedOperationException();
	}

	@Override public void setAsciiStream(int parameterIndex, InputStream x, int length) {
		throw new UnsupportedOperationException();
	}

	@Override public void setUnicodeStream(int parameterIndex, InputStream x, int length) {
		throw new UnsupportedOperationException();
	}

	@Override public void setBinaryStream(int parameterIndex, InputStream x, int length) {
		throw new UnsupportedOperationException();
	}

	@Override public void clearParameters() {
		throw new UnsupportedOperationException();
	}

	@Override public void setObject(int parameterIndex, Object x, int targetSqlType) {
		throw new UnsupportedOperationException();
	}

	@Override public void setObject(int parameterIndex, Object x) {
		throw new UnsupportedOperationException();
	}

	@Override public boolean execute() {
		return false;
	}

	@Override public void addBatch() {
		throw new UnsupportedOperationException();
	}

	@Override public void setCharacterStream(int parameterIndex, Reader reader, int length) {
		throw new UnsupportedOperationException();
	}

	@Override public void setRef(int parameterIndex, Ref x) {
		throw new UnsupportedOperationException();
	}

	@Override public void setBlob(int parameterIndex, Blob x) {
		throw new UnsupportedOperationException();
	}

	@Override public void setClob(int parameterIndex, Clob x) {
		throw new UnsupportedOperationException();
	}

	@Override public void setArray(int parameterIndex, Array x) {
		throw new UnsupportedOperationException();
	}

	@Override public ResultSetMetaData getMetaData() {
		return null;
	}

	@Override public void setDate(int parameterIndex, Date x, Calendar cal) {
		throw new UnsupportedOperationException();
	}

	@Override public void setTime(int parameterIndex, Time x, Calendar cal) {
		throw new UnsupportedOperationException();
	}

	@Override public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) {
		throw new UnsupportedOperationException();
	}

	@Override public void setNull(int parameterIndex, int sqlType, String typeName) {
		throw new UnsupportedOperationException();
	}

	@Override public void setURL(int parameterIndex, URL x) {
		throw new UnsupportedOperationException();
	}

	@Override public ParameterMetaData getParameterMetaData() {
		return null;
	}

	@Override public void setRowId(int parameterIndex, RowId x) {

	}

	@Override public void setNString(int parameterIndex, String value) {
		throw new UnsupportedOperationException();
	}

	@Override public void setNCharacterStream(int parameterIndex, Reader value, long length) {
		throw new UnsupportedOperationException();
	}

	@Override public void setNClob(int parameterIndex, NClob value) {
		throw new UnsupportedOperationException();
	}

	@Override public void setClob(int parameterIndex, Reader reader, long length) {
		throw new UnsupportedOperationException();
	}

	@Override public void setBlob(int parameterIndex, InputStream inputStream, long length) {
		throw new UnsupportedOperationException();
	}

	@Override public void setNClob(int parameterIndex, Reader reader, long length) {
		throw new UnsupportedOperationException();
	}

	@Override public void setSQLXML(int parameterIndex, SQLXML xmlObject) {
		throw new UnsupportedOperationException();
	}

	@Override public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) {
		throw new UnsupportedOperationException();
	}

	@Override public void setAsciiStream(int parameterIndex, InputStream x, long length) {
		throw new UnsupportedOperationException();
	}

	@Override public void setBinaryStream(int parameterIndex, InputStream x, long length) {
		throw new UnsupportedOperationException();
	}

	@Override public void setCharacterStream(int parameterIndex, Reader reader, long length) {
		throw new UnsupportedOperationException();
	}

	@Override public void setAsciiStream(int parameterIndex, InputStream x) {
		throw new UnsupportedOperationException();
	}

	@Override public void setBinaryStream(int parameterIndex, InputStream x) {
		throw new UnsupportedOperationException();
	}

	@Override public void setCharacterStream(int parameterIndex, Reader reader) {
		throw new UnsupportedOperationException();
	}

	@Override public void setNCharacterStream(int parameterIndex, Reader value) {
		throw new UnsupportedOperationException();
	}

	@Override public void setClob(int parameterIndex, Reader reader) {
		throw new UnsupportedOperationException();
	}

	@Override public void setBlob(int parameterIndex, InputStream inputStream) {
		throw new UnsupportedOperationException();
	}

	@Override public void setNClob(int parameterIndex, Reader reader) {
		throw new UnsupportedOperationException();
	}
}
