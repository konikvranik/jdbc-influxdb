package net.suteren.jdbc.influxdb.resultset.proxy;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Map;
import java.util.function.Function;

import net.suteren.jdbc.AbstractBaseResultSet;
import net.suteren.jdbc.influxdb.resultset.InfluxDbResultSet;

public abstract class AbstractProxyResultSet extends AbstractBaseResultSet {

	private final String[] columns;
	private final Object[] defaults;
	private final InfluxDbResultSet influxDbResultSet;

	public AbstractProxyResultSet(InfluxDbResultSet influxDbResultSet, String[] columns, Object[] defaults) {
		this.influxDbResultSet = influxDbResultSet;
		this.columns = columns;
		this.defaults = defaults;
	}

	@Override public boolean next() {
		return influxDbResultSet.next();
	}

	@Override public boolean isBeforeFirst() {
		return influxDbResultSet.isBeforeFirst();
	}

	@Override public boolean isAfterLast() {
		return influxDbResultSet.isAfterLast();
	}

	@Override public boolean isFirst() {
		return influxDbResultSet.isFirst();
	}

	@Override public boolean isLast() {
		return influxDbResultSet.isLast();
	}

	@Override public void beforeFirst() {
		influxDbResultSet.beforeFirst();
	}

	@Override public void afterLast() {
		influxDbResultSet.afterLast();
	}

	@Override public boolean first() {
		return influxDbResultSet.first();
	}

	@Override public boolean last() {
		return influxDbResultSet.last();
	}

	@Override public int getRow() {
		return influxDbResultSet.getRow();
	}

	@Override public boolean absolute(int row) {
		return influxDbResultSet.absolute(row);
	}

	@Override public boolean relative(int rows) {
		return influxDbResultSet.relative(rows);
	}

	@Override public boolean previous() {
		return influxDbResultSet.previous();
	}

	@Override public void setFetchDirection(int direction) {
		influxDbResultSet.setFetchDirection(direction);
	}

	@Override public int getFetchDirection() {
		return influxDbResultSet.getFetchDirection();
	}

	@Override public void setFetchSize(int rows) {
		influxDbResultSet.setFetchSize(rows);
	}

	@Override public int getFetchSize() {
		return influxDbResultSet.getFetchSize();
	}

	@Override public int getType() {
		return influxDbResultSet.getType();
	}

	@Override public int getConcurrency() {
		return influxDbResultSet.getConcurrency();
	}

	@Override public void moveToInsertRow() {
		influxDbResultSet.moveToInsertRow();
	}

	@Override public void moveToCurrentRow() {
		influxDbResultSet.moveToCurrentRow();
	}

	@Override public SQLWarning getWarnings() {
		return influxDbResultSet.getWarnings();
	}

	@Override public void clearWarnings() {
		influxDbResultSet.clearWarnings();
	}

	@Override public ResultSetMetaData getMetaData() {
		return new AbstractProxyResultSetMetadata(influxDbResultSet.getMetaData(), this, columns);
	}

	@Override public String getCursorName() {
		return influxDbResultSet.getCursorName();
	}

	@Override public void close() {
		influxDbResultSet.close();
	}

	@Override public boolean isClosed() {
		return influxDbResultSet.isClosed();
	}

	@Override public int getHoldability() {
		return influxDbResultSet.getHoldability();
	}

	@Override public <U> U getValue(int index, Class<U> clzz, Function<Object, U> convert) {
		return influxDbResultSet.getValue(index, clzz, convert);
	}

	@Override public String getString(int columnIndex) {
		return mapOrDefault(columnIndex, influxDbResultSet::getString);
	}

	protected abstract int remapIndex(int columnIndex);

	@Override public boolean getBoolean(int columnIndex) {
		return mapOrDefault(columnIndex, influxDbResultSet::getBoolean);
	}

	@Override public byte getByte(int columnIndex) {
		return mapOrDefault(columnIndex, influxDbResultSet::getByte);
	}

	@Override public short getShort(int columnIndex) {
		return mapOrDefault(columnIndex, influxDbResultSet::getShort);
	}

	@Override public int getInt(int columnIndex) {
		return mapOrDefault(columnIndex, influxDbResultSet::getInt);
	}

	@Override public long getLong(int columnIndex) {
		return mapOrDefault(columnIndex, influxDbResultSet::getLong);
	}

	@Override public float getFloat(int columnIndex) {
		return mapOrDefault(columnIndex, influxDbResultSet::getFloat);
	}

	@Override public double getDouble(int columnIndex) {
		return mapOrDefault(columnIndex, influxDbResultSet::getDouble);
	}

	@Override public BigDecimal getBigDecimal(int columnIndex, int scale) {
		return mapOrDefault(columnIndex, index -> influxDbResultSet.getBigDecimal(index, scale));
	}

	@Override public byte[] getBytes(int columnIndex) {
		return mapOrDefault(columnIndex, influxDbResultSet::getBytes);
	}

	@Override public Date getDate(int columnIndex) {
		return mapOrDefault(columnIndex, influxDbResultSet::getDate);
	}

	@Override public Time getTime(int columnIndex) {
		return mapOrDefault(columnIndex, influxDbResultSet::getTime);
	}

	@Override public Timestamp getTimestamp(int columnIndex) {
		return mapOrDefault(columnIndex, influxDbResultSet::getTimestamp);
	}

	@Override public InputStream getAsciiStream(int columnIndex) {
		return mapOrDefault(columnIndex, influxDbResultSet::getAsciiStream);
	}

	@Override public InputStream getUnicodeStream(int columnIndex) {
		return mapOrDefault(columnIndex, influxDbResultSet::getUnicodeStream);
	}

	@Override public InputStream getBinaryStream(int columnIndex) {
		return mapOrDefault(columnIndex, influxDbResultSet::getBinaryStream);
	}

	@Override public Object getObject(int columnIndex) {
		return mapOrDefault(columnIndex, influxDbResultSet::getObject);
	}

	@Override public Reader getCharacterStream(int columnIndex) {
		return mapOrDefault(columnIndex, influxDbResultSet::getCharacterStream);
	}

	@Override public BigDecimal getBigDecimal(int columnIndex) {
		return mapOrDefault(columnIndex, influxDbResultSet::getBigDecimal);
	}

	@Override public Statement getStatement() {
		return influxDbResultSet.getStatement();
	}

	@Override public Object getObject(int columnIndex, Map<String, Class<?>> map) {
		return mapOrDefault(columnIndex, index -> influxDbResultSet.getObject(index, map));
	}

	@Override public Ref getRef(int columnIndex) {
		return mapOrDefault(columnIndex, influxDbResultSet::getRef);
	}

	@Override public Blob getBlob(int columnIndex) {
		return mapOrDefault(columnIndex, influxDbResultSet::getBlob);
	}

	@Override public Clob getClob(int columnIndex) {
		return mapOrDefault(columnIndex, influxDbResultSet::getClob);
	}

	@Override public Array getArray(int columnIndex) {
		return mapOrDefault(columnIndex, influxDbResultSet::getArray);
	}

	@Override public Date getDate(int columnIndex, Calendar cal) {
		return mapOrDefault(columnIndex, index -> influxDbResultSet.getDate(index, cal));
	}

	@Override public Time getTime(int columnIndex, Calendar cal) {
		return mapOrDefault(columnIndex, index -> influxDbResultSet.getTime(index, cal));
	}

	@Override public Timestamp getTimestamp(int columnIndex, Calendar cal) {
		return mapOrDefault(columnIndex, index -> influxDbResultSet.getTimestamp(index, cal));
	}

	@Override public URL getURL(int columnIndex) {
		return mapOrDefault(columnIndex, influxDbResultSet::getURL);
	}

	@Override public RowId getRowId(int columnIndex) {
		return mapOrDefault(columnIndex, influxDbResultSet::getRowId);
	}

	@Override public NClob getNClob(int columnIndex) {
		return mapOrDefault(columnIndex, influxDbResultSet::getNClob);
	}

	@Override public SQLXML getSQLXML(int columnIndex) {
		return mapOrDefault(columnIndex, influxDbResultSet::getSQLXML);
	}

	@Override public String getNString(int columnIndex) {
		return mapOrDefault(columnIndex, influxDbResultSet::getNString);
	}

	@Override public Reader getNCharacterStream(int columnIndex) {
		return mapOrDefault(columnIndex, influxDbResultSet::getNCharacterStream);
	}

	@Override public <T> T getObject(int columnIndex, Class<T> type) {
		return mapOrDefault(columnIndex, index -> influxDbResultSet.getObject(index, type));
	}

	@Override public boolean wasNull() {
		return influxDbResultSet.wasNull();
	}

	@Override public void refreshRow() {
		influxDbResultSet.refreshRow();
	}

	@Override public <T> T unwrap(Class<T> iface) {
		return influxDbResultSet.unwrap(iface);
	}

	@Override public boolean isWrapperFor(Class<?> iface) {
		return influxDbResultSet.isWrapperFor(iface);
	}

	@Override public boolean rowUpdated() {
		return influxDbResultSet.rowUpdated();
	}

	@Override public boolean rowInserted() {
		return influxDbResultSet.rowInserted();
	}

	@Override public boolean rowDeleted() {
		return influxDbResultSet.rowDeleted();
	}

	@Override public void updateNull(int columnIndex) {
		influxDbResultSet.updateNull(remapIndex(columnIndex));
	}

	@Override public void updateBoolean(int columnIndex, boolean x) {
		influxDbResultSet.updateBoolean(remapIndex(columnIndex), x);
	}

	@Override public void updateByte(int columnIndex, byte x) {
		influxDbResultSet.updateByte(remapIndex(columnIndex), x);
	}

	@Override public void updateShort(int columnIndex, short x) {
		influxDbResultSet.updateShort(remapIndex(columnIndex), x);
	}

	@Override public void updateInt(int columnIndex, int x) {
		influxDbResultSet.updateInt(remapIndex(columnIndex), x);
	}

	@Override public void updateLong(int columnIndex, long x) {
		influxDbResultSet.updateLong(remapIndex(columnIndex), x);
	}

	@Override public void updateFloat(int columnIndex, float x) {
		influxDbResultSet.updateFloat(remapIndex(columnIndex), x);
	}

	@Override public void updateDouble(int columnIndex, double x) {
		influxDbResultSet.updateDouble(remapIndex(columnIndex), x);
	}

	@Override public void updateBigDecimal(int columnIndex, BigDecimal x) {
		influxDbResultSet.updateBigDecimal(remapIndex(columnIndex), x);
	}

	@Override public void updateString(int columnIndex, String x) {
		influxDbResultSet.updateString(remapIndex(columnIndex), x);
	}

	@Override public void updateBytes(int columnIndex, byte[] x) {
		influxDbResultSet.updateBytes(remapIndex(columnIndex), x);
	}

	@Override public void updateDate(int columnIndex, Date x) {
		influxDbResultSet.updateDate(remapIndex(columnIndex), x);
	}

	@Override public void updateTime(int columnIndex, Time x) {
		influxDbResultSet.updateTime(remapIndex(columnIndex), x);
	}

	@Override public void updateTimestamp(int columnIndex, Timestamp x) {
		influxDbResultSet.updateTimestamp(remapIndex(columnIndex), x);
	}

	@Override public void updateAsciiStream(int columnIndex, InputStream x, int length) {
		influxDbResultSet.updateAsciiStream(remapIndex(columnIndex), x, length);
	}

	@Override public void updateBinaryStream(int columnIndex, InputStream x, int length) {
		influxDbResultSet.updateBinaryStream(remapIndex(columnIndex), x, length);
	}

	@Override public void updateCharacterStream(int columnIndex, Reader x, int length) {
		influxDbResultSet.updateCharacterStream(remapIndex(columnIndex), x, length);
	}

	@Override public void updateObject(int columnIndex, Object x, int scaleOrLength) {
		influxDbResultSet.updateObject(remapIndex(columnIndex), x, scaleOrLength);
	}

	@Override public void updateObject(int columnIndex, Object x) {
		influxDbResultSet.updateObject(remapIndex(columnIndex), x);
	}

	@Override public void insertRow() {
		influxDbResultSet.insertRow();
	}

	@Override public void updateRow() {
		influxDbResultSet.updateRow();
	}

	@Override public void deleteRow() {
		influxDbResultSet.deleteRow();
	}

	@Override public void cancelRowUpdates() {
		influxDbResultSet.cancelRowUpdates();
	}

	@Override public void updateRef(int columnIndex, Ref x) {
		influxDbResultSet.updateRef(remapIndex(columnIndex), x);
	}

	@Override public void updateBlob(int columnIndex, Blob x) {
		influxDbResultSet.updateBlob(remapIndex(columnIndex), x);
	}

	@Override public void updateClob(int columnIndex, Clob x) {
		influxDbResultSet.updateClob(remapIndex(columnIndex), x);
	}

	@Override public void updateArray(int columnIndex, Array x) {
		influxDbResultSet.updateArray(remapIndex(columnIndex), x);
	}

	@Override public void updateRowId(int columnIndex, RowId x) {
		influxDbResultSet.updateRowId(remapIndex(columnIndex), x);
	}

	@Override public void updateNString(int columnIndex, String nString) {
		influxDbResultSet.updateNString(remapIndex(columnIndex), nString);
	}

	@Override public void updateNClob(int columnIndex, NClob nClob) {
		influxDbResultSet.updateNClob(remapIndex(columnIndex), nClob);
	}

	@Override public void updateSQLXML(int columnIndex, SQLXML xmlObject) {
		influxDbResultSet.updateSQLXML(remapIndex(columnIndex), xmlObject);
	}

	@Override public void updateNCharacterStream(int columnIndex, Reader x, long length) {
		influxDbResultSet.updateNCharacterStream(remapIndex(columnIndex), x, length);
	}

	@Override public void updateAsciiStream(int columnIndex, InputStream x, long length) {
		influxDbResultSet.updateAsciiStream(remapIndex(columnIndex), x, length);
	}

	@Override public void updateBinaryStream(int columnIndex, InputStream x, long length) {
		influxDbResultSet.updateBinaryStream(remapIndex(columnIndex), x, length);
	}

	@Override public void updateCharacterStream(int columnIndex, Reader x, long length) {
		influxDbResultSet.updateCharacterStream(remapIndex(columnIndex), x, length);
	}

	@Override public void updateBlob(int columnIndex, InputStream inputStream, long length) {
		influxDbResultSet.updateBlob(remapIndex(columnIndex), inputStream, length);
	}

	@Override public void updateClob(int columnIndex, Reader reader, long length) {
		influxDbResultSet.updateClob(remapIndex(columnIndex), reader, length);
	}

	@Override public void updateNClob(int columnIndex, Reader reader, long length) {
		influxDbResultSet.updateNClob(remapIndex(columnIndex), reader, length);
	}

	@Override public void updateNCharacterStream(int columnIndex, Reader x) {
		influxDbResultSet.updateNCharacterStream(remapIndex(columnIndex), x);
	}

	@Override public void updateAsciiStream(int columnIndex, InputStream x) {
		influxDbResultSet.updateAsciiStream(remapIndex(columnIndex), x);
	}

	@Override public void updateBinaryStream(int columnIndex, InputStream x) {
		influxDbResultSet.updateBinaryStream(remapIndex(columnIndex), x);
	}

	@Override public void updateCharacterStream(int columnIndex, Reader x) {
		influxDbResultSet.updateCharacterStream(remapIndex(columnIndex), x);
	}

	@Override public void updateBlob(int columnIndex, InputStream inputStream) {
		influxDbResultSet.updateBlob(remapIndex(columnIndex), inputStream);
	}

	@Override public void updateClob(int columnIndex, Reader reader) {
		influxDbResultSet.updateClob(remapIndex(columnIndex), reader);
	}

	@Override public void updateNClob(int columnIndex, Reader reader) {
		influxDbResultSet.updateNClob(remapIndex(columnIndex), reader);
	}

	@Override public void updateObject(int columnIndex, Object x, SQLType targetSqlType, int scaleOrLength)
		throws SQLException {
		influxDbResultSet.updateObject(remapIndex(columnIndex), x, targetSqlType, scaleOrLength);
	}

	@Override public void updateObject(String columnLabel, Object x, SQLType targetSqlType, int scaleOrLength)
		throws SQLException {
		influxDbResultSet.updateObject(columnLabel, x, targetSqlType, scaleOrLength);
	}

	@Override public void updateObject(int columnIndex, Object x, SQLType targetSqlType) throws SQLException {
		influxDbResultSet.updateObject(columnIndex, x, targetSqlType);
	}

	@Override public void updateObject(String columnLabel, Object x, SQLType targetSqlType) throws SQLException {
		influxDbResultSet.updateObject(columnLabel, x, targetSqlType);
	}

	@Override public int findColumn(String columnLabel) throws SQLException {
		int index = Arrays.asList(columns).indexOf(columnLabel.toUpperCase());
		if (index < 0) {
			throw new SQLException(String.format("No column named %s", columnLabel));
		}
		return index + 1;
	}

	private <T> T mapOrDefault(int columnIndex, Function<Integer, T> o) {
		int indexToProxyTable = remapIndex(columnIndex);
		if (indexToProxyTable <= 0 || indexToProxyTable > influxDbResultSet.getMetaData().getColumnCount()) {
			return (T) defaults[columnIndex - 1];
		} else {
			return o.apply(indexToProxyTable);
		}
	}
}
