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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.logging.Logger;

import org.influxdb.dto.QueryResult;

import net.suteren.jdbc.AbstractBaseResultSet;
import net.suteren.jdbc.influxdb.resultset.InfluxDbResultSet;

public abstract class AbstractProxyResultSet extends AbstractBaseResultSet {

	private final String[] columns;
	private final Object[] defaults;
	private final InfluxDbResultSet influxDbResultSet;
	private final Logger log;

	public AbstractProxyResultSet(InfluxDbResultSet influxDbResultSet, String[] columns, Object[] defaults) {
		this.influxDbResultSet = influxDbResultSet;
		this.columns = columns;
		this.defaults = defaults;
		log = influxDbResultSet.getStatement().getConnection().getMetaData().getDriver().getParentLogger();
	}

	@Override public boolean next() {
		log.fine("Next row.");
		if (influxDbResultSet.getRowPosition().intValue() < influxDbResultSet.getCurrentRows().size()) {
			influxDbResultSet.getRowPosition().addAndGet(1);
		} else {
			influxDbResultSet.getMoreResults();
		}
		return influxDbResultSet.getRowPosition().intValue() < influxDbResultSet.getCurrentRows().size();
	}

	@Override public boolean isBeforeFirst() {
		return influxDbResultSet.getRowPosition().get() < 0 && influxDbResultSet.getSeriesPosition().intValue() <= 0
			&& influxDbResultSet.getResultPosition().intValue() <= 0;
	}

	@Override public boolean isAfterLast() {
		return influxDbResultSet.getRowPosition().intValue() >= influxDbResultSet.getCurrentRows().size()
			&& influxDbResultSet.getCurrentResult()
			.map(QueryResult.Result::getSeries)
			.map(List::size)
			.filter(s -> influxDbResultSet.getSeriesPosition().intValue() >= s)
			.isPresent() && influxDbResultSet.getResultPosition().intValue() >= influxDbResultSet.getResults().size();
	}

	@Override public boolean isFirst() {
		return influxDbResultSet.getRowPosition().get() == 0 && influxDbResultSet.getSeriesPosition().intValue() == 0
			&& influxDbResultSet.getResultPosition().intValue() == 0;
	}

	@Override public boolean isLast() {
		return influxDbResultSet.getRowPosition().get() == influxDbResultSet.getCurrentRows().size() - 1
			&& influxDbResultSet.getCurrentResult()
			.map(QueryResult.Result::getSeries)
			.map(List::size)
			.filter(s -> influxDbResultSet.getSeriesPosition().intValue() == s - 1)
			.isPresent()
			&& influxDbResultSet.getResultPosition().intValue() == influxDbResultSet.getResults().size() - 1;
	}

	@Override public void beforeFirst() {
		influxDbResultSet.getRowPosition().set(-1);
		influxDbResultSet.getSeriesPosition().set(0);
		influxDbResultSet.getResultPosition().set(0);
	}

	@Override public void afterLast() {
		influxDbResultSet.getRowPosition().set(influxDbResultSet.getCurrentRows().size());
		influxDbResultSet.getSeriesPosition().set(influxDbResultSet.getCurrentResult()
			.map(QueryResult.Result::getSeries)
			.map(List::size)
			.orElse(0));
		influxDbResultSet.getResultPosition().set(influxDbResultSet.getResults().size() - 1);
	}

	@Override public boolean first() {
		influxDbResultSet.getResultPosition().set(0);
		influxDbResultSet.getSeriesPosition().set(0);
		if (influxDbResultSet.getCurrentRows().isEmpty()) {
			return false;
		} else {
			influxDbResultSet.getRowPosition().set(0);
			return true;
		}
	}

	@Override public boolean last() {
		influxDbResultSet.getResultPosition().set(influxDbResultSet.getResults().size() - 1);
		influxDbResultSet.getSeriesPosition().set(influxDbResultSet.getCurrentResult()
			.map(QueryResult.Result::getSeries)
			.map(List::size)
			.map(s -> s - 1)
			.orElse(0));
		if (influxDbResultSet.getCurrentRows().isEmpty()) {
			return false;
		} else {
			influxDbResultSet.getRowPosition().set(influxDbResultSet.getCurrentRows().size() - 1);
			return true;
		}
	}

	@Override public int getRow() {
		int count = influxDbResultSet.getResults().stream().limit(influxDbResultSet.getResultPosition().intValue())
			.flatMapToInt(r -> r.getSeries().stream()
				.map(QueryResult.Series::getValues)
				.mapToInt(List::size))
			.sum();
		count += influxDbResultSet.getCurrentResult()
			.map(QueryResult.Result::getSeries).stream()
			.limit(influxDbResultSet.getSeriesPosition().intValue())
			.flatMap(Collection::stream)
			.map(QueryResult.Series::getValues)
			.mapToInt(List::size)
			.sum();
		return count + influxDbResultSet.getRowPosition().intValue() + 1;
	}

	@Override public boolean absolute(int row) {
		if (row < 0) {
			influxDbResultSet.getRowPosition().set(influxDbResultSet.getCurrentRows().size() - row);
			return !isBeforeFirst();
		} else {
			influxDbResultSet.getRowPosition().set(row - 1);
			return !isAfterLast() && !isBeforeFirst();
		}
	}

	@Override public boolean relative(int rows) {
		influxDbResultSet.getRowPosition().addAndGet(rows);
		return !isAfterLast() && !isBeforeFirst();
	}

	@Override public boolean previous() {
		if (influxDbResultSet.getRowPosition().intValue() > 0) {
			influxDbResultSet.getRowPosition().decrementAndGet();
		} else if (influxDbResultSet.getSeriesPosition().intValue() > 0) {
			influxDbResultSet.getSeriesPosition().decrementAndGet();
			influxDbResultSet.getRowPosition().set(influxDbResultSet.getCurrentRows().size() - 1);
		} else if (influxDbResultSet.getResultPosition().intValue() > 0) {
			influxDbResultSet.getResultPosition().decrementAndGet();
			influxDbResultSet.getSeriesPosition().set(influxDbResultSet.getCurrentResult()
				.map(QueryResult.Result::getSeries)
				.map(List::size)
				.orElse(0) - 1);
			influxDbResultSet.getRowPosition().set(influxDbResultSet.getCurrentRows().size() - 1);
		}
		return !isBeforeFirst();
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

	@Override public AbstractProxyResultSetMetadata getMetaData() {
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

	@Override public <U> U getValue(int index, Class<U> clzz, Function<Object, U> convert) throws SQLException {
		return mapOrDefault(index, i -> influxDbResultSet.getValue(i, clzz, convert));
	}

	@Override public String getString(int columnIndex) throws SQLException {
		return mapOrDefault(columnIndex, influxDbResultSet::getString);
	}

	protected abstract int remapIndex(int columnIndex);

	@Override public boolean getBoolean(int columnIndex) throws SQLException {
		return mapOrDefault(columnIndex, influxDbResultSet::getBoolean);
	}

	@Override public byte getByte(int columnIndex) throws SQLException {
		return mapOrDefault(columnIndex, influxDbResultSet::getByte);
	}

	@Override public short getShort(int columnIndex) throws SQLException {
		return mapOrDefault(columnIndex, influxDbResultSet::getShort);
	}

	@Override public int getInt(int columnIndex) throws SQLException {
		return mapOrDefault(columnIndex, influxDbResultSet::getInt);
	}

	@Override public long getLong(int columnIndex) throws SQLException {
		return mapOrDefault(columnIndex, influxDbResultSet::getLong);
	}

	@Override public float getFloat(int columnIndex) throws SQLException {
		return mapOrDefault(columnIndex, influxDbResultSet::getFloat);
	}

	@Override public double getDouble(int columnIndex) throws SQLException {
		return mapOrDefault(columnIndex, influxDbResultSet::getDouble);
	}

	@Override public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
		return mapOrDefault(columnIndex, index -> influxDbResultSet.getBigDecimal(index, scale));
	}

	@Override public byte[] getBytes(int columnIndex) throws SQLException {
		return mapOrDefault(columnIndex, influxDbResultSet::getBytes);
	}

	@Override public Date getDate(int columnIndex) throws SQLException {
		return mapOrDefault(columnIndex, influxDbResultSet::getDate);
	}

	@Override public Time getTime(int columnIndex) throws SQLException {
		return mapOrDefault(columnIndex, influxDbResultSet::getTime);
	}

	@Override public Timestamp getTimestamp(int columnIndex) throws SQLException {
		return mapOrDefault(columnIndex, influxDbResultSet::getTimestamp);
	}

	@Override public InputStream getAsciiStream(int columnIndex) throws SQLException {
		return mapOrDefault(columnIndex, influxDbResultSet::getAsciiStream);
	}

	@Override public InputStream getUnicodeStream(int columnIndex) throws SQLException {
		return mapOrDefault(columnIndex, influxDbResultSet::getUnicodeStream);
	}

	@Override public InputStream getBinaryStream(int columnIndex) throws SQLException {
		return mapOrDefault(columnIndex, influxDbResultSet::getBinaryStream);
	}

	@Override public Object getObject(int columnIndex) throws SQLException {
		return mapOrDefault(columnIndex, influxDbResultSet::getObject);
	}

	@Override public Reader getCharacterStream(int columnIndex) throws SQLException {
		return mapOrDefault(columnIndex, influxDbResultSet::getCharacterStream);
	}

	@Override public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
		return mapOrDefault(columnIndex, influxDbResultSet::getBigDecimal);
	}

	@Override public Statement getStatement() {
		return influxDbResultSet.getStatement();
	}

	@Override public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
		return mapOrDefault(columnIndex, index -> influxDbResultSet.getObject(index, map));
	}

	@Override public Ref getRef(int columnIndex) throws SQLException {
		return mapOrDefault(columnIndex, influxDbResultSet::getRef);
	}

	@Override public Blob getBlob(int columnIndex) throws SQLException {
		return mapOrDefault(columnIndex, influxDbResultSet::getBlob);
	}

	@Override public Clob getClob(int columnIndex) throws SQLException {
		return mapOrDefault(columnIndex, influxDbResultSet::getClob);
	}

	@Override public Array getArray(int columnIndex) throws SQLException {
		return mapOrDefault(columnIndex, influxDbResultSet::getArray);
	}

	@Override public Date getDate(int columnIndex, Calendar cal) throws SQLException {
		return mapOrDefault(columnIndex, index -> influxDbResultSet.getDate(index, cal));
	}

	@Override public Time getTime(int columnIndex, Calendar cal) throws SQLException {
		return mapOrDefault(columnIndex, index -> influxDbResultSet.getTime(index, cal));
	}

	@Override public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
		return mapOrDefault(columnIndex, index -> influxDbResultSet.getTimestamp(index, cal));
	}

	@Override public URL getURL(int columnIndex) throws SQLException {
		return mapOrDefault(columnIndex, influxDbResultSet::getURL);
	}

	@Override public RowId getRowId(int columnIndex) throws SQLException {
		return mapOrDefault(columnIndex, influxDbResultSet::getRowId);
	}

	@Override public NClob getNClob(int columnIndex) throws SQLException {
		return mapOrDefault(columnIndex, influxDbResultSet::getNClob);
	}

	@Override public SQLXML getSQLXML(int columnIndex) throws SQLException {
		return mapOrDefault(columnIndex, influxDbResultSet::getSQLXML);
	}

	@Override public String getNString(int columnIndex) throws SQLException {
		return mapOrDefault(columnIndex, influxDbResultSet::getNString);
	}

	@Override public Reader getNCharacterStream(int columnIndex) throws SQLException {
		return mapOrDefault(columnIndex, influxDbResultSet::getNCharacterStream);
	}

	@Override public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
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

	protected <T> T mapOrDefault(int columnIndex, Function<Integer, T> o) throws SQLException {
		int indexToProxyTable = remapIndex(columnIndex);
		if (indexToProxyTable <= 0 || indexToProxyTable > influxDbResultSet.getMetaData().getColumnCount()) {
			return (T) defaults[columnIndex - 1];
		} else {
			return o.apply(indexToProxyTable);
		}
	}
}
