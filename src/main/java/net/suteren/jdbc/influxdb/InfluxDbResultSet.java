package net.suteren.jdbc.influxdb;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.influxdb.dto.QueryResult;

public class InfluxDbResultSet extends net.suteren.jdbc.AbstractBaseResultSet  {
	final QueryResult.Result result;
	final AtomicInteger valuesPosition = new AtomicInteger(-1);
	final AtomicInteger seriesPosition = new AtomicInteger(0);
	private final InfluxDbStatement statement;
	private boolean isClosed = false;
	private String cursorName;

	public InfluxDbResultSet(InfluxDbStatement statement) {
		this.statement = statement;
		this.result = statement.results.get(statement.resultPosition);
	}

	protected <U> U getValue(int index, Class<U> clzz, Function<Object, U> convert) {
		Object obj = getValues().get(seriesPosition.get()).get(index - 1);
		if (clzz.isInstance(obj)) {
			return (U) obj;
		} else {
			return convert.apply(obj);
		}
	}

	@Override public boolean next() {
		List<QueryResult.Series> series = result.getSeries();
		if (series != null && seriesPosition.intValue() < series.size()) {
			seriesPosition.addAndGet(1);
			return true;
		} else {
			return false;
		}
	}

	@Override public void close() {
		seriesPosition.set(0);
		isClosed = true;
	}

	@Override public boolean wasNull() {
		return false;
	}

	@Override public String getString(int columnIndex) {
		return getValue(columnIndex, String.class, String::valueOf);
	}

	@Override public boolean getBoolean(int columnIndex) {
		return getValue(columnIndex, Boolean.class, x -> Boolean.parseBoolean(String.valueOf(x)));
	}

	@Override public byte getByte(int columnIndex) {
		return getValue(columnIndex, Byte.class, x -> Byte.parseByte(String.valueOf(x)));
	}

	@Override public short getShort(int columnIndex) {
		return getValue(columnIndex, Short.class, x -> Short.parseShort(String.valueOf(x)));
	}

	@Override public int getInt(int columnIndex) {
		return getValue(columnIndex, Integer.class, x -> Integer.parseInt(String.valueOf(x)));
	}

	@Override public long getLong(int columnIndex) {
		return getValue(columnIndex, Long.class, x -> Long.parseLong(String.valueOf(x)));
	}

	@Override public float getFloat(int columnIndex) {
		return getValue(columnIndex, Float.class, x -> Float.parseFloat(String.valueOf(x)));
	}

	@Override public double getDouble(int columnIndex) {
		return getValue(columnIndex, Double.class, x -> Double.parseDouble(String.valueOf(x)));
	}

	@Override public BigDecimal getBigDecimal(int columnIndex, int scale) {
		return getValue(columnIndex, BigDecimal.class,
			x -> BigDecimal.valueOf(Long.parseLong(String.valueOf(x)), scale));
	}

	@Override public byte[] getBytes(int columnIndex) {
		return getValue(columnIndex, byte[].class, x -> String.valueOf(x).getBytes());
	}

	@Override public Date getDate(int columnIndex) {
		return getValue(columnIndex, Date.class, x -> Date.valueOf(String.valueOf(x)));
	}

	@Override public Time getTime(int columnIndex) {
		return getValue(columnIndex, Time.class, x -> Time.valueOf(String.valueOf(x)));
	}

	@Override public Timestamp getTimestamp(int columnIndex) {
		return getValue(columnIndex, Timestamp.class, x -> Timestamp.valueOf(String.valueOf(x)));
	}

	@Override public InputStream getAsciiStream(int columnIndex) {
		return getValue(columnIndex, InputStream.class,
			x -> new ByteArrayInputStream(String.valueOf(x).getBytes(StandardCharsets.US_ASCII)));
	}

	@Override public InputStream getUnicodeStream(int columnIndex) {
		return getValue(columnIndex, InputStream.class,
			x -> new ByteArrayInputStream(String.valueOf(x).getBytes(StandardCharsets.UTF_8)));
	}

	@Override public InputStream getBinaryStream(int columnIndex) {
		return getValue(columnIndex, InputStream.class,
			x -> new ByteArrayInputStream(String.valueOf(x).getBytes()));
	}

	List<List<Object>> getValues() {
		List<QueryResult.Series> series = this.result.getSeries();
		if (series == null) {
			return List.of();
		} else {
			return series.get(seriesPosition.intValue()).getValues();
		}
	}

	@Override public SQLWarning getWarnings() {
		return new SQLWarning(result.getError());
	}

	@Override public void clearWarnings() {

	}

	@Override public String getCursorName() {
		return cursorName;
	}

	@Override public ResultSetMetaData getMetaData() {
		return new InfluxDbResultSetMetaData(this);
	}

	@Override public Object getObject(int columnIndex) {
		return getValue(columnIndex, Object.class, Function.identity());
	}

	@Override public Reader getCharacterStream(int columnIndex) {
		return new InputStreamReader(getUnicodeStream(columnIndex));
	}

	@Override public BigDecimal getBigDecimal(int columnIndex) {
		return getValue(columnIndex, BigDecimal.class, x -> BigDecimal.valueOf(Double.parseDouble(String.valueOf(x))));
	}

	@Override public boolean isBeforeFirst() {
		return valuesPosition.get() < 0;
	}

	@Override public boolean isAfterLast() {
		return valuesPosition.get() >= getValues().size();
	}

	@Override public boolean isFirst() {
		return valuesPosition.get() == 0;
	}

	@Override public boolean isLast() {
		return valuesPosition.get() == getValues().size() - 1;
	}

	@Override public void beforeFirst() {
		valuesPosition.set(-1);
	}

	@Override public void afterLast() {
		valuesPosition.set(getValues().size());
	}

	@Override public boolean first() {
		if (getValues().isEmpty()) {
			return false;
		} else {
			valuesPosition.set(0);
			return true;
		}
	}

	@Override public boolean last() {
		if (getValues().isEmpty()) {
			return false;
		} else {
			valuesPosition.set(getValues().size() - 1);
			return true;
		}
	}

	@Override public int getRow() {
		return valuesPosition.get();
	}

	@Override public boolean absolute(int row) {
		if (row < 0) {
			valuesPosition.set(getValues().size() - row);
			return !isBeforeFirst();
		} else {
			valuesPosition.set(row - 1);
			return !isAfterLast() && !isBeforeFirst();
		}
	}

	@Override public boolean relative(int rows) {
		valuesPosition.addAndGet(rows);
		return !isAfterLast() && !isBeforeFirst();
	}

	@Override public boolean previous() {
		valuesPosition.addAndGet(-1);
		return !isBeforeFirst();
	}

	@Override public void setFetchDirection(int direction) {

	}

	@Override public int getFetchDirection() {
		return FETCH_UNKNOWN;
	}

	@Override public void setFetchSize(int rows) {

	}

	@Override public int getFetchSize() {
		return 0;
	}

	@Override public int getType() {
		return TYPE_SCROLL_INSENSITIVE;
	}

	@Override public int getConcurrency() {
		return CONCUR_READ_ONLY;
	}

	@Override public boolean rowUpdated() {
		return false;
	}

	@Override public boolean rowInserted() {
		return false;
	}

	@Override public boolean rowDeleted() {
		return false;
	}

	@Override public void updateNull(int columnIndex) {

	}

	@Override public void updateBoolean(int columnIndex, boolean x) {

	}

	@Override public void updateByte(int columnIndex, byte x) {

	}

	@Override public void updateShort(int columnIndex, short x) {

	}

	@Override public void updateInt(int columnIndex, int x) {

	}

	@Override public void updateLong(int columnIndex, long x) {

	}

	@Override public void updateFloat(int columnIndex, float x) {

	}

	@Override public void updateDouble(int columnIndex, double x) {

	}

	@Override public void updateBigDecimal(int columnIndex, BigDecimal x) {

	}

	@Override public void updateString(int columnIndex, String x) {

	}

	@Override public void updateBytes(int columnIndex, byte[] x) {

	}

	@Override public void updateDate(int columnIndex, Date x) {

	}

	@Override public void updateTime(int columnIndex, Time x) {

	}

	@Override public void updateTimestamp(int columnIndex, Timestamp x) {

	}

	@Override public void updateAsciiStream(int columnIndex, InputStream x, int length) {

	}

	@Override public void updateBinaryStream(int columnIndex, InputStream x, int length) {

	}

	@Override public void updateCharacterStream(int columnIndex, Reader x, int length) {

	}

	@Override public void updateObject(int columnIndex, Object x, int scaleOrLength) {

	}

	@Override public void updateObject(int columnIndex, Object x) {

	}

	@Override public void insertRow() {

	}

	@Override public void updateRow() {

	}

	@Override public void deleteRow() {

	}

	@Override public void refreshRow() {

	}

	@Override public void cancelRowUpdates() {

	}

	@Override public void moveToInsertRow() {

	}

	@Override public void moveToCurrentRow() {

	}

	@Override public Statement getStatement() {
		return statement;
	}

	@Override public Object getObject(int columnIndex, Map<String, Class<?>> map) {
		return null;
	}

	@Override public Ref getRef(int columnIndex) {
		return null;
	}

	@Override public Blob getBlob(int columnIndex) {
		return null;
	}

	@Override public Clob getClob(int columnIndex) {
		return null;
	}

	@Override public Array getArray(int columnIndex) {
		return null;
	}

	@Override public Date getDate(int columnIndex, Calendar cal) {
		cal.setTimeInMillis(getDate(columnIndex).getTime());
		return new Date(cal.getTimeInMillis());
	}

	@Override public Time getTime(int columnIndex, Calendar cal) {
		cal.setTimeInMillis(getTime(columnIndex).getTime());
		return new Time(cal.getTimeInMillis());
	}

	@Override public Timestamp getTimestamp(int columnIndex, Calendar cal) {
		cal.setTimeInMillis(getTimestamp(columnIndex).getTime());
		return new Timestamp(cal.getTimeInMillis());
	}

	@Override public URL getURL(int columnIndex) {
		return null;
	}

	@Override public void updateRef(int columnIndex, Ref x) {

	}

	@Override public void updateBlob(int columnIndex, Blob x) {

	}

	@Override public void updateClob(int columnIndex, Clob x) {

	}

	@Override public void updateArray(int columnIndex, Array x) {

	}

	@Override public RowId getRowId(int columnIndex) {
		return null;
	}

	@Override public void updateRowId(int columnIndex, RowId x) {

	}

	@Override public int getHoldability() {
		return HOLD_CURSORS_OVER_COMMIT;
	}

	@Override public boolean isClosed() {
		return isClosed;
	}

	@Override public void updateNString(int columnIndex, String nString) {

	}

	@Override public void updateNClob(int columnIndex, NClob nClob) {

	}

	@Override public NClob getNClob(int columnIndex) {
		return null;
	}

	@Override public SQLXML getSQLXML(int columnIndex) {
		return null;
	}

	@Override public void updateSQLXML(int columnIndex, SQLXML xmlObject) {

	}

	@Override public String getNString(int columnIndex) {
		return null;
	}

	@Override public Reader getNCharacterStream(int columnIndex) {
		return null;
	}

	@Override public void updateNCharacterStream(int columnIndex, Reader x, long length) {

	}

	@Override public void updateAsciiStream(int columnIndex, InputStream x, long length) {

	}

	@Override public void updateBinaryStream(int columnIndex, InputStream x, long length) {

	}

	@Override public void updateCharacterStream(int columnIndex, Reader x, long length) {

	}

	@Override public void updateBlob(int columnIndex, InputStream inputStream, long length) {

	}

	@Override public void updateClob(int columnIndex, Reader reader, long length) {

	}

	@Override public void updateNClob(int columnIndex, Reader reader, long length) {

	}

	@Override public void updateNCharacterStream(int columnIndex, Reader x) {

	}

	@Override public void updateAsciiStream(int columnIndex, InputStream x) {

	}

	@Override public void updateBinaryStream(int columnIndex, InputStream x) {

	}

	@Override public void updateCharacterStream(int columnIndex, Reader x) {

	}

	@Override public void updateBlob(int columnIndex, InputStream inputStream) {

	}

	@Override public void updateClob(int columnIndex, Reader reader) {

	}

	@Override public void updateNClob(int columnIndex, Reader reader) {

	}

	@Override public <T> T getObject(int columnIndex, Class<T> type) {
		return null;
	}

	@Override public <T> T unwrap(Class<T> iface) {
		return null;
	}

	@Override public boolean isWrapperFor(Class<?> iface) {
		return false;
	}

	@Override public int findColumn(String columnLabel) throws SQLException {
		List<QueryResult.Series> series = this.result.getSeries();
		if (series == null) {
			throw new SQLException(String.format("No columen named %s", columnLabel));
		}
		return series.get(seriesPosition.intValue()).getColumns().indexOf(columnLabel) + 1;
	}
}
