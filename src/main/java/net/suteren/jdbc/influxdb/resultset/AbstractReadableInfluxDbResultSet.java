package net.suteren.jdbc.influxdb.resultset;

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
import java.sql.RowId;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.influxdb.dto.QueryResult;

import net.suteren.jdbc.influxdb.statement.InfluxDbStatement;

public abstract class AbstractReadableInfluxDbResultSet extends AbstractInfluxDbMultiResultSet {
	protected final InfluxDbStatement statement;

	public AbstractReadableInfluxDbResultSet(List<QueryResult.Result> results, InfluxDbStatement statement) {
		super(results);
		this.statement = statement;
		log = statement.getConnection().getMetaData().getDriver().getParentLogger();
	}

	public <U> U getValue(int index, Class<U> clzz, Function<Object, U> convert) {
		log.fine(() -> String.format("Getting value at #%s", index));
		Object obj = getCurrentRow().get(index - 1);
		if (clzz.isInstance(obj)) {
			log.fine(() -> String.format("Getting value %s", obj));
			return (U) obj;
		} else {
			U apply = convert.apply(obj);
			log.fine(() -> String.format("Getting value %s", apply));
			return apply;
		}
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

	@Override public Object getObject(int columnIndex) {
		return getValue(columnIndex, Object.class, Function.identity());
	}

	@Override public Reader getCharacterStream(int columnIndex) {
		return new InputStreamReader(getUnicodeStream(columnIndex));
	}

	@Override public BigDecimal getBigDecimal(int columnIndex) {
		return getValue(columnIndex, BigDecimal.class, x -> BigDecimal.valueOf(Double.parseDouble(String.valueOf(x))));
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

	@Override public RowId getRowId(int columnIndex) {
		return null;
	}

	@Override public NClob getNClob(int columnIndex) {
		return null;
	}

	@Override public SQLXML getSQLXML(int columnIndex) {
		return null;
	}

	@Override public String getNString(int columnIndex) {
		return null;
	}

	@Override public Reader getNCharacterStream(int columnIndex) {
		return null;
	}

	@Override public <T> T getObject(int columnIndex, Class<T> type) {
		return null;
	}

	@Override public boolean wasNull() {
		return false;
	}

	@Override public void refreshRow() {

	}

	@Override public <T> T unwrap(Class<T> iface) {
		return null;
	}

	@Override public boolean isWrapperFor(Class<?> iface) {
		return false;
	}
}
