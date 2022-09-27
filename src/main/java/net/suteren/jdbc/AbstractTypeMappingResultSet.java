package net.suteren.jdbc;

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
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

public abstract class AbstractTypeMappingResultSet extends AbstractBaseResultSet {

	@Override public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
		return null;
	}

	@Override public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
		return null;
	}

	@Override public String getString(int columnIndex) throws SQLException {
		return getObject(columnIndex, String.class);
	}

	@Override public boolean getBoolean(int columnIndex) throws SQLException {
		return getObject(columnIndex, Boolean.class);
	}

	@Override public byte getByte(int columnIndex) throws SQLException {
		return getObject(columnIndex, Byte.class);
	}

	@Override public short getShort(int columnIndex) throws SQLException {
		return getObject(columnIndex, Short.class);
	}

	@Override public int getInt(int columnIndex) throws SQLException {
		return getObject(columnIndex, Integer.class);
	}

	@Override public long getLong(int columnIndex) throws SQLException {
		return getObject(columnIndex, Long.class);
	}

	@Override public float getFloat(int columnIndex) throws SQLException {
		return getObject(columnIndex, Float.class);
	}

	@Override public double getDouble(int columnIndex) throws SQLException {
		return getObject(columnIndex, Double.class);
	}

	@Override public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
		return getObject(columnIndex, BigDecimal.class);
	}

	@Override public byte[] getBytes(int columnIndex) throws SQLException {
		return getObject(columnIndex, byte[].class);
	}

	@Override public Date getDate(int columnIndex) throws SQLException {
		return getObject(columnIndex, Date.class);
	}

	@Override public Time getTime(int columnIndex) throws SQLException {
		return getObject(columnIndex, Time.class);
	}

	@Override public Timestamp getTimestamp(int columnIndex) throws SQLException {
		return getObject(columnIndex, Timestamp.class);
	}

	@Override public InputStream getAsciiStream(int columnIndex) throws SQLException {
		return getObject(columnIndex, InputStream.class);
	}

	@Override public InputStream getUnicodeStream(int columnIndex) throws SQLException {
		return getObject(columnIndex, InputStream.class);
	}

	@Override public InputStream getBinaryStream(int columnIndex) throws SQLException {
		return getObject(columnIndex, InputStream.class);
	}

	@Override public Object getObject(int columnIndex) throws SQLException {
		return getObject(columnIndex, Object.class);
	}

	@Override public Reader getCharacterStream(int columnIndex) throws SQLException {
		return getObject(columnIndex, Reader.class);
	}

	@Override public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
		return getObject(columnIndex, BigDecimal.class);
	}

	@Override public Ref getRef(int columnIndex) throws SQLException {
		return getObject(columnIndex, Ref.class);
	}

	@Override public Blob getBlob(int columnIndex) throws SQLException {
		return getObject(columnIndex, Blob.class);
	}

	@Override public Clob getClob(int columnIndex) throws SQLException {
		return getObject(columnIndex, Clob.class);
	}

	@Override public Array getArray(int columnIndex) throws SQLException {
		return getObject(columnIndex, Array.class);
	}

	@Override public Date getDate(int columnIndex, Calendar cal) throws SQLException {
		cal.setTimeInMillis(getDate(columnIndex).getTime());
		return new Date(cal.getTimeInMillis());
	}

	@Override public Time getTime(int columnIndex, Calendar cal) throws SQLException {
		cal.setTimeInMillis(getTime(columnIndex).getTime());
		return new Time(cal.getTimeInMillis());
	}

	@Override public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
		cal.setTimeInMillis(getTimestamp(columnIndex).getTime());
		return new Timestamp(cal.getTimeInMillis());
	}

	@Override public URL getURL(int columnIndex) throws SQLException {
		return getObject(columnIndex, URL.class);
	}

	@Override public RowId getRowId(int columnIndex) throws SQLException {
		return getObject(columnIndex, RowId.class);
	}

	@Override public NClob getNClob(int columnIndex) throws SQLException {
		return getObject(columnIndex, NClob.class);
	}

	@Override public SQLXML getSQLXML(int columnIndex) throws SQLException {
		return getObject(columnIndex, SQLXML.class);
	}

	@Override public String getNString(int columnIndex) throws SQLException {
		return getObject(columnIndex, String.class);
	}

	@Override public Reader getNCharacterStream(int columnIndex) throws SQLException {
		return getObject(columnIndex, Reader.class);
	}

}
