package net.suteren.jdbc;

import java.io.ByteArrayInputStream;
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
import java.util.Optional;

import lombok.SneakyThrows;

public abstract class AbstractTypeMappingResultSet extends AbstractBaseResultSet {

	private boolean wasNull;

	@SuppressWarnings("unchecked")
	@Override public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
		Object object = getObject(columnIndex);
		if (object == null) {
			wasNull = true;
			return null;
		}
		wasNull = false;
		if (type.isInstance(object)) {
			return (T) object;
		} else if (type == String.class) {
			return (T) String.valueOf(object);
		} else if (type == Boolean.class) {
			return (T) Boolean.valueOf(String.valueOf(object));
		} else if (type == Byte.class) {
			return (T) Byte.valueOf(String.valueOf(object));
		} else if (type == Short.class) {
			return (T) Short.valueOf(String.valueOf(object));
		} else if (type == Integer.class) {
			return (T) Integer.valueOf(String.valueOf(object));
		} else if (type == Long.class) {
			return (T) Long.valueOf(String.valueOf(object));
		} else if (type == Float.class) {
			return (T) Float.valueOf(String.valueOf(object));
		} else if (type == Double.class) {
			return (T) Double.valueOf(String.valueOf(object));
		} else if (type == byte[].class) {
			return (T) String.valueOf(object).getBytes();
		} else if (type == Date.class) {
			return (T) Date.valueOf(String.valueOf(object));
		} else if (type == Time.class) {
			return (T) Time.valueOf(String.valueOf(object));
		} else if (type == Timestamp.class) {
			return (T) Timestamp.valueOf(String.valueOf(object));
		} else if (type == InputStream.class) {
			return (T) new ByteArrayInputStream(String.valueOf(object).getBytes());
		} else if (type == BigDecimal.class) {
			return (T) BigDecimal.valueOf(Double.parseDouble(String.valueOf(object)));
		} else if (type == Ref.class) {
			return null;
		} else if (type == Blob.class) {
			return null;
		} else if (type == Clob.class) {
			return null;
		} else if (type == URL.class) {
			return null;
		} else if (type == RowId.class) {
			return null;
		} else if (type == Array.class) {
			return null;
		} else if (type == NClob.class) {
			return null;
		} else if (type == SQLXML.class) {
			return null;
		} else if (type == Reader.class) {
			return null;
		} else {
			return (T) object;
		}
	}

	@Override public boolean wasNull() throws SQLException {
		return wasNull;
	}

	@SneakyThrows
	@Override public Object getObject(int columnIndex, Map<String, Class<?>> map) {
		Class<?> type = map.get(getMetaData().getColumnTypeName(columnIndex));
		return type == null ? getObject(columnIndex) : getObject(columnIndex, type);
	}

	@Override public String getString(int columnIndex) throws SQLException {
		return getObject(columnIndex, String.class);
	}

	@Override public boolean getBoolean(int columnIndex) throws SQLException {
		return Optional.ofNullable(getObject(columnIndex, Boolean.class)).orElse(false);
	}

	@Override public byte getByte(int columnIndex) throws SQLException {
		return Optional.ofNullable(getObject(columnIndex, Byte.class)).orElse((byte) 0);
	}

	@Override public short getShort(int columnIndex) throws SQLException {
		return Optional.ofNullable(getObject(columnIndex, Short.class)).orElse((short) 0);
	}

	@Override public int getInt(int columnIndex) throws SQLException {
		return Optional.ofNullable(getObject(columnIndex, Integer.class)).orElse(0);
	}

	@Override public long getLong(int columnIndex) throws SQLException {
		return Optional.ofNullable(getObject(columnIndex, Long.class)).orElse(0L);
	}

	@Override public float getFloat(int columnIndex) throws SQLException {
		return Optional.ofNullable(getObject(columnIndex, Float.class)).orElse(0f);
	}

	@Override public double getDouble(int columnIndex) throws SQLException {
		return Optional.ofNullable(getObject(columnIndex, Double.class)).orElse(0d);
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
