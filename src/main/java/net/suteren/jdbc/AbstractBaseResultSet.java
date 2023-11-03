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
import java.sql.ResultSet;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

public abstract class AbstractBaseResultSet implements ResultSet {

	public String getString(String columnLabel) throws SQLException {
		return getString(findColumn(columnLabel));
	}

	public boolean getBoolean(String columnLabel) throws SQLException {
		return getBoolean(findColumn(columnLabel));
	}

	public byte getByte(String columnLabel) throws SQLException {
		return getByte(findColumn(columnLabel));
	}

	public short getShort(String columnLabel) throws SQLException {
		return getShort(findColumn(columnLabel));
	}

	public int getInt(String columnLabel) throws SQLException {
		return getInt(findColumn(columnLabel));
	}

	public long getLong(String columnLabel) throws SQLException {
		return getLong(findColumn(columnLabel));
	}

	public float getFloat(String columnLabel) throws SQLException {
		return getFloat(findColumn(columnLabel));
	}

	public double getDouble(String columnLabel) throws SQLException {
		return getDouble(findColumn(columnLabel));
	}

	/**
	 * @deprecated See {@link AbstractBaseResultSet#getBigDecimal(String, int)}
	 */
	@Deprecated(since = "0.1.0")
	public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
		return getBigDecimal(findColumn(columnLabel), scale);
	}

	public byte[] getBytes(String columnLabel) throws SQLException {
		return getBytes(findColumn(columnLabel));
	}

	public Date getDate(String columnLabel) throws SQLException {
		return getDate(findColumn(columnLabel));
	}

	public Time getTime(String columnLabel) throws SQLException {
		return getTime(findColumn(columnLabel));
	}

	public Timestamp getTimestamp(String columnLabel) throws SQLException {
		return getTimestamp(findColumn(columnLabel));
	}

	public InputStream getAsciiStream(String columnLabel) throws SQLException {
		return getAsciiStream(findColumn(columnLabel));
	}

	/**
	 * @deprecated see {@link ResultSet#getUnicodeStream(int)}
	 */
	@Deprecated(since = "0.1.0")
	public InputStream getUnicodeStream(String columnLabel) throws SQLException {
		return getUnicodeStream(findColumn(columnLabel));
	}

	public InputStream getBinaryStream(String columnLabel) throws SQLException {
		return getBinaryStream(findColumn(columnLabel));
	}

	public Reader getCharacterStream(String columnLabel) throws SQLException {
		return getCharacterStream(findColumn(columnLabel));
	}

	public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
		return getBigDecimal(findColumn(columnLabel));
	}

	public Object getObject(String columnLabel, Map<String, Class<?>> map) {
		return null;
	}

	public Ref getRef(String columnLabel) {
		return null;
	}

	public Blob getBlob(String columnLabel) {
		return null;
	}

	public Clob getClob(String columnLabel) {
		return null;
	}

	public Array getArray(String columnLabel) {
		return null;
	}

	public Date getDate(String columnLabel, Calendar cal) throws SQLException {
		return getDate(findColumn(columnLabel), cal);
	}

	public Time getTime(String columnLabel, Calendar cal) throws SQLException {
		return getTime(findColumn(columnLabel), cal);
	}

	public URL getURL(String columnLabel) {
		return null;
	}

	public void updateRef(String columnLabel, Ref x) {

	}

	public void updateBlob(String columnLabel, Blob x) {

	}

	public void updateClob(String columnLabel, Clob x) {

	}

	public void updateArray(String columnLabel, Array x) {

	}

	public RowId getRowId(String columnLabel) {
		return null;
	}

	public void updateRowId(String columnLabel, RowId x) {

	}

	public NClob getNClob(String columnLabel) {
		return null;
	}

	public SQLXML getSQLXML(String columnLabel) {
		return null;
	}

	public void updateSQLXML(String columnLabel, SQLXML xmlObject) {

	}

	public String getNString(String columnLabel) {
		return null;
	}

	public Reader getNCharacterStream(String columnLabel) {
		return null;
	}

	public void updateNCharacterStream(String columnLabel, Reader reader, long length) {

	}

	public void updateAsciiStream(String columnLabel, InputStream x, long length) {

	}

	public void updateBinaryStream(String columnLabel, InputStream x, long length) {

	}

	public void updateCharacterStream(String columnLabel, Reader reader, long length) {

	}

	public void updateBlob(String columnLabel, InputStream inputStream, long length) {

	}

	public void updateClob(String columnLabel, Reader reader, long length) {

	}

	public void updateNClob(String columnLabel, Reader reader, long length) {

	}

	public void updateNCharacterStream(String columnLabel, Reader reader) {

	}

	public void updateAsciiStream(String columnLabel, InputStream x) {

	}

	public void updateBinaryStream(String columnLabel, InputStream x) {

	}

	@Override public Object getObject(String columnLabel) throws SQLException {
		return getObject(findColumn(columnLabel));
	}

	@Override public void updateNull(String columnLabel) {

	}

	@Override public void updateBoolean(String columnLabel, boolean x) {

	}

	@Override public void updateByte(String columnLabel, byte x) {

	}

	@Override public void updateShort(String columnLabel, short x) {

	}

	@Override public void updateInt(String columnLabel, int x) {

	}

	@Override public void updateLong(String columnLabel, long x) {

	}

	@Override public void updateFloat(String columnLabel, float x) {

	}

	@Override public void updateDouble(String columnLabel, double x) {

	}

	@Override public void updateBigDecimal(String columnLabel, BigDecimal x) {

	}

	@Override public void updateString(String columnLabel, String x) {

	}

	@Override public void updateBytes(String columnLabel, byte[] x) {

	}

	@Override public void updateDate(String columnLabel, Date x) {

	}

	@Override public void updateTime(String columnLabel, Time x) {

	}

	@Override public void updateTimestamp(String columnLabel, Timestamp x) {

	}

	@Override public void updateAsciiStream(String columnLabel, InputStream x, int length) {

	}

	@Override public void updateBinaryStream(String columnLabel, InputStream x, int length) {

	}

	@Override public void updateCharacterStream(String columnLabel, Reader reader, int length) {

	}

	@Override public void updateObject(String columnLabel, Object x, int scaleOrLength) {

	}

	@Override public void updateObject(String columnLabel, Object x) {

	}

	@Override public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
		return getTimestamp(findColumn(columnLabel), cal);
	}

	@Override public void updateNString(String columnLabel, String nString) {

	}

	@Override public void updateNClob(String columnLabel, NClob nClob) {

	}

	@Override public void updateCharacterStream(String columnLabel, Reader reader) {

	}

	@Override public void updateBlob(String columnLabel, InputStream inputStream) {

	}

	@Override public void updateClob(String columnLabel, Reader reader) {

	}

	@Override public void updateNClob(String columnLabel, Reader reader) {

	}

	@Override public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
		return getObject(findColumn(columnLabel), type);
	}

	protected static String quoteName(String tableNamePattern) {
		return tableNamePattern.replace("\"", "\\\"");
	}

	protected static String getWithClause(String tableNamePattern) {
		return StringUtils.isNotBlank(tableNamePattern) ? String.format(" FROM \"%s\"", quoteName(tableNamePattern)) : "";
	}
}
