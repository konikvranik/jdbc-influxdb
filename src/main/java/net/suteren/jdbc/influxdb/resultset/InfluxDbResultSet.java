package net.suteren.jdbc.influxdb.resultset;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.RowId;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;

import org.influxdb.dto.QueryResult;

import net.suteren.jdbc.influxdb.InfluxDbStatement;

public class InfluxDbResultSet extends AbstractReadableInfluxDbResultSet {

	public InfluxDbResultSet(InfluxDbStatement statement, List<QueryResult.Result> results) {
		super(results, statement);
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

	@Override public void cancelRowUpdates() {

	}

	@Override public void updateRef(int columnIndex, Ref x) {

	}

	@Override public void updateBlob(int columnIndex, Blob x) {

	}

	@Override public void updateClob(int columnIndex, Clob x) {

	}

	@Override public void updateArray(int columnIndex, Array x) {

	}

	@Override public void updateRowId(int columnIndex, RowId x) {

	}

	@Override public void updateNString(int columnIndex, String nString) {

	}

	@Override public void updateNClob(int columnIndex, NClob nClob) {

	}

	@Override public void updateSQLXML(int columnIndex, SQLXML xmlObject) {

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

}
