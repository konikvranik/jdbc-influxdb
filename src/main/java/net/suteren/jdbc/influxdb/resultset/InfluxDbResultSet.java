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

import net.suteren.jdbc.influxdb.statement.InfluxDbStatement;

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
		throw new UnsupportedOperationException();
	}

	@Override public void updateBoolean(int columnIndex, boolean x) {
		throw new UnsupportedOperationException();
	}

	@Override public void updateByte(int columnIndex, byte x) {
		throw new UnsupportedOperationException();
	}

	@Override public void updateShort(int columnIndex, short x) {
		throw new UnsupportedOperationException();
	}

	@Override public void updateInt(int columnIndex, int x) {
		throw new UnsupportedOperationException();
	}

	@Override public void updateLong(int columnIndex, long x) {
		throw new UnsupportedOperationException();
	}

	@Override public void updateFloat(int columnIndex, float x) {
		throw new UnsupportedOperationException();
	}

	@Override public void updateDouble(int columnIndex, double x) {
		throw new UnsupportedOperationException();
	}

	@Override public void updateBigDecimal(int columnIndex, BigDecimal x) {
		throw new UnsupportedOperationException();
	}

	@Override public void updateString(int columnIndex, String x) {
		throw new UnsupportedOperationException();
	}

	@Override public void updateBytes(int columnIndex, byte[] x) {
		throw new UnsupportedOperationException();
	}

	@Override public void updateDate(int columnIndex, Date x) {
		throw new UnsupportedOperationException();
	}

	@Override public void updateTime(int columnIndex, Time x) {
		throw new UnsupportedOperationException();
	}

	@Override public void updateTimestamp(int columnIndex, Timestamp x) {
		throw new UnsupportedOperationException();
	}

	@Override public void updateAsciiStream(int columnIndex, InputStream x, int length) {
		throw new UnsupportedOperationException();
	}

	@Override public void updateBinaryStream(int columnIndex, InputStream x, int length) {
		throw new UnsupportedOperationException();
	}

	@Override public void updateCharacterStream(int columnIndex, Reader x, int length) {
		throw new UnsupportedOperationException();
	}

	@Override public void updateObject(int columnIndex, Object x, int scaleOrLength) {
		throw new UnsupportedOperationException();
	}

	@Override public void updateObject(int columnIndex, Object x) {
		throw new UnsupportedOperationException();
	}

	@Override public void insertRow() {
		throw new UnsupportedOperationException();
	}

	@Override public void updateRow() {
		throw new UnsupportedOperationException();
	}

	@Override public void deleteRow() {
		throw new UnsupportedOperationException();
	}

	@Override public void cancelRowUpdates() {
		throw new UnsupportedOperationException();
	}

	@Override public void updateRef(int columnIndex, Ref x) {
		throw new UnsupportedOperationException();
	}

	@Override public void updateBlob(int columnIndex, Blob x) {
		throw new UnsupportedOperationException();
	}

	@Override public void updateClob(int columnIndex, Clob x) {
		throw new UnsupportedOperationException();
	}

	@Override public void updateArray(int columnIndex, Array x) {
		throw new UnsupportedOperationException();
	}

	@Override public void updateRowId(int columnIndex, RowId x) {
		throw new UnsupportedOperationException();
	}

	@Override public void updateNString(int columnIndex, String nString) {
		throw new UnsupportedOperationException();
	}

	@Override public void updateNClob(int columnIndex, NClob nClob) {
		throw new UnsupportedOperationException();
	}

	@Override public void updateSQLXML(int columnIndex, SQLXML xmlObject) {
		throw new UnsupportedOperationException();
	}

	@Override public void updateNCharacterStream(int columnIndex, Reader x, long length) {
		throw new UnsupportedOperationException();
	}

	@Override public void updateAsciiStream(int columnIndex, InputStream x, long length) {
		throw new UnsupportedOperationException();
	}

	@Override public void updateBinaryStream(int columnIndex, InputStream x, long length) {
		throw new UnsupportedOperationException();
	}

	@Override public void updateCharacterStream(int columnIndex, Reader x, long length) {
		throw new UnsupportedOperationException();
	}

	@Override public void updateBlob(int columnIndex, InputStream inputStream, long length) {
		throw new UnsupportedOperationException();
	}

	@Override public void updateClob(int columnIndex, Reader reader, long length) {
		throw new UnsupportedOperationException();
	}

	@Override public void updateNClob(int columnIndex, Reader reader, long length) {
		throw new UnsupportedOperationException();
	}

	@Override public void updateNCharacterStream(int columnIndex, Reader x) {
		throw new UnsupportedOperationException();
	}

	@Override public void updateAsciiStream(int columnIndex, InputStream x) {
		throw new UnsupportedOperationException();
	}

	@Override public void updateBinaryStream(int columnIndex, InputStream x) {
		throw new UnsupportedOperationException();
	}

	@Override public void updateCharacterStream(int columnIndex, Reader x) {
		throw new UnsupportedOperationException();
	}

	@Override public void updateBlob(int columnIndex, InputStream inputStream) {
		throw new UnsupportedOperationException();
	}

	@Override public void updateClob(int columnIndex, Reader reader) {
		throw new UnsupportedOperationException();
	}

	@Override public void updateNClob(int columnIndex, Reader reader) {
		throw new UnsupportedOperationException();
	}
}
