package net.suteren.jdbc.influxdb.resultset.proxy;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import lombok.Getter;
import net.suteren.jdbc.influxdb.resultset.InfluxDbResultSetMetaData;

public class AbstractProxyResultSetMetadata implements ResultSetMetaData {

	private final InfluxDbResultSetMetaData influxDbResultSetMetaData;
	private final AbstractProxyResultSet abstractProxyResultSet;
	@Getter private final int columnCount;

	public AbstractProxyResultSetMetadata(InfluxDbResultSetMetaData influxDbResultSetMetaData,
		AbstractProxyResultSet abstractProxyResultSet, int columnCount) {
		this.influxDbResultSetMetaData = influxDbResultSetMetaData;
		this.abstractProxyResultSet = abstractProxyResultSet;
		this.columnCount = columnCount;
	}

	@Override public boolean isAutoIncrement(int column) {
		return influxDbResultSetMetaData.isAutoIncrement(abstractProxyResultSet.remapIndex(column));
	}

	@Override public boolean isCaseSensitive(int column) {
		return influxDbResultSetMetaData.isCaseSensitive(abstractProxyResultSet.remapIndex(column));
	}

	@Override public boolean isSearchable(int column) {
		return influxDbResultSetMetaData.isSearchable(abstractProxyResultSet.remapIndex(column));
	}

	@Override public boolean isCurrency(int column) {
		return influxDbResultSetMetaData.isCurrency(abstractProxyResultSet.remapIndex(column));
	}

	@Override public int isNullable(int column) {
		return influxDbResultSetMetaData.isNullable(abstractProxyResultSet.remapIndex(column));
	}

	@Override public boolean isSigned(int column) {
		return influxDbResultSetMetaData.isSigned(abstractProxyResultSet.remapIndex(column));
	}

	@Override public int getColumnDisplaySize(int column) {
		return influxDbResultSetMetaData.getColumnDisplaySize(abstractProxyResultSet.remapIndex(column));
	}

	@Override public String getColumnLabel(int column) {
		return influxDbResultSetMetaData.getColumnLabel(abstractProxyResultSet.remapIndex(column));
	}

	@Override public String getColumnName(int column) {
		return influxDbResultSetMetaData.getColumnName(abstractProxyResultSet.remapIndex(column));
	}

	@Override public String getSchemaName(int column) {
		return influxDbResultSetMetaData.getSchemaName(abstractProxyResultSet.remapIndex(column));
	}

	@Override public int getPrecision(int column) {
		return influxDbResultSetMetaData.getPrecision(abstractProxyResultSet.remapIndex(column));
	}

	@Override public int getScale(int column) {
		return influxDbResultSetMetaData.getScale(abstractProxyResultSet.remapIndex(column));
	}

	@Override public String getTableName(int column) {
		return influxDbResultSetMetaData.getTableName(abstractProxyResultSet.remapIndex(column));
	}

	@Override public String getCatalogName(int column) {
		return influxDbResultSetMetaData.getCatalogName(abstractProxyResultSet.remapIndex(column));
	}

	@Override public int getColumnType(int column) {
		return influxDbResultSetMetaData.getColumnType(abstractProxyResultSet.remapIndex(column));
	}

	@Override public String getColumnTypeName(int column) {
		return influxDbResultSetMetaData.getColumnTypeName(abstractProxyResultSet.remapIndex(column));
	}

	@Override public boolean isReadOnly(int column) {
		return influxDbResultSetMetaData.isReadOnly(abstractProxyResultSet.remapIndex(column));
	}

	@Override public boolean isWritable(int column) {
		return influxDbResultSetMetaData.isWritable(abstractProxyResultSet.remapIndex(column));
	}

	@Override public boolean isDefinitelyWritable(int column) {
		return influxDbResultSetMetaData.isDefinitelyWritable(abstractProxyResultSet.remapIndex(column));
	}

	@Override public String getColumnClassName(int column) {
		return influxDbResultSetMetaData.getColumnClassName(abstractProxyResultSet.remapIndex(column));
	}

	@Override public <T> T unwrap(Class<T> iface) {
		return influxDbResultSetMetaData.unwrap(iface);
	}

	@Override public boolean isWrapperFor(Class<?> iface) {
		return influxDbResultSetMetaData.isWrapperFor(iface);
	}
}
