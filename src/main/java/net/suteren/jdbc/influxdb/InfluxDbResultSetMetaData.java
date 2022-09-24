package net.suteren.jdbc.influxdb;

import java.sql.ResultSetMetaData;
import java.util.List;

import org.influxdb.dto.QueryResult;

public class InfluxDbResultSetMetaData implements ResultSetMetaData {
	private final InfluxDbResultSet influxDbResultSet;

	public InfluxDbResultSetMetaData(InfluxDbResultSet influxDbResultSet) {
		this.influxDbResultSet = influxDbResultSet;
	}

	@Override public int getColumnCount() {
		List<QueryResult.Series> series = influxDbResultSet.result.getSeries();
		return series == null ? 0 : series.get(influxDbResultSet.seriesPosition.get()).getColumns().size();
	}

	@Override public boolean isAutoIncrement(int column) {
		return false;
	}

	@Override public boolean isCaseSensitive(int column) {
		return false;
	}

	@Override public boolean isSearchable(int column) {
		return false;
	}

	@Override public boolean isCurrency(int column) {
		return false;
	}

	@Override public int isNullable(int column) {
		return ResultSetMetaData.columnNullableUnknown;
	}

	@Override public boolean isSigned(int column) {
		return false;
	}

	@Override public int getColumnDisplaySize(int column) {
		return 0;
	}

	@Override public String getColumnLabel(int column) {
		return getColumnName(column);
	}

	@Override public String getColumnName(int column) {
		List<QueryResult.Series> series = influxDbResultSet.result.getSeries();
		if (series == null) {
			return null;
		}
		return series.get(influxDbResultSet.seriesPosition.get()).getColumns()
			.get(column - 1);
	}

	@Override public String getSchemaName(int column) {
		return "";
	}

	@Override public int getPrecision(int column) {
		return 0;
	}

	@Override public int getScale(int column) {
		return 0;
	}

	@Override public String getTableName(int column) {
		List<QueryResult.Series> series = influxDbResultSet.result.getSeries();
		if (series == null) {
			return null;
		}
		return series.get(influxDbResultSet.seriesPosition.get()).getName();
	}

	@Override public String getCatalogName(int column) {
		return "";
	}

	@Override public int getColumnType(int column) {
		return 0;
	}

	@Override public String getColumnTypeName(int column) {
		return getColumnClassName(column);
	}

	@Override public boolean isReadOnly(int column) {
		return false;
	}

	@Override public boolean isWritable(int column) {
		return false;
	}

	@Override public boolean isDefinitelyWritable(int column) {
		return false;
	}

	@Override public String getColumnClassName(int column) {
		return influxDbResultSet.getValues().get(influxDbResultSet.seriesPosition.get()).get(column).getClass()
			.getName();
	}

	@Override public <T> T unwrap(Class<T> iface) {
		return null;
	}

	@Override public boolean isWrapperFor(Class<?> iface) {
		return false;
	}
}
