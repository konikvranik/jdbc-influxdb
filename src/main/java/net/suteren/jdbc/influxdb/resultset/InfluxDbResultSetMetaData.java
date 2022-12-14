package net.suteren.jdbc.influxdb.resultset;

import java.sql.ResultSetMetaData;
import java.util.List;

import org.influxdb.dto.QueryResult;

public class InfluxDbResultSetMetaData implements ResultSetMetaData {
	private final AbstractInfluxDbMultiResultSet influxDbResultSet;

	public InfluxDbResultSetMetaData(AbstractInfluxDbMultiResultSet influxDbResultSet) {
		this.influxDbResultSet = influxDbResultSet;
	}

	@Override public int getColumnCount() {
		return influxDbResultSet.getCurrentSeries()
			.map(QueryResult.Series::getColumns)
			.map(List::size)
			.orElse(0);
	}

	@Override public boolean isAutoIncrement(int column) {
		return false;
	}

	@Override public boolean isCaseSensitive(int column) {
		return true;
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
		return influxDbResultSet.getCurrentSeries()
			.map(QueryResult.Series::getColumns)
			.map(c -> c.get(column - 1))
			.orElse(null);
	}

	@Override public String getSchemaName(int column) {
		return null;
	}

	@Override public int getPrecision(int column) {
		return 0;
	}

	@Override public int getScale(int column) {
		return 0;
	}

	@Override public String getTableName(int column) {
		return influxDbResultSet.getCurrentSeries()
			.map(QueryResult.Series::getName)
			.orElse(null);
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
		List<List<Object>> currentRows = influxDbResultSet.getCurrentRows();
		if (currentRows.isEmpty()) {
			return null;
		} else {
			Object o = currentRows.get(0).get(column - 1);
			return o == null ? null : o.getClass().getName();
		}
	}

	@Override public <T> T unwrap(Class<T> iface) {
		return null;
	}

	@Override public boolean isWrapperFor(Class<?> iface) {
		return false;
	}
}
