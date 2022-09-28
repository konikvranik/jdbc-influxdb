package net.suteren.jdbc.influxdb.resultset.proxy;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.function.Function;

import net.suteren.jdbc.influxdb.InfluxDbConnection;

public class GetIndexResultSet extends AbstractProxyResultSet {
	public GetIndexResultSet(InfluxDbConnection influxDbConnection, String tableNamePattern) throws SQLException {
		super(influxDbConnection.createStatement()
				.executeQuery(String.format(" SHOW TAG KEYS%1$s", getWithClause(tableNamePattern))),
			new String[] { "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "NON_UNIQUE", "INDEX_QUALIFIER", "INDEX_NAME",
				"TYPE", "ORDINAL_POSITION", "COLUMN_NAME", "ASC_OR_DESC", "CARDINALITY", "PAGES", "FILTER_CONDITION", },
			new Object[] { null, null, null, true, null, null, DatabaseMetaData.tableIndexOther, 0, null, "A", 0,
				null, null });
	}

	private static String getWithClause(String tableNamePattern) {
		return tableNamePattern != null && !tableNamePattern.isBlank() ?
			String.format(" FROM \"%s\"", tableNamePattern) : "";
	}

	@Override protected int remapIndex(int columnIndex) {
		switch (columnIndex) {
		case 6:
		case 9:
			return 1;
		default:
			return 0;
		}
	}

	@Override protected Object mapOrDefault(int columnIndex, Function<Integer, Object> function) {
		if (columnIndex == 1) {
			return catalog == null ? super.mapOrDefault(columnIndex, function) : catalog;
		} else if (columnIndex == 3) {
			return getMetaData().getTableName(columnIndex);
		} else {
			return super.mapOrDefault(columnIndex, function);
		}
	}
}
