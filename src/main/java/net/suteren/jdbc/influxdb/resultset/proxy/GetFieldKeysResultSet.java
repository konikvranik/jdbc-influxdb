package net.suteren.jdbc.influxdb.resultset.proxy;

import java.sql.SQLException;
import java.sql.Types;
import java.util.function.Function;

import net.suteren.jdbc.influxdb.InfluxDbConnection;

public class GetFieldKeysResultSet extends AbstractProxyResultSet {
	public GetFieldKeysResultSet(InfluxDbConnection influxDbConnection, String tableNamePattern) throws SQLException {
		super(influxDbConnection.createStatement()
				.executeQuery(String.format("SHOW FIELD KEYS%1$s; SHOW TAG KEYS%1$s", getWithClause(tableNamePattern))),
			new String[] { "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME",
				"COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "NUM_PREC_RADIX", "NULLABLE", "REMARKS", "COLUMN_DEF",
				"SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH", "ORDINAL_POSITION", "IS_NULLABLE",
				"SCOPE_CATALOG", "SCOPE_SCHEMA", "SCOPE_TABLE", "SOURCE_DATA_TYPE", "IS_AUTOINCREMENT",
				"IS_GENERATEDCOLUMN", },
			new Object[] { null, null, null, null, Types.VARCHAR, "string", null, null, null, null, true, null, null,
				null, null, null, null, true, null, null, null, null, null, null });
	}

	private static String getWithClause(String tableNamePattern) {
		return tableNamePattern != null && !tableNamePattern.isBlank() ?
			String.format(" FROM \"%s\"", tableNamePattern) : "";
	}

	@Override protected int remapIndex(int columnIndex) {
		switch (columnIndex) {
		case 4:
			return 1;
		case 5:
		case 6:
		case 22:
			return 2;
		default:
			return 0;
		}
	}

	@Override protected <T> T mapOrDefault(int columnIndex, Function<Integer, T> o) throws SQLException {
		if (columnIndex == 3 || columnIndex == 21) {
			return (T) getMetaData().getTableName(columnIndex);
		} else {
			return super.mapOrDefault(columnIndex, o);
		}
	}
}
