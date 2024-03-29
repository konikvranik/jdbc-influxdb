package net.suteren.jdbc.influxdb.resultset.proxy;

import java.sql.SQLException;
import java.sql.Types;
import java.util.function.IntFunction;

import net.suteren.jdbc.influxdb.InfluxDbConnection;

public class GetColumnResultSet extends AbstractProxyResultSet {
	public GetColumnResultSet(InfluxDbConnection influxDbConnection, String tableNamePattern, String catalog)
		throws SQLException {
		super(influxDbConnection.createStatement()
				.executeQuery(String.format("SHOW FIELD KEYS%1$s%2$s; SHOW TAG KEYS%1$s%2$s",
					databaseRestriction(catalog),
					getWithClause(tableNamePattern))),
			new String[] { "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME",
				"COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "NUM_PREC_RADIX", "NULLABLE", "REMARKS", "COLUMN_DEF",
				"SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH", "ORDINAL_POSITION", "IS_NULLABLE",
				"SCOPE_CATALOG", "SCOPE_SCHEMA", "SCOPE_TABLE", "SOURCE_DATA_TYPE", "IS_AUTOINCREMENT",
				"IS_GENERATEDCOLUMN", },
			new Object[] { null, null, null, null, Types.VARCHAR, "string", null, null, null, null, true, null, null,
				null, null, null, null, true, null, null, Types.VARCHAR, null, null, null }, catalog, null);
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

	@Override protected Object mapOrDefault(int columnIndex, IntFunction<Object> function) {
		if (columnIndex == 1) {
			return catalog == null ? super.mapOrDefault(columnIndex, function) : catalog;
		} else if (columnIndex == 3 || columnIndex == 21) {
			return getMetaData().getTableName(columnIndex);
		} else if (columnIndex == 5 || columnIndex == 22) {
			return Types.NUMERIC;
		} else {
			return super.mapOrDefault(columnIndex, function);
		}
	}
}
