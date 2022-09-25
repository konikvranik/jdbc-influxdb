package net.suteren.jdbc.influxdb.resultset.proxy;

import java.sql.SQLException;

import net.suteren.jdbc.influxdb.InfluxDbConnection;

public class GetFieldKeysResultSet extends AbstractProxyResultSet {
	public GetFieldKeysResultSet(InfluxDbConnection influxDbConnection, String tableNamePattern) throws SQLException {
		super(influxDbConnection.createStatement().executeQuery(String.format("SHOW FIELD KEYS%s",
				tableNamePattern != null && !tableNamePattern.isBlank() ?
					String.format(" WITH MEASUREMENT =~ /%s/", tableNamePattern) : "")),
			new String[] { "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME",
				"COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "NUM_PREC_RADIX", "NULLABLE", "REMARKS", "COLUMN_DEF",
				"SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH", "ORDINAL_POSITION", "IS_NULLABLE",
				"SCOPE_CATALOG", "SCOPE_SCHEMA", "SCOPE_TABLE", "SOURCE_DATA_TYPE", "IS_AUTOINCREMENT",
				"IS_GENERATEDCOLUMN", },
			new String[] { null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
				null, null, null, null, null, null, null, null, null });
	}

	@Override protected int remapIndex(int columnIndex) {
		return columnIndex == 4 ? 1 : 0;
	}
}
