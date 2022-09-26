package net.suteren.jdbc.influxdb.resultset.proxy;

import java.sql.SQLException;

import net.suteren.jdbc.influxdb.InfluxDbConnection;

public class GetTablesResultSet extends AbstractProxyResultSet {
	public GetTablesResultSet(InfluxDbConnection influxDbConnection, String tableNamePattern, String catalog)
		throws SQLException {
		super(influxDbConnection.createStatement().executeQuery(String.format("SHOW MEASUREMENTS%s%s",
				catalog != null && !catalog.isBlank() ? String.format(" ON %s", catalog) : "",
				tableNamePattern != null && !tableNamePattern.isBlank() ?
					String.format(" WITH MEASUREMENT =~ /%s/", tableNamePattern) : "")),
			new String[] { "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS", "TYPE_CAT", "TYPE_SCHEM",
				"TYPE_NAME", "SELF_REFERENCING_COL_NAME", "REF_GENERATION" },
			new String[] { null, null, null, "TABLE", null, null, null, null, null, null }, catalog, null);
	}

	@Override protected int remapIndex(int columnIndex) {
		return columnIndex == 3 ? 1 : 0;
	}
}
