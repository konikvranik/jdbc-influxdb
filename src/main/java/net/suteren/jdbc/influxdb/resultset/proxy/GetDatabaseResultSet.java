package net.suteren.jdbc.influxdb.resultset.proxy;

import java.sql.SQLException;

import net.suteren.jdbc.influxdb.InfluxDbConnection;

public class GetDatabaseResultSet extends AbstractProxyResultSet {
	public GetDatabaseResultSet(InfluxDbConnection influxDbConnection) throws SQLException {
		super(influxDbConnection.createStatement().executeQuery("SHOW DATABASES"),
			new String[] { "TABLE_SCHEM", "TABLE_CATALOG" },
			new String[] { null, null });
	}

	@Override protected int remapIndex(int columnIndex) {
		return columnIndex == 1 ? 1 : 0;
	}
}
