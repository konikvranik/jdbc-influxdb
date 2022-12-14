package net.suteren.jdbc.influxdb.resultset.proxy;

import java.sql.SQLException;

import net.suteren.jdbc.influxdb.InfluxDbConnection;

public class GetCatalogResultSet extends AbstractProxyResultSet {
	public GetCatalogResultSet(InfluxDbConnection influxDbConnection) throws SQLException {
		super(influxDbConnection.createStatement().executeQuery("SHOW DATABASES"),
			new String[] { "TABLE_CAT" },
			new String[] { null });
	}

	@Override protected int remapIndex(int columnIndex) {
		return columnIndex == 1 ? 1 : 0;
	}
}
