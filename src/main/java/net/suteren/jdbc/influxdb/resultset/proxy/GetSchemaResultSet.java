package net.suteren.jdbc.influxdb.resultset.proxy;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.function.IntFunction;

import org.apache.commons.lang3.StringUtils;

import net.suteren.jdbc.influxdb.InfluxDbConnection;
import net.suteren.jdbc.influxdb.resultset.InfluxDbResultSet;

public class GetSchemaResultSet extends AbstractProxyResultSet {
	public GetSchemaResultSet(InfluxDbConnection influxDbConnection, String catalog)
		throws SQLException {
		super(prepareResults(influxDbConnection, catalog),
			new String[] { "TABLE_SCHEM", "TABLE_CATALOG" },
			new String[] { null, null }, catalog, null);
	}

	private static InfluxDbResultSet prepareResults(InfluxDbConnection influxDbConnection, String catalog)
		throws SQLException {
		InfluxDbResultSet retentionPolicies = influxDbConnection.createStatement()
			.executeQuery(String.format("SHOW RETENTION POLICIES%s", StringUtils.isNotBlank(catalog) ? " ON " + catalog : ""));
		retentionPolicies.getResults().forEach(r ->
			r.getSeries().forEach(s ->
				s.setValues(new ArrayList<>(s.getValues())))
		);
		return retentionPolicies;
	}

	@Override protected int remapIndex(int columnIndex) {
		return columnIndex == 2 ? 1 : 0;
	}

	@Override protected Object mapOrDefault(int columnIndex, IntFunction<Object> function) {
		if (columnIndex == 2) {
			return catalog == null ? super.mapOrDefault(columnIndex, function) : catalog;
		} else {
			return super.mapOrDefault(columnIndex, function);
		}
	}
}
