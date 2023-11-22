package net.suteren.jdbc.influxdb.resultset.proxy;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

import org.influxdb.InfluxDB;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

import net.suteren.jdbc.influxdb.InfluxDbConnection;
import net.suteren.jdbc.influxdb.resultset.InfluxDbResultSet;
import net.suteren.jdbc.influxdb.statement.InfluxDbStatement;

public class GetSchemaResultSet extends AbstractProxyResultSet {
	public GetSchemaResultSet(InfluxDbConnection influxDbConnection, String catalog)
		throws SQLException {
		super(prepareResults(influxDbConnection, catalog),
			new String[] { "TABLE_SCHEM", "TABLE_CATALOG" },
			new String[] { null, null }, catalog, null);
	}

	private static InfluxDbResultSet prepareResults(InfluxDbConnection influxDbConnection, String catalog)
		throws SQLException {
		if (catalog == null) {
			InfluxDB client = influxDbConnection.getClient();
			List<List<Object>> results = client.query(new Query("SHOW DATABASES")).getResults().stream()
				.map(QueryResult.Result::getSeries)
				.flatMap(Collection::stream)
				.map(QueryResult.Series::getValues)
				.flatMap(Collection::stream)
				.map(v -> v.get(0))
				.flatMap(cat ->
					client.query(new Query(String.format("SHOW RETENTION POLICIES ON \"%s\"", cat))).getResults().stream()
						.map(QueryResult.Result::getSeries)
						.flatMap(Collection::stream)
						.map(QueryResult.Series::getValues)
						.flatMap(Collection::stream)
						.map(v -> List.of(v.get(0), cat))
				)
				.collect(Collectors.toList());
			QueryResult.Series s = new QueryResult.Series();
			s.setColumns(List.of("TABLE_SCHEM", "TABLE_CATALOG"));
			s.setName("SCHEMAS");
			s.setValues(results);
			QueryResult.Result r = new QueryResult.Result();
			r.setSeries(List.of(s));
			return new InfluxDbResultSet(new InfluxDbStatement(influxDbConnection, client), List.of(r));
		} else {
			InfluxDbResultSet retentionPolicies = influxDbConnection.createStatement()
				.executeQuery(String.format("SHOW RETENTION POLICIES ON \"%s\"", catalog));
			retentionPolicies.getResults()
				.forEach(r -> r.getSeries()
					.forEach(s -> s.setValues(new ArrayList<>(s.getValues())))
				);
			return retentionPolicies;
		}
	}

	@Override protected int remapIndex(int columnIndex) {
		return columnIndex < 0 || columnIndex > 2 ? 0 : columnIndex;
	}

	@Override protected Object mapOrDefault(int columnIndex, IntFunction<Object> function) {
		if (columnIndex == 2) {
			return catalog == null ? super.mapOrDefault(columnIndex, function) : catalog;
		} else {
			return super.mapOrDefault(columnIndex, function);
		}
	}
}
