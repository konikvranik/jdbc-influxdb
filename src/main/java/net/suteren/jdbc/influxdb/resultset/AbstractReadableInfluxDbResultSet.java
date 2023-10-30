package net.suteren.jdbc.influxdb.resultset;

import java.util.List;

import org.influxdb.dto.QueryResult;

import net.suteren.jdbc.influxdb.statement.InfluxDbStatement;

public abstract class AbstractReadableInfluxDbResultSet extends AbstractInfluxDbMultiResultSet {
	protected final InfluxDbStatement statement;

	protected AbstractReadableInfluxDbResultSet(List<QueryResult.Result> results, InfluxDbStatement statement) {
		super(results);
		this.statement = statement;
		log = statement.getConnection().getMetaData().getDriver().getParentLogger();
	}

	@Override public Object getObject(int columnIndex) {
		return getCurrentRow().get(columnIndex - 1);
	}

	@Override public InfluxDbStatement getStatement() {
		return statement;
	}

	@Override public boolean wasNull() {
		return false;
	}

	@Override public void refreshRow() {

	}

	@Override public <T> T unwrap(Class<T> iface) {
		return null;
	}

	@Override public boolean isWrapperFor(Class<?> iface) {
		return false;
	}
}
