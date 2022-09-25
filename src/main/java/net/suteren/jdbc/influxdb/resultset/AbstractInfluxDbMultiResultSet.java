package net.suteren.jdbc.influxdb.resultset;

import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import org.influxdb.dto.QueryResult;

public abstract class AbstractInfluxDbMultiResultSet
	extends net.suteren.jdbc.AbstractBaseResultSet {
	private final List<QueryResult.Result> results;
	private final AtomicInteger resultPosition = new AtomicInteger(0);
	final AtomicInteger seriesPosition = new AtomicInteger(0);
	final AtomicInteger rowPosition = new AtomicInteger(-1);
	private boolean isClosed = false;
	private String cursorName;

	public AbstractInfluxDbMultiResultSet(List<QueryResult.Result> results) {
		this.results = results;
	}

	public boolean getMoreResults() {
		if (results.get(resultPosition.get()).getSeries().size() > seriesPosition.get()) {
			seriesPosition.incrementAndGet();
			return true;
		} else if (results.size() > resultPosition.get()) {
			resultPosition.incrementAndGet();
			seriesPosition.set(0);
			return true;
		} else {
			return false;
		}
	}

	public Optional<QueryResult.Result> getCurrentResult() {
		return Optional.ofNullable(results)
			.map(r -> r.get(resultPosition.get()));
	}

	public Optional<QueryResult.Series> getCurrentSeries() {
		return getCurrentResult()
			.map(QueryResult.Result::getSeries)
			.map(s -> s.get(seriesPosition.get()));
	}

	public List<List<Object>> getCurrentValues() {
		return getCurrentSeries()
			.map(QueryResult.Series::getValues)
			.orElse(List.of());
	}

	public List<Object> getCurrentRow() {
		return getCurrentValues().get(rowPosition.get());
	}

	@Override public boolean next() {
		if (seriesPosition.intValue() < getCurrentValues().size()) {
			seriesPosition.addAndGet(1);
			return true;
		} else {
			return false;
		}
	}

	@Override public boolean isBeforeFirst() {
		return rowPosition.get() < 0;
	}

	@Override public boolean isAfterLast() {
		return rowPosition.get() >= getCurrentValues().size();
	}

	@Override public boolean isFirst() {
		return rowPosition.get() == 0;
	}

	@Override public boolean isLast() {
		return rowPosition.get() == getCurrentValues().size() - 1;
	}

	@Override public void beforeFirst() {
		rowPosition.set(-1);
	}

	@Override public void afterLast() {
		rowPosition.set(getCurrentValues().size());
	}

	@Override public boolean first() {
		if (getCurrentValues().isEmpty()) {
			return false;
		} else {
			rowPosition.set(0);
			return true;
		}
	}

	@Override public boolean last() {
		if (getCurrentValues().isEmpty()) {
			return false;
		} else {
			rowPosition.set(getCurrentValues().size() - 1);
			return true;
		}
	}

	@Override public int getRow() {
		return rowPosition.get();
	}

	@Override public boolean absolute(int row) {
		if (row < 0) {
			rowPosition.set(getCurrentValues().size() - row);
			return !isBeforeFirst();
		} else {
			rowPosition.set(row - 1);
			return !isAfterLast() && !isBeforeFirst();
		}
	}

	@Override public boolean relative(int rows) {
		rowPosition.addAndGet(rows);
		return !isAfterLast() && !isBeforeFirst();
	}

	@Override public boolean previous() {
		rowPosition.addAndGet(-1);
		return !isBeforeFirst();
	}

	@Override public void setFetchDirection(int direction) {

	}

	@Override public int getFetchDirection() {
		return FETCH_UNKNOWN;
	}

	@Override public void setFetchSize(int rows) {

	}

	@Override public int getFetchSize() {
		return 0;
	}

	@Override public int getType() {
		return TYPE_SCROLL_INSENSITIVE;
	}

	@Override public int getConcurrency() {
		return CONCUR_READ_ONLY;
	}

	@Override public void moveToInsertRow() {

	}

	@Override public void moveToCurrentRow() {

	}

	@Override public SQLWarning getWarnings() {
		return getCurrentResult()
			.map(QueryResult.Result::getError)
			.map(SQLWarning::new)
			.orElse(null);
	}

	@Override public void clearWarnings() {

	}

	@Override public InfluxDbResultSetMetaData getMetaData() {
		return new InfluxDbResultSetMetaData(this);
	}

	@Override public String getCursorName() {
		return cursorName;
	}

	@Override public void close() {
		resultPosition.set(0);
		seriesPosition.set(0);
		rowPosition.set(-1);
		isClosed = true;
	}

	public void setCursorName(String name) {
		cursorName = name;
	}

	@Override public int findColumn(String columnLabel) throws SQLException {
		return getCurrentSeries()
			.map(QueryResult.Series::getColumns)
			.map(c -> c.indexOf(columnLabel) + 1)
			.orElseThrow(() -> new SQLException(String.format("No column named %s", columnLabel)));
	}

	@Override public boolean isClosed() {
		return isClosed;
	}

	@Override public int getHoldability() {
		return HOLD_CURSORS_OVER_COMMIT;
	}
}
