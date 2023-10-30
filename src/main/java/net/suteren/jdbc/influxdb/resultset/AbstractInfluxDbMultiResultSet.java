package net.suteren.jdbc.influxdb.resultset;

import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import org.influxdb.dto.QueryResult;

import lombok.Getter;
import net.suteren.jdbc.AbstractTypeMappingResultSet;

public abstract class AbstractInfluxDbMultiResultSet extends AbstractTypeMappingResultSet {
	private final List<QueryResult.Result> results;
	@Getter protected final AtomicInteger resultPosition = new AtomicInteger(0);
	@Getter protected final AtomicInteger seriesPosition = new AtomicInteger(0);
	@Getter protected final AtomicInteger rowPosition = new AtomicInteger(-1);
	private boolean isClosed = false;
	private String cursorName;
	protected Logger log;

	protected AbstractInfluxDbMultiResultSet(List<QueryResult.Result> results) {
		this.results = results;
	}

	public boolean getMoreResults() {
		if (getCurrentResult()
			.map(QueryResult.Result::getSeries)
			.map(List::size)
			.filter(s -> seriesPosition.intValue() + 1 < s)
			.isPresent()) {
			seriesPosition.incrementAndGet();
			rowPosition.set(0);
			return true;
		} else if (resultPosition.intValue() + 1 < results.size()) {
			resultPosition.incrementAndGet();
			seriesPosition.set(0);
			rowPosition.set(0);
			return true;
		} else {
			return false;
		}
	}

	public Optional<QueryResult.Result> getCurrentResult() {
		return Optional.ofNullable(results)
			.map(r -> r.get(resultPosition.intValue()));
	}

	public Optional<QueryResult.Series> getCurrentSeries() {
		return getCurrentResult()
			.map(QueryResult.Result::getSeries)
			.map(s -> s.get(seriesPosition.intValue()));
	}

	public List<List<Object>> getCurrentRows() {
		return getCurrentSeries()
			.map(QueryResult.Series::getValues)
			.orElse(List.of());
	}

	public List<Object> getCurrentRow() {
		return getCurrentRows().get(rowPosition.get());
	}

	@Override public boolean next() {
		log.fine("Next row.");
		if (rowPosition.intValue() < getCurrentRows().size()) {
			rowPosition.addAndGet(1);
		}
		return rowPosition.intValue() < getCurrentRows().size();
	}

	@Override public boolean isBeforeFirst() {
		return rowPosition.get() < 0;
	}

	@Override public boolean isAfterLast() {
		return rowPosition.intValue() >= getCurrentRows().size();
	}

	@Override public boolean isFirst() {
		return rowPosition.get() == 0;
	}

	@Override public boolean isLast() {
		return rowPosition.get() == getCurrentRows().size() - 1;
	}

	@Override public void beforeFirst() {
		rowPosition.set(-1);
	}

	@Override public void afterLast() {
		rowPosition.set(getCurrentRows().size());
	}

	@Override public boolean first() {
		if (getCurrentRows().isEmpty()) {
			return false;
		} else {
			rowPosition.set(0);
			return true;
		}
	}

	@Override public boolean last() {
		if (getCurrentRows().isEmpty()) {
			return false;
		} else {
			rowPosition.set(getCurrentRows().size() - 1);
			return true;
		}
	}

	@Override public int getRow() {
		return rowPosition.intValue() + 1;
	}

	@Override public boolean absolute(int row) {
		if (row < 0) {
			rowPosition.set(getCurrentRows().size() - row);
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
			.map(c -> c.indexOf(columnLabel.toLowerCase()) + 1)
			.orElseThrow(() -> new SQLException(String.format("No column named %s", columnLabel)));
	}

	@Override public boolean isClosed() {
		return isClosed;
	}

	@Override public int getHoldability() {
		return HOLD_CURSORS_OVER_COMMIT;
	}

	public List<QueryResult.Result> getResults() {
		return results;
	}
}
