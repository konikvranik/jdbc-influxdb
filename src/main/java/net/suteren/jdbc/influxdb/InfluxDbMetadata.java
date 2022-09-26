package net.suteren.jdbc.influxdb;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.util.regex.Pattern;

import net.suteren.jdbc.influxdb.resultset.proxy.AbstractProxyResultSet;
import net.suteren.jdbc.influxdb.resultset.proxy.GetFieldKeysResultSet;
import net.suteren.jdbc.influxdb.resultset.proxy.GetTablesResultSet;
import net.suteren.jdbc.influxdb.resultset.proxy.GetTagKeysResultSet;

public class InfluxDbMetadata implements DatabaseMetaData {
	private final String url;
	private final String userName;
	private final String version;
	private final InfluxDbDriver influxDbDriver;
	private final InfluxDbConnection influxDbConnection;
	private final static Pattern PERCENT_PATTERN = Pattern.compile("%");

	public InfluxDbMetadata(String url, String userName, String version, InfluxDbDriver influxDbDriver,
		InfluxDbConnection influxDbConnection) {
		this.url = url;
		this.userName = userName;
		this.version = version;
		this.influxDbDriver = influxDbDriver;
		this.influxDbConnection = influxDbConnection;
	}

	@Override public boolean allProceduresAreCallable() {
		return false;
	}

	@Override public boolean allTablesAreSelectable() {
		return false;
	}

	@Override public String getURL() {
		return url;
	}

	@Override public String getUserName() {
		return userName;
	}

	@Override public boolean isReadOnly() {
		return true;
	}

	@Override public boolean nullsAreSortedHigh() {
		return false;
	}

	@Override public boolean nullsAreSortedLow() {
		return false;
	}

	@Override public boolean nullsAreSortedAtStart() {
		return false;
	}

	@Override public boolean nullsAreSortedAtEnd() {
		return false;
	}

	@Override public String getDatabaseProductName() {
		return "InfluxDB";
	}

	@Override public String getDatabaseProductVersion() {
		return version;
	}

	@Override public String getDriverName() {
		return "InfluxDB JDBC driver";
	}

	@Override public String getDriverVersion() {
		return "0.1.0";
	}

	@Override public int getDriverMajorVersion() {
		return influxDbDriver.getMajorVersion();
	}

	@Override public int getDriverMinorVersion() {
		return influxDbDriver.getMinorVersion();
	}

	@Override public boolean usesLocalFiles() {
		return false;
	}

	@Override public boolean usesLocalFilePerTable() {
		return false;
	}

	@Override public boolean supportsMixedCaseIdentifiers() {
		return false;
	}

	@Override public boolean storesUpperCaseIdentifiers() {
		return false;
	}

	@Override public boolean storesLowerCaseIdentifiers() {
		return false;
	}

	@Override public boolean storesMixedCaseIdentifiers() {
		return false;
	}

	@Override public boolean supportsMixedCaseQuotedIdentifiers() {
		return false;
	}

	@Override public boolean storesUpperCaseQuotedIdentifiers() {
		return false;
	}

	@Override public boolean storesLowerCaseQuotedIdentifiers() {
		return false;
	}

	@Override public boolean storesMixedCaseQuotedIdentifiers() {
		return false;
	}

	@Override public String getIdentifierQuoteString() {
		return "\"";
	}

	@Override public String getSQLKeywords() {
		return "";
	}

	@Override public String getNumericFunctions() {
		return "";
	}

	@Override public String getStringFunctions() {
		return "";
	}

	@Override public String getSystemFunctions() {
		return "";
	}

	@Override public String getTimeDateFunctions() {
		return "";
	}

	@Override public String getSearchStringEscape() {
		return "'";
	}

	@Override public String getExtraNameCharacters() {
		return "";
	}

	@Override public boolean supportsAlterTableWithAddColumn() {
		return false;
	}

	@Override public boolean supportsAlterTableWithDropColumn() {
		return false;
	}

	@Override public boolean supportsColumnAliasing() {
		return false;
	}

	@Override public boolean nullPlusNonNullIsNull() {
		return false;
	}

	@Override public boolean supportsConvert() {
		return false;
	}

	@Override public boolean supportsConvert(int fromType, int toType) {
		return false;
	}

	@Override public boolean supportsTableCorrelationNames() {
		return false;
	}

	@Override public boolean supportsDifferentTableCorrelationNames() {
		return false;
	}

	@Override public boolean supportsExpressionsInOrderBy() {
		return false;
	}

	@Override public boolean supportsOrderByUnrelated() {
		return false;
	}

	@Override public boolean supportsGroupBy() {
		return false;
	}

	@Override public boolean supportsGroupByUnrelated() {
		return false;
	}

	@Override public boolean supportsGroupByBeyondSelect() {
		return false;
	}

	@Override public boolean supportsLikeEscapeClause() {
		return false;
	}

	@Override public boolean supportsMultipleResultSets() {
		return false;
	}

	@Override public boolean supportsMultipleTransactions() {
		return false;
	}

	@Override public boolean supportsNonNullableColumns() {
		return false;
	}

	@Override public boolean supportsMinimumSQLGrammar() {
		return false;
	}

	@Override public boolean supportsCoreSQLGrammar() {
		return false;
	}

	@Override public boolean supportsExtendedSQLGrammar() {
		return false;
	}

	@Override public boolean supportsANSI92EntryLevelSQL() {
		return false;
	}

	@Override public boolean supportsANSI92IntermediateSQL() {
		return false;
	}

	@Override public boolean supportsANSI92FullSQL() {
		return false;
	}

	@Override public boolean supportsIntegrityEnhancementFacility() {
		return false;
	}

	@Override public boolean supportsOuterJoins() {
		return false;
	}

	@Override public boolean supportsFullOuterJoins() {
		return false;
	}

	@Override public boolean supportsLimitedOuterJoins() {
		return false;
	}

	@Override public String getSchemaTerm() {
		return "";
	}

	@Override public String getProcedureTerm() {
		return "";
	}

	@Override public String getCatalogTerm() {
		return "";
	}

	@Override public boolean isCatalogAtStart() {
		return false;
	}

	@Override public String getCatalogSeparator() {
		return "";
	}

	@Override public boolean supportsSchemasInDataManipulation() {
		return false;
	}

	@Override public boolean supportsSchemasInProcedureCalls() {
		return false;
	}

	@Override public boolean supportsSchemasInTableDefinitions() {
		return false;
	}

	@Override public boolean supportsSchemasInIndexDefinitions() {
		return false;
	}

	@Override public boolean supportsSchemasInPrivilegeDefinitions() {
		return false;
	}

	@Override public boolean supportsCatalogsInDataManipulation() {
		return false;
	}

	@Override public boolean supportsCatalogsInProcedureCalls() {
		return false;
	}

	@Override public boolean supportsCatalogsInTableDefinitions() {
		return false;
	}

	@Override public boolean supportsCatalogsInIndexDefinitions() {
		return false;
	}

	@Override public boolean supportsCatalogsInPrivilegeDefinitions() {
		return false;
	}

	@Override public boolean supportsPositionedDelete() {
		return false;
	}

	@Override public boolean supportsPositionedUpdate() {
		return false;
	}

	@Override public boolean supportsSelectForUpdate() {
		return false;
	}

	@Override public boolean supportsStoredProcedures() {
		return false;
	}

	@Override public boolean supportsSubqueriesInComparisons() {
		return false;
	}

	@Override public boolean supportsSubqueriesInExists() {
		return false;
	}

	@Override public boolean supportsSubqueriesInIns() {
		return false;
	}

	@Override public boolean supportsSubqueriesInQuantifieds() {
		return false;
	}

	@Override public boolean supportsCorrelatedSubqueries() {
		return false;
	}

	@Override public boolean supportsUnion() {
		return false;
	}

	@Override public boolean supportsUnionAll() {
		return false;
	}

	@Override public boolean supportsOpenCursorsAcrossCommit() {
		return false;
	}

	@Override public boolean supportsOpenCursorsAcrossRollback() {
		return false;
	}

	@Override public boolean supportsOpenStatementsAcrossCommit() {
		return false;
	}

	@Override public boolean supportsOpenStatementsAcrossRollback() {
		return false;
	}

	@Override public int getMaxBinaryLiteralLength() {
		return 0;
	}

	@Override public int getMaxCharLiteralLength() {
		return 0;
	}

	@Override public int getMaxColumnNameLength() {
		return 0;
	}

	@Override public int getMaxColumnsInGroupBy() {
		return 0;
	}

	@Override public int getMaxColumnsInIndex() {
		return 0;
	}

	@Override public int getMaxColumnsInOrderBy() {
		return 0;
	}

	@Override public int getMaxColumnsInSelect() {
		return 0;
	}

	@Override public int getMaxColumnsInTable() {
		return 0;
	}

	@Override public int getMaxConnections() {
		return 0;
	}

	@Override public int getMaxCursorNameLength() {
		return 0;
	}

	@Override public int getMaxIndexLength() {
		return 0;
	}

	@Override public int getMaxSchemaNameLength() {
		return 0;
	}

	@Override public int getMaxProcedureNameLength() {
		return 0;
	}

	@Override public int getMaxCatalogNameLength() {
		return 0;
	}

	@Override public int getMaxRowSize() {
		return 0;
	}

	@Override public boolean doesMaxRowSizeIncludeBlobs() {
		return false;
	}

	@Override public int getMaxStatementLength() {
		return 0;
	}

	@Override public int getMaxStatements() {
		return 0;
	}

	@Override public int getMaxTableNameLength() {
		return 0;
	}

	@Override public int getMaxTablesInSelect() {
		return 0;
	}

	@Override public int getMaxUserNameLength() {
		return 0;
	}

	@Override public int getDefaultTransactionIsolation() {
		return 0;
	}

	@Override public boolean supportsTransactions() {
		return false;
	}

	@Override public boolean supportsTransactionIsolationLevel(int level) {
		return false;
	}

	@Override public boolean supportsDataDefinitionAndDataManipulationTransactions() {
		return false;
	}

	@Override public boolean supportsDataManipulationTransactionsOnly() {
		return false;
	}

	@Override public boolean dataDefinitionCausesTransactionCommit() {
		return false;
	}

	@Override public boolean dataDefinitionIgnoredInTransactions() {
		return false;
	}

	@Override public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) {
		return null;
	}

	@Override public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern,
		String columnNamePattern) {
		return null;
	}

	@Override
	public GetTablesResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types)
		throws SQLException {
		return new GetTablesResultSet(influxDbConnection,
			tableNamePattern == null || PERCENT_PATTERN.matcher(tableNamePattern).matches() ? null : tableNamePattern);
	}

	@Override public ResultSet getSchemas() {
		return null;
	}

	@Override public ResultSet getCatalogs() {
		return null;
	}

	@Override public ResultSet getTableTypes() {
		return null;
	}

	@Override
	public AbstractProxyResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern,
		String columnNamePattern) throws SQLException {
		return new GetFieldKeysResultSet(influxDbConnection,
			tableNamePattern == null || PERCENT_PATTERN.matcher(tableNamePattern).matches() ? null : tableNamePattern);
	}

	@Override
	public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) {
		return null;
	}

	@Override public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) {
		return null;
	}

	@Override
	public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) {
		return null;
	}

	@Override public ResultSet getVersionColumns(String catalog, String schema, String table) {
		return null;
	}

	@Override public ResultSet getPrimaryKeys(String catalog, String schema, String table) {
		return null;
	}

	@Override public ResultSet getImportedKeys(String catalog, String schema, String table) {
		return null;
	}

	@Override public ResultSet getExportedKeys(String catalog, String schema, String table) {
		return null;
	}

	@Override public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable,
		String foreignCatalog, String foreignSchema, String foreignTable) {
		return null;
	}

	@Override public ResultSet getTypeInfo() {
		return null;
	}

	@Override
	public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate)
		throws SQLException {
		return new GetTagKeysResultSet(influxDbConnection,
			table == null || PERCENT_PATTERN.matcher(table).matches() ? null : table);
	}

	@Override public boolean supportsResultSetType(int type) {
		return false;
	}

	@Override public boolean supportsResultSetConcurrency(int type, int concurrency) {
		return false;
	}

	@Override public boolean ownUpdatesAreVisible(int type) {
		return false;
	}

	@Override public boolean ownDeletesAreVisible(int type) {
		return false;
	}

	@Override public boolean ownInsertsAreVisible(int type) {
		return false;
	}

	@Override public boolean othersUpdatesAreVisible(int type) {
		return false;
	}

	@Override public boolean othersDeletesAreVisible(int type) {
		return false;
	}

	@Override public boolean othersInsertsAreVisible(int type) {
		return false;
	}

	@Override public boolean updatesAreDetected(int type) {
		return false;
	}

	@Override public boolean deletesAreDetected(int type) {
		return false;
	}

	@Override public boolean insertsAreDetected(int type) {
		return false;
	}

	@Override public boolean supportsBatchUpdates() {
		return false;
	}

	@Override public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) {
		return null;
	}

	@Override public Connection getConnection() {
		return influxDbConnection;
	}

	@Override public boolean supportsSavepoints() {
		return false;
	}

	@Override public boolean supportsNamedParameters() {
		return false;
	}

	@Override public boolean supportsMultipleOpenResults() {
		return false;
	}

	@Override public boolean supportsGetGeneratedKeys() {
		return false;
	}

	@Override public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) {
		return null;
	}

	@Override public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) {
		return null;
	}

	@Override public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern,
		String attributeNamePattern) {
		return null;
	}

	@Override public boolean supportsResultSetHoldability(int holdability) {
		return false;
	}

	@Override public int getResultSetHoldability() {
		return 0;
	}

	@Override public int getDatabaseMajorVersion() {
		return Integer.parseInt(version.split("\\.")[0]);
	}

	@Override public int getDatabaseMinorVersion() {
		return Integer.parseInt(version.split("\\.")[1]);
	}

	@Override public int getJDBCMajorVersion() {
		return 0;
	}

	@Override public int getJDBCMinorVersion() {
		return 0;
	}

	@Override public int getSQLStateType() {
		return sqlStateXOpen;
	}

	@Override public boolean locatorsUpdateCopy() {
		return false;
	}

	@Override public boolean supportsStatementPooling() {
		return false;
	}

	@Override public RowIdLifetime getRowIdLifetime() {
		return null;
	}

	@Override public ResultSet getSchemas(String catalog, String schemaPattern) {
		return null;
	}

	@Override public boolean supportsStoredFunctionsUsingCallSyntax() {
		return false;
	}

	@Override public boolean autoCommitFailureClosesAllResultSets() {
		return false;
	}

	@Override public ResultSet getClientInfoProperties() {
		return null;
	}

	@Override public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) {
		return null;
	}

	@Override public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern,
		String columnNamePattern) {
		return null;
	}

	@Override public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern,
		String columnNamePattern) {
		return null;
	}

	@Override public boolean generatedKeyAlwaysReturned() {
		return false;
	}

	@Override public <T> T unwrap(Class<T> iface) {
		return null;
	}

	@Override public boolean isWrapperFor(Class<?> iface) {
		return false;
	}

	public InfluxDbDriver getDriver() {
		return influxDbDriver;
	}
}