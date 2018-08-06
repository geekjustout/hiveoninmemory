/**
 * 
 */
package com.innovativeintelli.hiveoninmemory.dao;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.Decimal;
import org.apache.log4j.Logger;

import com.innovativeintelli.hiveoninmemory.exception.InMemoryProcessFailedException;

/**
 * @author Amithesh Merugu
 *
 */

public class DataLoadProcessDAOImpl implements DataLoadProcessDAO {

	private Logger log = Logger.getLogger(DataLoadProcessDAOImpl.class);
	private String tables = null;
	private Connection sqlConnection = null;
	private Connection inMemoryConnection = null;

	public Connection load(Connection sqlConnection, Connection inMemoryConnection, String tables)
			throws SQLException, InMemoryProcessFailedException {
		this.tables = tables;
		this.sqlConnection = sqlConnection;
		this.inMemoryConnection = inMemoryConnection;
		String[] tableNames = getTablesList();
		for (String table : tableNames) {
			loadTableIntoInmemory(table.trim());
		}
		return inMemoryConnection;
	}

	private void loadTableIntoInmemory(String table) throws SQLException {
		ResultSet resultSet = getTableMetaData(table);
		ResultSetMetaData metaData = resultSet.getMetaData();
		String createTableQueryString = createQueryStringUsingMetaData(metaData, table);
		createTableinInmemory(createTableQueryString);
		String getRowCount = "select count(1) from "+table;
		Statement st = sqlConnection.createStatement();
		ResultSet rs = st.executeQuery(getRowCount);
		Integer count = 0;
		while(rs.next()){
			count = rs.getInt(1);
		}
		st.close();
		rs.close();
		insertIntoInmemoryDatabase(metaData, resultSet, table, count);
		resultSet.close();
	}

	private void createTableinInmemory(String createTableQuery) throws SQLException {
		Statement stmt = inMemoryConnection.createStatement();
		stmt.executeUpdate(createTableQuery);
		stmt.close();
	}

	private void insertIntoInmemoryDatabase(ResultSetMetaData resultSetMetaData, ResultSet rs, String tableName, Integer DATA_SIZE)
			throws SQLException {
		PreparedStatement ps = null;
		String insertColumns = "";
		String insertValues = "";
		String column = null;
		String removetableName = "\\b" + tableName + "." + "\\b";
		int columncount = resultSetMetaData.getColumnCount();
		column= resultSetMetaData.getColumnLabel(1).replaceFirst(removetableName, "");
		insertColumns += "`"+column+"`";
		insertValues += "?";
		for (int i = 2; i <= columncount; i++) {
			 column = resultSetMetaData.getColumnLabel(i).replaceFirst(removetableName, "");
			insertColumns += ", " + "`"+column+"`";
			insertValues += ",?";
		}
		String insertSql = "INSERT INTO " + tableName + " (" + insertColumns + ") values(" + insertValues + ")";
		try {
			ps = inMemoryConnection.prepareStatement(insertSql);
			final int BATCH_SIZE = 3000;//should determine this based on table row count
			int count =0;
			while (rs.next()) {
				count++;
				for (int i = 1; i <= columncount; i++) {
					Object obj = rs.getObject(i);
					setValueToPreparedStaement(rs, ps, i, obj);
				}
				ps.addBatch();
				if (count % BATCH_SIZE == BATCH_SIZE - 1){
					ps.executeBatch();
				}
			}
			if (DATA_SIZE % BATCH_SIZE != 0)
				 ps.executeBatch();
			log.info("Data inserted into inmemory with table name "+tableName);
		} catch (SQLException sqlException) {
			log.error(sqlException);
			throw sqlException;
		}

	}
	
	private void setValueToPreparedStaement(ResultSet rs, PreparedStatement ps, int i, Object obj) throws SQLException {
		if (obj == null)
			ps.setNull(i, Types.NULL);
		else if (obj instanceof String) {
			String data = (String) obj;
			ps.setString(i, data);
		} else if (obj instanceof Integer) {
			Integer data = (Integer) obj;
			ps.setInt(i, data);
		} else if (obj instanceof Double) {
			Double data = (Double) obj;
			ps.setDouble(i, data);
		} else if (obj instanceof Float) {
			Float data = (Float) obj;
			ps.setFloat(i, data);
		} else if (obj instanceof Decimal) {
			Double data = rs.getBigDecimal(i) == null ? null : rs.getBigDecimal(i).doubleValue();
			handleNull(ps, i, data);
		} else if (obj instanceof BigDecimal) {
			Double data = rs.getBigDecimal(i) == null ? null : rs.getBigDecimal(i).doubleValue();
			handleNull(ps, i, data);
		}else if (obj instanceof Date) {
			Date data = (Date) obj;
			ps.setDate(i, data);
		}else if (obj instanceof Byte) {
			Byte data = (Byte) obj;
			ps.setByte(i, data);
		}else {
			log.info("Please add this data type to above condition list "+obj.getClass());
		}
	}

	

	private String createQueryStringUsingMetaData(ResultSetMetaData resultSetMetaData, String tableName)
			throws SQLException {
		int columnCount = resultSetMetaData.getColumnCount();
		StringBuilder sb = new StringBuilder(1024);
		if (columnCount > 0) {
			sb.append("Create table ").append(tableName).append(" ( ");
		}
		for (int i = 1; i <= columnCount; i++) {
			boolean addPrecision = true;
			if (i > 1)
				sb.append(", ");
			String columnName = resultSetMetaData.getColumnLabel(i);
			String removetableName = "\\b" + tableName + "." + "\\b";
			columnName = columnName.replaceFirst(removetableName, "");
			String columnType = resultSetMetaData.getColumnTypeName(i);
			if (columnType.equalsIgnoreCase("string")) {
				columnType = "VARCHAR(MAX)";
				addPrecision = false;
			}
			sb.append("`").append(columnName).append("`").append(" ").append(columnType);

			int precision = resultSetMetaData.getPrecision(i);
			if (precision != 0 && addPrecision) {
				sb.append("( ").append(precision).append(" )");
			}
		}
		sb.append(" ) ");

		log.info(sb.toString());
		return sb.toString();
	}

	private ResultSet getTableMetaData(String table) throws SQLException {
		String sql = "select * from " + table;
		PreparedStatement ps = sqlConnection.prepareStatement(sql);
		ResultSet rs = ps.executeQuery(sql);
		return rs;
	}

	private String[] getTablesList() throws InMemoryProcessFailedException {
		String tablenames[] = tables.split("\\,");
		if (tablenames.length < 1) {
			throw new InMemoryProcessFailedException("Atleast one table should present to load the data into Inmemory");
		}
		return tablenames;
	}

	public List<String> getColumns(String tableName) throws SQLException {
		ResultSet rs = null;
		ResultSetMetaData rsmd = null;
		PreparedStatement stmt = null;
		List<String> columnNames = null;
		try {
			stmt = sqlConnection.prepareStatement("select * from " + tableName + " where 0=1");
			rs = stmt.executeQuery();
			rsmd = rs.getMetaData();
			columnNames = new ArrayList<String>();
			for (int i = 1; i <= rsmd.getColumnCount(); i++)
				columnNames.add(rsmd.getColumnLabel(i));
		} catch (SQLException e) {
			throw e;
		} finally {
			if (rs != null)
				try {
					rs.close();
				} catch (SQLException e) {
					log.error(e);
					throw e;
				}
			if (stmt != null)
				try {
					stmt.close();
				} catch (SQLException e) {
					log.error(e);
					throw e;
				}
		}
		return columnNames;
	}

	void handleNull(PreparedStatement ps, Integer index, Double value) throws SQLException {
		if (value == null) {
			ps.setNull(index, Types.NULL);
		} else {
			ps.setDouble(index, value);
		}
	}
}
