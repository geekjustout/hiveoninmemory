/**
 * 
 */
package com.innovativeintelli.hiveoninmemory.main;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;

import com.innovativeintelli.hiveoninmemory.dao.DataLoadProcessDAO;
import com.innovativeintelli.hiveoninmemory.dao.DataLoadProcessDAOImpl;
import com.innovativeintelli.hiveoninmemory.exception.InMemoryProcessFailedException;
import com.innovativeintelli.hiveoninmemory.util.ApplicationConstants;
import com.innovativeintelli.hiveoninmemory.vo.DataLoadProcessVO;

/**
 * @author Amithesh Merugu
 *
 */
public class DataLoadProcessImpl implements DataLoadProcess {
	
	private static final Logger log = Logger.getLogger(DataLoadProcessImpl.class);
	private static String databaseURL = null;
	private static String environment = null;
	private static String prodEnvironment = "prod";
	private static String userName = null;
	private static String password = null;
	private static String tables = null; 
	public static Connection sqlConnection = null;
	public  static Connection inMemoryConnection = null;
	
	public void initialize(DataLoadProcessVO dataLoadProcessVO) throws InMemoryProcessFailedException {
		try {
		FileInputStream input = new FileInputStream("config.properties");
		Properties properties = new Properties();
		properties.load(input);
		databaseURL = properties.getProperty(ApplicationConstants.DATA_BASE_URL);
		environment = properties.getProperty(ApplicationConstants.ENVIRONMENT);
		userName = properties.getProperty(ApplicationConstants.USERNAME);
		password = properties.getProperty(ApplicationConstants.PASSWORD);
		tables = properties.getProperty(ApplicationConstants.TABLE_LIST);
		databaseURL = dataLoadProcessVO.getDatabaseURL();
		environment = dataLoadProcessVO.getEnvironment();
		userName = dataLoadProcessVO.getUserName();
		password = dataLoadProcessVO.getPassword();
		tables = dataLoadProcessVO.getTables();
		if (databaseURL == null)
			throw new InMemoryProcessFailedException("Please add databaseURL property in config.properties file");	
		else if(environment == null)
			throw new InMemoryProcessFailedException("Please add environment property in config.properties file");	
		else if (!environment.equalsIgnoreCase("prod") && userName == null)
			throw new InMemoryProcessFailedException("Please add userName property in config.properties file");
		else if (!environment.equalsIgnoreCase("prod") && password == null)
			throw new InMemoryProcessFailedException("Please add password property in config.properties file");
		else if(tables == null)
			throw new InMemoryProcessFailedException("Please add tables property in config.properties file.The content should be comma separated table names");
		sqlConnection= getHiveDBConnection();
		inMemoryConnection = getInMemoryDBConnection();
		}catch(FileNotFoundException exception) {
			exception.printStackTrace();
			throw new InMemoryProcessFailedException("Please add config.properties to project build path");
		} catch (IOException exception) {
			exception.printStackTrace();
			throw new InMemoryProcessFailedException("Please add config.properties to project build path");
		}
		
	}
 
	public  Connection getInMemoryDBConnection() throws InMemoryProcessFailedException {
		return getAsynchronizedInMemoryDBConnection();
	}

	public Connection load() throws InMemoryProcessFailedException  {
		try {
		DataLoadProcessDAO dataProcessingDAO = new DataLoadProcessDAOImpl();
		dataProcessingDAO.load(sqlConnection, inMemoryConnection,tables);
		}catch(SQLException e) {
			throw new InMemoryProcessFailedException(e.getMessage(),e);
		}
		log.info("Data Loaded Into InMemory Successfully");
		return inMemoryConnection;
	}
	
	 public static Connection getAsynchronizedInMemoryDBConnection() throws InMemoryProcessFailedException  {
			Connection h2con = null;
				try {
					h2con = DriverManager.getConnection("jdbc:h2:mem:hiveInMemoryDb;DB_CLOSE_DELAY=-1", "sa", "sa");
				} catch (SQLException e) {
					throw new InMemoryProcessFailedException(e.getMessage(),e);
				}
			return h2con;

		}
	
	public static Connection getHiveDBConnection() throws InMemoryProcessFailedException {

		Connection con = null;
		try {
			if(environment.equalsIgnoreCase(prodEnvironment)) {
				log.info("Database Servver URL : "+databaseURL);
				Configuration conf = new Configuration();
				conf.set("fs.defaultFS", "");
				conf.set("hadoop.security.authentication", "kerberos");
				UserGroupInformation.setConfiguration(conf);
				UserGroupInformation.loginUserFromSubject(null);
				con = DriverManager.getConnection(databaseURL);
			}else {
				con = DriverManager.getConnection(databaseURL,userName,password);
			}
			log.info("Connection established successfully");
		} catch (SQLException  e) {
			log.error("Exception occured while getting database connection");
			throw new InMemoryProcessFailedException(e.getMessage(), e.getCause());
		}
		catch (IOException e) {
			log.error("Hadoop Security Authentication exception occured");
			throw new InMemoryProcessFailedException(e.getMessage(), e.getCause());
		}
		return con;	
	}

}
