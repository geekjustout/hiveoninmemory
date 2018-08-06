/**
 * 
 */
package com.innovativeintelli.hiveoninmemory.vo;

/**
 * @author Amithesh Merugu
 *
 */
public class DataLoadProcessVO {
	
	private String databaseURL = null; 
	private String environment = null;
	private String userName = null;
	private String password = null;
	private String tables = null;
	
	public String getDatabaseURL() {
		return databaseURL;
	}
	public void setDatabaseURL(String databaseURL) {
		this.databaseURL = databaseURL;
	}
	public String getEnvironment() {
		return environment;
	}
	public void setEnvironment(String environment) {
		this.environment = environment;
	}
	public String getUserName() {
		return userName;
	}
	public void setUserName(String userName) {
		this.userName = userName;
	}
	public String getPassword() {
		return password;
	}
	public void setPassword(String password) {
		this.password = password;
	}
	public String getTables() {
		return tables;
	}
	public void setTables(String tables) {
		this.tables = tables;
	}
}
