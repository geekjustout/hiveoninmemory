/**
 * 
 */
package com.innovativeintelli.hiveoninmemory.dao;

import java.sql.Connection;
import java.sql.SQLException;

import com.innovativeintelli.hiveoninmemory.exception.InMemoryProcessFailedException;

/**
 * @author Amithesh Merugu
 *
 */
public interface DataLoadProcessDAO {
	
	Connection load(Connection sqlConnection, Connection inMemoryConnection,String tables) throws SQLException,InMemoryProcessFailedException;

}
