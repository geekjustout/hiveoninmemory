/**
 * 
 */
package com.innovativeintelli.hiveoninmemory.main;

import java.sql.Connection;

import com.innovativeintelli.hiveoninmemory.exception.InMemoryProcessFailedException;
import com.innovativeintelli.hiveoninmemory.vo.DataLoadProcessVO;

/**
 * @author Amithesh Merugu
 *
 */
public interface DataLoadProcess {
	void initialize(DataLoadProcessVO dataLoadProcessVO) throws InMemoryProcessFailedException;
    Connection load()  throws InMemoryProcessFailedException;
    Connection getInMemoryDBConnection() throws InMemoryProcessFailedException;
}
