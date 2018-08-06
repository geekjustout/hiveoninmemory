package com.innovativeintelli.hiveoninmemory.test;

import java.sql.Connection;
import java.sql.SQLException;

import com.innovativeintelli.hiveoninmemory.exception.InMemoryProcessFailedException;
import com.innovativeintelli.hiveoninmemory.main.DataLoadProcess;
import com.innovativeintelli.hiveoninmemory.main.DataLoadProcessImpl;
import com.innovativeintelli.hiveoninmemory.vo.DataLoadProcessVO;

import junit.framework.TestCase;

public class LoadHiveDataIntoInMemoryTest extends TestCase {
	
	DataLoadProcess dataLoadProcess;
	
	@Override
	protected void setUp() throws Exception {
		dataLoadProcess = new DataLoadProcessImpl();
		DataLoadProcessVO dataLoadProcessVO = new DataLoadProcessVO();
		dataLoadProcessVO.setDatabaseURL("jdbc:hive2://........");
		dataLoadProcessVO.setEnvironment("dev");
		dataLoadProcessVO.setPassword("yourpassword");
		dataLoadProcessVO.setUserName("yourusername");
		dataLoadProcessVO.setTables("table1,table2,table3");
		dataLoadProcess.initialize(dataLoadProcessVO);
	}
	
	public void test(){
		    try {
			Connection inMemory = dataLoadProcess.load();
			assertNotNull(inMemory);
			assertTrue(inMemory.isValid(0));
		  	inMemory.close();
		    }catch (SQLException e) {
		    	e.printStackTrace();
		    }catch(InMemoryProcessFailedException e) {
		    	e.printStackTrace();
		    }
	}
}
