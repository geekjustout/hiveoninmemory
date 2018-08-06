package com.innovativeintelli.hiveoninmemory.main;

import com.innovativeintelli.hiveoninmemory.exception.InMemoryProcessFailedException;
import com.innovativeintelli.hiveoninmemory.vo.DataLoadProcessVO;

public class ExecutePackage {

	public static void main(String[] args) throws InMemoryProcessFailedException {
		DataLoadProcess dataLoadProcess = new DataLoadProcessImpl();
		DataLoadProcessVO dataLoadProcessVO = new DataLoadProcessVO();
		dataLoadProcessVO.setDatabaseURL("jdbc:hive2://.......");
		dataLoadProcessVO.setEnvironment("dev");
		dataLoadProcessVO.setPassword("yourpassword");
		dataLoadProcessVO.setUserName("yourusername");
		dataLoadProcessVO.setTables("table1,table2");
		dataLoadProcess.initialize(dataLoadProcessVO);
        System.out.println("Success");
	}

}
