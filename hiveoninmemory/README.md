Project Title
The HiveOnInMemory project is used to load hive tables in H2 database dynamically

Getting Started

To use HiveOnInMemory api in your project.Please add the hiveoninmemory-0.0.1.jar,h2-1.4.196.jar to your project classpath  

Prerequisites
Java
Maven
JDBC

Installing
Build the jar using "mvn install" command


Example code to include in main class for production environment:

DataLoadProcess dataLoadProcess = new DataLoadProcessImpl();
DataLoadProcessVO dataLoadProcessVO = new DataLoadProcessVO();
dataLoadProcessVO.setDatabaseURL("jdbc prod connection url");
dataLoadProcessVO.setEnvironment("prod");
dataLoadProcessVO.setTables("table1,table1,table3");//list of tables to load in inmemory
dataLoadProcess.initialize(dataLoadProcessVO);
Connection inMemory = dataLoadProcess.load();
if(inMemory == null)
System.out.println("Tables loading into InMemory process failed"); 
else 
System.out.println("Tables loaded into InMemory");
Use "inMemory" connection to interactice queries 

Example for dev

DataLoadProcess dataLoadProcess = new DataLoadProcessImpl();
DataLoadProcessVO dataLoadProcessVO = new DataLoadProcessVO();
dataLoadProcessVO.setDatabaseURL("jdbc prod connection url");
dataLoadProcessVO.setEnvironment("dev");
dataLoadProcessVO.setPassword("your password");
dataLoadProcessVO.setUserName("your username");
dataLoadProcessVO.setTables("table1,table1,table3");
dataLoadProcess.initialize(dataLoadProcessVO);
Connection inMemory = dataLoadProcess.load();
if(inMemory == null)
System.out.println("Tables loading into InMemory process failed"); 
else 
System.out.println("Tables loaded into InMemory");


Running the tests

maven "mvn install" will detect and run the testcases automatically
set all the properties for test in LoadHiveDataIntoInMemoryTest.class

Test Expected result
Check: The test is to check if the returned inmemory connection is valid 

Deployment
Add the hiveoninmemory-0.0.1.jar and h2-1.4.196.jar to prod git along with application packaged jar

Built With
Java
Maven - Dependency Management
JDBC to load data from hive to inmemory

Authors
Amithesh Merugu







