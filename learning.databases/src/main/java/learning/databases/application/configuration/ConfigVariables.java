package learning.databases.application.configuration;

public class ConfigVariables {
	
	public static final String MYSQL_DATABASE_LEARNING = "learning";
	public static final String MYSQL_TABLE_MYSQL = "mysqldb";
	public static final String MYSQL_PORT = "3306";
	public static final String MYSQL_URL = "jdbc:mysql://localhost:" + MYSQL_PORT + "/" + MYSQL_DATABASE_LEARNING;
	public static final String MYSQL_USER = "root";
	public static final String MYSQL_PASSWORD = "r00t_passw0rd";

	public static final String MONGODB_PORT = "27017";
	public static final String MONGODB_CONNECTION_STRING = "mongodb://localhost:" + MONGODB_PORT;
//	 DATABASE names are case-sensitive in mongoDB
	public static final String MONGODB_DATABASE_LEARNING = "Learning";
//	 COLLECTION names are case-sensitive in mongoDB
	public static final String MONGODB_COLLECTION_MONGODB = "MongoDB";

}
