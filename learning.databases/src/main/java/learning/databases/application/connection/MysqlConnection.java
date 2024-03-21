package learning.databases.application.connection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import learning.databases.application.configuration.ConfigVariables;

public class MysqlConnection {

	private static Connection MYSQL_CONNECTION;

	private MysqlConnection() {
		if (MysqlConnection.MYSQL_CONNECTION != null) {
			throw new IllegalStateException();
		}
	}

	public static Connection getConnection() {
		// double-check locking
		if (MYSQL_CONNECTION == null)
			synchronized (MysqlConnection.class) {
				if (MysqlConnection.MYSQL_CONNECTION == null) {

					// constructor call
					new MysqlConnection();
					try {
						// connection establishment
						MysqlConnection.MYSQL_CONNECTION = DriverManager.getConnection(ConfigVariables.MYSQL_URL,
								ConfigVariables.MYSQL_USER, ConfigVariables.MYSQL_PASSWORD);
					} catch (SQLException ex) {
						System.err.println(
								"error occured while initiallizing variable 'MysqlConnection.MYSQL_CONNECTION'");
						Runtime.getRuntime().exit(1);
					}
				}
			}

		return MYSQL_CONNECTION;
	}
}
