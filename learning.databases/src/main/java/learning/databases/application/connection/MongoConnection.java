package learning.databases.application.connection;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

import learning.databases.application.configuration.ConfigVariables;

public class MongoConnection {
	
	private static MongoClient MONGO_CLIENT;
	
	private MongoConnection() {
		if(MongoConnection.MONGO_CLIENT != null) {
			throw new IllegalStateException();
		}
	}
	
	public static MongoClient getMongoClient() {
		
		if(MongoConnection.MONGO_CLIENT == null) {
			synchronized(MongoConnection.class){
				if(MongoConnection.MONGO_CLIENT == null) {
					
					new MongoConnection();
					MongoConnection.MONGO_CLIENT = MongoClients.create(ConfigVariables.MONGODB_CONNECTION_STRING);
				
				}
			}
		}
		return MongoConnection.MONGO_CLIENT;
	}
}
