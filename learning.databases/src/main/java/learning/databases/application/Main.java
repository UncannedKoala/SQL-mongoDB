package learning.databases.application;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import org.bson.BsonRegularExpression;
import org.bson.Document;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.InsertManyResult;

import learning.databases.application.configuration.ConfigVariables;
import learning.databases.application.connection.MongoConnection;
import learning.databases.application.connection.MysqlConnection;

/**
 * <b>SQL</b>
 * <li>The _ wildcard represents a single character.</li>
 * <li>The % wildcard represents any number of characters, even zero
 * characters.</li>
 * 
 * <br>
 * <b>MONGO DB</b><br>
 * <br>
 * <li>Prefer enclosing the regex in // rather than '' or ""</li>
 * <li>.*: Matches any sequence of characters (zero or more times). <br>
 * This is a greedy wildcard, meaning it will match the longest possible
 * sequence.</li>
 * <li>.: Matches any single character.</li>
 * <li>^: Matches the beginning of the string.</li>
 * <li>$: Matches the end of the string.</li>
 * <li></li>
 */
@SuppressWarnings("unused")
public class Main {

	private static final Consumer<Document> MONGO_PRINT_CONSUMER = document -> System.out.println(document);
	private static final Consumer<ResultSet> SQL_PRINT_CONSUMER = resultSet -> {
		try {
			int id = resultSet.getInt("id");
			String productName = resultSet.getString("product_name");
			double product_price = resultSet.getDouble("product_price");
			String quantity = resultSet.getString("quantity");
			String department = resultSet.getString("department");
			System.out.println("id = " + id + ", product name = " + productName + ", product price = " + product_price
					+ ", quantity = " + quantity + ", department = " + department);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	};

	public static final String TABLE = ConfigVariables.MYSQL_DATABASE_LEARNING.concat(".")
			.concat(ConfigVariables.MYSQL_TABLE_MYSQL);

	public static void main(String[] args) throws SQLException {

		try (Connection conn = MysqlConnection.getConnection();
				Statement statement = conn.createStatement();
				MongoClient client = MongoConnection.getMongoClient()) {

//			MONGO DB set-up
			MongoDatabase db = client.getDatabase(ConfigVariables.MONGODB_DATABASE_LEARNING);
			MongoCollection<Document> collection = db.getCollection(ConfigVariables.MONGODB_COLLECTION_MONGODB);

			Main howTo = new Main();

			howTo.addOne(statement, collection);
			howTo.getAll(statement, collection);
			howTo.addMany(statement, collection);
			howTo.getAllStartingWith(statement, collection, "D");
			howTo.getAllContains(statement, collection, "D");
			howTo.getAllEndingWith(statement, collection, "p");
			howTo.startingWithAorB(statement, collection, "u", "d");
			howTo.containingAandB(statement, collection, "d", "o");
			howTo.countWhereNameStartsFrom(statement, collection, "d");
			howTo.getNameAs(statement, collection, "item");
		}

	}

	/**
	 * <p>
	 * Insert a single row/document into the table/collection<br>
	 * <br>
	 * <strong>MYSQL</strong><br>
	 * INSERT INTO table VALUES(null, 'name', 20, '2 pieces', 'department')<br>
	 * <br>
	 * <strong>MONGODB</strong><br>
	 * db.customers.insertOne({ product name: "Dove bathing soap", product_price:
	 * 60, quantity: "60gms", department: "body care" })
	 * </p>
	 * 
	 * @throws SQLException
	 */
	private void addOne(Statement statement, MongoCollection<Document> collection) throws SQLException {
		// MYSQL
		int rowsInserted = statement.executeUpdate(
				"insert into " + TABLE + " VALUES(null, 'Dove bathing soap', 60.0, '60gms', 'body care')");
		System.out.println("rows inserted :" + rowsInserted);

		// MONGODB
		collection.insertOne(new Document("product name", "Dove bathing soap").append("product price", 60)
				.append("quantity", "60gms").append("department", "body care"));
		System.out.println(collection.countDocuments());
	}

	/**
	 * <p>
	 * Retrieves all the rows/documents from the table/collection<br>
	 * <br>
	 * <strong>MYSQL</strong><br>
	 * SELECT * FROM table <br>
	 * <br>
	 * <strong>MOGODB</strong><br>
	 * db.collection.find()
	 * </p>
	 * 
	 * @param statement
	 * @param collection
	 * @throws SQLException
	 */
	private void getAll(Statement statement, MongoCollection<Document> collection) throws SQLException {
		// MYSQL
		ResultSet resultSet = statement.executeQuery("SELECT * FROM " + TABLE);
		while (resultSet.next()) {
			Main.printMysqlData(resultSet);
		}

		// MONGODB
		FindIterable<Document> iterable = collection.find();
		for (Document doc : iterable) {
			System.out.println(doc);
		}
	}

	/**
	 * <p>
	 * Add multiple row/documents to table/collection<br>
	 * <br>
	 * <strong>MQSQL</strong><br>
	 * INSERT INTO table VALUES( ... ), ( ... ), ( ... )<br>
	 * <br>
	 * <strong>MONGODB</strong><br>
	 * db.collection.insertMany( {JSON obj} , {JSON obj} , {JSON obj} )
	 * </p>
	 * 
	 * @param statement
	 * @param collection
	 * @throws SQLException
	 */
	private void addMany(Statement statement, MongoCollection<Document> collection) throws SQLException {
		// MYSQL
		int insertedRows = statement
				.executeUpdate("insert into " + TABLE + " VALUES(null, 'Dairy Milk', 40.0, '120gms', 'munchies'), "
						+ "(null, 'Lays cream & onion', 80.0, '150gms', 'munchies'), "
						+ "(null, 'Uncle chips', 40.0, '90gms', 'munchies')");
		System.out.println("added rows: " + insertedRows);

		// MONGODB
		List<Document> docs = new ArrayList<>();
		docs.add(new Document("product name", "Dairy Milk").append("product price", 40.0).append("quantity", "120gms")
				.append("department", "munchies"));
		docs.add(new Document("product name", "Lays cream & onion").append("product price", 80.0)
				.append("quantity", "150gms").append("department", "munchies"));
		docs.add(new Document("product name", "Uncle chips").append("product price", 40.0).append("quantity", "90gms")
				.append("department", "munchies"));
		InsertManyResult res = collection.insertMany(docs);
		System.out.println(res.wasAcknowledged() ? "inserted" : "failed");
	}

	/**
	 * <p>
	 * Return row/documents from table/collection whose value start from
	 * {@code startWith}<br>
	 * <br>
	 * <b>MQSQL</b><br>
	 * SELECT * FROM table WHERE column LIKE 'X%'<br>
	 * </br>
	 * <b>MONGODB</b><br>
	 * db.collection.find({'product name':{'$regex': '^D'}}),<br>
	 * <b>OR</b> db.collection.find({'product name':{'$regex': /^D/}})
	 * </p>
	 * 
	 * @param statement
	 * @param collection
	 * @throws SQLException
	 */
	private void getAllStartingWith(Statement statement, MongoCollection<Document> collection, String startWith)
			throws SQLException {
		statement.execute("SELECT * FROM " + TABLE + " WHERE product_name LIKE '" + startWith + "%'");
		ResultSet rs = statement.getResultSet();
		while (rs.next()) {
			printMysqlData(rs);
		}

//		FindIterable<Document> iterable = collection.find(Document.parse("{'product name':{'$regex': '^"+startWith+"'}}"));
//		OR (enclosing the regex withing '' or //, but always prefer //)
		FindIterable<Document> iterable = collection
				.find(Document.parse("{'product name':{'$regex': /^" + startWith + "/}}"));
		for (Document doc : iterable) {
			System.out.println(doc);
		}

	}

	/**
	 * <p>
	 * Return row/documents from table/collection whose value start from
	 * {@code startWith}<br>
	 * <br>
	 * <b>MQSQL</b><br>
	 * SELECT * FROM table WHERE column LIKE '%X%'<br>
	 * </br>
	 * <b>MONGODB</b><br>
	 * db.collection.find({'product name':{'$regex': /.*X.* /i } })<br>
	 * where, '.*' indicates (like % in sql) presence on any number of characters,
	 * and 'i' defined case-insensitivity.
	 * </p>
	 * 
	 * @implNote mongodb regex search is case-sensitive by default, where as sql is
	 *           case-insensitive
	 * @param statement
	 * @param collection
	 * @throws SQLException
	 * @param containing the string expected to apart of the value
	 */
	private void getAllContains(Statement statement, MongoCollection<Document> collection, String containing)
			throws SQLException {

//		case-insensitive search by default
		statement.execute("SELECT * FROM " + TABLE + " WHERE product_name LIKE '%" + containing + "%'");
		ResultSet rs = statement.getResultSet();
		while (rs.next())
			printMysqlData(rs);

//		case sensitive by default
		FindIterable<Document> iterable = collection
				.find(Document.parse("{'product name':{'$regex': /.*" + containing + ".*/i}}"));
		for (Document doc : iterable)
			System.out.println(doc);
	}

	/**
	 * <p>
	 * Get all the rows/documents from table/collection where
	 * 'produck_name'/'product name' ends with {@code endsWith}<br>
	 * <br>
	 * <b>MYSQL</b><br>
	 * SELECT * FROM table WHERE column LIKE '%X'<br>
	 * <br>
	 * <b>MONGO DB</b><br>
	 * db.collection.find({ field: { $regex: /X$/ } })<br>
	 * db.collection.find({ field: { $regex: /X$/i } })<br>
	 * added i for case-insensitivity
	 * </p>
	 * 
	 * @param statement
	 * @param collection
	 * @param endsWith
	 */
	private void getAllEndingWith(Statement statement, MongoCollection<Document> collection, String endsWith)
			throws SQLException {

		statement.execute("SELECT * FROM " + TABLE + " WHERE product_name LIKE '%" + endsWith + "'");
		ResultSet rs = statement.getResultSet();
		while (rs.next())
			printMysqlData(rs);

		Iterable<Document> iterable = collection
				.find(Document.parse("{'product name' : { $regex : /" + endsWith + "$/i}}"));
		iterable.forEach(doc -> System.out.println(doc));
	}

	/**
	 * <p>
	 * Get all the rows/documents from table/collection where
	 * 'produck_name'/'product name' starts with {@code A} OR {@code B} <br>
	 * <br>
	 * <b>MYSQL</b><br>
	 * SELECT * FROM table WHERE product_name LIKE 'A%' OR product_name LIKE
	 * 'B%'<br>
	 * SELECT * FROM table WHERE LEFT(product_name, 1) IN ('A', 'B')"<br>
	 * SELECT * FROM table WHERE product_name REGEXP'^[AB]'<br>
	 * <br>
	 * <b>MONGO DB</b><br>
	 * db.collection.find({ 'product name' : { $in : [ /^A/i, /^B/i] } })<br>
	 * added i for case-insensitivity
	 * </p>
	 * 
	 * @param statement
	 * @param collection
	 * @param A          string1 to start with
	 * @param B          string2 to start with
	 */
	private void startingWithAorB(Statement statement, MongoCollection<Document> collection, String A, String B)
			throws SQLException {
//		String sqlQuery = "SELECT * FROM " + TABLE + " WHERE product_name LIKE '" + A + "%' OR product_name LIKE '" + B + "%'";
//		String sqlQuery = "SELECT * FROM " + TABLE + " WHERE LEFT(product_name, 1) IN ('" + A + "', '" + B + "')";
		String sqlQuery = "SELECT * FROM " + TABLE + " WHERE product_name REGEXP'^[" + A + B + "]'";
		statement.execute(sqlQuery);
		ResultSet rs = statement.getResultSet();
		while (rs.next())
			SQL_PRINT_CONSUMER.accept(rs);
		;

		Document filter = Document.parse("{ 'product name' : { $in : [ /^" + A + "/i, /^" + B + "/i]}}");
		Iterable<Document> iterable = collection.find(filter);
		iterable.forEach(MONGO_PRINT_CONSUMER);
	}

	/**
	 * <p>
	 * Get all the rows/documents from table/collection where
	 * 'produck_name'/'product name' containing both {@code A} AND {@code B} <br>
	 * <br>
	 * <b>MYSQL</b><br>
	 * SELECT * FROM table WHERE product_name LIKE '%A%' AND product_name LIKE
	 * '%B%'<br>
	 * SELECT * FROM learning.mysqldb WHERE product_name REGEXP 'A.*B|B.*A'<br>
	 * <br>
	 * <b>MONGO DB</b><br>
	 * db.collection.find({$and: [{'product name': /.*A. * /},{'product name':/.*B.*
	 * /}]})<br>
	 * added i for case-insensitivity
	 * </p>
	 * 
	 * @param statement
	 * @param collection
	 * @param A          stringA to contain
	 * @param B          stringB to contain
	 */
	private void containingAandB(Statement statement, MongoCollection<Document> collection, String A, String B)
			throws SQLException {
//		MYSQL
//		String sqlQuery = "SELECT * FROM " + TABLE + " WHERE product_name LIKE '%" + A + "%' AND product_name LIKE '%" + B + "%'";
		String sqlQuery = "SELECT * FROM " + TABLE + " WHERE product_name REGEXP '" + A + ".*" + B + "|" + B + ".*" + A
				+ "'";
		ResultSet rs = statement.executeQuery(sqlQuery);
		while (rs.next())
			SQL_PRINT_CONSUMER.accept(rs);

//		MONGO
		Document filterA = new Document("product name", new BsonRegularExpression(".*" + A + ".*", "i"));
		Document filterB = new Document("product name", new BsonRegularExpression(".*" + B + ".*", "i"));
		Document mongo_query = new Document("$and", Arrays.asList(filterA, filterB));

		collection.find(mongo_query).forEach(MONGO_PRINT_CONSUMER);
	}

	/**
	 * <p>
	 * Get the COUNT of rows/documents from table/collection where
	 * 'produck_name'/'product name' starts from {@code A} <br>
	 * <br>
	 * <b>MYSQL</b><br>
	 * SELECT COUNT(*) FROM learning.mysqldb WHERE product_name LIKE 'A%';<br>
	 * COUNT(*) FROM learning.mysqldb WHERE LEFT(product_name, 1) = 'A';<br>
	 * <br>
	 * <b>MONGO DB</b><br>
	 * db.collection.countDocuments({ 'product name': { $regex : /^A/i } })<br>
	 * added i for case-insensitivity
	 * </p>
	 * 
	 * @param statement
	 * @param collection
	 * @param startsFrom string to start from
	 */
	private void countWhereNameStartsFrom(Statement statement, MongoCollection<Document> collection, String startsFrom)
			throws SQLException {

//		MYSQL
//		String sqlQuery = "SELECT COUNT(*) FROM " + TABLE + " WHERE product_name LIKE '" + startsFrom + "%'";
		String sqlQuery = "SELECT COUNT(*) FROM " + TABLE + " WHERE LEFT(product_name, 1) = '" + startsFrom + "'";
		statement.execute(sqlQuery);
		ResultSet rs = statement.getResultSet();
		rs.next();
		System.out.println(rs.getLong(1));

//		MONGO
		Document filter = new Document("product name", new BsonRegularExpression("^" + startsFrom, "i"));
		long count = collection.countDocuments(filter);
		System.out.println(count);
	}

	/**
	 * <p>
	 * Get 'product_name' value from all rows/documents from table/collection where
	 * 'product_name'/'product name' starts from {@code A}<br>
	 * <br>
	 * <b>MYSQL</b><br>
	 * SELECT product_name FROM learning.mysqldb AS item;<br>
	 * <b>MONGO DB</b><br>
	 * db.collection.arrgigate([{$project:{item: "$product name"}}])<br>
	 * </p>
	 * 
	 * @param statement
	 * @param collection
	 * @param as         string to start from
	 */
	private void getNameAs(Statement statement, MongoCollection<Document> collection, String as) throws SQLException {
		Consumer<ResultSet> printConsumer_Name = rs -> {
			try {
				System.out.println(rs.getString(1));
			} catch (SQLException e) {
				e.printStackTrace();
			}
		};

//		MYSQL
		String sqlQuery = "SELECT product_name FROM " + TABLE + " AS " + as;
		ResultSet rs = statement.executeQuery(sqlQuery);
		while (rs.next())
			printConsumer_Name.accept(rs);

//		MONGO
		collection.aggregate(Arrays.asList(new Document("$project", new Document("item", "$product name"))))
				.forEach(MONGO_PRINT_CONSUMER);

	}

	private static void printMysqlData(ResultSet resultSet) throws SQLException {
		int id = resultSet.getInt("id");
		String productName = resultSet.getString("product_name");
		double product_price = resultSet.getDouble("product_price");
		String quantity = resultSet.getString("quantity");
		String department = resultSet.getString("department");
		System.out.print("id = " + id);
		System.out.print(", product name = " + productName);
		System.out.print(", product price = " + product_price);
		System.out.print(", quantity = " + quantity);
		System.out.println(", department = " + department);
	}
}
