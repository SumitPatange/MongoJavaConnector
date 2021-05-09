package com.learning.mongodb.MongoJavaConnector;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import jdk.nashorn.internal.parser.JSONParser;
import org.bson.Document;
import org.bson.json.JsonReader;
import org.json.JSONArray;
import org.json.JSONObject;


import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class MongoJavaConnectorApplication {

	public static void main(String[] args) throws Exception {

		MongoClient atlasClient = null;
		String connString = "mongodb+srv://m001-student:m001-mongodb-basics@cluster0-jxeqq.mongodb.net/test";
		atlasClient = MongoClients.create(connString);
		//publishToEndpoint(atlasClient, "mflix", "movies");


		readConfigData();
	}

	public static void readConfigData() throws IOException {
		String configData = Files.lines(Paths.get("src/main/resources/DownstreamConfigOneLiner.json")).toString();

		Document document = Document.parse("{\"dataset_id\":\"bc_position\",\"dataset_types\":[{\"type\":\"security\",\"sources\":[{\"source_id\":\"A\",\"client_id\":\"A1\"},{\"source_id\":\"B\",\"client_id\":\"B1\"}],\"bcml_collection_name\":\"securityMongoCollection\"},{\"type\":\"cash\",\"sources\":[{\"source_id\":\"C\",\"client_id\":\"C1\"},{\"source_id\":\"D\",\"client_id\":\"D1\"}],\"bcml_collection_name\":\"cashMongoCollection\"}]}");
		JSONObject configObject = new JSONObject(document.toJson());
		String datasetId = configObject.getString("dataset_id");
		System.out.println("dataset id : "+ datasetId);
		JSONArray datasetTypes = configObject.getJSONArray("dataset_types");

		for(int i=0; i<datasetTypes.length(); i++){
			JSONObject typeObject = datasetTypes.getJSONObject(i);
			if(typeObject.getString("type").equalsIgnoreCase("cash")){
				System.out.println(typeObject.get("bcml_collection_name"));
				JSONArray sources =  typeObject.getJSONArray("sources");
				for(int j=0; j<sources.length(); j++){
					JSONObject source = sources.getJSONObject(j);
					System.out.println(source.getString("source_id"));
					System.out.println(source.getString("client_id"));
				}
			}

		}

	}

	public static void publishToEndpoint(MongoClient atlasClient, String dbName, String collectionName) {
		try {
			int batchSize = 1000;
			int publishedRecords = 0;
			List<Document> messageToPublish = new ArrayList<>();
			FileOutputStream publisherStream = null;
			publisherStream = new FileOutputStream("src/main/resources/moviesDetails.txt");
			for (Document movieEntry :
					atlasClient
							.getDatabase(dbName)
							.getCollection(collectionName)
							.find()
							.batchSize(batchSize)) {
				messageToPublish.add(movieEntry);
				if (messageToPublish.size() == batchSize) {
					publisherStream.write(messageToPublish.toString().getBytes());
					publishedRecords += batchSize;
					System.out.println("Records published : " + publishedRecords);
					messageToPublish.clear();
				}
			}
		}catch (Exception exp){
			exp.printStackTrace();
		}
	}
}
