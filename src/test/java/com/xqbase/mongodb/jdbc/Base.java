// Base.java

package com.xqbase.mongodb.jdbc;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;

import com.mongodb.*;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoDatabase;

public abstract class Base {
	final MongoClient _mongo;
	final MongoDatabase _db;

	public Base() {
		try {
			_mongo = new MongoClient();
			_db = _mongo.getDatabase( "jdbctest" );
		} catch ( Exception e ) {
			throw new RuntimeException( e );
		}
	}

	public static Integer __(int i) {
		return Integer.valueOf(i);
	}

	public static Document __(Object... pairs) {
		Document doc = new Document();
		for (int i = 0; i < pairs.length; i += 2) {
			doc.put((String) pairs[i], pairs[i + 1]);
		}
		return doc;
	}

	public static List<?> __(FindIterable<Document> result) {
		ArrayList<Document> list = new ArrayList<>();
		for (Document doc : result) {
			list.add(doc);
		}
		return list;
	}
}