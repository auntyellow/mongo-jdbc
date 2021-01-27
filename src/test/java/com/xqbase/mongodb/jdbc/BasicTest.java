// BasicTest.java

package com.xqbase.mongodb.jdbc;

import org.bson.Document;
import org.junit.Assert;
import org.junit.Test;

import com.mongodb.client.MongoCollection;
import com.xqbase.mongodb.jdbc.Executor;

public class BasicTest extends Base {
	public BasicTest() {/**/}
	
	@Test
	public void test1() throws Exception {
		String name = "simple.test1";
		MongoCollection<Document> c = _db.getCollection( name );
		c.drop();

		for ( int i=1; i<=3; i++ ) {
			c.insertOne(__("a", __(i), "b", __(i), "x", __(i)));
		}

		Document empty = __();
		Document ab = __("a", __(1), "b", __(1));

		Assert.assertEquals(__(c.find()), __(new Executor(_db, "select * from " + name ).query()));
		Assert.assertEquals(__(c.find(empty).projection(ab)), __(new Executor( _db , "select a,b from " + name ).query()));
		Assert.assertEquals(__(c.find(__("x", __(3))).projection(ab)), __(new Executor( _db , "select a,b from " + name + " where x=3" ).query()));
	}
}