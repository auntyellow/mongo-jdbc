// MongoResultSetTest.java

package com.xqbase.mongodb.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.bson.Document;
import org.junit.Test;

import com.mongodb.client.MongoCollection;
import com.xqbase.mongodb.jdbc.MongoResultSet;

public class MongoResultSetTest extends Base {
	@Test
	public void testbasic1() {
		MongoCollection<Document> c = _db.getCollection( "rs.basic1" );
		c.drop();

		c.insertOne(__("x", __(1), "y", "foo"));
		c.insertOne(__("x", __(2), "y", "bar"));
		
		try (MongoResultSet res = new MongoResultSet(c.find().sort(__("x", __(1))))) {
			assertTrue( res.next() );
			assertEquals( 1 , res.getInt("x" ) );
			assertEquals( "foo" , res.getString("y" ) );
			assertTrue( res.next() );
			assertEquals( 2 , res.getInt("x" ) );
			assertEquals( "bar" , res.getString("y" ) );
			assertFalse( res.next() );
		}
	}

	@Test
	public void testorder1() {
		MongoCollection<Document> c = _db.getCollection( "rs.basic1" );
		c.drop();

		c.insertOne(__("x", __(1), "y", "foo"));
		c.insertOne(__("x", __(2), "y", "bar"));

		try (MongoResultSet res = new MongoResultSet(c.find().projection(__("x", __(1), "y", __(1))).sort(__("x", __(1))))) {
			assertTrue( res.next() );
			assertEquals( 1 , res.getInt(1) );
			assertEquals( "foo" , res.getString(2) );
			assertTrue( res.next() );
			assertEquals( 2 , res.getInt(1) );
			assertEquals( "bar" , res.getString(2) );
			assertFalse( res.next() );
		}

		try (MongoResultSet res = new MongoResultSet(c.find().projection(__("y", __(1), "x", __(1))).sort(__("x", __(1))))) {
			assertTrue( res.next() );
			assertEquals( 1 , res.getInt(2) );
			assertEquals( "foo" , res.getString(1) );
			assertTrue( res.next() );
			assertEquals( 2 , res.getInt(2) );
			assertEquals( "bar" , res.getString(1) );
			assertFalse( res.next() );
		}
	}
}