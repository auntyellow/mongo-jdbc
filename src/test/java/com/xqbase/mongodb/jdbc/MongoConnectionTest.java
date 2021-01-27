// MongoConnectionTest.java

package com.xqbase.mongodb.jdbc;

import java.util.*;

import org.bson.Document;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.*;
import com.mongodb.client.MongoCollection;
import com.xqbase.mongodb.jdbc.MongoConnection;

public class MongoConnectionTest extends Base {
	MongoConnection _conn;

	public MongoConnectionTest() {
		super();
		_conn = new MongoConnection(_mongo, _db.getName());
	}

	@Test
	public void testBasic1() throws SQLException {
		String name = "conn.basic1";
		MongoCollection<Document> coll = _db.getCollection( name );
		coll.drop();

		coll.insertOne(__("x", __(1), "y" , "foo"));
		coll.insertOne(__("x", __(2), "y" , "bar"));

		try (Statement stmt = _conn.createStatement()) {
			try (ResultSet res = stmt.executeQuery( "select * from " + name + " order by x" )) {
				assertTrue( res.next() );
				assertEquals( 1 , res.getInt("x" ) );
				assertEquals( "foo" , res.getString("y" ) );
				assertTrue( res.next() );
				assertEquals( 2 , res.getInt("x" ) );
				assertEquals( "bar" , res.getString("y" ) );
				assertFalse( res.next() );
			}
			try (ResultSet res = stmt.executeQuery( "select * from " + name + " order by x DESC" )) {
				assertTrue( res.next() );
				assertEquals( 2 , res.getInt("x" ) );
				assertEquals( "bar" , res.getString("y" ) );
				assertTrue( res.next() );
				assertEquals( 1 , res.getInt("x" ) );
				assertEquals( "foo" , res.getString("y" ) );
				assertFalse( res.next() );
			}
		}
	}

	@Test
	public void testBasic2() throws SQLException {
		String name = "conn.basic2";
		MongoCollection<Document> coll = _db.getCollection( name );
		coll.drop();

		try (Statement stmt = _conn.createStatement()) {
			stmt.executeUpdate( "insert into " + name + " ( x , y ) values ( 1 , 'foo' )" );
			stmt.executeUpdate( "insert into " + name + " ( y , x ) values ( 'bar' , 2 )" );
			try (ResultSet res = stmt.executeQuery( "select * from " + name + " order by x" )) {
				assertTrue( res.next() );
				assertEquals( 1 , res.getInt("x" ) );
				assertEquals( "foo" , res.getString("y" ) );
				assertTrue( res.next() );
				assertEquals( 2 , res.getInt("x" ) );
				assertEquals( "bar" , res.getString("y" ) );
				assertFalse( res.next() );
			}
			stmt.executeUpdate( "update " + name + " set x=3 where y='foo' " );
			try (ResultSet res = stmt.executeQuery( "select * from " + name + " order by x" )) {
				assertTrue( res.next() );
				assertEquals( 2 , res.getInt("x" ) );
				assertEquals( "bar" , res.getString("y" ) );
				assertTrue( res.next() );
				assertEquals( 3 , res.getInt("x" ) );
				assertEquals( "foo" , res.getString("y" ) );
				assertFalse( res.next() );
			}
		}
	}

	@Test
	public void testBasic3() throws SQLException {
		String name = "connbasic3";

		try (Statement stmt = _conn.createStatement()) {
			stmt.executeUpdate( "drop table " + name );

			stmt.executeUpdate( "insert into " + name + " ( x , y ) values ( 1 , 'foo' )" );
			stmt.executeUpdate( "insert into " + name + " ( y , x ) values ( 'bar' , 2 )" );

			try (ResultSet res = stmt.executeQuery( "select * from " + name + " order by x" )) {
				assertTrue( res.next() );
				assertEquals( 1 , res.getInt("x" ) );
				assertEquals( "foo" , res.getString("y" ) );
				assertTrue( res.next() );
				assertEquals( 2 , res.getInt("x" ) );
				assertEquals( "bar" , res.getString("y" ) );
				assertFalse( res.next() );
			}

			stmt.executeUpdate( "update " + name + " set x=3 where y='foo' " );

			try (ResultSet res = stmt.executeQuery( "select * from " + name + " order by x" )) {
				assertTrue( res.next() );
				assertEquals( 2 , res.getInt("x" ) );
				assertEquals( "bar" , res.getString("y" ) );
				assertTrue( res.next() );
				assertEquals( 3 , res.getInt("x" ) );
				assertEquals( "foo" , res.getString("y" ) );
				assertFalse( res.next() );
			}
		}
	}

	@Test
	public void testEmbed1() throws SQLException {
		String name = "connembed1";
		MongoCollection<Document> coll = _db.getCollection( name );

		Statement stmt = _conn.createStatement();
		stmt.executeUpdate( "drop table " + name );
		coll.insertOne(__("x", __(1), "y", __("z", __(2))));
		coll.insertOne(__("x", __(11), "y", __("z", __(12))));
		try (ResultSet res = stmt.executeQuery( "select * from " + name + " order by x" )) {
			assertTrue( res.next() );
			assertEquals(1 , res.getInt("x" ));
			assertEquals(__(2), ((Map<?, ?>) (res.getObject("y"))).get("z"));
			assertTrue( res.next() );
		}
		try (ResultSet res = stmt.executeQuery( "select * from " + name + " where y.z=12 order by x" )) {
			assertTrue( res.next() );
			assertEquals(11, res.getInt("x" ) );
			assertEquals(__(12), ((Map<?, ?>) (res.getObject("y"))).get("z"));
			assertFalse( res.next() );
		}
	}

	@Test
	public void testPrepared1() throws SQLException {
		final String name = "connprepare1";

		try (
			Statement stmt = _conn.createStatement();
			PreparedStatement ps = _conn.prepareStatement( "insert into " + name + " ( x , y ) values ( ? , ? )" );
		) {
			stmt.executeUpdate( "drop table " + name );
			ps.setInt( 1 , 1 );
			ps.setString( 2 , "foo" );
			ps.executeUpdate();
			ps.setInt( 1 , 2 );
			ps.setString( 2 , "bar" );
			ps.executeUpdate();
			try (ResultSet res = stmt.executeQuery( "select * from " + name + " order by x" )) {
				assertTrue( res.next() );
				assertEquals( 1 , res.getInt("x" ) );
				assertEquals( "foo" , res.getString("y" ) );
				assertTrue( res.next() );
				assertEquals( 2 , res.getInt("x" ) );
				assertEquals( "bar" , res.getString("y" ) );
				assertFalse( res.next() );
			}
			stmt.executeUpdate( "update " + name + " set x=3 where y='foo' " );
			try (ResultSet res = stmt.executeQuery( "select * from " + name + " order by x" )) {
				assertTrue( res.next() );
				assertEquals( 2 , res.getInt("x" ) );
				assertEquals( "bar" , res.getString("y" ) );
				assertTrue( res.next() );
				assertEquals( 3 , res.getInt("x" ) );
				assertEquals( "foo" , res.getString("y" ) );
				assertFalse( res.next() );
			}
		}
	}	
}