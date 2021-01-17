// MongoConnectionTest.java

package com.xqbase.mongodb.jdbc;

import java.util.*;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.*;
import com.mongodb.*;
import com.xqbase.mongodb.jdbc.MongoConnection;

public class MongoConnectionTest extends Base {
    MongoConnection _conn;

    public MongoConnectionTest() {
        super();
        _conn = new MongoConnection( _db );
    }

    @Test
    public void testBasic1() throws SQLException {
        String name = "conn.basic1";
        DBCollection coll = _db.getCollection( name );
        coll.drop();

        coll.insert( BasicDBObjectBuilder.start( "x" , Integer.valueOf(1) ).add( "y" , "foo" ).get() );
        coll.insert( BasicDBObjectBuilder.start( "x" , Integer.valueOf(2) ).add( "y" , "bar" ).get() );

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
        DBCollection coll = _db.getCollection( name );
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

        Statement stmt = _conn.createStatement();
        
        stmt.executeUpdate( "drop table " + name );

        stmt.executeUpdate( "insert into " + name + " ( x , y ) values ( 1 , 'foo' )" );
        stmt.executeUpdate( "insert into " + name + " ( y , x ) values ( 'bar' , 2 )" );
        
        ResultSet res = stmt.executeQuery( "select * from " + name + " order by x" );
        assertTrue( res.next() );
        assertEquals( 1 , res.getInt("x" ) );
        assertEquals( "foo" , res.getString("y" ) );
        assertTrue( res.next() );
        assertEquals( 2 , res.getInt("x" ) );
        assertEquals( "bar" , res.getString("y" ) );
        assertFalse( res.next() );
        res.close();
        
        stmt.executeUpdate( "update " + name + " set x=3 where y='foo' " );
        res = stmt.executeQuery( "select * from " + name + " order by x" );
        assertTrue( res.next() );
        assertEquals( 2 , res.getInt("x" ) );
        assertEquals( "bar" , res.getString("y" ) );
        assertTrue( res.next() );
        assertEquals( 3 , res.getInt("x" ) );
        assertEquals( "foo" , res.getString("y" ) );
        assertFalse( res.next() );
        res.close();
        
        stmt.close();
        
    }


    @Test
    public void testEmbed1() throws SQLException {
        String name = "connembed1";
        DBCollection coll = _db.getCollection( name );

        Statement stmt = _conn.createStatement();
        stmt.executeUpdate( "drop table " + name );
        coll.insert( BasicDBObjectBuilder.start( "x" , 1 ).add( "y" , new BasicDBObject( "z" , 2 ) ).get() );
        coll.insert( BasicDBObjectBuilder.start( "x" , 11 ).add( "y" , new BasicDBObject( "z" , 12 ) ).get() );
        try (ResultSet res = stmt.executeQuery( "select * from " + name + " order by x" )) {
            assertTrue( res.next() );
            assertEquals( 1 , res.getInt("x" ) );
            assertEquals( 2 , ((Map)(res.getObject( "y" ))).get( "z" ) );
            assertTrue( res.next() );
        }
        try (ResultSet res = stmt.executeQuery( "select * from " + name + " where y.z=12 order by x" )) {
            assertTrue( res.next() );
            assertEquals( 11 , res.getInt("x" ) );
            assertEquals( 12 , ((Map)(res.getObject( "y" ))).get( "z" ) );
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
