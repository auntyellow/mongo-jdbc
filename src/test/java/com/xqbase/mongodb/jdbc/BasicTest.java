// BasicTest.java

package com.xqbase.mongodb.jdbc;

import org.junit.Assert;
import org.junit.Test;

import com.mongodb.*;
import com.xqbase.mongodb.jdbc.Executor;

public class BasicTest extends Base {
    public BasicTest() {/**/}
    
    @Test
    public void test1() throws Exception {
        String name = "simple.test1";
        DBCollection c = _db.getCollection( name );
        c.drop();

        for ( int i=1; i<=3; i++ ) {
            c.insert( BasicDBObjectBuilder.start( "a" , Integer.valueOf(i) ).add( "b" , Integer.valueOf(i) ).add( "x" , Integer.valueOf(i) ).get() );
        }

        DBObject empty = new BasicDBObject();
        DBObject ab = BasicDBObjectBuilder.start( "a" , Integer.valueOf(1) ).add( "b" , Integer.valueOf(1) ).get();

        Assert.assertEquals( c.find().toArray() , new Executor( _db , "select * from " + name ).query().toArray() );
        Assert.assertEquals( c.find( empty , ab ).toArray(), new Executor( _db , "select a,b from " + name ).query().toArray() );
        Assert.assertEquals( c.find( new BasicDBObject( "x" , Integer.valueOf(3) ) , ab ).toArray() , new Executor( _db , "select a,b from " + name + " where x=3" ).query().toArray() );
    }
}
