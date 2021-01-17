// DriverTest.java

package com.xqbase.mongodb.jdbc;

import java.sql.*;

import org.junit.Assert;
import org.junit.Test;

import com.xqbase.mongodb.jdbc.MongoDriver;

public class DriverTest {
    @Test
    public void test1() throws Exception {
        Connection c = null;
        try {
            c = DriverManager.getConnection( "jdbc:mongodb://localhost/test" );
        } catch ( Exception e ) {/**/}

        Assert.assertNull( c );

        MongoDriver.install();
        c = DriverManager.getConnection( "jdbc:mongodb://localhost/test" );
    }
}
