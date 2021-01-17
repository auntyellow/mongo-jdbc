// MongoDriver.java

/**
 *      Copyright (C) 2008 10gen Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.xqbase.mongodb.jdbc;

import java.sql.*;
import java.util.*;
import java.util.logging.Logger;

import com.mongodb.*;

public class MongoDriver implements Driver {

    static final String PREFIX = "jdbc:mongodb://";

    public MongoDriver() {/**/}

    @Override
	public boolean acceptsURL(String url) {
        return url.startsWith( PREFIX );
    }
    
    @Override
	public Connection connect(String url, Properties info) throws SQLException {
        if ( info != null && info.size() > 0 )
            throw new UnsupportedOperationException( "properties not supported yet" );

        // Remove "jdbc:"
        MongoClientURI uri = new MongoClientURI(url.substring(5));
        return new MongoConnection(new MongoClient(uri).getDatabase(uri.getDatabase()));
    }
    
    @Override
    public int getMajorVersion() {
        return 0;
    }

    @Override
    public int getMinorVersion() {
        return 1;
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
        throw new UnsupportedOperationException( "getPropertyInfo doesn't work yet" );
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    public static void install() {
        // NO-OP, handled in static
    }

    static {
        try {
            DriverManager.registerDriver( new MongoDriver() );
        } catch ( SQLException e ) {
            throw new RuntimeException( e );
        }
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }
}
