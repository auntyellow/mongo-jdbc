// Base.java

package com.xqbase.mongodb.jdbc;

import com.mongodb.*;

public abstract class Base {
    final Mongo _mongo;
    final DB _db;

    public Base() {
        try {
            _mongo = new Mongo();
            _db = _mongo.getDB( "jdbctest" );
        } catch ( Exception e ) {
            throw new RuntimeException( e );
        }
    }
}
