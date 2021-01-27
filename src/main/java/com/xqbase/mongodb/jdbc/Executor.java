// Executor.java

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

import java.io.*;
import java.util.*;

import org.bson.Document;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.*;
import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.statement.insert.*;
import net.sf.jsqlparser.statement.update.*;
import net.sf.jsqlparser.statement.drop.*;

import com.mongodb.*;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class Executor {
	static final boolean D = false;

	Executor( MongoDatabase db , String sql ) throws MongoSQLException {
		_db = db;
		_sql = sql;
		_statement = parse( sql );

		if ( D ) System.out.println( sql );
	}

	void setParams( List<Object> params ) {
		_pos = 1;
		_params = params;
	}

	FindIterable<Document> query() {
		if ( ! ( _statement instanceof Select ) )
			throw new IllegalArgumentException( "not a query sql statement" );
		
		Select select = (Select)_statement;
		if ( ! ( select.getSelectBody() instanceof PlainSelect ) )
			throw new UnsupportedOperationException( "can only handle PlainSelect so far" );
		
		PlainSelect ps = (PlainSelect)select.getSelectBody();
		if ( ! ( ps.getFromItem() instanceof Table ) )
			throw new UnsupportedOperationException( "can only handle regular tables" );
		
		MongoCollection<Document> coll = getCollection( (Table)ps.getFromItem() );

		BasicDBObject fields = new BasicDBObject();
		for ( Object o : ps.getSelectItems() ) {
			SelectItem si = (SelectItem)o;
			if ( si instanceof AllColumns ) {
				if ( fields.size() > 0 )
					throw new UnsupportedOperationException( "can't have * and fields" );
				break;
			}
			else if ( si instanceof SelectExpressionItem ) {
				SelectExpressionItem sei = (SelectExpressionItem)si;
				fields.put( toFieldName( sei.getExpression() ) , Integer.valueOf(1) );
			}
			else {
				throw new UnsupportedOperationException( "unknown select item: " + si.getClass() );
			}
		}
		
		// where
		BasicDBObject query = parseWhere( ps.getWhere() );
		
		// done with basics, build DBCursor
		if ( D ) System.out.println( "\t" + "table: " + coll );
		if ( D ) System.out.println( "\t" + "fields: " + fields );
		if ( D ) System.out.println( "\t" + "query : " + query );
		FindIterable<Document> c = coll.find( query ).projection( fields );

		{ // order by
			List<OrderByElement> orderBylist = ps.getOrderByElements();
			if ( orderBylist != null && orderBylist.size() > 0 ) {
				BasicDBObject order = new BasicDBObject();
				for ( int i=0; i<orderBylist.size(); i++ ) {
					OrderByElement o = orderBylist.get(i);
					// order.put( o.getColumnReference().toString() , o.isAsc() ? 1 : -1 );
					// TODO o.getExpression() work?
					order.put(o.getExpression().toString(), Integer.valueOf(o.isAsc() ? 1 : -1));
				}
				c.sort( order );
			}
		}

		return c;
	}

	int writeop()
		throws MongoSQLException {
		
		if ( _statement instanceof Insert )
			return insert( (Insert)_statement );
		else if ( _statement instanceof Update )
			return update( (Update)_statement );
		else if ( _statement instanceof Drop )
			return drop( (Drop)_statement );

		throw new RuntimeException( "unknown write: " + _statement.getClass() );
	}
	
	int insert( Insert in ) throws MongoSQLException {
		if ( in.getColumns() == null )
			throw new MongoSQLException.BadSQL( "have to give column names to insert" );

		MongoCollection<Document> coll = getCollection( in.getTable() );
		if ( D ) System.out.println( "\t" + "table: " + coll );
		
		if ( ! ( in.getItemsList() instanceof ExpressionList ) )
			throw new UnsupportedOperationException( "need ExpressionList" );
		
		Document o = new Document();

		List<Expression> valueList = ((ExpressionList)in.getItemsList()).getExpressions();
		if ( in.getColumns().size() != valueList.size() )
			throw new MongoSQLException.BadSQL( "number of values and columns have to match" );

		for ( int i=0; i<valueList.size(); i++ ) {
			o.put( in.getColumns().get(i).toString() , toConstant( valueList.get(i) ) );
		}

		coll.insertOne( o );		
		return 1; // TODO - this is wrong
	}

	int update( Update up ) {
		BasicDBObject query = parseWhere( up.getWhere() );

		BasicDBObject set = new BasicDBObject();

		for ( int i=0; i<up.getColumns().size(); i++ ) {
			String k = up.getColumns().get(i).toString();
			Expression v = up.getExpressions().get(i);
			set.put( k.toString() , toConstant( v ) );
		}

		BasicDBObject mod = new BasicDBObject( "$set" , set );

		MongoCollection<Document> coll = getCollection( up.getTable() );
		return (int) coll.updateMany( query , mod ).getMatchedCount();
	}

	int drop( Drop d ) {
		MongoCollection<Document> c = _db.getCollection(d.getName().getName());
		c.drop();
		return 0;
	}

	// ---- helpers -----

	String toFieldName( Expression e ) {
		if ( e instanceof StringValue )
			return e.toString();
		if ( e instanceof Column )
			return e.toString();
		throw new UnsupportedOperationException( "can't turn [" + e + "] " + e.getClass() + " into field name" );
	}

	Object toConstant( Expression e ) {
		if ( e instanceof StringValue )
			return ((StringValue)e).getValue();
		else if ( e instanceof DoubleValue )
			return Double.valueOf(((DoubleValue)e).getValue());
		else if ( e instanceof LongValue )
			return Long.valueOf(((LongValue)e).getValue());
		else if ( e instanceof NullValue )
			return null;
		else if ( e instanceof JdbcParameter )
			return _params.get( _pos++ );
				 
		throw new UnsupportedOperationException( "can't turn [" + e + "] " + e.getClass().getName() + " into constant " );
	}


	BasicDBObject parseWhere( Expression e ) {
		BasicDBObject o = new BasicDBObject();
		if ( e == null )
			return o;
		
		if ( e instanceof EqualsTo ) {
			EqualsTo eq = (EqualsTo)e;
			o.put( toFieldName( eq.getLeftExpression() ) , toConstant( eq.getRightExpression() ) );
		}
		else {
			throw new UnsupportedOperationException( "can't handle: " + e.getClass() + " yet" );
		}

		return o;
	}

	Statement parse( String sql ) throws MongoSQLException {
		String s = sql.trim();

		try {
			return (new CCJSqlParserManager()).parse( new StringReader( s ) );
		} catch ( Exception e ) {
			e.printStackTrace();
			throw new MongoSQLException.BadSQL( s );
		}
		
	}

	// ----

	MongoCollection<Document> getCollection( Table t ) {
		return _db.getCollection( t.toString() );
	}

	final MongoDatabase _db;
	final String _sql;
	final Statement _statement;

	List<Object> _params;
	int _pos;
}