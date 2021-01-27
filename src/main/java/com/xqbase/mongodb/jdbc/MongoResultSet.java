// MongoResultSet.java

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
import java.net.*;
import java.sql.*;
import java.util.*;

import org.bson.Document;

import java.sql.Date;
import java.math.*;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;

public class MongoResultSet implements ResultSet {

	MongoResultSet( FindIterable<Document> cursor ) {
		_cursor = cursor.cursor();
		_fields.init( cursor.first() );
	}

	@Override
	public void clearWarnings() {
		// NO-OP
	}

	@Override
	public void close() {
		_closed = true;
	}

	@Override
	public boolean isClosed() {
		return _closed;
	}

	//  meta data

	@Override
	public int getConcurrency() {
		return CONCUR_READ_ONLY;
	}

	@Override
	public int getType() {
		return TYPE_FORWARD_ONLY;
	}

	@Override
	public void setFetchDirection(int direction) {
		if ( direction == getFetchDirection() )
			return;
		throw new UnsupportedOperationException();
	}

	@Override
	public int getFetchDirection() {
		return 1;
	}

	@Override
	public String getCursorName() {
		return "MongoResultSet: " + _cursor.toString();
	}

	@Override
	public ResultSetMetaData getMetaData() {
		throw new UnsupportedOperationException();
	}

	@Override
	public SQLWarning getWarnings() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setFetchSize(int rows) {
		throw new UnsupportedOperationException();		
	}

	@Override
	public int getFetchSize() {
		throw new UnsupportedOperationException();		
	}

	@Override
	public Statement getStatement() {
		throw new UnsupportedOperationException();	   
	}

	@Override
	public int getHoldability() {
		return ResultSet.HOLD_CURSORS_OVER_COMMIT;
	}

	// cursor moving methods

	@Override
	public boolean absolute(int row) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void afterLast() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void beforeFirst() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean first() {
		throw new UnsupportedOperationException();
	}

	@Override
	public int getRow() {
		return _row;
	}

	@Override
	public boolean isAfterLast() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isBeforeFirst() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isFirst() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isLast() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean last() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void moveToCurrentRow() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void moveToInsertRow() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean previous() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void refreshRow() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean relative(int rows) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean rowDeleted() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean rowInserted() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean rowUpdated() {
		throw new UnsupportedOperationException();
	}

	// modifications

	@Override
	public void insertRow() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void cancelRowUpdates() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void deleteRow() {
		throw new UnsupportedOperationException();		
	}

	@Override
	public void updateRow() {
		throw new UnsupportedOperationException();
	}
	
	// field updates

	@Override
	public void updateArray(int columnIndex, Array x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateArray(String columnName, Array x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x, int length) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateAsciiStream(String columnName, InputStream x, int length) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x, long length) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateAsciiStream(String columnName, InputStream x, long length) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x ) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateAsciiStream(String columnName, InputStream x ) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateBigDecimal(int columnIndex, BigDecimal x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateBigDecimal(String columnName, BigDecimal x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x, int length) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateBinaryStream(String columnName, InputStream x, int length) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x, long length) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateBinaryStream(String columnName, InputStream x, long length) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateBinaryStream(String columnName, InputStream x ) {
		throw new UnsupportedOperationException(); }

	@Override
	public void updateBlob(int columnIndex, Blob x) {
		throw new UnsupportedOperationException(); }

	@Override
	public void updateBlob(String columnName, Blob x) {
		throw new UnsupportedOperationException(); }

	@Override
	public void updateBlob(int columnIndex, InputStream x) {
		throw new UnsupportedOperationException(); }

	@Override
	public void updateBlob(String columnName, InputStream x) {
		throw new UnsupportedOperationException(); }

	@Override
	public void updateBlob(int columnIndex, InputStream x, long l) {
		throw new UnsupportedOperationException(); }

	@Override
	public void updateBlob(String columnName, InputStream x, long l) {
		throw new UnsupportedOperationException(); }

	@Override
	public void updateBoolean(int columnIndex, boolean x) {
		throw new UnsupportedOperationException(); }

	@Override
	public void updateBoolean(String columnName, boolean x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateByte(int columnIndex, byte x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateByte(String columnName, byte x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateBytes(int columnIndex, byte[] x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateBytes(String columnName, byte[] x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x, int length) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateCharacterStream(String columnName, Reader reader, int length) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x, long  length) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateCharacterStream(String columnName, Reader reader, long length) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x ) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateCharacterStream(String columnName, Reader reader ) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateClob(int columnIndex, Clob x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateClob(String columnName, Clob x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateClob(int columnIndex, Reader x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateClob(String columnName, Reader x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateClob(int columnIndex, Reader x, long l ) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateClob(String columnName, Reader x, long l ) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateDate(int columnIndex, Date x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateDate(String columnName, Date x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateDouble(int columnIndex, double x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateDouble(String columnName, double x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateFloat(int columnIndex, float x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateFloat(String columnName, float x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateInt(int columnIndex, int x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateInt(String columnName, int x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateLong(int columnIndex, long x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateLong(String columnName, long x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateNull(int columnIndex) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateNull(String columnName) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateObject(int columnIndex, Object x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateObject(int columnIndex, Object x, int scale) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateObject(String columnName, Object x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateObject(String columnName, Object x, int scale) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateRef(int columnIndex, Ref x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateRef(String columnName, Ref x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateRowId(int columnIndex, RowId x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateRowId(String columnName, RowId x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateShort(int columnIndex, short x) {
		throw new UnsupportedOperationException(); 
	}

	@Override
	public void updateShort(String columnName, short x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateSQLXML(int columnIndex, SQLXML xmlObject) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateSQLXML(String columnName, SQLXML xmlObject) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateString(int columnIndex, String x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateString(String columnName, String x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateTime(int columnIndex, Time x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateTime(String columnName, Time x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateTimestamp(int columnIndex, Timestamp x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateTimestamp(String columnName, Timestamp x) {
		throw new UnsupportedOperationException();
	}

	// accessors

	@Override
	public Array getArray(int i) {
		return getArray( _find( i ) );
	}

	@Override
	public Array getArray(String colName) {
		throw new UnsupportedOperationException();
	}

	@Override
	public InputStream getAsciiStream(int columnIndex) {
		return getAsciiStream( _find( columnIndex ) );
	}

	@Override
	public InputStream getAsciiStream(String columnName) {
		throw new UnsupportedOperationException();
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex) {
		return getBigDecimal( _find( columnIndex ) );
	}

	@Deprecated
	@Override
	public BigDecimal getBigDecimal(int columnIndex, int scale) {
		return getBigDecimal( _find( columnIndex ) , scale );
	}

	@Override
	public BigDecimal getBigDecimal(String columnName) {
		throw new UnsupportedOperationException();
	}

	@Deprecated
	@Override
	public BigDecimal getBigDecimal(String columnName, int scale) {
		throw new UnsupportedOperationException();
	}

	@Override
	public InputStream getBinaryStream(int columnIndex) {
		return getBinaryStream( _find( columnIndex ) );
	}

	@Override
	public InputStream getBinaryStream(String columnName) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Blob getBlob(int i) {
		return getBlob( _find( i ) );
	}

	@Override
	public Blob getBlob(String colName) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean getBoolean(int columnIndex) {
		return getBoolean( _find( columnIndex ) );
	}

	@Override
	public boolean getBoolean(String columnName) {
		Object x = _cur.get( columnName );
		if ( x == null ) {
			return false;
		}
		return ((Boolean) x).booleanValue();
	}

	@Override
	public byte getByte(int columnIndex) {
		return getByte( _find( columnIndex ) );
	}

	@Override
	public byte getByte(String columnName) {
		throw new UnsupportedOperationException();
	}

	@Override
	public byte[] getBytes(int columnIndex) {
		return getBytes( _find( columnIndex ) );
	}

	@Override
	public byte[] getBytes(String columnName) {
		return (byte[])_cur.get( columnName );
	}

	@Override
	public Reader getCharacterStream(int columnIndex) {
		return getCharacterStream( _find( columnIndex ) );
	}

	@Override
	public Reader getCharacterStream(String columnName) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Clob getClob(int i) {
		return getClob( _find( i ) );
	}

	@Override
	public Clob getClob(String colName) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Date getDate(int columnIndex) {
		return getDate( _find( columnIndex ) );
	}

	@Override
	public Date getDate(int columnIndex, Calendar cal) {
		return getDate( _find( columnIndex ) , cal );
	}

	@Override
	public Date getDate(String columnName) {
		return (Date)_cur.get( columnName );
	}

	@Override
	public Date getDate(String columnName, Calendar cal) {
		throw new UnsupportedOperationException();		
	}

	@Override
	public double getDouble(int columnIndex) {
		return getDouble( _find( columnIndex ) );
	}

	@Override
	public double getDouble(String columnName) {
		return _getNumber( columnName ).doubleValue();
	}

	@Override
	public float getFloat(int columnIndex) {
		return getFloat( _find( columnIndex ) );
	}

	@Override
	public float getFloat(String columnName) {
		return _getNumber( columnName ).floatValue();
	}

	@Override
	public int getInt(int columnIndex) {
		return getInt( _find( columnIndex ) );
	}

	@Override
	public int getInt(String columnName) {
		return _getNumber( columnName ).intValue();
	}

	@Override
	public long getLong(int columnIndex) {
		return getLong( _find( columnIndex ) );
	}

	@Override
	public long getLong(String columnName) {
		return _getNumber( columnName ).longValue();
	}

	@Override
	public short getShort(int columnIndex) {
		return getShort( _find( columnIndex ) );
	}

	@Override
	public short getShort(String columnName) {
		return _getNumber( columnName ).shortValue();
	}

	Number _getNumber( String n ) {
		Number x = (Number) (_cur.get(n ) );
		if ( x == null ) {
			return Integer.valueOf(0);
		}
		return x;
	}

	@Override
	public Object getObject(int columnIndex) {
		if ( columnIndex == 0 )
			return _cur;
		return getObject( _find( columnIndex ) );
	}

	@Override
	public Object getObject(int i, Map<String,Class<?>> map) {
		if ( i == 0 )
			return _cur;
		return getObject( _find( i ) , map );
	}

	@Override
	public Object getObject(String columnName) {
		return _cur.get( columnName );
	}

	@Override
	public Object getObject(String colName, Map<String,Class<?>> map) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Ref getRef(int i) {
		return getRef( _find( i ) );
	}

	@Override
	public Ref getRef(String colName) {
		throw new UnsupportedOperationException();
	}

	@Override
	public RowId getRowId( int i ) {
		return getRowId( _find( i ) );
	}

	@Override
	public RowId getRowId( String name ) {
		throw new UnsupportedOperationException();
	}

	@Override
	public SQLXML getSQLXML(int columnIndex) {
		return getSQLXML( _find( columnIndex ) );
	}

	@Override
	public SQLXML getSQLXML(String columnName) {
		throw new UnsupportedOperationException();
	}

	@Override
	public String getString(int columnIndex) {
		return getString( _find( columnIndex ) );
	}

	@Override
	public String getString(String columnName) {
		Object x = _cur.get( columnName );
		if ( x == null )
			return null;
		return x.toString();
	}

	@Override
	public Time getTime(int columnIndex) {
		return getTime( _find( columnIndex ) );
	}

	@Override
	public Time getTime(int columnIndex, Calendar cal) {
		return getTime( _find( columnIndex ) , cal );
	}

	@Override
	public Time getTime(String columnName) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Time getTime(String columnName, Calendar cal) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Timestamp getTimestamp(int columnIndex) {
		return getTimestamp( _find( columnIndex ) );
	}

	@Override
	public Timestamp getTimestamp(int columnIndex, Calendar cal) {
		return getTimestamp( _find( columnIndex ) , cal );
	}

	@Override
	public Timestamp getTimestamp(String columnName) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Timestamp getTimestamp(String columnName, Calendar cal) {
		throw new UnsupportedOperationException();
	}

	@Deprecated
	@Override
	public InputStream getUnicodeStream(int columnIndex) {
		return getUnicodeStream( _find( columnIndex ) );
	}

	@Deprecated
	@Override
	public InputStream getUnicodeStream(String columnName) {
		throw new UnsupportedOperationException();
	}

	@Override
	public URL getURL(int columnIndex) throws SQLException {
		return getURL( _find( columnIndex ) );
	}

	@Override
	public URL getURL(String columnName)
		throws SQLException {
		try {
			return new URL( getString( columnName ) );
		}
		catch ( java.net.MalformedURLException m ) {
			throw new SQLException( "bad url [" + getString( columnName ) + "]" );
		}
	}

	// N stuff

	@Override
	public String getNString(int columnIndex) {
		return getNString( _find( columnIndex ) );
	}

	@Override
	public String getNString(String columnName) {
		throw new UnsupportedOperationException();
	}

	@Override
	public NClob getNClob(int columnIndex) {
		return getNClob( _find( columnIndex ) );
	}

	@Override
	public NClob getNClob(String columnName) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Reader getNCharacterStream(int columnIndex) {
		return getNCharacterStream( _find( columnIndex ) );
	}

	@Override
	public Reader getNCharacterStream(String columnName) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateNCharacterStream(int columnIndex, Reader x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateNCharacterStream(int columnIndex, Reader x, long length) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateNCharacterStream(String columnLabel, Reader reader) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateNCharacterStream(String columnLabel, Reader reader, long length) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateNClob(int columnIndex, NClob nClob) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateNClob(int columnIndex, Reader reader) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateNClob(int columnIndex, Reader reader, long length) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateNClob(String columnLabel, NClob nClob) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateNClob(String columnLabel, Reader reader) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateNClob(String columnLabel, Reader reader, long length) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateNString(int columnIndex, String nString) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateNString(String columnLabel, String nString) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean wasNull() {
		throw new UnsupportedOperationException();
	}

	// column <-> int mapping

	@Override
	public int findColumn( String columnName ) {
		return _fields.get( columnName );
	}

	public String _find( int i ) {
		return _fields.get( i );
	}

	// random stuff

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throw new UnsupportedOperationException();
	}

	// moving throgh cursor

	@Override
	public boolean next() {
		if ( ! _cursor.hasNext() ) {
			return false;
		}
		_cur = _cursor.next();
		return true;
	}

	// members

	final MongoCursor<Document> _cursor;
	final FieldLookup _fields = new FieldLookup();
	Document _cur;
	int _row = 0;
	boolean _closed = false;

	class FieldLookup {

		void init( Document o ) {
			if ( o == null )
				return;
			for ( String key : o.keySet() )
				get( key );
		}
		
		int get( String s ) {
			int i;
			Integer i_ = _strings.get(s);
			if ( i_ != null ) {
				return i_.intValue();
			}
			i = _strings.size() + 1;
			_store( i , s );
			return i;
		}
		
		String get( int i ) {
			String s = _ids.get(Integer.valueOf(i));
			if ( s != null )
				return s;
			
			init( _cur );
			
			s = _ids.get(Integer.valueOf(i));
			if ( s != null )
				return s;
			throw new IllegalArgumentException( i + " is not a valid column id" );
		}
		
		void _store( int i , String s ) {
			_ids.put( Integer.valueOf(i) , s );
			_strings.put( s , Integer.valueOf(i) );
		}

		final Map<Integer,String> _ids = new HashMap<>();
		final Map<String,Integer> _strings = new HashMap<>();
	}

	@Override
	public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
		throw new UnsupportedOperationException();
	}

	@Override
	public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
		throw new UnsupportedOperationException();
	}
}