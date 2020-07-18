/**
DDBC - D DataBase Connector - abstraction layer for RDBMS access, with interface similar to JDBC. 

Source file ddbc/drivers/pgsqlddbc.d.
 DDBC library attempts to provide implementation independent interface to different databases.
 
 Set of supported RDBMSs can be extended by writing Drivers for particular DBs.
 
 JDBC documentation can be found here:
 $(LINK http://docs.oracle.com/javase/1.5.0/docs/api/java/sql/package-summary.html)$(BR)

 This module contains implementation POD utilities.
----
import ddbc;
import std.stdio;

// prepare database connectivity
auto conn = createConnection("sqlite:ddbctest.sqlite");
scope(exit) conn.close();
Statement stmt = conn.createStatement();
scope(exit) stmt.close();
// fill database with test data
stmt.executeUpdate("DROP TABLE IF EXISTS user");
stmt.executeUpdate("CREATE TABLE user (id INTEGER PRIMARY KEY, name VARCHAR(255) NOT NULL, flags int null)");
stmt.executeUpdate(`INSERT INTO user (id, name, flags) VALUES (1, "John", 5), (2, "Andrei", 2), (3, "Walter", 2), (4, "Rikki", 3), (5, "Iain", 0), (6, "Robert", 1)`);

// our POD object
struct User {
    long id;
    string name;
    int flags;
}

writeln("reading all user table rows");
foreach(e; stmt.select!User) {
    writeln("id:", e.id, " name:", e.name, " flags:", e.flags);
}

writeln("reading user table rows with where and order by");
foreach(e; stmt.select!User.where("id < 6").orderBy("name desc")) {
    writeln("id:", e.id, " name:", e.name, " flags:", e.flags);
}
----

 Copyright: Copyright 2013
 License:   $(LINK www.boost.org/LICENSE_1_0.txt, Boost License 1.0).
 Author:   Vadim Lopatin
*/
module ddbc.pods;

import std.algorithm;
import std.traits;
import std.meta;
import std.typecons;
import std.conv;
import std.datetime;
import std.string;
import std.variant;

static import std.ascii;

import ddbc.core;
import ddbc.attr;

alias Nullable!byte Byte;
alias Nullable!ubyte Ubyte;
alias Nullable!short Short;
alias Nullable!ushort Ushort;
alias Nullable!int Int;
alias Nullable!uint Uint;
alias Nullable!long Long;
alias Nullable!ulong Ulong;
alias Nullable!float Float;
alias Nullable!double Double;
alias Nullable!SysTime NullableSysTime;
alias Nullable!DateTime NullableDateTime;
alias Nullable!Date NullableDate;
alias Nullable!TimeOfDay NullableTimeOfDay;

/// Wrapper around string, to distinguish between Null and NotNull fields: string is NotNull, String is Null -- same interface as in Nullable
// Looks ugly, but I tried `typedef string String`, but it is deprecated; `alias string String` cannot be distinguished from just string. How to define String better?
struct String
{
    string _value;

    /**
    Returns $(D true) if and only if $(D this) is in the null state.
    */
    @property bool isNull() const pure nothrow @safe
    {
        return _value is null;
    }

    /**
    Forces $(D this) to the null state.
    */
    void nullify()
    {
        _value = null;
    }

    alias _value this;
}

enum PropertyMemberType : int {
    BOOL_TYPE,    // bool
    BYTE_TYPE,    // byte
    SHORT_TYPE,   // short
    INT_TYPE,     // int
    LONG_TYPE,    // long
    UBYTE_TYPE,   // ubyte
    USHORT_TYPE,  // ushort
    UINT_TYPE,    // uint
    ULONG_TYPE,   // ulong
    NULLABLE_BYTE_TYPE,  // Nullable!byte
    NULLABLE_SHORT_TYPE, // Nullable!short
    NULLABLE_INT_TYPE,   // Nullable!int
    NULLABLE_LONG_TYPE,  // Nullable!long
    NULLABLE_UBYTE_TYPE, // Nullable!ubyte
    NULLABLE_USHORT_TYPE,// Nullable!ushort
    NULLABLE_UINT_TYPE,  // Nullable!uint
    NULLABLE_ULONG_TYPE, // Nullable!ulong
    FLOAT_TYPE,   // float
    DOUBLE_TYPE,   // double
    NULLABLE_FLOAT_TYPE, // Nullable!float
    NULLABLE_DOUBLE_TYPE,// Nullable!double
    STRING_TYPE,   // string
    NULLABLE_STRING_TYPE,   // nullable string - String struct
    SYSTIME_TYPE,
    DATETIME_TYPE, // std.datetime.DateTime
    DATE_TYPE, // std.datetime.Date
    TIME_TYPE, // std.datetime.TimeOfDay
    NULLABLE_SYSTIME_TYPE,
    NULLABLE_DATETIME_TYPE, // Nullable!std.datetime.DateTime
    NULLABLE_DATE_TYPE, // Nullable!std.datetime.Date
    NULLABLE_TIME_TYPE, // Nullable!std.datetime.TimeOfDay
    BYTE_ARRAY_TYPE, // byte[]
    UBYTE_ARRAY_TYPE, // ubyte[]
    
    LONG_CONVERTIBLE_TYPE,                      // @convBy!Proxy for long
    ULONG_CONVERTIBLE_TYPE,                     // @convBy!Proxy for ulong
    NULLABLE_LONG_CONVERTIBLE_TYPE,             // @convBy!Proxy for Nullable!long
    NULLABLE_ULONG_CONVERTIBLE_TYPE,            // @convBy!Proxy for Nullable!ulong
    DOUBLE_CONVERTIBLE_TYPE,                    // @convBy!Proxy for double
    NULLABLE_DOUBLE_CONVERTIBLE_TYPE,           // @convBy!Proxy for Nullable!double
    STRING_CONVERTIBLE_TYPE,                    // @convBy!Proxy for string
    NULLABLE_STRING_CONVERTIBLE_TYPE,           // @convBy!Proxy for String
    BYTE_ARRAY_CONVERTIBLE_TYPE,                // @convBy!Proxy for byte[]
    UBYTE_ARRAY_CONVERTIBLE_TYPE,               // @convBy!Proxy for ubyte[]
}

/// converts camel case MyEntityName to my_entity_name
string camelCaseToUnderscoreDelimited(immutable string s) {
    string res;
    bool lastLower = false;
    static import std.ascii;

    foreach(ch; s) {
        if (ch >= 'A' && ch <= 'Z') {
            if (lastLower) {
                lastLower = false;
                res ~= "_";
            }
            res ~= std.ascii.toLower(ch);
        } else if (ch >= 'a' && ch <= 'z') {
            lastLower = true;
            res ~= ch;
        } else {
            res ~= ch;
        }
    }
    return res;
}

unittest {
    static assert(camelCaseToUnderscoreDelimited("User") == "user");
    static assert(camelCaseToUnderscoreDelimited("MegaTableName") == "mega_table_name");
}


template getFieldTraits(alias value) {
    static if (is(typeof(value) == function)) {
        alias ti = ReturnType!(typeof(value));
    } else {
        alias ti = typeof(value);
    }
    
    static if (isConvertible!(value, long)) {
        enum bool               isSupportedSimpleType       = true;
        enum PropertyMemberType propertyMemberType          = PropertyMemberType.LONG_CONVERTIBLE_TYPE;
        enum bool               columnTypeCanHoldNulls      = false;
        enum string             columnTypeKeyIsSetCode      = "(%s != 0)";
        enum string             columnTypeIsNullCode        = "(false)";
        enum string             columnTypeSetNullCode       = "long nv;";
        enum string             columnTypePropertyToVariant = "Variant(%s)";
        enum string             columnTypeDatasetReaderCode = "r.getLong(index)";
        enum bool               hasConvertibleType          = true;
        alias                   convertibleType             = long;
        enum string             convertibleTypeCode         = "long";
    } else static if (isConvertible!(value, ulong)) {
        enum bool               isSupportedSimpleType       = true;
        enum PropertyMemberType propertyMemberType          = PropertyMemberType.ULONG_CONVERTIBLE_TYPE;
        enum bool               columnTypeCanHoldNulls      = false;
        enum string             columnTypeKeyIsSetCode      = "(%s != 0)";
        enum string             columnTypeIsNullCode        = "(false)";
        enum string             columnTypeSetNullCode       = "ulong nv;";
        enum string             columnTypePropertyToVariant = "Variant(%s)";
        enum string             columnTypeDatasetReaderCode = "r.getUlong(index)";
        enum bool               hasConvertibleType          = true;
        alias                   convertibleType             = ulong;
        enum string             convertibleTypeCode         = "ulong";
    } else static if (isConvertible!(value, double)) {
        enum bool               isSupportedSimpleType       = true;
        enum PropertyMemberType propertyMemberType          = PropertyMemberType.DOUBLE_CONVERTIBLE_TYPE;
        enum bool               columnTypeCanHoldNulls      = false;
        enum string             columnTypeKeyIsSetCode      = "(%s != 0)";
        enum string             columnTypeIsNullCode        = "(false)";
        enum string             columnTypeSetNullCode       = "double nv;";
        enum string             columnTypePropertyToVariant = "Variant(%s)";
        enum string             columnTypeDatasetReaderCode = "r.getDouble(index)";
        enum bool               hasConvertibleType          = true;
        alias                   convertibleType             = double;
        enum string             convertibleTypeCode         = "double";
    } else static if (isConvertible!(value, Nullable!long)) {
        enum bool               isSupportedSimpleType       = true;
        enum PropertyMemberType propertyMemberType          = PropertyMemberType.NULLABLE_LONG_CONVERTIBLE_TYPE;
        enum bool               columnTypeCanHoldNulls      = true;
        enum string             columnTypeKeyIsSetCode      = "(!%s.isNull)";
        enum string             columnTypeIsNullCode        = "(%s.isNull)";
        enum string             columnTypeSetNullCode       = "Nullable!long nv;";
        enum string             columnTypePropertyToVariant = "(%s.isNull ? Variant(null) : Variant(%s.get()))";
        enum string             columnTypeDatasetReaderCode = "Nullable!long(r.getLong(index))";
        enum bool               hasConvertibleType          = true;
        alias                   convertibleType             = Nullable!long;
        enum string             convertibleTypeCode         = "Nullable!long";
    } else static if (isConvertible!(value, Nullable!ulong)) {
        enum bool               isSupportedSimpleType       = true;
        enum PropertyMemberType propertyMemberType          = PropertyMemberType.NULLABLE_ULONG_CONVERTIBLE_TYPE;
        enum bool               columnTypeCanHoldNulls      = true;
        enum string             columnTypeKeyIsSetCode      = "(!%s.isNull)";
        enum string             columnTypeIsNullCode        = "(%s.isNull)";
        enum string             columnTypeSetNullCode       = "Nullable!ulong nv;";
        enum string             columnTypePropertyToVariant = "(%s.isNull ? Variant(null) : Variant(%s.get()))";
        enum string             columnTypeDatasetReaderCode = "Nullable!ulong(r.getUlong(index))";
        enum bool               hasConvertibleType          = true;
        alias                   convertibleType             = Nullable!ulong;
        enum string             convertibleTypeCode         = "Nullable!ulong";
    } else static if (isConvertible!(value, Nullable!double)) {
        enum bool               isSupportedSimpleType       = true;
        enum PropertyMemberType propertyMemberType          = PropertyMemberType.NULLABLE_DOUBLE_CONVERTIBLE_TYPE;
        enum bool               columnTypeCanHoldNulls      = true;
        enum string             columnTypeKeyIsSetCode      = "(!%s.isNull)";
        enum string             columnTypeIsNullCode        = "(%s.isNull)";
        enum string             columnTypeSetNullCode       = "Nullable!double nv;";
        enum string             columnTypePropertyToVariant = "(%s.isNull ? Variant(null) : Variant(%s.get()))";
        enum string             columnTypeDatasetReaderCode = "Nullable!double(r.getDouble(index))";
        enum bool               hasConvertibleType          = true;
        alias                   convertibleType             = Nullable!double;
        enum string             convertibleTypeCode         = "Nullable!double";
    } else static if (isConvertible!(value, string)) {
        enum bool               isSupportedSimpleType       = true;
        enum PropertyMemberType propertyMemberType          = PropertyMemberType.STRING_CONVERTIBLE_TYPE;
        enum bool               columnTypeCanHoldNulls      = false;
        enum string             columnTypeKeyIsSetCode      = "(%s !is null)";
        enum string             columnTypeIsNullCode        = "(%s is null)";
        enum string             columnTypeSetNullCode       = "string nv;";
        enum string             columnTypePropertyToVariant = "Variant(%s)";
        enum string             columnTypeDatasetReaderCode = "r.getString(index)";
        enum bool               hasConvertibleType          = true;
        alias                   convertibleType             = string;
        enum string             convertibleTypeCode         = "string";
    } else static if (isConvertible!(value, String)) {
        enum bool               isSupportedSimpleType       = true;
        enum PropertyMemberType propertyMemberType          = PropertyMemberType.NULLABLE_STRING_CONVERTIBLE_TYPE;
        enum bool               columnTypeCanHoldNulls      = true;
        enum string             columnTypeKeyIsSetCode      = "(!%s.isNull)";
        enum string             columnTypeIsNullCode        = "(%s.isNull)";
        enum string             columnTypeSetNullCode       = "string nv;";
        enum string             columnTypePropertyToVariant = "Variant(%s)";
        enum string             columnTypeDatasetReaderCode = "r.getString(index)";
        enum bool               hasConvertibleType          = true;
        alias                   convertibleType             = String;
        enum string             convertibleTypeCode         = "String";
    } else static if (isConvertible!(value, byte[])) {
        enum bool               isSupportedSimpleType       = true;
        enum PropertyMemberType propertyMemberType          = PropertyMemberType.BYTE_ARRAY_CONVERTIBLE_TYPE;
        enum bool               columnTypeCanHoldNulls      = true;
        enum string             columnTypeKeyIsSetCode      = "(%s !is null)";
        enum string             columnTypeIsNullCode        = "(%s is null)";
        enum string             columnTypeSetNullCode       = "byte[] nv;";
        enum string             columnTypePropertyToVariant = "Variant(%s)";
        enum string             columnTypeDatasetReaderCode = "r.getBytes(index)";
        enum bool               hasConvertibleType          = true;
        alias                   convertibleType             = byte[];
        enum string             convertibleTypeCode         = "byte[]";
    } else static if (isConvertible!(value, ubyte[])) {
        enum bool               isSupportedSimpleType       = true;
        enum PropertyMemberType propertyMemberType          = PropertyMemberType.UBYTE_ARRAY_CONVERTIBLE_TYPE;
        enum bool               columnTypeCanHoldNulls      = true;
        enum string             columnTypeKeyIsSetCode      = "(%s !is null)";
        enum string             columnTypeIsNullCode        = "(%s is null)";
        enum string             columnTypeSetNullCode       = "ubyte[] nv;";
        enum string             columnTypePropertyToVariant = "Variant(%s)";
        enum string             columnTypeDatasetReaderCode = "r.getUbytes(index)";
        enum bool               hasConvertibleType          = true;
        alias                   convertibleType             = ubyte[];
        enum string             convertibleTypeCode         = "ubyte[]";
    } else static if (is(ti == bool)) {
        enum bool               isSupportedSimpleType       = true;
        enum PropertyMemberType propertyMemberType          = PropertyMemberType.BOOL_TYPE;
        enum bool               columnTypeCanHoldNulls      = false;
        enum string             columnTypeKeyIsSetCode      = "(%s != 0)";
        enum string             columnTypeIsNullCode        = "(false)";
        enum string             columnTypeSetNullCode       = "bool nv;";
        enum string             columnTypePropertyToVariant = "Variant(%s)";
        enum string             columnTypeDatasetReaderCode = "r.getBoolean(index)";
        enum bool               hasConvertibleType          = false;
    } else static if (is(ti == byte)) {
        enum bool               isSupportedSimpleType       = true;
        enum PropertyMemberType propertyMemberType          = PropertyMemberType.BYTE_TYPE;
        enum bool               columnTypeCanHoldNulls      = false;
        enum string             columnTypeKeyIsSetCode      = "(%s != 0)";
        enum string             columnTypeIsNullCode        = "(false)";
        enum string             columnTypeSetNullCode       = "byte nv;";
        enum string             columnTypePropertyToVariant = "Variant(%s)";
        enum string             columnTypeDatasetReaderCode = "r.getByte(index)";
        enum bool               hasConvertibleType          = false;
    } else static if (is(ti == short)) {
        enum bool               isSupportedSimpleType       = true;
        enum PropertyMemberType propertyMemberType          = PropertyMemberType.SHORT_TYPE;
        enum bool               columnTypeCanHoldNulls      = false;
        enum string             columnTypeKeyIsSetCode      = "(%s != 0)";
        enum string             columnTypeIsNullCode        = "(false)";
        enum string             columnTypeSetNullCode       = "ubyte nv;";
        enum string             columnTypePropertyToVariant = "Variant(%s)";
        enum string             columnTypeDatasetReaderCode = "r.getShort(index)";
        enum bool               hasConvertibleType          = false;
    } else static if (is(ti == int)) {
        enum bool               isSupportedSimpleType       = true;
        enum PropertyMemberType propertyMemberType          = PropertyMemberType.INT_TYPE;
        enum bool               columnTypeCanHoldNulls      = false;
        enum string             columnTypeKeyIsSetCode      = "(%s != 0)";
        enum string             columnTypeIsNullCode        = "(false)";
        enum string             columnTypeSetNullCode       = "int nv;";
        enum string             columnTypePropertyToVariant = "Variant(%s)";
        enum string             columnTypeDatasetReaderCode = "r.getInt(index)";
        enum bool               hasConvertibleType          = false;
    } else static if (is(ti == long)) {
        enum bool               isSupportedSimpleType       = true;
        enum PropertyMemberType propertyMemberType          = PropertyMemberType.LONG_TYPE;
        enum bool               columnTypeCanHoldNulls      = false;
        enum string             columnTypeKeyIsSetCode      = "(%s != 0)";
        enum string             columnTypeIsNullCode        = "(false)";
        enum string             columnTypeSetNullCode       = "long nv;";
        enum string             columnTypePropertyToVariant = "Variant(%s)";
        enum string             columnTypeDatasetReaderCode = "r.getLong(index)";
        enum bool               hasConvertibleType          = false;
    } else static if (is(ti == ubyte)) {
        enum bool               isSupportedSimpleType       = true;
        enum PropertyMemberType propertyMemberType          = PropertyMemberType.UBYTE_TYPE;
        enum bool               columnTypeCanHoldNulls      = false;
        enum string             columnTypeKeyIsSetCode      = "(%s != 0)";
        enum string             columnTypeIsNullCode        = "(false)";
        enum string             columnTypeSetNullCode       = "ubyte nv;";
        enum string             columnTypePropertyToVariant = "Variant(%s)";
        enum string             columnTypeDatasetReaderCode = "r.getUbyte(index)";
        enum bool               hasConvertibleType          = false;
    } else static if (is(ti == ushort)) {
        enum bool               isSupportedSimpleType       = true;
        enum PropertyMemberType propertyMemberType          = PropertyMemberType.USHORT_TYPE;
        enum bool               columnTypeCanHoldNulls      = false;
        enum string             columnTypeKeyIsSetCode      = "(%s != 0)";
        enum string             columnTypeIsNullCode        = "(false)";
        enum string             columnTypeSetNullCode       = "ushort nv;";
        enum string             columnTypePropertyToVariant = "Variant(%s)";
        enum string             columnTypeDatasetReaderCode = "r.getUshort(index)";
        enum bool               hasConvertibleType          = false;
    } else static if (is(ti == uint)) {
        enum bool               isSupportedSimpleType       = true;
        enum PropertyMemberType propertyMemberType          = PropertyMemberType.UINT_TYPE;
        enum bool               columnTypeCanHoldNulls      = false;
        enum string             columnTypeKeyIsSetCode      = "(%s != 0)";
        enum string             columnTypeIsNullCode        = "(false)";
        enum string             columnTypeSetNullCode       = "uint nv;";
        enum string             columnTypePropertyToVariant = "Variant(%s)";
        enum string             columnTypeDatasetReaderCode = "r.getUint(index)";
        enum bool               hasConvertibleType          = false;
    } else static if (is(ti == ulong)) {
        enum bool               isSupportedSimpleType       = true;
        enum PropertyMemberType propertyMemberType          = PropertyMemberType.ULONG_TYPE;
        enum bool               columnTypeCanHoldNulls      = false;
        enum string             columnTypeKeyIsSetCode      = "(%s != 0)";
        enum string             columnTypeIsNullCode        = "(false)";
        enum string             columnTypeSetNullCode       = "ulong nv;";
        enum string             columnTypePropertyToVariant = "Variant(%s)";
        enum string             columnTypeDatasetReaderCode = "r.getUlong(index)";
        enum bool               hasConvertibleType          = false;
    } else static if (is(ti == float)) {
        enum bool               isSupportedSimpleType       = true;
        enum PropertyMemberType propertyMemberType          = PropertyMemberType.FLOAT_TYPE;
        enum bool               columnTypeCanHoldNulls      = false;
        enum string             columnTypeKeyIsSetCode      = "(%s != 0)";
        enum string             columnTypeIsNullCode        = "(false)";
        enum string             columnTypeSetNullCode       = "float nv;";
        enum string             columnTypePropertyToVariant = "Variant(%s)";
        enum string             columnTypeDatasetReaderCode = "r.getFloat(index)";
        enum bool               hasConvertibleType          = false;
    } else static if (is(ti == double)) {
        enum bool               isSupportedSimpleType       = true;
        enum PropertyMemberType propertyMemberType          = PropertyMemberType.DOUBLE_TYPE;
        enum bool               columnTypeCanHoldNulls      = false;
        enum string             columnTypeKeyIsSetCode      = "(%s != 0)";
        enum string             columnTypeIsNullCode        = "(false)";
        enum string             columnTypeSetNullCode       = "double nv;";
        enum string             columnTypePropertyToVariant = "Variant(%s)";
        enum string             columnTypeDatasetReaderCode = "r.getDouble(index)";
        enum bool               hasConvertibleType          = false;
    } else static if (is(ti == Nullable!byte)) {
        enum bool               isSupportedSimpleType       = true;
        enum PropertyMemberType propertyMemberType          = PropertyMemberType.NULLABLE_BYTE_TYPE;
        enum bool               columnTypeCanHoldNulls      = true;
        enum string             columnTypeKeyIsSetCode      = "(!%s.isNull)";
        enum string             columnTypeIsNullCode        = "(%s.isNull)";
        enum string             columnTypeSetNullCode       = "Nullable!byte nv;";
        enum string             columnTypePropertyToVariant = "(%s.isNull ? Variant(null) : Variant(%s.get()))";
        enum string             columnTypeDatasetReaderCode = "Nullable!byte(r.getByte(index))";
        enum bool               hasConvertibleType          = false;
    } else static if (is(ti == Nullable!short)) {
        enum bool               isSupportedSimpleType       = true;
        enum PropertyMemberType propertyMemberType          = PropertyMemberType.NULLABLE_SHORT_TYPE;
        enum bool               columnTypeCanHoldNulls      = true;
        enum string             columnTypeKeyIsSetCode      = "(!%s.isNull)";
        enum string             columnTypeIsNullCode        = "(%s.isNull)";
        enum string             columnTypeSetNullCode       = "Nullable!short nv;";
        enum string             columnTypePropertyToVariant = "(%s.isNull ? Variant(null) : Variant(%s.get()))";
        enum string             columnTypeDatasetReaderCode = "Nullable!short(r.getShort(index))";
        enum bool               hasConvertibleType          = false;
    } else static if (is(ti == Nullable!int)) {
        enum bool               isSupportedSimpleType       = true;
        enum PropertyMemberType propertyMemberType          = PropertyMemberType.NULLABLE_INT_TYPE;
        enum bool               columnTypeCanHoldNulls      = true;
        enum string             columnTypeKeyIsSetCode      = "(!%s.isNull)";
        enum string             columnTypeIsNullCode        = "(%s.isNull)";
        enum string             columnTypeSetNullCode       = "Nullable!int nv;";
        enum string             columnTypePropertyToVariant = "(%s.isNull ? Variant(null) : Variant(%s.get()))";
        enum string             columnTypeDatasetReaderCode = "Nullable!int(r.getInt(index))";
        enum bool               hasConvertibleType          = false;
    } else static if (is(ti == Nullable!long)) {
        enum bool               isSupportedSimpleType       = true;
        enum PropertyMemberType propertyMemberType          = PropertyMemberType.NULLABLE_LONG_TYPE;
        enum bool               columnTypeCanHoldNulls      = true;
        enum string             columnTypeKeyIsSetCode      = "(!%s.isNull)";
        enum string             columnTypeIsNullCode        = "(%s.isNull)";
        enum string             columnTypeSetNullCode       = "Nullable!long nv;";
        enum string             columnTypePropertyToVariant = "(%s.isNull ? Variant(null) : Variant(%s.get()))";
        enum string             columnTypeDatasetReaderCode = "Nullable!long(r.getLong(index))";
        enum bool               hasConvertibleType          = false;
    } else static if (is(ti == Nullable!ubyte)) {
        enum bool               isSupportedSimpleType       = true;
        enum PropertyMemberType propertyMemberType          = PropertyMemberType.NULLABLE_UBYTE_TYPE;
        enum bool               columnTypeCanHoldNulls      = true;
        enum string             columnTypeKeyIsSetCode      = "(!%s.isNull)";
        enum string             columnTypeIsNullCode        = "(%s.isNull)";
        enum string             columnTypeSetNullCode       = "Nullable!ubyte nv;";
        enum string             columnTypePropertyToVariant = "(%s.isNull ? Variant(null) : Variant(%s.get()))";
        enum string             columnTypeDatasetReaderCode = "Nullable!ubyte(r.getUbyte(index))";
        enum bool               hasConvertibleType          = false;
    } else static if (is(ti == Nullable!ushort)) {
        enum bool               isSupportedSimpleType       = true;
        enum PropertyMemberType propertyMemberType          = PropertyMemberType.NULLABLE_USHORT_TYPE;
        enum bool               columnTypeCanHoldNulls      = true;
        enum string             columnTypeKeyIsSetCode      = "(!%s.isNull)";
        enum string             columnTypeIsNullCode        = "(%s.isNull)";
        enum string             columnTypeSetNullCode       = "Nullable!ushort nv;";
        enum string             columnTypePropertyToVariant = "(%s.isNull ? Variant(null) : Variant(%s.get()))";
        enum string             columnTypeDatasetReaderCode = "Nullable!ushort(r.getUshort(index))";
        enum bool               hasConvertibleType          = false;
    } else static if (is(ti == Nullable!uint)) {
        enum bool               isSupportedSimpleType       = true;
        enum PropertyMemberType propertyMemberType          = PropertyMemberType.NULLABLE_UINT_TYPE;
        enum bool               columnTypeCanHoldNulls      = true;
        enum string             columnTypeKeyIsSetCode      = "(!%s.isNull)";
        enum string             columnTypeIsNullCode        = "(%s.isNull)";
        enum string             columnTypeSetNullCode       = "Nullable!uint nv;";
        enum string             columnTypePropertyToVariant = "(%s.isNull ? Variant(null) : Variant(%s.get()))";
        enum string             columnTypeDatasetReaderCode = "Nullable!uint(r.getUint(index))";
        enum bool               hasConvertibleType          = false;
    } else static if (is(ti == Nullable!ulong)) {
        enum bool               isSupportedSimpleType       = true;
        enum PropertyMemberType propertyMemberType          = PropertyMemberType.NULLABLE_ULONG_TYPE;
        enum bool               columnTypeCanHoldNulls      = true;
        enum string             columnTypeKeyIsSetCode      = "(!%s.isNull)";
        enum string             columnTypeIsNullCode        = "(%s.isNull)";
        enum string             columnTypeSetNullCode       = "Nullable!ulong nv;";
        enum string             columnTypePropertyToVariant = "(%s.isNull ? Variant(null) : Variant(%s.get()))";
        enum string             columnTypeDatasetReaderCode = "Nullable!ulong(r.getUlong(index))";
        enum bool               hasConvertibleType          = false;
    } else static if (is(ti == Nullable!float)) {
        enum bool               isSupportedSimpleType       = true;
        enum PropertyMemberType propertyMemberType          = PropertyMemberType.NULLABLE_FLOAT_TYPE;
        enum bool               columnTypeCanHoldNulls      = true;
        enum string             columnTypeKeyIsSetCode      = "(!%s.isNull)";
        enum string             columnTypeIsNullCode        = "(%s.isNull)";
        enum string             columnTypeSetNullCode       = "Nullable!float nv;";
        enum string             columnTypePropertyToVariant = "(%s.isNull ? Variant(null) : Variant(%s.get()))";
        enum string             columnTypeDatasetReaderCode = "Nullable!float(r.getFloat(index))";
        enum bool               hasConvertibleType          = false;
    } else static if (is(ti == Nullable!double)) {
        enum bool               isSupportedSimpleType       = true;
        enum PropertyMemberType propertyMemberType          = PropertyMemberType.NULLABLE_DOUBLE_TYPE;
        enum bool               columnTypeCanHoldNulls      = true;
        enum string             columnTypeKeyIsSetCode      = "(!%s.isNull)";
        enum string             columnTypeIsNullCode        = "(%s.isNull)";
        enum string             columnTypeSetNullCode       = "Nullable!double nv;";
        enum string             columnTypePropertyToVariant = "(%s.isNull ? Variant(null) : Variant(%s.get()))";
        enum string             columnTypeDatasetReaderCode = "Nullable!double(r.getDouble(index))";
        enum bool               hasConvertibleType          = false;
    } else static if (is(ti == string)) {
        enum bool               isSupportedSimpleType       = true;
        enum PropertyMemberType propertyMemberType          = PropertyMemberType.STRING_TYPE;
        enum bool               columnTypeCanHoldNulls      = false;
        enum string             columnTypeKeyIsSetCode      = "(%s !is null)";
        enum string             columnTypeIsNullCode        = "(%s is null)";
        enum string             columnTypeSetNullCode       = "string nv;";
        enum string             columnTypePropertyToVariant = "Variant(%s)";
        enum string             columnTypeDatasetReaderCode = "r.getString(index)";
        enum bool               hasConvertibleType          = false;
    } else static if (is(ti == String)) {
        enum bool               isSupportedSimpleType       = true;
        enum PropertyMemberType propertyMemberType          = PropertyMemberType.NULLABLE_STRING_TYPE;
        enum bool               columnTypeCanHoldNulls      = true;
        enum string             columnTypeKeyIsSetCode      = "(!%s.isNull)";
        enum string             columnTypeIsNullCode        = "(%s is null)";
        enum string             columnTypeSetNullCode       = "string nv;";
        enum string             columnTypePropertyToVariant = "Variant(%s)";
        enum string             columnTypeDatasetReaderCode = "r.getString(index)";
        enum bool               hasConvertibleType          = false;
    } else static if (is(ti == SysTime)) {
        enum bool               isSupportedSimpleType       = true;
        enum PropertyMemberType propertyMemberType          = PropertyMemberType.SYSTIME_TYPE;
        enum bool               columnTypeCanHoldNulls      = false;
        enum string             columnTypeKeyIsSetCode      = "(%s != SysTime())";
        enum string             columnTypeIsNullCode        = "(false)";
        enum string             columnTypeSetNullCode       = "SysTime nv;";
        enum string             columnTypePropertyToVariant = "Variant(%s)";
        enum string             columnTypeDatasetReaderCode = "r.getSysTime(index)";
        enum bool               hasConvertibleType          = false;
    } else static if (is(ti == DateTime)) {
        enum bool               isSupportedSimpleType       = true;
        enum PropertyMemberType propertyMemberType          = PropertyMemberType.DATETIME_TYPE;
        enum bool               columnTypeCanHoldNulls      = false;
        enum string             columnTypeKeyIsSetCode      = "(%s != DateTime())";
        enum string             columnTypeIsNullCode        = "(false)";
        enum string             columnTypeSetNullCode       = "DateTime nv;";
        enum string             columnTypePropertyToVariant = "Variant(%s)";
        enum string             columnTypeDatasetReaderCode = "r.getDateTime(index)";
        enum bool               hasConvertibleType          = false;
    } else static if (is(ti == Date)) {
        enum bool               isSupportedSimpleType       = true;
        enum PropertyMemberType propertyMemberType          = PropertyMemberType.DATE_TYPE;
        enum bool               columnTypeCanHoldNulls      = false;
        enum string             columnTypeKeyIsSetCode      = "(%s != Date())";
        enum string             columnTypeIsNullCode        = "(false)";
        enum string             columnTypeSetNullCode       = "Date nv;";
        enum string             columnTypePropertyToVariant = "Variant(%s)";
        enum string             columnTypeDatasetReaderCode = "r.getDate(index)";
        enum bool               hasConvertibleType          = false;
    } else static if (is(ti == TimeOfDay)) {
        enum bool               isSupportedSimpleType       = true;
        enum PropertyMemberType propertyMemberType          = PropertyMemberType.TIME_TYPE;
        enum bool               columnTypeCanHoldNulls      = false;
        enum string             columnTypeKeyIsSetCode      = "(%s != TimeOfDay())";
        enum string             columnTypeIsNullCode        = "(false)";
        enum string             columnTypeSetNullCode       = "TimeOfDay nv;";
        enum string             columnTypePropertyToVariant = "Variant(%s)";
        enum string             columnTypeDatasetReaderCode = "r.getTime(index)";
        enum bool               hasConvertibleType          = false;
    } else static if (is(ti == Nullable!SysTime)) {
        enum bool               isSupportedSimpleType       = true;
        enum PropertyMemberType propertyMemberType          = PropertyMemberType.NULLABLE_SYSTIME_TYPE;
        enum bool               columnTypeCanHoldNulls      = true;
        enum string             columnTypeKeyIsSetCode      = "(!%s.isNull)";
        enum string             columnTypeIsNullCode        = "(%s.isNull)";
        enum string             columnTypeSetNullCode       = "Nullable!SysTime nv;";
        enum string             columnTypePropertyToVariant = "(%s.isNull ? Variant(null) : Variant(%s.get()))";
        enum string             columnTypeDatasetReaderCode = "Nullable!SysTime(r.getSysTime(index))";
        enum bool               hasConvertibleType          = false;
    } else static if (is(ti == Nullable!DateTime)) {
        enum bool               isSupportedSimpleType       = true;
        enum PropertyMemberType propertyMemberType          = PropertyMemberType.NULLABLE_DATETIME_TYPE;
        enum bool               columnTypeCanHoldNulls      = true;
        enum string             columnTypeKeyIsSetCode      = "(!%s.isNull)";
        enum string             columnTypeIsNullCode        = "(%s.isNull)";
        enum string             columnTypeSetNullCode       = "Nullable!DateTime nv;";
        enum string             columnTypePropertyToVariant = "(%s.isNull ? Variant(null) : Variant(%s.get()))";
        enum string             columnTypeDatasetReaderCode = "Nullable!DateTime(r.getDateTime(index))";
        enum bool               hasConvertibleType          = false;
    } else static if (is(ti == Nullable!Date)) {
        enum bool               isSupportedSimpleType       = true;
        enum PropertyMemberType propertyMemberType          = PropertyMemberType.NULLABLE_DATE_TYPE;
        enum bool               columnTypeCanHoldNulls      = true;
        enum string             columnTypeKeyIsSetCode      = "(!%s.isNull)";
        enum string             columnTypeIsNullCode        = "(%s.isNull)";
        enum string             columnTypeSetNullCode       = "Nullable!Date nv;";
        enum string             columnTypePropertyToVariant = "(%s.isNull ? Variant(null) : Variant(%s.get()))";
        enum string             columnTypeDatasetReaderCode = "Nullable!Date(r.getDate(index))";
        enum bool               hasConvertibleType          = false;
    } else static if (is(ti == Nullable!TimeOfDay)) {
        enum bool               isSupportedSimpleType       = true;
        enum PropertyMemberType propertyMemberType          = PropertyMemberType.NULLABLE_TIME_TYPE;
        enum bool               columnTypeCanHoldNulls      = true;
        enum string             columnTypeKeyIsSetCode      = "(!%s.isNull)";
        enum string             columnTypeIsNullCode        = "(%s.isNull)";
        enum string             columnTypeSetNullCode       = "Nullable!TimeOfDay nv;";
        enum string             columnTypePropertyToVariant = "(%s.isNull ? Variant(null) : Variant(%s.get()))";
        enum string             columnTypeDatasetReaderCode = "Nullable!TimeOfDay(r.getTime(index))";
        enum bool               hasConvertibleType          = false;
    } else static if (is(ti == byte[])) {
        enum bool               isSupportedSimpleType       = true;
        enum PropertyMemberType propertyMemberType          = PropertyMemberType.BYTE_ARRAY_TYPE;
        enum bool               columnTypeCanHoldNulls      = true;
        enum string             columnTypeKeyIsSetCode      = "(%s !is null)";
        enum string             columnTypeIsNullCode        = "(%s is null)";
        enum string             columnTypeSetNullCode       = "byte[] nv;";
        enum string             columnTypePropertyToVariant = "Variant(%s)";
        enum string             columnTypeDatasetReaderCode = "r.getBytes(index)";
        enum bool               hasConvertibleType          = false;
    } else static if (is(ti == ubyte[])) {
        enum bool               isSupportedSimpleType       = true;
        enum PropertyMemberType propertyMemberType          = PropertyMemberType.UBYTE_ARRAY_TYPE;
        enum bool               columnTypeCanHoldNulls      = true;
        enum string             columnTypeKeyIsSetCode      = "(%s !is null)";
        enum string             columnTypeIsNullCode        = "(%s is null)";
        enum string             columnTypeSetNullCode       = "ubyte[] nv;";
        enum string             columnTypePropertyToVariant = "Variant(%s)";
        enum string             columnTypeDatasetReaderCode = "r.getUbytes(index)";
        enum bool               hasConvertibleType          = false;
    } else static if (true) {
        enum bool               isSupportedSimpleType       = false;
        enum PropertyMemberType propertyMemberType          = PropertyMemberType.init; // dummy
        enum bool               columnTypeCanHoldNulls      = false; // dummy
        enum bool               hasConvertibleType          = false; // dummy
    }
}

enum hasConvertibleType(alias value) = getFieldTraits!value.hasConvertibleType;
alias getConvertibleType(alias value) = getFieldTraits!value.convertibleType;
alias getConvertibleType(T, string m) = getFieldTraits!(__traits(getMember, T, m)).convertibleType;
enum getConvertibleTypeCode(alias value) = getFieldTraits!value.convertibleTypeCode;

enum bool isSupportedSimpleType(alias value) = getFieldTraits!value.isSupportedSimpleType;
enum bool isSupportedSimpleType(T, string m) = isSupportedSimpleType!(__traits(getMember, T, m));

enum PropertyMemberType getPropertyType(alias value) = getFieldTraits!value.propertyMemberType;
enum PropertyMemberType getPropertyMemberType(T, string m) = getFieldTraits!(__traits(getMember, T, m)).propertyMemberType;

string getPropertyReadCode(T, string m)() {
    return "entity." ~ m;
}

string getPropertyReadCode(alias T)() {
    return "entity." ~ T.stringof;
}

enum bool columnTypeCanHoldNulls(alias value) = getFieldTraits!value.columnTypeCanHoldNulls;
enum bool columnTypeCanHoldNulls(T, string m) = getFieldTraits!(__traits(getMember, T, m)).columnTypeCanHoldNulls;

enum bool isColumnTypeNullableByDefault(T, string m) = columnTypeCanHoldNulls!(T,m);


enum string getColumnTypeKeyIsSetCode(T, string m) = getFieldTraits!(__traits(getMember, T, m)).columnTypeKeyIsSetCode;
enum string getColumnTypeIsNullCode(T, string m)   = getFieldTraits!(__traits(getMember, T, m)).columnTypeIsNullCode;


enum string getColumnTypeDatasetReadCode(T, string m) = getFieldTraits!(__traits(getMember, T, m)).columnTypeDatasetReaderCode;
enum string getVarTypeDatasetReadCode(alias arg)      = getFieldTraits!arg.columnTypeDatasetReaderCode;

string getPropertyWriteCode(T, string m)() {
    //immutable PropertyMemberKind kind = getPropertyMemberKind!(T, m)();
    immutable string nullValueCode = getFieldTraits!(__traits(getMember, T, m)).columnTypeSetNullCode;
    immutable string datasetReader = "(!r.isNull(index) ? " ~ getColumnTypeDatasetReadCode!(T, m) ~ " : nv)";
    static if (hasConvertibleType!(__traits(getMember, T, m))) {
        return nullValueCode ~ "convertFrom!(entity." ~ m ~ ")("~ datasetReader ~ ", entity." ~ m ~ ");";
    } else {
        return nullValueCode ~ "entity." ~ m ~ " = " ~ datasetReader ~ ";";
    }
}

string getArgsWriteCode(alias value)() {
    alias T = typeof(value);
    //immutable PropertyMemberKind kind = getPropertyMemberKind!(T, m)();
    immutable string nullValueCode = getFieldTraits!value.columnTypeSetNullCode;
    immutable string datasetReader = "(!r.isNull(index) ? " ~ getVarTypeDatasetReadCode!value ~ " : nv)";
    static if (hasConvertibleType!value) {
        return nullValueCode ~ "convertFrom!(a)(" ~ datasetReader ~ ", a);";
    } else {
        return nullValueCode ~ "a = " ~ datasetReader ~ ";";
    }
}

template isValidFieldMember(T, string m) {
    static if (__traits(compiles, (typeof(__traits(getMember, T, m))))){
        // skip non-public members
        static if (__traits(getProtection, __traits(getMember, T, m)) == "public") {
            static if (isSupportedSimpleType!(T, m) && !hasIgnore!(__traits(getMember, T, m))) {
                enum bool isValidFieldMember = true;
            } else {
                enum bool isValidFieldMember = false;
            }
        } else {
            enum bool isValidFieldMember = false;
        }
    } else {
        enum bool isValidFieldMember = false;
    }
}

template isSupportedDataType(T) {
    enum bool isNotIgnore(string m) = !hasIgnore!(__traits(getMember, T, m));
    alias nonIgnoredMembers = Filter!(isNotIgnore, FieldNameTuple!T);
    enum bool isSupportedSimpleTypeMember(string m) = isSupportedSimpleType!(T, m);
    enum bool isSupportedDataType = allSatisfy!(isSupportedSimpleTypeMember, nonIgnoredMembers);
}

unittest {
    struct Data1 {int a; int b;}
    struct Data2 {int a; int* b;}
    static assert( isSupportedDataType!Data1);
    static assert(!isSupportedDataType!Data2);
}

template getColumnNameForMember(T, string m) {
    static if (hasColumnName!(__traits(getMember, T, m))) {
        enum string getColumnNameForMember = getColumnName!(__traits(getMember, T, m));
    } else {
        enum string getColumnNameForMember = m;
    }
}

template getMemberNamesAt(T, alias uda) {
    alias symbolsByUDA = getSymbolsByUDA!(T, uda);
    alias symbolsNameByUDA = staticMap!(getMemberNamesAt_getName, symbolsByUDA);
    alias isValidMember(string m) = isValidFieldMember!(T, m);
    alias getMemberNamesAt = Filter!(isValidMember, symbolsNameByUDA);
}
enum string getMemberNamesAt_getName(alias member) = member.stringof;

template getColumnNamesAt(T, alias uda) {
    enum string getColumnNameFromMember(string m) = getColumnNameForMember!(T, m);
    alias getColumnNamesAt = staticMap!(getColumnNameFromMember, getMemberNamesAt!(T, uda));
}

unittest {
    enum { x, y }
    struct Data1 {@x int a; @x @y @columnName("xxx") int b; @y @ignore string c;}
    static assert(getColumnNamesAt!(Data1, x).length == 2);
    static assert(getColumnNamesAt!(Data1, x)[0] == "a");
    static assert(getColumnNamesAt!(Data1, x)[1] == "xxx");
    static assert(getColumnNamesAt!(Data1, y).length == 1);
    static assert(getColumnNamesAt!(Data1, y)[0] == "xxx");
}


/// returns array of field names
string[] getColumnNamesForType(T)()  if (isSupportedDataType!T) {
    string[] res;
    foreach(m; FieldNameTuple!T) {
        static if (isValidFieldMember!(T, m)) {
            res ~= getColumnNameForMember!(T, m);
        }
    }
    return res;
}

string getColumnReadCode(T, string m)() {
    return "{" ~ getPropertyWriteCode!(T,m)() ~ "index++;}\n";
}

string getAllColumnsReadCode(T)() {
    string res = "int index = 1;\n";
    foreach(m; FieldNameTuple!T) {
        static if (isValidFieldMember!(T, m)) {
            res ~= getColumnReadCode!(T, m);
        }
    }
    return res;
}

string getAllColumnsReadCode(T, fieldList...)() {
    string res = "int index = 1;\n";
    foreach(m; fieldList) {
        res ~= getColumnReadCode!(T, m);
    }
    return res;
}

unittest {
    struct User1 {
        long id;
        string name;
        int flags;
    }
    //pragma(msg, "nullValueCode = " ~ getFieldTraits!(User1.id).columnTypeSetNullCode);
    //pragma(msg, "datasetReader = " ~ getColumnTypeDatasetReadCode!(User1, "id"));
    //pragma(msg, "getPropertyWriteCode: " ~ getPropertyWriteCode!(User1, "id"));
    //pragma(msg, "getAllColumnsReadCode:\n" ~ getAllColumnsReadCode!(User1));
    //static assert(getPropertyWriteCode!(User1, "id") == "long nv = 0;entity.id = (!r.isNull(index) ? r.getLong(index) : nv);");
}

unittest {
    struct User1 {
        long id;
        string name;
        static struct Proxy {
            string to(int x) { static import std.conv; return std.conv.to!string(x); }
            int from(string x) { static import std.conv; return std.conv.to!int(x); }
        }
        @convBy!Proxy
        int flags;
    }
    static assert(getPropertyMemberType!(User1, "id") == PropertyMemberType.LONG_TYPE);
    static assert(getPropertyMemberType!(User1, "name") == PropertyMemberType.STRING_TYPE);
    static assert(getPropertyMemberType!(User1, "flags") == PropertyMemberType.STRING_CONVERTIBLE_TYPE);
    //pragma(msg, "getPropertyMemberType unit test passed");
}



/// returns table name for struct type
string getTableNameForType(T)() if (isSupportedDataType!T) {
    static if (hasTableName!T) {
        return getTableName!T;
    } else {
        return camelCaseToUnderscoreDelimited(T.stringof);
    }
}

unittest {
    struct User1 {
        long id;
        string name;
        int flags;
    }
    static assert(getTableNameForType!User1() == "user1");
    
    @tableName("user_2")
    struct User2 {
        long id;
        string name;
        int flags;
    }
    static assert(getTableNameForType!User2() == "user_2");
}

/// returns "SELECT <field list> FROM <table name>"
string generateSelectSQL(T)() {
    return "SELECT " ~ join(getColumnNamesForType!(T)(), ",") ~ " FROM " ~ getTableNameForType!(T)();
}

unittest {
    struct User1 {
        long id;
        string name;
        int flags;
    }
    static assert(generateSelectSQL!User1() == "SELECT id,name,flags FROM user1");
    
    @tableName("user_2")
    struct User2 {
        long id;
        
        @columnName("name_of_user")
        string name;
        
        static struct Proxy {
            string to(int x) { static import std.conv; return std.conv.to!string(x); }
            int from(string x) { static import std.conv; return std.conv.to!int(x); }
        }
        @convBy!Proxy
        int flags;
    }
    static assert(generateSelectSQL!User2() == "SELECT id,name_of_user,flags FROM user_2");
}

string joinFieldList(T, fieldList...)() {
    string res;
    static foreach(f; fieldList) {
        if (res.length)
            res ~= ",";
        res ~= getColumnNameForMember!(T, f);
    }
    return res;
}

/// returns "SELECT <field list> FROM <table name>"
string generateSelectSQL(T, fieldList...)() {
    string res = "SELECT ";
    res ~= joinFieldList!(T, fieldList);
    res ~= " FROM " ~ getTableNameForType!(T)();
    return res;
}

unittest {
    //pragma(msg, "column names: " ~ join(getColumnNamesForType!(User)(), ","));
    //pragma(msg, "select SQL: " ~ generateSelectSQL!(User)());
}

/// returns "SELECT <field list> FROM <table name>"
string generateSelectForGetSQL(T)() {
    string res = generateSelectSQL!T();
    res ~= " WHERE id=";
    return res;
}

string generateSelectForGetSQLWithFilter(T)() {
  string res = generateSelectSQL!T();
  res ~= " WHERE ";
  return res;
}

T get(T)(Statement stmt, long id) {
  T entity;
  static immutable getSQL = generateSelectForGetSQL!T();
  ResultSet r;
  r = stmt.executeQuery(getSQL ~ to!string(id));
  r.next();
  mixin(getAllColumnsReadCode!T());
  return entity;
}

T get(T)(Statement stmt, string filter) {
  T entity;
  static immutable getSQL = generateSelectForGetSQLWithFilter!T();
  ResultSet r;
  r = stmt.executeQuery(getSQL ~ filter);
  r.next();
  mixin(getAllColumnsReadCode!T());
  return entity;
}

string getColumnTypeDatasetReadCodeByName(T, string m)() {
    PropertyMemberType pmt = getPropertyMemberType!(T,m)();
    final switch(pmt) with (PropertyMemberType) {
        case BOOL_TYPE:
            return `r.getBoolean("` ~ m ~ `")`;
        case BYTE_TYPE:
            return `r.getByte("` ~ m ~ `")`;
        case SHORT_TYPE:
            return `r.getShort("` ~ m ~ `")`;
        case INT_TYPE:
            return `r.getInt("` ~ m ~ `")`;
        case LONG_TYPE:
            return `r.getLong("` ~ m ~ `")`;
        case UBYTE_TYPE:
            return `r.getUbyte("` ~ m ~ `")`;
        case USHORT_TYPE:
            return `r.getUshort("` ~ m ~ `")`;
        case UINT_TYPE:
            return `r.getUint("` ~ m ~ `")`;
        case ULONG_TYPE:
            return `r.getUlong("` ~ m ~ `")`;
        case FLOAT_TYPE:
            return `r.getFloat("` ~ m ~ `")`;
        case DOUBLE_TYPE:
            return `r.getDouble("` ~ m ~ `")`;
        case STRING_TYPE:
            return `r.getString("` ~ m ~ `")`;
        case DATE_TYPE:
            return `r.getDate("` ~ m ~ `")`;
        case TIME_TYPE:
            return `r.getTime("` ~ m ~ `")`;
        case SYSTIME_TYPE:
            return `r.getSysTime("` ~ m ~ `")`;
        case DATETIME_TYPE:
            return `r.getDateTime("` ~ m ~ `")`;
        case BYTE_ARRAY_TYPE:
            return `r.getBytes("` ~ m ~ `")`;
        case UBYTE_ARRAY_TYPE:
            return `r.getUbytes("` ~ m ~ `")`;
        case NULLABLE_BYTE_TYPE:
            return `Nullable!byte(r.getByte("` ~ m ~ `"))`;
        case NULLABLE_SHORT_TYPE:
            return `Nullable!short(r.getShort("` ~ m ~ `"))`;
        case NULLABLE_INT_TYPE:
            return `Nullable!int(r.getInt("` ~ m ~ `"))`;
        case NULLABLE_LONG_TYPE:
            return `Nullable!long(r.getLong("` ~ m ~ `"))`;
        case NULLABLE_UBYTE_TYPE:
            return `Nullable!ubyte(r.getUbyte("` ~ m ~ `"))`;
        case NULLABLE_USHORT_TYPE:
            return `Nullable!ushort(r.getUshort("` ~ m ~ `"))`;
        case NULLABLE_UINT_TYPE:
            return `Nullable!uint(r.getUint("` ~ m ~ `"))`;
        case NULLABLE_ULONG_TYPE:
            return `Nullable!ulong(r.getUlong("` ~ m ~ `"))`;
        case NULLABLE_FLOAT_TYPE:
            return `Nullable!float(r.getFloat("` ~ m ~ `"))`;
        case NULLABLE_DOUBLE_TYPE:
            return `Nullable!double(r.getDouble("` ~ m ~ `"))`;
        case NULLABLE_STRING_TYPE:
            return `r.getString("` ~ m ~ `")`;
        case NULLABLE_DATE_TYPE:
            return `Nullable!Date(r.getDate("` ~ m ~ `"))`;
        case NULLABLE_TIME_TYPE:
            return `Nullable!Time(r.getTime("` ~ m ~ `"))`;
        case NULLABLE_SYSTIME_TYPE:
            return `Nullable!SysTime(r.getSysTime("` ~ m ~ `"))`;
        case NULLABLE_DATETIME_TYPE:
            return `Nullable!DateTime(r.getDateTime("` ~ m ~ `"))`;
    
        case LONG_CONVERTIBLE_TYPE:
            return `r.getLong("` ~ m ~ `")`;
        case ULONG_CONVERTIBLE_TYPE:
            return `r.getUlong("` ~ m ~ `")`;
        case NULLABLE_LONG_CONVERTIBLE_TYPE:
            return `Nullable!long(r.getLong("` ~ m ~ `"))`;
        case NULLABLE_ULONG_CONVERTIBLE_TYPE:
            return `Nullable!ulong(r.getUlong("` ~ m ~ `"))`;
        case DOUBLE_CONVERTIBLE_TYPE:
            return `r.getDouble("` ~ m ~ `")`;
        case NULLABLE_DOUBLE_CONVERTIBLE_TYPE:
            return `Nullable!double(r.getDouble("` ~ m ~ `"))`;
        case STRING_CONVERTIBLE_TYPE:
            return `r.getString("` ~ m ~ `")`;
        case NULLABLE_STRING_CONVERTIBLE_TYPE:
            return `r.getString("` ~ m ~ `")`;
        case BYTE_ARRAY_CONVERTIBLE_TYPE:
            return `r.getBytes("` ~ m ~ `")`;
        case UBYTE_ARRAY_CONVERTIBLE_TYPE:
            return `r.getUbytes("` ~ m ~ `")`;
    }
}

string getPropertyWriteCodeByName(T, string m)() {
    immutable string nullValueCode = getFieldTraits!(__traits(getMember, T, m)).columnTypeSetNullCode;
    mixin(`alias value = T.` ~ m ~ ";"); // alias value = __traits(getMember, T, m);
    static if (hasConvertibleType!value) {
        immutable string propertyWriter = nullValueCode
                ~ "convertFrom!(entity." ~ m ~ ", " ~ getConvertibleTypeCode!value ~ ")"
                ~             "(" ~ getColumnTypeDatasetReadCodeByName!(T, m)() ~ ", entity." ~ m ~ ");\n";
        return propertyWriter ~ "if (r.wasNull) entity." ~ m ~ " = nv;";
    } else {
        immutable string propertyWriter = nullValueCode ~ "entity." ~ m ~ " = " ~ getColumnTypeDatasetReadCodeByName!(T, m)() ~ ";\n";
        return propertyWriter ~ "if (r.wasNull) entity." ~ m ~ " = nv;";
    }
}

string getColumnReadCodeByName(T, string m)() {
    return "{" ~ getPropertyWriteCodeByName!(T,m)() ~ "}\n";
}

string getAllColumnsReadCodeByName(T)() {
    string res;
    foreach(m; FieldNameTuple!T) {
        static if (isValidFieldMember!(T, m)) {
            res ~= getColumnReadCodeByName!(T, m);
        }
    }
    return res;
}

/**
 * Extract a row from the result set as the specified type.
 * Requires that next has already been checked.
 * Can be used for example to extract rows from executing a PreparedStatement.
 */
T get(T)(ResultSet r) {
    T entity;
    mixin(getAllColumnsReadCodeByName!T());
    return entity;
}

/// range for select query
auto select(T, fieldList...)(Statement stmt) if (isSupportedDataType!T) {
    static struct Select {
        T entity;
        private Statement stmt;
        private ResultSet r;
        static immutable selectSQL = generateSelectSQL!(T, fieldList)();
        string whereCondSQL;
        string orderBySQL;
        this(Statement stmt) {
            this.stmt = stmt;
        }
        ref Select where(string whereCond) {
            whereCondSQL = " WHERE " ~ whereCond;
            return this;
        }
        ref Select orderBy(string order) {
            orderBySQL = " ORDER BY " ~ order;
            return this;
        }
        ref T front() {
            return entity;
        }
        void popFront() {
        }
        @property bool empty() {
            if (!r)
                r = stmt.executeQuery(selectSQL ~ whereCondSQL ~ orderBySQL);
            if (!r.next())
                return true;
            mixin(getAllColumnsReadCode!(T, fieldList));
            return false;
        }
        ~this() {
            if (r)
                r.close();
        }
    }
    return Select(stmt);
}

enum string getIdentity(alias value) = __traits(identifier, value);

template getIdentityFieldMembers(T) {
    static if (getSymbolsByUDA!(T, identity).length > 0) {
        alias getIdentityFieldMembers = getSymbolsByUDA!(T, identity);
    } else static if (getSymbolsByUDA!(T, columnName("id")).length > 0) {
        static assert(getSymbolsByUDA!(T, columnName("id")).length == 1);
        alias getIdentityFieldMembers = getSymbolsByUDA!(T, columnName("id"));
    } else static if (__traits(hasMember, T, "id")) {
        static if (!hasIdentity!(__traits(getMember, T, "id"))) {
            alias getIdentityFieldMembers = AliasSeq!(__traits(getMember, T, "id"));
        } else {
            alias getIdentityFieldMembers = AliasSeq!();
        }
    } else {
        alias getIdentityFieldMembers = AliasSeq!();
    }
}
enum getIdentityFieldMemberNames(T) = staticMap!(getIdentity, getIdentityFieldMembers!T);
template getIdentityFieldMembers(alias o) {
    template getMember(string m) { mixin(`alias getMember = o.` ~ m ~ ";"); } // alias getMember(string m) = __traits(getMember, o, m);
    alias getIdentityFieldMembers = staticMap!(getMember, getIdentityFieldMemberNames!(typeof(o)));
}
enum hasIdentityFieldMember(T) = getIdentityFieldMembers!T.length > 0;
enum isIdentityFieldMember(T, string m) = staticIndexOf!(m, getIdentityFieldMemberNames!T) != -1;

unittest {
    struct A {
        int x = 1;
        @identity int y = 2;
    }
    A a;
    static assert(!isIdentityFieldMember!(A, "x"));
    static assert( isIdentityFieldMember!(A, "y"));
    static assert(getIdentityFieldMembers!A.length == 1);
    static assert(getIdentityFieldMemberNames!A[0] == "y");
    static assert(getIdentityFieldMembers!a[0].offsetof == A.y.offsetof);
    
    struct B {
        int x = 1;
        int id = 2;
    }
    B b;
    static assert( isIdentityFieldMember!(B, "id"));
    static assert(getIdentityFieldMemberNames!B[0] == "id");
    static assert(getIdentityFieldMembers!b[0].offsetof == B.id.offsetof);
    
    struct C {
        int x = 1;
        int id = 2;
        @identity int y = 3;
    }
    C c;
    static assert(getIdentityFieldMembers!C.length == 1);
    static assert(!isIdentityFieldMember!(C, "id"));
    static assert( isIdentityFieldMember!(C, "y"));
    static assert(getIdentityFieldMemberNames!C[0] == "y");
    static assert(getIdentityFieldMembers!c[0].offsetof == C.y.offsetof);
    
    struct D {
        @ignore int id;
        @columnName("id") int x;
    }
    D d;
    static assert(getIdentityFieldMembers!D.length == 1);
    static assert(!isIdentityFieldMember!(D, "id"));
    static assert( isIdentityFieldMember!(D, "x"));
    static assert(getIdentityFieldMemberNames!D[0] == "x");
    static assert(getIdentityFieldMembers!d[0].offsetof == D.x.offsetof);
    
    struct E {
        @ignore int id;
        @columnName("id") int x;
        @identity int y;
    }
    E e;
    static assert(getIdentityFieldMembers!E.length == 1);
    static assert(!isIdentityFieldMember!(E, "id"));
    static assert(!isIdentityFieldMember!(E, "x"));
    static assert( isIdentityFieldMember!(E, "y"));
    static assert(getIdentityFieldMemberNames!E[0] == "y");
    static assert(getIdentityFieldMembers!e[0].offsetof == E.y.offsetof);
    
    struct F {
        int id;
        @columnName("id") int x;
        @identity int y;
    }
    F f;
    static assert(getIdentityFieldMembers!F.length == 1);
    static assert(!isIdentityFieldMember!(F, "id"));
    static assert(!isIdentityFieldMember!(F, "x"));
    static assert( isIdentityFieldMember!(F, "y"));
    static assert(getIdentityFieldMemberNames!F[0] == "y");
    static assert(getIdentityFieldMembers!f[0].offsetof == F.y.offsetof);
    
    struct G {
        @identity int id;
        @columnName("id") int x;
        @identity int y;
    }
    G g;
    static assert(getIdentityFieldMembers!G.length == 2);
    static assert( isIdentityFieldMember!(G, "id"));
    static assert(!isIdentityFieldMember!(G, "x"));
    static assert( isIdentityFieldMember!(G, "y"));
    static assert(getIdentityFieldMemberNames!G[0] == "id");
    static assert(getIdentityFieldMemberNames!G[1] == "y");
    static assert(getIdentityFieldMembers!g[0].offsetof == G.id.offsetof);
    static assert(getIdentityFieldMembers!g[1].offsetof == G.y.offsetof);
}



string toFieldString(T)(T arg)
{
    import std.conv, std.array;
    static if (is(Unqual!T == bool)) {
        return arg ? "true" : "false";
    } else static if (is(Unqual!T == byte) || is(Unqual!T == ubyte)
                   || is(Unqual!T == short) || is(Unqual!T == ushort)
                   || is(Unqual!T == int) || is(Unqual!T == uint)
                   || is(Unqual!T == long) || is(Unqual!T == ulong)
                   || is(Unqual!T == float) || is(Unqual!T == double)) {
        return arg.to!string();
    } else static if (is(Unqual!T == SysTime)) {
        return text("'", arg.toUTC().toISOExtString(), "'");
    } else static if (is(Unqual!T == DateTime)) {
        return text("'", arg.toISOExtString(), "'");
    } else static if (is(Unqual!T == Date)) {
        return text("'", arg.toISOExtString(), "'");
    } else static if (is(Unqual!T == TimeOfDay)) {
        return text("'", arg.toISOExtString(), "'");
    } else static if (is(Unqual!T U == Nullable!U)) {
        return arg.isNull ? "NULL" : toFieldString(arg.get());
    } else static if (is(Unqual!T: const(char)[])) {
        return "'" ~ arg.replace("'", "''") ~ "'";
    } else static assert("Cannot convert to string.");
}


template convertToField(T, string m) {
    mixin(`alias member = T.` ~ m ~ ";"); // alias member = __traits(getMember, T, m);
    static if (isColumnTypeNullableByDefault!(T, m)) {
        string convertToField(U)(U arg) {
            if (arg.isNull)
                return "NULL";
            static if (hasConvertibleType!member) {
                return toFieldString(convTo!(member, getFieldTraits!member.convertibleType)(arg));
            } else {
                return toFieldString(arg);
            }
        }
    } else static if (hasConvertibleType!member) {
        string convertToField(U)(U arg) {
            return toFieldString(convTo!(member, getFieldTraits!member.convertibleType)(arg));
        }
    } else {
        alias convertToField = toFieldString;
    }
}

/// returns "INSERT INTO <table name> (<field list>) VALUES (value list)
string generateInsertSQL(T)() {
    string res = "INSERT INTO " ~ getTableNameForType!(T)();
    string []values;
    foreach(m; FieldNameTuple!T) {
        static if (isValidFieldMember!(T, m) && !isIdentityFieldMember!(T, m)) {
            values ~= getColumnNameForMember!(T, m);
        }
    }
    res ~= "(" ~ join(values, ",") ~ ")";
    res ~= " VALUES ";
    return res;
}

bool insert(T)(Statement stmt, ref T o) if (isSupportedDataType!T) {
    auto insertSQL = generateInsertSQL!(T)();
    string []values;
    static foreach(m; FieldNameTuple!T) {
        static if (isValidFieldMember!(T, m) && !isIdentityFieldMember!(T, m)) {
            // pragma(msg,addFieldValue!(T)(m));
            values ~= convertToField!(T, m)(__traits(getMember, o, m));
        }
    }
    insertSQL ~= "(" ~ join(values, ",") ~ ")";
    static if (hasIdentityFieldMember!T) {
        Variant insertId;
        stmt.executeUpdate(insertSQL, insertId);
        static foreach (m; getIdentityFieldMemberNames!T) {
            static if (hasConvertibleType!(__traits(getMember, o, m))) {
                convertFrom!(__traits(getMember, o, m))(insertId.get!(long), __traits(getMember, o, m));
            } else {
                __traits(getMember, o, m) = insertId.get!(long);
            }
        }
    } else {
        stmt.executeUpdate(insertSQL);
    }
    return true;
}

/// returns "UPDATE <table name> SET field1=value1 WHERE id=id
string generateUpdateSQL(T)() {
    string res = "UPDATE " ~ getTableNameForType!(T)();
    string []values;
    foreach(m; FieldNameTuple!T) {
        static if (isValidFieldMember!(T, m) && !isIdentityFieldMember!(T, m)) {
            values ~= getColumnNameForMember!(T, m);
        }
    }
    res ~= " SET ";
    return res;
}

bool update(T)(Statement stmt, ref T o) if (isSupportedDataType!T) {
    auto updateSQL = generateUpdateSQL!(T)();
    string []values;
    static foreach(m; FieldNameTuple!T) {
        static if (isValidFieldMember!(T, m) && !isIdentityFieldMember!(T, m)) {
            values ~= getColumnNameForMember!(T, m) ~ "="~ convertToField!(T, m)(__traits(getMember, o, m));
        }
    }
    updateSQL ~= join(values, ",");
    updateSQL ~= " WHERE ";
    static foreach (idx, idMemberName; getIdentityFieldMemberNames!T)
    {
        static if (idx != 0)
            updateSQL ~= ",";
        updateSQL ~= getColumnNameForMember!(T, idMemberName)
            ~ "=" ~ convertToField!(T, idMemberName)(__traits(getMember, o, idMemberName));
    }
    Variant updateId;
    stmt.executeUpdate(updateSQL, updateId);
    return true;
}

/// returns "DELETE FROM <table name> WHERE id=id
string generateDeleteSQL(T)() {
  string res = "DELETE FROM " ~ getTableNameForType!(T)();
  return res;
}

bool remove(T)(Statement stmt, ref T o) if (isSupportedDataType!T) {
  auto deleteSQL = generateDeleteSQL!(T)();
  deleteSQL ~= mixin(`" WHERE id="~ to!string(o.id) ~ ";"`);
  Variant deleteId;
  stmt.executeUpdate(deleteSQL, deleteId);
  return true;
}

// TODO: use better way to count parameters
int paramCount(destList...)() {
    int res = 0;
    foreach(p; destList) {
        res++;
    }
    return res;
}

auto ref select(Args...)(Statement stmt, string sql, ref Args args){ 
    static struct Select {
        Statement stmt;
        string selectSQL;
        void delegate(ResultSet r) _copyFunction;
        ResultSet r;
        int rowIndex;
    
        string whereCondSQL;
        string orderBySQL;
        ref Select where(string whereCond) {
            whereCondSQL = " WHERE " ~ whereCond;
            return this;
        }
        ref Select orderBy(string order) {
            orderBySQL = " ORDER BY " ~ order;
            return this;
        }
        int front() {
            return rowIndex;
        }
        void popFront() {
            rowIndex++;
        }
        @property bool empty() {
            if (!r)
                r = stmt.executeQuery(selectSQL ~ whereCondSQL ~ orderBySQL);
            if (!r.next())
                return true;
            _copyFunction(r);
            return false;
        }
        ~this() {
            if (r)
                r.close();
        }
    }
    return Select(stmt, sql, delegate(ResultSet r) {
        static foreach(i, a; args) {{
            static assert(__traits(isRef, a));
            enum index = i + 1;
            mixin(getArgsWriteCode!a);
        }}
    });
}

// for compatibility
template select() {
    pragma(inline) auto select(Args...)(Statement stmt, string sql, ref Args args){ return .select!Args(stmt, sql, args); }
}

version (unittest) {
    import ddbc.common: ResultSetImpl;
    class StubResultSet: ResultSetImpl {
        Variant[][] values;
        string[]    columnNames;
        size_t      currentRow = -1;
        Statement   parent;
        this(Statement s) {
            parent = s;
        }
        override void close() {}
        override bool first() { currentRow = 0; return currentRow >= 0 && currentRow < values.length; }
        override bool isFirst() { return currentRow == 0; }
        override bool isLast() { return values.length > 0 && currentRow >= values.length - 1; }
        override bool next() {
            if (currentRow + 1 >= values.length)
                return false;
            currentRow++;
            return true;
        }

        override Statement getStatement() { return parent; }
        //Retrieves the current row number
        override int getRow() { return cast(int)currentRow; }

        // from DataSetReader
        override bool      getBoolean(int columnIndex)   { return values[currentRow][columnIndex - 1].get!bool(); }
        override ubyte     getUbyte(int columnIndex)     { return values[currentRow][columnIndex - 1].get!ubyte(); }
        override ubyte[]   getUbytes(int columnIndex)    { return values[currentRow][columnIndex - 1].get!(ubyte[])(); }
        override byte[]    getBytes(int columnIndex)     { return values[currentRow][columnIndex - 1].get!(byte[])(); }
        override byte      getByte(int columnIndex)      { return values[currentRow][columnIndex - 1].get!byte(); }
        override short     getShort(int columnIndex)     { return values[currentRow][columnIndex - 1].get!short(); }
        override ushort    getUshort(int columnIndex)    { return values[currentRow][columnIndex - 1].get!ushort(); }
        override int       getInt(int columnIndex)       { return values[currentRow][columnIndex - 1].get!int(); }
        override uint      getUint(int columnIndex)      { return values[currentRow][columnIndex - 1].get!uint(); }
        override long      getLong(int columnIndex)      { return values[currentRow][columnIndex - 1].get!long(); }
        override ulong     getUlong(int columnIndex)     { return values[currentRow][columnIndex - 1].get!ulong(); }
        override double    getDouble(int columnIndex)    { return values[currentRow][columnIndex - 1].get!double(); }
        override float     getFloat(int columnIndex)     { return values[currentRow][columnIndex - 1].get!float(); }
        override string    getString(int columnIndex)    { return values[currentRow][columnIndex - 1].get!string(); }
        override Variant   getVariant(int columnIndex)   { return values[currentRow][columnIndex - 1]; }
        override SysTime   getSysTime(int columnIndex)   { return values[currentRow][columnIndex - 1].get!SysTime(); }
        override DateTime  getDateTime(int columnIndex)  { return values[currentRow][columnIndex - 1].get!DateTime(); }
        override Date      getDate(int columnIndex)      { return values[currentRow][columnIndex - 1].get!Date(); }
        override TimeOfDay getTime(int columnIndex)      { return values[currentRow][columnIndex - 1].get!TimeOfDay(); }

        override bool isNull(int columnIndex) { return !values[currentRow][columnIndex - 1].hasValue(); }
        override bool wasNull() { return false; }

        // additional methods
        override int findColumn(string columnName)  {
            import std.algorithm: countUntil;
            return cast(int)columnNames.countUntil(columnName) + 1;
        }
        
        void reset() {
            currentRow = -1;
        }
        void clear() {
            values = null;
            columnNames = null;
            currentRow = -1;
        }
        void make(T = Variant)(size_t rows, size_t cols, T defaultValue = T.init) {
            values = null;
            values.length = rows;
            currentRow = -1;
            foreach (ref row; values) {
                row.length = cols;
                row[] = defaultValue;
            }
        }
        void make(T = Variant)(size_t rows, string[] names, T defaultValue = T.init) {
            columnNames = names;
            make(rows, names.length, defaultValue);
        }
        ref Variant opIndex(size_t row, size_t col) {
            return values[row][col];
        }
        ref Variant[] opIndex(size_t row) {
            return values[row];
        }
    }
    class StubStatement: Statement {
        string lastQuery;
        long[] nextInsertIds;
        int[]  nextReturnValues;
        
        StubResultSet resultSet;
        
        this() { resultSet = new StubResultSet(this); }
        
        override ResultSet executeQuery(string query) {
            lastQuery = query;
            return resultSet;
        }
        
        override int executeUpdate(string query) {
            lastQuery = query;
            if (nextReturnValues.length == 0)
                return 0;
            auto ret = nextReturnValues[0];
            nextReturnValues = nextReturnValues[1..$];
            return ret;
        }
        
        override int executeUpdate(string query, out Variant insertId) {
            lastQuery = query;
            if (nextInsertIds.length != 0) {
                insertId = nextInsertIds[0];
                nextInsertIds = nextInsertIds[1..$];
            }
            
            if (nextReturnValues.length != 0) {
                auto ret = nextReturnValues[0];
                nextReturnValues = nextReturnValues[1..$];
                return ret;
            }
            return 0;
        }
        override void close() { }
        void clear() {
            resultSet.clear();
            lastQuery = null;
            nextInsertIds = null;
            nextReturnValues = null;
        }
    }
    
    string shrinkWhite(string x) {
        import std.regex, std.string;
        return x.replaceAll(ctRegex!`[\s\r\n]+`, " ").strip();
    }
}

unittest {
    auto stmt = new StubStatement;
    scope (exit) stmt.close();
    struct Data {
        int a;
        int b;
    }
    
    // select for Type
    foreach (e; stmt.select!Data.where("a == 1")) {
        assert(0);
    }
    assert(stmt.lastQuery.shrinkWhite == "SELECT a,b FROM data WHERE a == 1");
    Data dat;
    stmt.clear();
    foreach (i; stmt.select("SELECT a,b    \n\t\tFROM data", dat.a, dat.b).where("a == 2").orderBy("b ASC")) {
        assert(0);
    }
    assert(stmt.lastQuery.shrinkWhite == "SELECT a,b FROM data WHERE a == 2 ORDER BY b ASC");
    
    // select for args
    stmt.resultSet.make(3, ["a", "b"], Variant(0));
    stmt.resultSet[0][] = [Variant(20), Variant(1)];
    stmt.resultSet[1][] = [Variant(20), Variant(2)];
    stmt.resultSet[2][] = [Variant(20), Variant(3)];
    Tuple!(int, Data)[] results;
    foreach (r; stmt.select("SELECT a,b FROM data", dat.a, dat.b).where("a == 20").orderBy("b ASC")) {
        results ~= tuple(r, dat);
    }
    assert(stmt.lastQuery.shrinkWhite == "SELECT a,b FROM data WHERE a == 20 ORDER BY b ASC");
    assert(results.equal([tuple(0, Data(20, 1)), tuple(1, Data(20, 2)), tuple(2, Data(20, 3))]));
    
    // test for compatibility
    stmt.clear();
    foreach (i; stmt.select!()("SELECT a,b FROM data", dat.a, dat.b).where("a == 5")) {
        assert(0);
    }
    assert(stmt.lastQuery.shrinkWhite == "SELECT a,b FROM data WHERE a == 5");
    
    
    // test for uda
    import std.stdio: File;
    enum { sel1, sel2, }
    
    @tableName("user_2")
    struct User2 {
        static struct Proxy {
            static string to(int x) { static import std.conv; return std.conv.to!string(x); }
            static int from(string x) { static import std.conv; return std.conv.to!int(x); }
        }
                                    @sel1       long    id;
        @columnName("name_of_user")       @sel2 string  name;
        @ignore                                 File    file;
        @convBy!Proxy               @sel1 @sel2
                                                int     flags;
    }
    static assert(hasIdentityFieldMember!User2);
    
    alias U = User2;
    alias V = Variant;
    enum  f = File.init;
    import std.array: array;
    
    stmt.resultSet.make(3, ["id", "name_of_user", "flags"]);
    stmt.resultSet[0][] = [V(1), V("Alice"),     V("1011")];
    stmt.resultSet[1][] = [V(2), V("Marisa"),    V("1110")];
    stmt.resultSet[2][] = [V(3), V("Patchouli"), V("1001")];
    auto users = stmt.select!U.array;
    assert(stmt.lastQuery.shrinkWhite == "SELECT id,name_of_user,flags FROM user_2");
    assert(users.equal([U(1, "Alice", f, 1011), U(2, "Marisa", f, 1110), U(3, "Patchouli", f, 1001)]));
    
    stmt.resultSet.make(3, ["id", "flags"]);
    stmt.resultSet[0][] = [V(1), V("1011")];
    stmt.resultSet[1][] = [V(2), V("1110")];
    stmt.resultSet[2][] = [V(3), V("1001")];
    users = stmt.select!(U, getMemberNamesAt!(U, sel1)).array;
    assert(stmt.lastQuery.shrinkWhite == "SELECT id,flags FROM user_2");
    assert(users.equal([U(1, null, f, 1011), U(2, null, f, 1110), U(3, null, f, 1001)]));
    
    stmt.resultSet.make(3, ["name_of_user", "flags"]);
    stmt.resultSet[0][] = [V("Alice"),     V("1011")];
    stmt.resultSet[1][] = [V("Marisa"),    V("1110")];
    stmt.resultSet[2][] = [V("Patchouli"), V("1001")];
    users = stmt.select!(U, getMemberNamesAt!(U, sel2)).array;
    assert(stmt.lastQuery.shrinkWhite == "SELECT name_of_user,flags FROM user_2");
    assert(users.equal([U(0, "Alice", f, 1011), U(0, "Marisa", f, 1110), U(0, "Patchouli", f, 1001)]));
}

unittest {
    auto stmt = new StubStatement;
    scope (exit) stmt.close();
    struct Data1 {
        long id;
        int x;
        int y;
    }
    auto data1 = Data1(0);
    stmt.nextInsertIds ~= 1;
    stmt.insert(data1);
    assert(stmt.lastQuery.shrinkWhite == "INSERT INTO data1(x,y) VALUES (0,0)");
    
    // UDA and id-less
    @tableName("data_2")
    struct Data2 {
        @columnName("data_x")
        int x;
        @columnName("data_y")
        int y;
    }
    auto data2 = Data2(0);
    stmt.insert(data2);
    assert(stmt.lastQuery.shrinkWhite == "INSERT INTO data_2(data_x,data_y) VALUES (0,0)");
    
    // id with UDA
    struct Data3 {
        int x;
        int y;
        static struct Proxy {
            static long to(Data1 dat) { return dat.id; }
            static Data1 from(long v) { return Data1(v); }
        }
        @identity @convBy!Proxy
        Data1 num;
    }
    auto data3 = Data3(0);
    stmt.nextInsertIds ~= 2;
    stmt.insert(data3);
    assert(stmt.lastQuery.shrinkWhite == "INSERT INTO data3(x,y) VALUES (0,0)");
    assert(data3.num.id == 2);
}

unittest {
    auto stmt = new StubStatement;
    scope (exit) stmt.close();
    struct Data1 {
        long id;
        int x;
        int y;
    }
    auto data1 = Data1(1);
    stmt.update(data1);
    assert(stmt.lastQuery.shrinkWhite == "UPDATE data1 SET x=0,y=0 WHERE id=1");
    
    struct Data2 {
        long id;
        @identity
        int x;
        int y;
    }
    auto data2 = Data2(2);
    stmt.update(data2);
    assert(stmt.lastQuery.shrinkWhite == "UPDATE data2 SET id=2,y=0 WHERE x=0");
    
    @tableName("data333333")
    struct Data3 {
        @ignore
        long id;
        int x;
        @columnName("id")
        int y;
    }
    auto data3 = Data3(3);
    stmt.update(data3);
    assert(stmt.lastQuery.shrinkWhite == "UPDATE data333333 SET x=0 WHERE id=0");
    
    struct Data4 {
        string x;
        @identity int y;
        @identity Date z;
    }
    auto data4 = Data4("ab'c'de", 10, Date(2020,4,1));
    stmt.update(data4);
    assert(stmt.lastQuery.shrinkWhite == "UPDATE data4 SET x='ab''c''de' WHERE y=10,z='2020-04-01'");
    
}
