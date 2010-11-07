/*
** LuaSQL, MySQL driver
** Authors:  Eduardo Quintao
** See Copyright Notice in license.html
** $Id: ls_mysql.c,v 1.29 2008/05/04 02:46:17 tomas Exp $
*/

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#ifdef WIN32
#include <winsock2.h>
#define NO_CLIENT_LONG_LONG
#endif

#include "mysql.h"

#include "lua.h"
#include "lauxlib.h"
#if ! defined (LUA_VERSION_NUM) || LUA_VERSION_NUM < 501
#include "compat-5.1.h"
#endif


#include "luasql.h"

#define LUASQL_ENVIRONMENT_MYSQL "MySQL environment"
#define LUASQL_CONNECTION_MYSQL "MySQL connection"
#define LUASQL_CURSOR_MYSQL "MySQL cursor"

/* For compat with old version 4.0 */
#if (MYSQL_VERSION_ID < 40100) 
#define MYSQL_TYPE_VAR_STRING   FIELD_TYPE_VAR_STRING 
#define MYSQL_TYPE_STRING       FIELD_TYPE_STRING 
#define MYSQL_TYPE_DECIMAL      FIELD_TYPE_DECIMAL 
#define MYSQL_TYPE_SHORT        FIELD_TYPE_SHORT 
#define MYSQL_TYPE_LONG         FIELD_TYPE_LONG 
#define MYSQL_TYPE_FLOAT        FIELD_TYPE_FLOAT 
#define MYSQL_TYPE_DOUBLE       FIELD_TYPE_DOUBLE 
#define MYSQL_TYPE_LONGLONG     FIELD_TYPE_LONGLONG 
#define MYSQL_TYPE_INT24        FIELD_TYPE_INT24 
#define MYSQL_TYPE_YEAR         FIELD_TYPE_YEAR 
#define MYSQL_TYPE_TINY         FIELD_TYPE_TINY 
#define MYSQL_TYPE_TINY_BLOB    FIELD_TYPE_TINY_BLOB 
#define MYSQL_TYPE_MEDIUM_BLOB  FIELD_TYPE_MEDIUM_BLOB 
#define MYSQL_TYPE_LONG_BLOB    FIELD_TYPE_LONG_BLOB 
#define MYSQL_TYPE_BLOB         FIELD_TYPE_BLOB 
#define MYSQL_TYPE_DATE         FIELD_TYPE_DATE 
#define MYSQL_TYPE_NEWDATE      FIELD_TYPE_NEWDATE 
#define MYSQL_TYPE_DATETIME     FIELD_TYPE_DATETIME 
#define MYSQL_TYPE_TIME         FIELD_TYPE_TIME 
#define MYSQL_TYPE_TIMESTAMP    FIELD_TYPE_TIMESTAMP 
#define MYSQL_TYPE_ENUM         FIELD_TYPE_ENUM 
#define MYSQL_TYPE_SET          FIELD_TYPE_SET
#define MYSQL_TYPE_NULL         FIELD_TYPE_NULL

#define mysql_commit(_) ((void)_)
#define mysql_rollback(_) ((void)_)
#define mysql_autocommit(_,__) ((void)_)

#endif

typedef struct {
	short      closed;
} env_data;

typedef struct {
	short      closed;
	int        env;                /* reference to environment */
	MYSQL     *my_conn;
	int		   auto_commit;		/* should each statment be commited */
} conn_data;

typedef struct {
	short      closed;
	int        conn;               /* reference to connection */
	int        numcols;            /* number of columns */
	int        colnames, coltypes; /* reference to column information tables */
	MYSQL_RES *my_res;
	char	  *modestring;
} cur_data;

LUASQL_API int luaopen_luasql_mysql (lua_State *L);


/*
** Generates a driver error plus the error message from the database
** The generated error message is preceded by LUASQL_PREFIX string
*/
static int luasql_failmessage(lua_State *L, const char *err, const char *m) {
    lua_pushnil(L);
	lua_pushstring(L, LUASQL_PREFIX);
	lua_pushstring(L, err);
    lua_pushstring(L, m);
	lua_concat(L, 3);
    return 2;
}


/*
** Check for valid environment.
*/
static env_data *getenvironment (lua_State *L) {
	env_data *env = (env_data *)luaL_checkudata (L, 1, LUASQL_ENVIRONMENT_MYSQL);
	luaL_argcheck (L, env != NULL, 1, "environment expected");
	luaL_argcheck (L, !env->closed, 1, "environment is closed");
	return env;
}


/*
** Check for valid connection.
*/
static conn_data *getconnection (lua_State *L) {
	conn_data *conn = (conn_data *)luaL_checkudata (L, 1, LUASQL_CONNECTION_MYSQL);
	luaL_argcheck (L, conn != NULL, 1, "connection expected");
	luaL_argcheck (L, !conn->closed, 1, "connection is closed");
	return conn;
}


/*
** Check for valid cursor.
*/
static cur_data *getcursor (lua_State *L) {
	cur_data *cur = (cur_data *)luaL_checkudata (L, 1, LUASQL_CURSOR_MYSQL);
	luaL_argcheck (L, cur != NULL, 1, "cursor expected");
	luaL_argcheck (L, !cur->closed, 1, "cursor is closed");
	return cur;
}


/*
** Push the value of #i field of #tuple row.
*/
static void pushvalue (lua_State *L, void *row, long int len) {
	if (row == NULL)
		lua_pushnil (L);
	else
		lua_pushlstring (L, row, len);
}


/*
** Get the internal database type of the given column.
*/
static char *getcolumntype (enum enum_field_types type) {

	switch (type) {
		case MYSQL_TYPE_VAR_STRING: case MYSQL_TYPE_STRING:
			return "string";
		case MYSQL_TYPE_DECIMAL: case MYSQL_TYPE_SHORT: case MYSQL_TYPE_LONG:
		case MYSQL_TYPE_FLOAT: case MYSQL_TYPE_DOUBLE: case MYSQL_TYPE_LONGLONG:
		case MYSQL_TYPE_INT24: case MYSQL_TYPE_YEAR: case MYSQL_TYPE_TINY: 
			return "number";
		case MYSQL_TYPE_TINY_BLOB: case MYSQL_TYPE_MEDIUM_BLOB:
		case MYSQL_TYPE_LONG_BLOB: case MYSQL_TYPE_BLOB:
			return "binary";
		case MYSQL_TYPE_DATE: case MYSQL_TYPE_NEWDATE:
			return "date";
		case MYSQL_TYPE_DATETIME:
			return "datetime";
		case MYSQL_TYPE_TIME:
			return "time";
		case MYSQL_TYPE_TIMESTAMP:
			return "timestamp";
		case MYSQL_TYPE_ENUM: case MYSQL_TYPE_SET:
			return "set";
		case MYSQL_TYPE_NULL:
			return "null";
		default:
			return "undefined";
	}
}


/*
** Creates the lists of fields names and fields types.
*/
static void create_colinfo (lua_State *L, cur_data *cur) {
	MYSQL_FIELD *fields;
	char typename[50];
	int i;
	fields = mysql_fetch_fields(cur->my_res);
	lua_newtable (L); /* names */
	lua_newtable (L); /* types */
	for (i = 1; i <= cur->numcols; i++) {
		lua_pushstring (L, fields[i-1].name);
		lua_rawseti (L, -3, i);
		sprintf (typename, "%.20s(%ld)", getcolumntype (fields[i-1].type), fields[i-1].length);
		lua_pushstring(L, typename);
		lua_rawseti (L, -2, i);
	}
	/* Stores the references in the cursor structure */
	cur->coltypes = luaL_ref (L, LUA_REGISTRYINDEX);
	cur->colnames = luaL_ref (L, LUA_REGISTRYINDEX);
}


/*
** Get another row of the given cursor.
*/
static int cur_fetch (lua_State *L) {
	cur_data *cur = getcursor (L);
	MYSQL_RES *res = cur->my_res;
	unsigned long *lengths;
	MYSQL_ROW row = mysql_fetch_row(res);
	if (row == NULL) {
		lua_pushnil(L);  /* no more results */
		return 1;
	}
	lengths = mysql_fetch_lengths(res);

	if (lua_istable (L, 2)) {
	    const char *opts = luasql_getfetchmodestring( L, cur->modestring );
		if (strchr (opts, 'n') != NULL) {
			/* Copy values to numerical indices */
			int i;
			for (i = 0; i < cur->numcols; i++) {
				pushvalue (L, row[i], lengths[i]);
				lua_rawseti (L, 2, i+1);
			}
		}
		if (strchr (opts, 'a') != NULL) {
			int i;
			/* Check if colnames exists */
			if (cur->colnames == LUA_NOREF)
		        create_colinfo(L, cur);
			lua_rawgeti (L, LUA_REGISTRYINDEX, cur->colnames);/* Push colnames*/
	
			/* Copy values to alphanumerical indices */
			for (i = 0; i < cur->numcols; i++) {
				lua_rawgeti(L, -1, i+1); /* push the field name */

				/* Actually push the value */
				pushvalue (L, row[i], lengths[i]);
				lua_rawset (L, 2);
			}
			/* lua_pop(L, 1);  Pops colnames table. Not needed */
		}
		lua_pushvalue(L, 2);
		return 1; /* return table */
	}
	else {
		int i;
		luaL_checkstack (L, cur->numcols, LUASQL_PREFIX"too many columns");
		for (i = 0; i < cur->numcols; i++)
			pushvalue (L, row[i], lengths[i]);
		return cur->numcols; /* return #numcols values */
	}
}


/*
** Close the cursor on top of the stack.
** Return 1
*/
static int cur_close (lua_State *L) {
	cur_data *cur = (cur_data *)luaL_checkudata (L, 1, LUASQL_CURSOR_MYSQL);
	luaL_argcheck (L, cur != NULL, 1, LUASQL_PREFIX"cursor expected");
	if (cur->closed) {
		lua_pushboolean (L, 0);
		return 1;
	}

	/* Nullify structure fields. */
	cur->closed = 1;
	mysql_free_result(cur->my_res);
	luaL_unref (L, LUA_REGISTRYINDEX, cur->conn);
	luaL_unref (L, LUA_REGISTRYINDEX, cur->colnames);
	luaL_unref (L, LUA_REGISTRYINDEX, cur->coltypes);

	lua_pushboolean (L, 1);
	return 1;
}


/*
** Pushes a column information table on top of the stack.
** If the table isn't built yet, call the creator function and stores
** a reference to it on the cursor structure.
*/
static void _pushtable (lua_State *L, cur_data *cur, size_t off) {
	int *ref = (int *)((char *)cur + off);

	/* If colnames or coltypes do not exist, create both. */
	if (*ref == LUA_NOREF)
		create_colinfo(L, cur);
	
	/* Pushes the right table (colnames or coltypes) */
	lua_rawgeti (L, LUA_REGISTRYINDEX, *ref);
}
#define pushtable(L,c,m) (_pushtable(L,c,offsetof(cur_data,m)))


/*
** Return the list of field names.
*/
static int cur_getcolnames (lua_State *L) {
	pushtable (L, getcursor(L), colnames);
	return 1;
}


/*
** Return the list of field types.
*/
static int cur_getcoltypes (lua_State *L) {
	pushtable (L, getcursor(L), coltypes);
	return 1;
}


/*
** Push the number of rows.
*/
static int cur_numrows (lua_State *L) {
	lua_pushnumber (L, (lua_Number)mysql_num_rows (getcursor(L)->my_res));
	return 1;
}


/*
 * Sets the cursor parameters
 */
static void cur_set(lua_State *L) {
	if( lua_istable( L, 2 ) ) {
		cur_data *cur = getcursor(L);
		char *key;
		lua_pushnil(L);

		while( lua_next(L, 2) != 0 ) {
			if( lua_isstring(L, -2) ) {
				key = lua_tostring(L, -2);

				if( strcmp(key, LUASQL_MODESTRING) == 0 ) {
					if( lua_isstring( L, -1 ) )
						cur->modestring = lua_tostring( L, -1 );
				}
			}

			lua_pop(L, 1);
		}		
	}
}


/*
 * Retrieve the specified cursor parameters
 */
static int cur_get( lua_State *L ) {
	if( lua_istable( L, 2 ) ) {
		lua_newtable(L);
		int rsp = lua_gettop(L);
		cur_data *cur = getcursor(L);
		char *key;
		lua_pushnil(L);

		while( lua_next(L, 2) != 0 ) {
			if( lua_isstring(L, -1) ) {
				key = lua_tostring(L, -1);

				if( strcmp(key, LUASQL_MODESTRING) == 0 ) {
					lua_pushstring( L, LUASQL_MODESTRING );
					lua_pushstring( L, cur->modestring );
					lua_settable( L, rsp );
				}
			}

			lua_pop(L, 1);
		}
	} else
		if( lua_isstring( L, 2 ) ) {
			const char *key = lua_tostring(L, 2);

			if( strcmp(key, LUASQL_MODESTRING) == 0 ) {
				cur_data *cur = getcursor(L);
				lua_pushstring( L, cur->modestring );
			} else
				lua_pushnil(L);
		} else 
			lua_pushnil(L);

	return 1;
}


/*
** Create a new Cursor object and push it on top of the stack.
*/
static int create_cursor (lua_State *L, int conn, MYSQL_RES *result, int cols) {
	cur_data *cur = (cur_data *)lua_newuserdata(L, sizeof(cur_data));
	luasql_setmeta (L, LUASQL_CURSOR_MYSQL);

	/* fill in structure */
	cur->closed = 0;
	cur->conn = LUA_NOREF;
	cur->numcols = cols;
	cur->colnames = LUA_NOREF;
	cur->coltypes = LUA_NOREF;
	cur->my_res = result;
	cur->modestring = "n";
	lua_pushvalue (L, conn);
	cur->conn = luaL_ref (L, LUA_REGISTRYINDEX);

	return 1;
}


/*
** Close a Connection object.
*/
static int conn_close (lua_State *L) {
	conn_data *conn=(conn_data *)luaL_checkudata(L, 1, LUASQL_CONNECTION_MYSQL);
	luaL_argcheck (L, conn != NULL, 1, LUASQL_PREFIX"connection expected");
	if (conn->closed) {
		lua_pushboolean (L, 0);
		return 1;
	}

	/* Nullify structure fields. */
	conn->closed = 1;
	luaL_unref (L, LUA_REGISTRYINDEX, conn->env);
	mysql_close (conn->my_conn);
	lua_pushboolean (L, 1);
	return 1;
}


static int escape_string (lua_State *L) {
  size_t size, new_size;
  conn_data *conn = getconnection (L);
  const char *from = luaL_checklstring(L, 2, &size);
  char *to;
  to = (char*)malloc(sizeof(char) * (2 * size + 1));
  if(to) {
    new_size = mysql_real_escape_string(conn->my_conn, to, from, size);
    lua_pushlstring(L, to, new_size);
    free(to);
    return 1;
  }
  luaL_error(L, "could not allocate escaped string");
  return 0;
}

/*
** Execute an SQL statement.
** Return a Cursor object if the statement is a query, otherwise
** return the number of tuples affected by the statement.
*/
static int conn_execute (lua_State *L) {
	conn_data *conn = getconnection (L);
	const char *statement = luaL_checkstring (L, 2);
	unsigned long st_len = strlen(statement);
	if (mysql_real_query(conn->my_conn, statement, st_len)) 
		/* error executing query */
		return luasql_failmessage(L, "Error executing query. MySQL: ", mysql_error(conn->my_conn));
	else
	{
		MYSQL_RES *res = mysql_store_result(conn->my_conn);
		unsigned int num_cols = mysql_field_count(conn->my_conn);

		if (res) { /* tuples returned */
			return create_cursor (L, 1, res, num_cols);
		}
		else { /* mysql_use_result() returned nothing; should it have? */
			if(num_cols == 0) { /* no tuples returned */
            	/* query does not return data (it was not a SELECT) */
				lua_pushnumber(L, mysql_affected_rows(conn->my_conn));
				return 1;
        	}
			else /* mysql_use_result() should have returned data */
				return luasql_failmessage(L, "Error retrieving result. MySQL: ", mysql_error(conn->my_conn));
		}
	}
}


/*
** Commit the current transaction.
*/
static int conn_commit (lua_State *L) {
	conn_data *conn = getconnection (L);
	lua_pushboolean(L, !mysql_commit(conn->my_conn));
	return 1;
}


/*
** Rollback the current transaction.
*/
static int conn_rollback (lua_State *L) {
	conn_data *conn = getconnection (L);
	lua_pushboolean(L, !mysql_rollback(conn->my_conn));
	return 1;
}


/*
** Set "auto commit" property of the connection. Modes ON/OFF
*/
static void conn_dosetautocommit(lua_State *L, conn_data *conn, int pos){
	if (lua_toboolean (L, pos)) {
		mysql_autocommit(conn->my_conn, 1); /* Set it ON */
		conn->auto_commit = 1;
	}
	else {
		mysql_autocommit(conn->my_conn, 0);
		conn->auto_commit = 0;
	}
}

static int conn_setautocommit (lua_State *L) {
	conn_data *conn = getconnection (L);
	conn_dosetautocommit(L, conn, 2);
	lua_pushboolean(L, 1);
	return 1;
}


/*
** Get Last auto-increment id generated
*/
static int conn_getlastautoid (lua_State *L) {
  conn_data *conn = getconnection(L);
  lua_pushnumber(L, mysql_insert_id(conn->my_conn));
  return 1;
}

/*
 * Sets the connection parameters
 */
static void conn_set(lua_State *L) {
	if( lua_istable( L, 2 ) ) {
		conn_data *conn = getconnection(L);
		char *key;
		lua_pushnil(L);

		while( lua_next(L, 2) != 0 ) {
			if( lua_isstring(L, -2) ) {
				key = lua_tostring(L, -2);

				if( strcmp(key, LUASQL_AUTOCOMMIT) == 0 ) {
					if( lua_isboolean( L, -1 ) )
						conn_dosetautocommit(L, conn, -1);
				}
			}

			lua_pop(L, 1);
		}		
	}
}


/*
 * Retrieve the specified connection parameters
 */
static int conn_get( lua_State *L ) {
	lua_newtable(L);

	if( lua_istable( L, 2 ) ) {
		int rsp = lua_gettop(L);
		conn_data *conn = getconnection(L);
		char *key;
		lua_pushnil(L);

		while( lua_next(L, 2) != 0 ) {
			if( lua_isstring(L, -1) ) {
				key = lua_tostring(L, -1);

				if( strcmp(key, LUASQL_AUTOCOMMIT) == 0 ) {
					lua_pushstring( L, LUASQL_AUTOCOMMIT );
					lua_pushboolean( L, conn->auto_commit );
					lua_settable( L, rsp );
				}
			}

			lua_pop(L, 1);
		}		
	} else
		if( lua_isstring( L, 2 ) ) {
			const char *key = lua_tostring(L, 2);

			if( strcmp(key, LUASQL_AUTOCOMMIT) == 0 ) {
				conn_data *conn = getconnection(L);
				lua_pushboolean( L, conn->auto_commit );
			} else
				lua_pushnil(L);
		} else 
			lua_pushnil(L);

	return 1;
}


/*
** Create a new Connection object and push it on top of the stack.
*/
static int create_connection (lua_State *L, int env, MYSQL *const my_conn) {
	conn_data *conn = (conn_data *)lua_newuserdata(L, sizeof(conn_data));
	luasql_setmeta (L, LUASQL_CONNECTION_MYSQL);

	/* fill in structure */
	conn->closed = 0;
	conn->env = LUA_NOREF;
	conn->my_conn = my_conn;
	conn->auto_commit = 1;
	lua_pushvalue (L, env);
	conn->env = luaL_ref (L, LUA_REGISTRYINDEX);
	return 1;
}


/*
** Connects to a data source.
**     param: one string for each connection parameter, said
**     datasource, username, password, host and port.
*/
static int env_connect (lua_State *L) {
	char *sourcename = NULL;
	char *username = NULL;
	char *password = NULL;
	char *host = NULL;
	int port = 0;
	MYSQL *conn;
	getenvironment(L); /* validade environment */

	if( lua_istable( L, 2 ) ) {
		lua_pushstring( L, LUASQL_SOURCENAME );
		lua_gettable( L, 2 );
		
		if( lua_isstring( L, -1 ) )
			sourcename = lua_tostring( L, -1 );

		lua_pop( L, 1 );		
		lua_pushstring( L, LUASQL_USERNAME );
		lua_gettable( L, 2 );
		
		if( lua_isstring( L, -1 ) )
			username = lua_tostring( L, -1 );

		lua_pop( L, 1 );
		lua_pushstring( L, LUASQL_PASSWORD );
		lua_gettable( L, 2 );
		
		if( lua_isstring( L, -1 ) )
			password = lua_tostring( L, -1 );

		lua_pop( L, 1 );
		lua_pushstring( L, LUASQL_HOSTNAME );
		lua_gettable( L, 2 );
		
		if( lua_isstring( L, -1 ) )
			host = lua_tostring( L, -1 );

		lua_pop( L, 1 );
		lua_pushstring( L, LUASQL_PORT );
		lua_gettable( L, 2 );
		
		if( lua_isnumber( L, -1 ) )
			port = lua_tointeger( L, -1 );

		lua_pop( L, 1 );
	} else {
		sourcename = luaL_checkstring(L, 2);
		username = luaL_optstring(L, 3, NULL);
		password = luaL_optstring(L, 4, NULL);
		host = luaL_optstring(L, 5, NULL);
		port = luaL_optint(L, 6, 0);
	}

	/* Try to init the connection object. */
	conn = mysql_init(NULL);
	if (conn == NULL)
		return luasql_faildirect(L, LUASQL_PREFIX"Error connecting: Out of memory.");

	if (!mysql_real_connect(conn, host, username, password, 
		sourcename, port, NULL, 0))
	{
		char error_msg[100];
		strncpy (error_msg,  mysql_error(conn), 99);
		mysql_close (conn); /* Close conn if connect failed */
		return luasql_failmessage (L, "Error connecting to database. MySQL: ", error_msg);
	}
	return create_connection(L, 1, conn);
}


/*
** Close environment object.
*/
static int env_close (lua_State *L) {
	env_data *env= (env_data *)luaL_checkudata (L, 1, LUASQL_ENVIRONMENT_MYSQL);
	luaL_argcheck (L, env != NULL, 1, LUASQL_PREFIX"environment expected");
	if (env->closed) {
		lua_pushboolean (L, 0);
		return 1;
	}

	env->closed = 1;
	lua_pushboolean (L, 1);
	return 1;
}


/*
 * Sets the environment parameters
 */
static void env_set(lua_State *L) {
	if( lua_istable( L, 2 ) ) {
		env_data *env = getenvironment(L);
		char *key;
		lua_pushnil(L);

		while( lua_next(L, 2) != 0 ) {
			if( lua_isstring(L, -2) ) {
				key = lua_tostring(L, -2);
/*
				if( strcmp(key, LUASQL_LOCKTIMEOUT) == 0 ) {
					if( lua_isnumber( L, -1 ) )
						env->locktimeout = lua_tointeger( L, -1 );
				} */
			}

			lua_pop(L, 1);
		}		
	}
}

/*
 * Retrieve the specified environment parameters
 */
static int env_get(lua_State *L) {
	lua_newtable(L);

	if( lua_istable( L, 2 ) ) {
		int rsp = lua_gettop(L);
		env_data *env = getenvironment(L);
		char *key;
		lua_pushnil(L);

		while( lua_next(L, 2) != 0 ) {
			if( lua_isstring(L, -1) ) {
				key = lua_tostring(L, -1);
/*
				if( strcmp(key, LUASQL_LOCKTIMEOUT) == 0 ) {
					lua_pushstring( L, LUASQL_LOCKTIMEOUT );
					lua_pushinteger( L, env->locktimeout );
					lua_settable( L, rsp );
				} */
			}

			lua_pop(L, 1);
		}		
	} else
		if( lua_isstring( L, 2 ) ) {
			const char *key = lua_tostring(L, 2);
/*
			if( strcmp(key, LUASQL_LOCKTIMEOUT) == 0 ) {
				env_data *env = getenvironment(L);
				lua_pushinteger( L, env->locktimeout );
			} else */
				lua_pushnil(L);
		} else 
			lua_pushnil(L);

	return 1;
}


/*
** Create metatables for each class of object.
*/
static void create_metatables (lua_State *L) {
    struct luaL_reg environment_methods[] = {
        {"__gc", env_close},
        {"close", env_close},
        {"connect", env_connect},
	    {"get", env_get},
	    {"set", env_set},
		{NULL, NULL},
	};
    struct luaL_reg connection_methods[] = {
        {"__gc", conn_close},
        {"close", conn_close},
        {"escape", escape_string},
        {"execute", conn_execute},
        {"commit", conn_commit},
        {"rollback", conn_rollback},
        {"setautocommit", conn_setautocommit},
		{"getlastautoid", conn_getlastautoid},
	    {"get", conn_get},
	    {"set", conn_set},
		{NULL, NULL},
    };
    struct luaL_reg cursor_methods[] = {
        {"__gc", cur_close},
        {"close", cur_close},
        {"getcolnames", cur_getcolnames},
        {"getcoltypes", cur_getcoltypes},
        {"fetch", cur_fetch},
        {"numrows", cur_numrows},
	    {"get", cur_get},
	    {"set", cur_set},
		{NULL, NULL},
    };
	luasql_createmeta (L, LUASQL_ENVIRONMENT_MYSQL, environment_methods);
	luasql_createmeta (L, LUASQL_CONNECTION_MYSQL, connection_methods);
	luasql_createmeta (L, LUASQL_CURSOR_MYSQL, cursor_methods);
	lua_pop (L, 3);
}


/*
** Creates an Environment and returns it.
*/
static int create_environment (lua_State *L) {
	env_data *env = (env_data *)lua_newuserdata(L, sizeof(env_data));
	luasql_setmeta (L, LUASQL_ENVIRONMENT_MYSQL);

	/* fill in structure */
	env->closed = 0;
	return 1;
}


/*
** Creates the metatables for the objects and registers the
** driver open method.
*/
LUASQL_API int luaopen_luasql_mysql (lua_State *L) { 
	struct luaL_reg driver[] = {
		{"mysql", create_environment},
		{NULL, NULL},
	};
	create_metatables (L);
	luaL_openlib (L, LUASQL_TABLENAME, driver, 0);
	luasql_set_info (L);
    lua_pushliteral (L, "_MYSQLVERSION");
    lua_pushliteral (L, MYSQL_SERVER_VERSION);
    lua_settable (L, -3);
	return 1;
}
