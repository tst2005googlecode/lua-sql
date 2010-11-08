/*
** LuaSQL, SQLite driver
** Author: Tiago Dionizio, Eduardo Quintao
** See Copyright Notice in license.html

** $Id: ls_sqlite3.c,v 1.12 2008/06/11 00:26:13 jasonsantos Exp $
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#include "sqlite3.h"

#include "lua.h"
#include "lauxlib.h"
#if ! defined (LUA_VERSION_NUM) || LUA_VERSION_NUM < 501
#include "compat-5.1.h"
#endif


#include "luasql.h"

#define LUASQL_ENVIRONMENT_SQLITE "SQLite3 environment"
#define LUASQL_CONNECTION_SQLITE "SQLite3 connection"
#define LUASQL_CURSOR_SQLITE "SQLite3 cursor"
#define LUASQL_LOCKTIMEOUT "locktimeout"

typedef struct
{
  short       closed;
  int		  locktimeout;
} env_data;


typedef struct
{
  short        closed;
  int          env;                /* reference to environment */
  short        auto_commit;        /* 0 for manual commit */
  unsigned int cur_counter;          
  sqlite3      *sql_conn;
} conn_data;


typedef struct
{
  short       closed;
  int         conn;               /* reference to connection */
  int         numcols;            /* number of columns */
  int         colnames, coltypes; /* reference to column information tables */
  conn_data   *conn_data;         /* reference to connection for cursor */
  sqlite3_stmt  *sql_vm;
  char			*modestring;
} cur_data;

LUASQL_API int luaopen_luasql_sqlite3(lua_State *L);


/*
** Check for valid environment.
*/
static env_data *getenvironment(lua_State *L) {
  env_data *env = (env_data *)luaL_checkudata(L, 1, LUASQL_ENVIRONMENT_SQLITE);
  luaL_argcheck(L, env != NULL, 1, LUASQL_PREFIX"environment expected");
  luaL_argcheck(L, !env->closed, 1, LUASQL_PREFIX"environment is closed");
  return env;
}


/*
** Check for valid connection.
*/
static conn_data *getconnection(lua_State *L) {
  conn_data *conn = (conn_data *)luaL_checkudata (L, 1, LUASQL_CONNECTION_SQLITE);
  luaL_argcheck(L, conn != NULL, 1, LUASQL_PREFIX"connection expected");
  luaL_argcheck(L, !conn->closed, 1, LUASQL_PREFIX"connection is closed");
  return conn;
}


/*
** Check for valid cursor.
*/
static cur_data *getcursor(lua_State *L) {
  cur_data *cur = (cur_data *)luaL_checkudata (L, 1, LUASQL_CURSOR_SQLITE);
  luaL_argcheck(L, cur != NULL, 1, LUASQL_PREFIX"cursor expected");
  luaL_argcheck(L, !cur->closed, 1, LUASQL_PREFIX"cursor is closed");
  return cur;
}

/*
** Finalizes the vm
** Return nil + errmsg or nil in case of sucess
*/
static int finalize(lua_State *L, cur_data *cur) {
  const char *errmsg;
  if (sqlite3_finalize(cur->sql_vm) != SQLITE_OK)
    {
      errmsg = sqlite3_errmsg(cur->conn_data->sql_conn);
      cur->sql_vm = NULL;
      lua_pushnil(L);
      lua_pushliteral(L, LUASQL_PREFIX);
      lua_pushstring(L, errmsg);
      lua_concat(L, 2);
      return 2;
    }
  cur->sql_vm = NULL;
  lua_pushnil(L);
  return 1;
}


static void push_column(lua_State *L, sqlite3_stmt *vm, int column) {
  switch (sqlite3_column_type(vm, column)) {
  case SQLITE_INTEGER:
    lua_pushinteger(L, sqlite3_column_int64(vm, column));
    break;
  case SQLITE_FLOAT:
    lua_pushnumber(L, sqlite3_column_double(vm, column));
    break;
  case SQLITE_TEXT:
    lua_pushlstring(L, sqlite3_column_text(vm, column),
		    sqlite3_column_bytes(vm, column));
    break;
  case SQLITE_BLOB:
    lua_pushlstring(L, sqlite3_column_blob(vm, column),
		    sqlite3_column_bytes(vm, column));
    break;
  case SQLITE_NULL:
    lua_pushnil(L);
    break;
  default:
    luaL_error(L, LUASQL_PREFIX"Unrecognized column type");
    break;
  }
}

/*
** Get another row of the given cursor.
*/
static int cur_fetch (lua_State *L) {
  cur_data *cur = getcursor(L);
  sqlite3_stmt *vm = cur->sql_vm;
  int res;

  if (vm == NULL)
    return 0;

  res = sqlite3_step(vm);

  /* no more results? */
  if (res == SQLITE_DONE)
    return finalize(L, cur);

  if (res != SQLITE_ROW)
    return finalize(L, cur);

  if (lua_istable (L, 2))
    {
      int i;
      const char *opts = luasql_getfetchmodestring( L, cur->modestring );

      if (strchr(opts, 'n') != NULL)
        {
	  /* Copy values to numerical indices */
	  for (i = 0; i < cur->numcols;)
            {
	      push_column(L, vm, i);
	      lua_rawseti(L, 2, ++i);
	    }
        }
      if (strchr(opts, 'a') != NULL)
        {
	  /* Copy values to alphanumerical indices */
	  lua_rawgeti(L, LUA_REGISTRYINDEX, cur->colnames);

	  for (i = 0; i < cur->numcols; i++)
            {
	      lua_rawgeti(L, -1, i+1);
	      push_column(L, vm, i);
	      lua_rawset (L, 2);
	    }
        }
      lua_pushvalue(L, 2);
      return 1; /* return table */
    }
  else
    {
      int i;
      luaL_checkstack (L, cur->numcols, LUASQL_PREFIX"too many columns");
      for (i = 0; i < cur->numcols; ++i)
	push_column(L, vm, i);
      return cur->numcols; /* return #numcols values */
    }
}


/*
** Close the cursor on top of the stack.
** Return 1
*/
static int cur_close(lua_State *L)
{
  conn_data *conn;
  cur_data *cur = (cur_data *)luaL_checkudata(L, 1, LUASQL_CURSOR_SQLITE);
  luaL_argcheck(L, cur != NULL, 1, LUASQL_PREFIX"cursor expected");
  if (cur->closed) {
    lua_pushboolean(L, 0);
    return 1;
  }

  /* Nullify structure fields. */
  cur->closed = 1;
  sqlite3_finalize(cur->sql_vm);
  /* Decrement cursor counter on connection object */
  lua_rawgeti (L, LUA_REGISTRYINDEX, cur->conn);
  conn = lua_touserdata (L, -1);
  conn->cur_counter--;

  luaL_unref(L, LUA_REGISTRYINDEX, cur->conn);
  luaL_unref(L, LUA_REGISTRYINDEX, cur->colnames);
  luaL_unref(L, LUA_REGISTRYINDEX, cur->coltypes);

  lua_pushboolean(L, 1);
  return 1;
}


/*
** Return the list of field names.
*/
static int cur_getcolnames(lua_State *L)
{
  cur_data *cur = getcursor(L);
  lua_rawgeti(L, LUA_REGISTRYINDEX, cur->colnames);
  return 1;
}


/*
** Return the list of field types.
*/
static int cur_getcoltypes(lua_State *L)
{
  cur_data *cur = getcursor(L);
  lua_rawgeti(L, LUA_REGISTRYINDEX, cur->coltypes);
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
/* static int create_cursor(lua_State *L, int conn, sqlite3_stmt *sql_vm,
   int numcols, const char **row, const char **col_info)*/
static int create_cursor(lua_State *L, int o, conn_data *conn, 
			 sqlite3_stmt *sql_vm, int numcols)
{
  int i;
  cur_data *cur = (cur_data*)lua_newuserdata(L, sizeof(cur_data));
  luasql_setmeta (L, LUASQL_CURSOR_SQLITE);

  /* increment cursor count for the connection creating this cursor */
  conn->cur_counter++;

  /* fill in structure */
  cur->closed = 0;
  cur->conn = LUA_NOREF;
  cur->numcols = numcols;
  cur->colnames = LUA_NOREF;
  cur->coltypes = LUA_NOREF;
  cur->sql_vm = sql_vm;
  cur->conn_data = conn;
  cur->modestring = "n";

  lua_pushvalue(L, o);
  cur->conn = luaL_ref(L, LUA_REGISTRYINDEX);

  /* create table with column names */
  lua_newtable(L);
  for (i = 0; i < numcols;)
    {
      lua_pushstring(L, sqlite3_column_name(sql_vm, i));
      lua_rawseti(L, -2, ++i);
    }
  cur->colnames = luaL_ref(L, LUA_REGISTRYINDEX);
	
  /* create table with column types */
  lua_newtable(L);
  for (i = 0; i < numcols;)
    {
      lua_pushstring(L, sqlite3_column_decltype(sql_vm, i));
      lua_rawseti(L, -2, ++i);
    }
  cur->coltypes = luaL_ref(L, LUA_REGISTRYINDEX);

  return 1;
}


/*
** Close a Connection object.
*/
static int conn_close(lua_State *L)
{
  conn_data *conn = (conn_data *)luaL_checkudata(L, 1, LUASQL_CONNECTION_SQLITE);
  luaL_argcheck (L, conn != NULL, 1, LUASQL_PREFIX"connection expected");
  if (conn->closed) 
    {
      lua_pushboolean(L, 0);
      return 1;
    }

  if (conn->cur_counter > 0)
    return luaL_error (L, LUASQL_PREFIX"there are open cursors");

  /* Nullify structure fields. */
  conn->closed = 1;
  luaL_unref(L, LUA_REGISTRYINDEX, conn->env);
  sqlite3_close(conn->sql_conn);
  lua_pushboolean(L, 1);
  return 1;
}

static int conn_escape(lua_State *L)
{
  const char *from = luaL_checklstring (L, 2, 0);
  char *escaped = sqlite3_mprintf("%q", from);
  if (escaped == NULL) 
    {
      lua_pushnil(L);
    } 
  else
    {
      lua_pushstring(L, escaped);        
      sqlite3_free(escaped);
    }
  return 1;
}

/*
** Execute an SQL statement.
** Return a Cursor object if the statement is a query, otherwise
** return the number of tuples affected by the statement.
*/
static int conn_execute(lua_State *L)
{
  conn_data *conn = getconnection(L);
  const char *statement = luaL_checkstring(L, 2);
  int res;
  sqlite3_stmt *vm;
  const char *errmsg;
  int numcols;
  const char *tail;

  res = sqlite3_prepare(conn->sql_conn, statement, -1, &vm, &tail);
  if (res != SQLITE_OK)
    {
      errmsg = sqlite3_errmsg(conn->sql_conn);
      lua_pushnil(L);
      lua_pushliteral(L, LUASQL_PREFIX);
      lua_pushstring(L, errmsg);
      lua_concat(L, 2);
      return 2;
    }

  /* process first result to retrive query information and type */
  res = sqlite3_step(vm);
  numcols = sqlite3_column_count(vm);

  /* real query? if empty, must have numcols!=0 */
  if ((res == SQLITE_ROW) || ((res == SQLITE_DONE) && numcols))
    {
      sqlite3_reset(vm);
      return create_cursor(L, 1, conn, vm, numcols);
    }

  if (res == SQLITE_DONE) /* and numcols==0, INSERT,UPDATE,DELETE statement */
    {
      sqlite3_finalize(vm);
      /* return number of columns changed */
      lua_pushnumber(L, sqlite3_changes(conn->sql_conn));
      return 1;
    }

  /* error */
  errmsg = sqlite3_errmsg(conn->sql_conn);
  sqlite3_finalize(vm);
  lua_pushnil(L);
  lua_pushliteral(L, LUASQL_PREFIX);
  lua_pushstring(L, errmsg);
  lua_concat(L, 2);
  return 2;
}


/*
** Commit the current transaction.
*/
static int conn_commit(lua_State *L)
{
  char *errmsg;
  conn_data *conn = getconnection(L);
  int res;
  const char *sql = "COMMIT";

  if (conn->auto_commit == 0) sql = "COMMIT;BEGIN";

  res = sqlite3_exec(conn->sql_conn, sql, NULL, NULL, &errmsg);

  if (res != SQLITE_OK)
    {
      lua_pushnil(L);
      lua_pushliteral(L, LUASQL_PREFIX);
      lua_pushstring(L, errmsg);
      sqlite3_free(errmsg);
      lua_concat(L, 2);
      return 2;
    }
  lua_pushboolean(L, 1);
  return 1;
}


/*
** Rollback the current transaction.
*/
static int conn_rollback(lua_State *L)
{
  char *errmsg;
  conn_data *conn = getconnection(L);
  int res;
  const char *sql = "ROLLBACK";

  if (conn->auto_commit == 0) sql = "ROLLBACK;BEGIN";

  res = sqlite3_exec(conn->sql_conn, sql, NULL, NULL, &errmsg);
  if (res != SQLITE_OK)
    {
      lua_pushnil(L);
      lua_pushliteral(L, LUASQL_PREFIX);
      lua_pushstring(L, errmsg);
      sqlite3_free(errmsg);
      lua_concat(L, 2);
      return 2;
    }
  lua_pushboolean(L, 1);
  return 1;
}

static int conn_getlastautoid(lua_State *L)
{
  conn_data *conn = getconnection(L);
  lua_pushnumber(L, sqlite3_last_insert_rowid(conn->sql_conn));
  return 1;
}

/*
** Set "auto commit" property of the connection.
** If 'true', then rollback current transaction.
** If 'false', then start a new transaction.
*/
static void conn_dosetautocommit(lua_State *L, conn_data *conn, int pos) {
	if (lua_toboolean(L, pos))
	{
		conn->auto_commit = 1;
        /* undo active transaction - ignore errors */
	    sqlite3_exec(conn->sql_conn, "ROLLBACK", NULL, NULL, NULL);
	}
	else
	{
		char *errmsg;
	    int res;
	    conn->auto_commit = 0;
	    res = sqlite3_exec(conn->sql_conn, "BEGIN", NULL, NULL, &errmsg);

	    if (res != SQLITE_OK) {
	    	lua_pushliteral(L, LUASQL_PREFIX);
	    	lua_pushstring(L, errmsg);
	    	sqlite3_free(errmsg);
	    	lua_concat(L, 2);
	    	lua_error(L);
	    }
	}
}


static int conn_setautocommit(lua_State *L)
{
  conn_data *conn = getconnection(L);
  conn_dosetautocommit(L, conn, 2);
  lua_pushboolean(L, 1);
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
static int create_connection(lua_State *L, int env, sqlite3 *sql_conn)
{
  conn_data *conn = (conn_data*)lua_newuserdata(L, sizeof(conn_data));
  luasql_setmeta(L, LUASQL_CONNECTION_SQLITE);

  /* fill in structure */
  conn->closed = 0;
  conn->env = LUA_NOREF;
  conn->auto_commit = 1;
  conn->sql_conn = sql_conn;
  conn->cur_counter = 0;
  lua_pushvalue (L, env);
  conn->env = luaL_ref (L, LUA_REGISTRYINDEX);
  return 1;
}


/*
** Connects to a data source.
*/
static int env_connect(lua_State *L)
{
  char *sourcename = NULL;
  sqlite3 *conn;
  const char *errmsg;
  int res;
  env_data *env = getenvironment(L);  /* validate environment */
  int time_out = env->locktimeout;
    
  if( lua_istable( L, 2 ) ) {
		lua_pushstring( L, LUASQL_SOURCENAME );
		lua_gettable( L, 2 );
		
		if( lua_isstring( L, -1 ) )
			sourcename = lua_tostring( L, -1 );

		lua_pop( L, 1 );
		lua_pushstring( L, LUASQL_LOCKTIMEOUT );
		lua_gettable( L, 2 );
		
		if( lua_isnumber( L, -1 ) )
			time_out = lua_tointeger( L, -1 );

		lua_pop( L, 1 );
  } else {
	  sourcename = luaL_checkstring(L, 2);

	  if( lua_isnumber(L, 3) )
		  time_out = lua_tointeger(L, 3);
  }
  
  res = sqlite3_open(sourcename, &conn);
  if (res != SQLITE_OK)
    {
      errmsg = sqlite3_errmsg(conn);
      lua_pushnil(L);
      lua_pushliteral(L, LUASQL_PREFIX);
      lua_pushstring(L, errmsg);
      lua_concat(L, 2);
      sqlite3_close(conn);
      return 2;
    }
  
  if (time_out > -1) {
  	sqlite3_busy_timeout(conn, time_out);
  }
  
  return create_connection(L, 1, conn);
}


/*
** Close environment object.
*/
static int env_close (lua_State *L)
{
  env_data *env = (env_data *)luaL_checkudata(L, 1, LUASQL_ENVIRONMENT_SQLITE);
  luaL_argcheck(L, env != NULL, 1, LUASQL_PREFIX"environment expected");
  if (env->closed) {
    lua_pushboolean(L, 0);
    return 1;
  }

  env->closed = 1;
  lua_pushboolean(L, 1);
  return 1;
}


/*
** Sets the timeout for a lock in the connection.
*/
static int opts_settimeout  (lua_State *L)
{
	conn_data *conn = getconnection(L);
	int milisseconds = luaL_checknumber(L, 2);
	lua_pushnumber(L, sqlite3_busy_timeout(conn, milisseconds));
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

				if( strcmp(key, LUASQL_LOCKTIMEOUT) == 0 ) {
					if( lua_isnumber( L, -1 ) )
						env->locktimeout = lua_tointeger( L, -1 );
				}
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

				if( strcmp(key, LUASQL_LOCKTIMEOUT) == 0 ) {
					lua_pushstring( L, LUASQL_LOCKTIMEOUT );
					lua_pushinteger( L, env->locktimeout );
					lua_settable( L, rsp );
				}
			}

			lua_pop(L, 1);
		}		
	} else
		if( lua_isstring( L, 2 ) ) {
			const char *key = lua_tostring(L, 2);

			if( strcmp(key, LUASQL_LOCKTIMEOUT) == 0 ) {
				env_data *env = getenvironment(L);
				lua_pushinteger( L, env->locktimeout );
			} else
				lua_pushnil(L);
		} else 
			lua_pushnil(L);

	return 1;
}


/*
** Create metatables for each class of object.
*/
static void create_metatables (lua_State *L)
{
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
    {"escape", conn_escape},
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
    {"get", cur_get},
    {"set", cur_set},
    {NULL, NULL},
  };
  luasql_createmeta(L, LUASQL_ENVIRONMENT_SQLITE, environment_methods);
  luasql_createmeta(L, LUASQL_CONNECTION_SQLITE, connection_methods);
  luasql_createmeta(L, LUASQL_CURSOR_SQLITE, cursor_methods);
  lua_pop (L, 3);
}

/*
** Creates an Environment and returns it.
*/
static int create_environment (lua_State *L)
{
  env_data *env = (env_data *)lua_newuserdata(L, sizeof(env_data));
  luasql_setmeta(L, LUASQL_ENVIRONMENT_SQLITE);

  /* fill in structure */
  env->closed = 0;
  env->locktimeout = 100; 
  return 1;
}


/*
** Creates the metatables for the objects and registers the
** driver open method.
*/
LUASQL_API int luaopen_luasql_sqlite3(lua_State *L)
{
  struct luaL_reg driver[] = {
    {"sqlite3", create_environment},
    {NULL, NULL},
  };
  create_metatables (L);
  luaL_openlib (L, LUASQL_TABLENAME, driver, 0);
  luasql_set_info (L);
  return 1;
}
