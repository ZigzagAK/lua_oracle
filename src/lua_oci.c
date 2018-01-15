/*
** LuaSQL, Oracle driver
** Authors: Tomas Guisasola, Leonardo Godinho, Aleksey Konovkin
** See Copyright Notice in license.html
** $Id: ls_oci8.c,v 1.31 2009/02/07 23:16:23 tomas Exp $
*/

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <pthread.h>
#include <inttypes.h>

#include "oci.h"
#include "oratypes.h"
#include "ociapr.h"
#include "ocidem.h"

#include "lua.h"
#include "lauxlib.h"

#include "luasql.h"

#ifdef _WITH_INT64
#include "lua_int64.h"
#endif


LUASQL_API int
luaopen_luasql_oci8 (lua_State *L);


#define LUASQL_ENVIRONMENT_OCI8 "Oracle environment"
#define LUASQL_CONNECTION_OCI8  "Oracle connection"
#define LUASQL_CURSOR_OCI8      "Oracle cursor"


typedef struct {
    short           closed;
    int             conn_counter;
    OCIEnv         *envhp;
    OCIError       *errhp;
    pthread_mutex_t mtx;
} env_data;


typedef struct {
    short         closed;
    short         auto_commit;        /* 0 for manual commit */
    int           cur_counter;
    env_data     *env;                /* reference to environment */
    OCISvcCtx    *svchp;
    OCIServer    *srvhp;
    OCISession   *authp;
    OCIError     *errhp;
    pthread_t     tid;                /* connection create thread */
    volatile int  connecting;
    char          username[256];
    char          password[256];
    char          sourcename[256];
} conn_data;


typedef union {
    char        *text;
    double       dbl;
    OCIDateTime *date;
    OCINumber    ociNumber;
    int64_t      i64;
    uint64_t     u64;
} column_value;


typedef struct {
    ub2           type;    /* database type */
    text         *name;    /* column name */
    ub4           namelen; /* column name length */
    ub2           max;     /* maximum size */
    sb2           null;    /* is null? */
    OCIDefine    *define;  /* define handle */
    column_value  val;
} column_data;


typedef struct {
    short         closed;
    conn_data    *conn;               /* reference to connection */
    int           numcols;            /* number of columns */
    int           colnames;           /* luaref */
    int           coltypes;           /* luaref */
    int           columns;            /* luaref */
    char         *text;               /* text of SQL statement */
    OCIStmt      *stmthp;             /* statement handle */
    OCIError     *errhp;
    column_data  *cols;               /* array of columns */
} cur_data;


/*
** Raise on OCI error.
*/
static int
ASSERT_OCI (lua_State *L, sword status, OCIError *errhp) {
    switch (status) {
        case OCI_SUCCESS:
            return 0;

        case OCI_SUCCESS_WITH_INFO:
        	return 0;

        case OCI_NEED_DATA:
            return luaL_error (L, LUASQL_PREFIX"OCI_NEED_DATA");

        case OCI_NO_DATA:
            return luaL_error (L, LUASQL_PREFIX"OCI_NODATA");

        case OCI_ERROR: {
            text errbuf[512];
            sb4 errcode = 0;
            OCIErrorGet (errhp, (ub4) 1, (text *) NULL, &errcode,
                errbuf, (ub4) sizeof (errbuf), OCI_HTYPE_ERROR);
            return luaL_error (L, LUASQL_PREFIX"%s", errbuf);
        }

        case OCI_INVALID_HANDLE:
            return luaL_error (L, LUASQL_PREFIX"OCI_INVALID_HANDLE");

        case OCI_STILL_EXECUTING:
            return luaL_error (L, LUASQL_PREFIX"OCI_STILL_EXECUTE");

        case OCI_CONTINUE:
            return luaL_error (L, LUASQL_PREFIX"OCI_CONTINUE");

        default:
            break;
    }

    return luaL_error (L, LUASQL_PREFIX"CODE=%d", status);
}


/*
** Raise on NULL.
*/
static int
ASSERT_PTR (lua_State *L, void *p) {
    if (p == NULL)
        return luaL_error (L, LUASQL_PREFIX"no memory");
    return 0;
}


/*
** Check for valid environment.
*/
static env_data *
getenvironment (lua_State *L) {
    env_data *env = (env_data *)luaL_checkudata (L, 1, LUASQL_ENVIRONMENT_OCI8);
    luaL_argcheck (L, env != NULL, 1, LUASQL_PREFIX"environment expected");
    luaL_argcheck (L, !env->closed, 1, LUASQL_PREFIX"environment is closed");
    return env;
}


/*
** Check for valid connection.
*/
static conn_data *
getconnection (lua_State *L) {
    conn_data *conn = (conn_data *)luaL_checkudata (L, 1, LUASQL_CONNECTION_OCI8);
    luaL_argcheck (L, conn != NULL, 1, LUASQL_PREFIX"connection expected");
    luaL_argcheck (L, !conn->closed, 1, LUASQL_PREFIX"connection is closed");
    return conn;
}


/*
** Check for valid cursor.
*/
static cur_data *
getcursor (lua_State *L) {
    cur_data *cur = (cur_data *)luaL_checkudata (L, 1, LUASQL_CURSOR_OCI8);
    luaL_argcheck (L, cur != NULL, 1, LUASQL_PREFIX"cursor expected");
    luaL_argcheck (L, !cur->closed, 1, LUASQL_PREFIX"cursor is closed");
    return cur;
}


/*
** Copy the column name to the column structure and convert it to lower case.
*/
static int
copy_column_name (lua_State *L, column_data *col, text *name) {
    unsigned int i;
    col->name = (text *)strndup ((const char *) name, col->namelen);
    ASSERT_PTR (L, col->name);
    for (i = 0; i < col->namelen; i++)
        col->name[i] = tolower (col->name[i]);
    return 0;
}


/*
** Alloc buffers for column values.
*/
static int
alloc_column_buffer (lua_State *L, cur_data *cur, int i) {
    /* column index ranges from 1 to numcols */
    /* C array index ranges from 0 to numcols-1 */
    column_data *col = &(cur->cols[i-1]);
    OCIParam *param;
    text *name;

    ASSERT_OCI (L, OCIParamGet (cur->stmthp, OCI_HTYPE_STMT, cur->errhp,
        (dvoid **)&param, i), cur->errhp);
    ASSERT_OCI (L, OCIAttrGet (param, OCI_DTYPE_PARAM,
        (dvoid *)&(name), (ub4 *)&(col->namelen),
        OCI_ATTR_NAME, cur->errhp), cur->errhp);
    ASSERT_OCI (L, OCIAttrGet (param, OCI_DTYPE_PARAM,
        (dvoid *)&(col->type), (ub4 *)0, OCI_ATTR_DATA_TYPE,
        cur->errhp), cur->errhp);

    copy_column_name (L, col, name);

    switch (col->type) {
        case SQLT_CHR:
        case SQLT_STR:
        case SQLT_VCS:
        case SQLT_AFC:
        case SQLT_AVC:
            ASSERT_OCI (L, OCIAttrGet (param, OCI_DTYPE_PARAM,
                (dvoid *)&(col->max), 0, OCI_ATTR_DATA_SIZE,
                cur->errhp), cur->errhp);
            col->val.text = calloc (col->max + 1, sizeof(col->val.text));
            ASSERT_PTR (L, col->val.text);
            ASSERT_OCI (L, OCIDefineByPos (cur->stmthp, &(col->define),
                cur->errhp, (ub4)i, col->val.text, col->max+1,
                SQLT_STR /*col->type*/, (dvoid *)&(col->null), (ub2 *)0,
                (ub2 *)0, (ub4) OCI_DEFAULT), cur->errhp);
            {
                static ub2 UTF8 = 871; // SELECT NLS_CHARSET_ID('UTF8') FROM DUAL;
                ASSERT_OCI (L, OCIAttrSet( (dvoid *)col->define,
                    (ub4)OCI_HTYPE_DEFINE, (void *)&UTF8, (ub4)0, (ub4)OCI_ATTR_CHARSET_ID, cur->errhp), cur->errhp);
            }
            break;

        case SQLT_FLT:
            ASSERT_OCI (L, OCIDefineByPos (cur->stmthp, &(col->define),
                cur->errhp, (ub4)i, &(col->val.dbl), sizeof(col->val.dbl),
                SQLT_FLT, (dvoid *)&(col->null), (ub2 *)0,
                (ub2 *)0, (ub4) OCI_DEFAULT), cur->errhp);
            break;

        case SQLT_INT:
            ASSERT_OCI (L, OCIDefineByPos (cur->stmthp, &(col->define),
                cur->errhp, (ub4)i, &(col->val.i64), sizeof(col->val.i64),
                SQLT_INT, (dvoid *)&(col->null), (ub2 *)0,
                (ub2 *)0, (ub4) OCI_DEFAULT), cur->errhp);
            break;

        case SQLT_UIN:
            ASSERT_OCI (L, OCIDefineByPos (cur->stmthp, &(col->define),
                cur->errhp, (ub4)i, &(col->val.u64), sizeof(col->val.u64),
                SQLT_UIN, (dvoid *)&(col->null), (ub2 *)0,
                (ub2 *)0, (ub4) OCI_DEFAULT), cur->errhp);
            break;

        case SQLT_NUM:
        case SQLT_VNU:
            memset(col->val.ociNumber.OCINumberPart, 0, OCI_NUMBER_SIZE);
            ASSERT_OCI (L, OCIDefineByPos (cur->stmthp, &(col->define),
                cur->errhp, (ub4)i, col->val.ociNumber.OCINumberPart, OCI_NUMBER_SIZE,
                SQLT_VNU, (dvoid *)&(col->null), (ub2 *)0,
                (ub2 *)0, (ub4) OCI_DEFAULT), cur->errhp);
            break;

        case SQLT_DAT:
        case SQLT_TIMESTAMP:
        case SQLT_TIMESTAMP_TZ:
        case SQLT_TIMESTAMP_LTZ:
            ASSERT_OCI (L, OCIDescriptorAlloc(cur->conn->env->envhp, (dvoid *)&(col->val.date),
                    OCI_DTYPE_TIMESTAMP, (size_t)0, (dvoid **)0), cur->errhp);
            ASSERT_OCI (L, OCIDefineByPos (cur->stmthp, &(col->define),
                cur->errhp, (ub4)i, &(col->val.date), sizeof(OCIDateTime*),
                SQLT_TIMESTAMP, (dvoid *)&(col->null), (ub2 *)0,
                (ub2 *)0, (ub4) OCI_DEFAULT), cur->errhp);
            break;

        case SQLT_CLOB:
            ASSERT_OCI (L, OCIDescriptorAlloc (cur->conn->env->envhp, (dvoid *)&(col->val.text),
                OCI_DTYPE_LOB, (size_t)0, (dvoid **)0), cur->errhp);
            ASSERT_OCI (L, OCIDefineByPos (cur->stmthp, &(col->define),
                cur->errhp, (ub4)i, &(col->val.text), (sb4)sizeof(dvoid *),
                SQLT_CLOB, (dvoid *)&(col->null), (ub2 *)0, (ub2 *)0,
                OCI_DEFAULT), cur->errhp);
            break;

        default:
            return luaL_error (L, LUASQL_PREFIX"invalid type %d #%d", col->type, i);
    }

    return 0;
}


/*
** Deallocate column buffers.
*/
static int
free_column_buffers (lua_State *L, cur_data *cur, int i) {
    /* column index ranges from 1 to numcols */
    /* C array index ranges from 0 to numcols-1 */
    column_data *col = &(cur->cols[i-1]);
    if (col->name)
        free (col->name);

    switch (col->type) {
        case SQLT_FLT:
        case SQLT_NUM:
        case SQLT_VNU:
        case SQLT_UIN:
        case SQLT_INT:
            break;

        case SQLT_CHR:
        case SQLT_STR:
        case SQLT_VCS:
        case SQLT_AFC:
        case SQLT_AVC:
            if (col->val.text)
                free(col->val.text);
            break;

        case SQLT_DAT:
        case SQLT_TIMESTAMP:
        case SQLT_TIMESTAMP_TZ:
        case SQLT_TIMESTAMP_LTZ:
            if (col->val.date)
                OCIDescriptorFree (col->val.date, OCI_DTYPE_TIMESTAMP);
            break;
        case SQLT_CLOB:
            if (col->val.text)
                OCIDescriptorFree (col->val.text, OCI_DTYPE_LOB);
            break;
        default:
            break;
    }
    return 0;
}

/*
** Push a value on top of the stack.
*/
static int
pushvalue (lua_State *L, cur_data *cur, int i) {
    /* column index ranges from 1 to numcols */
    /* C array index ranges from 0 to numcols-1 */
    column_data *col = &(cur->cols[i-1]);
    if (col->null) {
        /* Oracle NULL => Lua nil */
        lua_pushnil (L);
        return 1;
    }

    switch (col->type) {

#ifdef _WITH_INT64

        case SQLT_INT:
            lua_pushinteger64(L, col->val.i64);
            break;

        case SQLT_UIN:
            lua_pushunsigned64(L, col->val.u64);
            break;

        case SQLT_NUM:
        case SQLT_VNU: {
            static int64_t z = 0;
            OCINumber zero;
            boolean isint;
            sword flag;
            ASSERT_OCI (L, OCINumberIsInt(cur->errhp, &col->val.ociNumber, &isint), cur->errhp);
            if (isint) {
                ASSERT_OCI (L, OCINumberFromInt(cur->errhp, &z, sizeof(z), OCI_NUMBER_SIGNED, &zero), cur->errhp);
                ASSERT_OCI (L, OCINumberCmp(cur->errhp, &col->val.ociNumber, &zero, &flag), cur->errhp);
                if (flag >= 0) {
                    ASSERT_OCI (L, OCINumberToInt(cur->errhp,
                        &col->val.ociNumber, sizeof(uint64_t), OCI_NUMBER_UNSIGNED, &col->val.u64), cur->errhp);
                    lua_pushunsigned64(L, col->val.u64);
                } else {
                    ASSERT_OCI (L, OCINumberToInt(cur->errhp,
                        &col->val.ociNumber, sizeof(int64_t), OCI_NUMBER_SIGNED, &col->val.i64), cur->errhp);
                    lua_pushinteger64(L, col->val.i64);
                }
            } else {
                ASSERT_OCI (L, OCINumberToReal(cur->errhp, &col->val.ociNumber, sizeof(double), &col->val.dbl), cur->errhp);
                lua_pushnumber(L, col->val.dbl);
            }
            break;
        }

#else

        case SQLT_INT:
            lua_pushnumber(L, col->val.i64);
            break;

        case SQLT_UIN:
            lua_pushnumber(L, col->val.u64);
            break;

        case SQLT_NUM:
        case SQLT_VNU:
            ASSERT_OCI (L, OCINumberToReal(cur->errhp, &col->val.ociNumber, sizeof(double), &col->val.dbl), cur->errhp);
            lua_pushnumber(L, col->val.dbl);
        	break;

#endif

        case SQLT_FLT:
            lua_pushnumber (L, col->val.dbl);
            break;

        case SQLT_CHR:
        case SQLT_STR:
        case SQLT_VCS:
        case SQLT_AFC:
        case SQLT_AVC:
            lua_pushstring (L, (char *)(col->val.text));
            break;

        case SQLT_DAT:
        case SQLT_TIMESTAMP:
        case SQLT_TIMESTAMP_TZ:
        case SQLT_TIMESTAMP_LTZ: {
            sb2 year;
            ub1 month, day, hour, min, sec;
            ub4 fsec;
            ASSERT_OCI (L, OCIDateTimeGetDate(cur->conn->env->envhp, cur->errhp,
                col->val.date, &year, &month, &day), cur->errhp);
            ASSERT_OCI (L, OCIDateTimeGetTime(cur->conn->env->envhp, cur->errhp,
                col->val.date, &hour, &min, &sec, &fsec), cur->errhp);

            lua_createtable(L, 0, 7);

            lua_pushliteral(L, "year");
            lua_pushnumber(L, year);
            lua_rawset(L, -3);

            lua_pushliteral(L, "month");
            lua_pushnumber(L, month);
            lua_rawset(L, -3);

            lua_pushliteral(L, "day");
            lua_pushnumber(L, day);
            lua_rawset(L, -3);

            lua_pushliteral(L, "hour");
            lua_pushnumber(L, hour);
            lua_rawset(L, -3);

            lua_pushliteral(L, "min");
            lua_pushnumber(L, min);
            lua_rawset(L, -3);

            lua_pushliteral(L, "sec");
            lua_pushnumber(L, sec);
            lua_rawset(L, -3);

            lua_pushliteral(L, "fsec");
            lua_pushnumber(L, fsec);
            lua_rawset(L, -3);

            break;
        }

        case SQLT_CLOB: {
            ub4 lob_len;
            ASSERT_OCI (L, OCILobGetLength (cur->conn->svchp, cur->errhp,
                (OCILobLocator *)col->val.text, &lob_len), cur->errhp);
            if (lob_len > 0) {
                char *lob_buffer = malloc(lob_len);
                ASSERT_PTR (L, lob_buffer);
                ub4 amount = lob_len;
                ASSERT_OCI (L, OCILobRead(cur->conn->svchp, cur->errhp,
                    (OCILobLocator *) col->val.text, &amount, (ub4) 1,
                    (dvoid *) lob_buffer, (ub4) lob_len, (dvoid *)0,
                    (sb4 (*) (dvoid *, CONST dvoid *, ub4, ub1)) 0,
                    (ub2) 0, (ub1) SQLCS_IMPLICIT), cur->errhp);
                lua_pushlstring (L, lob_buffer, amount);
                free(lob_buffer);
            } else
                lua_pushstring (L, "");
            break;
        }
        default:
            luaL_error (L, LUASQL_PREFIX"unexpected error");
    }
    return 1;
}


/*
** Close the cursor on top of the stack.
** Return 1
*/
static int
cur_close (lua_State *L) {
    int i;
    cur_data *cur = (cur_data *)luaL_checkudata (L, 1, LUASQL_CURSOR_OCI8);
    luaL_argcheck (L, cur != NULL, 1, LUASQL_PREFIX"cursor expected");
    if (cur->closed) {
        lua_pushboolean (L, 0);
        return 1;
    }

    /* Deallocate buffers. */
    for (i = 1; i <= cur->numcols; i++)
        free_column_buffers (L, cur, i);
    if (cur->cols)
        free (cur->cols);
    if (cur->text)
        free (cur->text);

    /* Nullify structure fields. */
    if (cur->stmthp)
        OCIHandleFree ((dvoid *)cur->stmthp, OCI_HTYPE_STMT);
    if (cur->errhp)
        OCIHandleFree ((dvoid *)cur->errhp, OCI_HTYPE_ERROR);

    luaL_unref (L, LUA_REGISTRYINDEX, cur->colnames);
    luaL_unref (L, LUA_REGISTRYINDEX, cur->coltypes);
    luaL_unref (L, LUA_REGISTRYINDEX, cur->columns);

    cur->closed = 1;
    cur->colnames = LUA_NOREF;
    cur->coltypes = LUA_NOREF;
    cur->columns = LUA_NOREF;

    lua_pushboolean (L, 1);

    /* Decrement cursor counter on connection object */
    cur->conn->cur_counter--;

    return 1;
}


/*
** Get another row of the given cursor.
*/
static int
cur_fetch (lua_State *L) {
    cur_data *cur = getcursor (L);
    sword status = OCIStmtFetch (cur->stmthp, cur->errhp, 1,
        OCI_FETCH_NEXT, OCI_DEFAULT);

    if (status == OCI_STILL_EXECUTING) {
        lua_pushnil(L);
        lua_pushinteger(L, OCI_STILL_EXECUTING);
        return 2;
    }

    if (status == OCI_NO_DATA) {
        /* No more rows */
        cur_close (L);
        lua_pop (L, 1);
        lua_pushnil (L);
        return 1;
    } else if (status != OCI_SUCCESS) {
        /* Error */
        ASSERT_OCI (L, status, cur->errhp);
    }

    if (lua_istable (L, 2)) {
        int i;
        const char *opts = luaL_optstring (L, 3, "n");
        if (strchr (opts, 'n') != NULL)
            /* Copy values to numerical indices */
            for (i = 1; i <= cur->numcols; i++) {
                int ret = pushvalue (L, cur, i);
                if (ret != 1)
                    return ret;
                lua_rawseti (L, 2, i);
            }
        if (strchr (opts, 'a') != NULL)
            /* Copy values to alphanumerical indices */
            for (i = 1; i <= cur->numcols; i++) {
                column_data *col = &(cur->cols[i-1]);
                int ret;
                lua_pushlstring (L, (char *) col->name, col->namelen);
                if ((ret = pushvalue (L, cur, i)) != 1)
                    return ret;
                lua_rawset (L, 2);
            }
        lua_pushvalue(L, 2);
        return 1; /* return table */
    }
    else {
        int i;
        luaL_checkstack (L, cur->numcols, LUASQL_PREFIX"too many columns");
        for (i = 1; i <= cur->numcols; i++) {
            int ret = pushvalue (L, cur, i);
            if (ret != 1)
                return ret;
        }
        return cur->numcols; /* return #numcols values */
    }
}


/*
** Return the list of field names as a table on top of the stack.
*/
static int
cur_getcolnames (lua_State *L) {
    cur_data *cur = getcursor (L);
    if (cur->colnames != LUA_NOREF)
        lua_rawgeti (L, LUA_REGISTRYINDEX, cur->colnames);
    else {
        int i;
        lua_newtable (L);
        for (i = 1; i <= cur->numcols; i++) {
            column_data *col = &(cur->cols[i-1]);
            lua_pushlstring (L, (char *) col->name, col->namelen);
            lua_rawseti (L, -2, i);
        }
        lua_pushvalue (L, -1);
        cur->colnames = luaL_ref (L, LUA_REGISTRYINDEX);
    }
    return 1;
}


/*
**
*/
static char *
getcolumntype (column_data *col) {
    switch (col->type) {
        case SQLT_CHR:
        case SQLT_STR:
        case SQLT_VCS:
        case SQLT_AFC:
        case SQLT_AVC:
            return "string";

#ifdef _WITH_INT64
        case SQLT_FLT:
            return "double";

        case SQLT_INT:
            return "integer";

        case SQLT_UIN:
            return "unsigned integer";

        case SQLT_NUM:
        case SQLT_VNU:
            return "number";
#else
        case SQLT_FLT:
        case SQLT_INT:
        case SQLT_UIN:
        case SQLT_NUM:
        case SQLT_VNU:
            return "number";
#endif

        case SQLT_DAT:
            return "datetime";

        case SQLT_TIMESTAMP:
        case SQLT_TIMESTAMP_TZ:
        case SQLT_TIMESTAMP_LTZ:
            return "timestamp";

        case SQLT_CLOB:
            return "string";

        default:
            return "unknown";
    }
}


/*
** Return the list of field types as a table on top of the stack.
*/
static int
cur_getcoltypes (lua_State *L) {
    cur_data *cur = getcursor (L);
    if (cur->coltypes != LUA_NOREF)
        lua_rawgeti (L, LUA_REGISTRYINDEX, cur->coltypes);
    else {
        int i;
        lua_newtable (L);
        for (i = 1; i <= cur->numcols; i++) {
            column_data *col = &(cur->cols[i-1]);
            lua_pushnumber (L, i);
            lua_pushstring (L, getcolumntype (col));
            lua_rawset (L, -3);
        }
        lua_pushvalue (L, -1);
        cur->coltypes = luaL_ref (L, LUA_REGISTRYINDEX);
    }
    return 1;
}


/*
** Return the map by field names with description.
*/
static int
cur_getcolumns (lua_State *L) {
    cur_data *cur = getcursor (L);
    if (cur->columns != LUA_NOREF)
        lua_rawgeti (L, LUA_REGISTRYINDEX, cur->columns);
    else {
        int i;
        lua_createtable(L, 0, cur->numcols);
        for (i = 1; i <= cur->numcols; i++) {
            column_data *col = &(cur->cols[i-1]);

            lua_pushlstring (L, (char *) col->name, col->namelen);

            lua_createtable(L, 0, 2);

            lua_pushliteral (L, "type");
            lua_pushstring (L, getcolumntype (col));
            lua_rawset(L, -3);

            lua_pushliteral (L, "maxsize");
            lua_pushinteger (L, col->max);
            lua_rawset(L, -3);

            lua_rawset(L, -3);
        }
        lua_pushvalue (L, -1);
        cur->columns = luaL_ref (L, LUA_REGISTRYINDEX);
    }
    return 1;
}


/*
** Push the number of rows.
*/
static int
cur_numrows (lua_State *L) {
    return luaL_error(L, LUASQL_PREFIX"unimplemented");
}


/*
** Close a Connection object.
*/
static int
conn_close (lua_State *L) {
    conn_data *conn = (conn_data *)luaL_checkudata (L, 1, LUASQL_CONNECTION_OCI8);
    luaL_argcheck (L, conn != NULL, 1, LUASQL_PREFIX"connection expected");
    if (conn->closed) {
        lua_pushboolean (L, 0);
        return 1;
    }
    if (conn->cur_counter > 0)
        return luaL_error (L, LUASQL_PREFIX"there are open cursors");

    OCISessionEnd(conn->svchp, conn->errhp, conn->authp, (ub4) 0);
    OCIServerDetach(conn->srvhp, conn->errhp, (ub4) OCI_DEFAULT);

    if (conn->srvhp)
        OCIHandleFree((dvoid *) conn->srvhp, (ub4) OCI_HTYPE_SERVER);
    if (conn->svchp)
        OCIHandleFree((dvoid *) conn->svchp, (ub4) OCI_HTYPE_SVCCTX);
    if (conn->authp)
        OCIHandleFree((dvoid *) conn->authp, (ub4) OCI_HTYPE_SESSION);
    if (conn->errhp)
        OCIHandleFree((dvoid *) conn->errhp, (ub4) OCI_HTYPE_ERROR);

    /* Nullify structure fields. */
    conn->closed = 1;
    conn->srvhp = NULL;
    conn->svchp = NULL;
    conn->authp = NULL;
    conn->errhp = NULL;

    conn->env->conn_counter--;

    lua_pushboolean (L, 1);

    return 1;
}

/*
** Abort current operation.
*/
static int
conn_abort (lua_State *L) {
    conn_data *conn = getconnection (L);
    ASSERT_OCI (L, OCIBreak(conn->srvhp, conn->errhp), conn->errhp);
    lua_pushboolean (L, 1);
    return 1;
}

/*
** Reset connection state.
*/
static int
conn_reset (lua_State *L) {
    conn_data *conn = getconnection (L);
    ASSERT_OCI (L, OCIReset(conn->srvhp, conn->errhp), conn->errhp);
    lua_pushboolean (L, 1);
    return 1;
}


/*
** Create a new Cursor object and push it on top of the stack.
*/
static int
create_cursor (lua_State *L, conn_data *conn, OCIStmt *stmt, const char *text) {
    int i;
    cur_data *cur = (cur_data *) lua_newuserdata(L, sizeof(cur_data));
    luasql_setmeta (L, LUASQL_CURSOR_OCI8);

    /* fill in structure */
    cur->conn = conn;
    cur->closed = 0;
    cur->numcols = 0;
    cur->colnames = LUA_NOREF;
    cur->coltypes = LUA_NOREF;
    cur->columns = LUA_NOREF;
    cur->stmthp = stmt;
    cur->errhp = NULL;
    cur->cols = NULL;
    cur->text = strdup (text);
    ASSERT_PTR (L, cur->text);

    /* error handler */
    ASSERT_OCI (L, OCIHandleAlloc((dvoid *) conn->env->envhp,
        (dvoid **) &(cur->errhp), (ub4) OCI_HTYPE_ERROR, (size_t) 0,
        (dvoid **) 0), conn->errhp);
    /* get number of columns */
    ASSERT_OCI (L, OCIAttrGet ((dvoid *)stmt, (ub4)OCI_HTYPE_STMT,
        (dvoid *) &cur->numcols, (ub4 *)0, (ub4)OCI_ATTR_PARAM_COUNT,
        cur->errhp), cur->errhp);

    cur->cols = (column_data *)calloc (cur->numcols, sizeof(column_data));
    ASSERT_PTR (L, cur->cols);

    /* define output variables */
    /* Oracle and Lua column indices ranges from 1 to numcols */
    /* C array indices ranges from 0 to numcols-1 */
    for (i = 1; i <= cur->numcols; i++)
        alloc_column_buffer (L, cur, i);

    conn->cur_counter++;

    return 1;
}


/*
** Execute an SQL statement.
** Return a Cursor object if the statement is a query, otherwise
** return the number of tuples affected by the statement.
*/
static int
conn_execute (lua_State *L) {
    conn_data *conn = getconnection (L);
    const char *statement = luaL_checkstring (L, 2);
    sword status;
    ub4 prefetch = 0;
    ub4 iters;
    ub4 mode;
    ub2 type;
    OCIStmt *stmthp = NULL;

    /* statement handle */
    if (lua_gettop(L) >= 3 && lua_isuserdata (L, -1)) {
        stmthp = (OCIStmt *) lua_touserdata(L, -1);
    } else {
        ASSERT_OCI (L, OCIHandleAlloc ((dvoid *)conn->env->envhp, (dvoid **)&stmthp,
            OCI_HTYPE_STMT, (size_t)0, (dvoid **)0), conn->errhp);
        ASSERT_OCI (L, OCIAttrSet ((dvoid *)stmthp, (ub4)OCI_HTYPE_STMT,
            (dvoid *)&prefetch, (ub4)0, (ub4)OCI_ATTR_PREFETCH_ROWS,
            conn->errhp), conn->errhp);
        ASSERT_OCI (L, OCIStmtPrepare (stmthp, conn->errhp, (text *)statement,
            (ub4) strlen(statement), (ub4) OCI_NTV_SYNTAX, (ub4) OCI_DEFAULT),
            conn->errhp);
    }

    /* statement type */
    ASSERT_OCI (L, OCIAttrGet ((dvoid *)stmthp, (ub4) OCI_HTYPE_STMT,
        (dvoid *)&type, (ub4 *)0, (ub4)OCI_ATTR_STMT_TYPE, conn->errhp),
        conn->errhp);

    iters = type == OCI_STMT_SELECT ? 0 : 1;
    mode = conn->auto_commit ? OCI_COMMIT_ON_SUCCESS : OCI_DEFAULT;

    /* execute statement */
    status = OCIStmtExecute (conn->svchp, stmthp, conn->errhp, iters,
        (ub4)0, (CONST OCISnapshot *)NULL, (OCISnapshot *)NULL, mode);
    if (status == OCI_STILL_EXECUTING) {
        lua_pushlightuserdata (L, (void *) stmthp);
        lua_pushnumber (L, OCI_STILL_EXECUTING);
        return 2;
    }
    if (status && (status != OCI_NO_DATA)) {
        OCIHandleFree ((dvoid *)stmthp, OCI_HTYPE_STMT);
        ASSERT_OCI (L, status, conn->errhp);
        /* unreachable */
        return 0;
    }
    if (type == OCI_STMT_SELECT) {
        /* create cursor */
        return create_cursor (L, conn, stmthp, statement);
    } else {
        /* return number of rows */
        int rows_affected;
        ASSERT_OCI (L, OCIAttrGet ((dvoid *)stmthp, (ub4)OCI_HTYPE_STMT,
            (dvoid *)&rows_affected, (ub4 *)0,
            (ub4)OCI_ATTR_ROW_COUNT, conn->errhp), conn->errhp);
        OCIHandleFree ((dvoid *)stmthp, OCI_HTYPE_STMT);
        lua_pushnumber (L, rows_affected);
        return 1;
    }
}


/*
** Commit the current transaction.
*/
static int
conn_commit (lua_State *L) {
    conn_data *conn = getconnection (L);
    sword status = OCITransCommit (conn->svchp, conn->errhp, OCI_DEFAULT);
    if (status == OCI_STILL_EXECUTING) {
        lua_pushnil (L);
        lua_pushinteger (L, OCI_STILL_EXECUTING);
        return 2;
    }
    ASSERT_OCI (L, status, conn->errhp);
    lua_pushboolean (L, 1);
    return 1;
}


/*
** Rollback the current transaction.
*/
static int
conn_rollback (lua_State *L) {
    conn_data *conn = getconnection (L);
    sword status = OCITransRollback (conn->svchp, conn->errhp, OCI_DEFAULT);
    if (status == OCI_STILL_EXECUTING) {
        lua_pushnil (L);
        lua_pushinteger (L, OCI_STILL_EXECUTING);
        return 2;
    }
    ASSERT_OCI (L, status, conn->errhp);
    lua_pushboolean (L, 1);
    return 1;
}


/*
** Set "auto commit" property of the connection.
** If 'true', then rollback current transaction.
** If 'false', then start a new transaction.
*/
static int
conn_setautocommit (lua_State *L) {
    conn_data *conn = getconnection (L);
    if (lua_toboolean (L, 2)) {
        conn->auto_commit = 1;
        /* Undo active transaction. */
        ASSERT_OCI (L, OCITransRollback (conn->svchp, conn->errhp,
            OCI_DEFAULT), conn->errhp);
    }
    else {
        conn->auto_commit = 0;
    }
    lua_pushboolean(L, 1);
    return 1;
}


/*
** Connects to a data source.
*/
static int
env_connect (lua_State *L) {
	env_data *env = getenvironment (L);

    const char *sourcename = luaL_checkstring(L, 2);
    const char *username = luaL_checkstring(L, 3);
    const char *password = luaL_checkstring(L, 4);

	/* Alloc connection object */
	conn_data *conn = (conn_data *)lua_newuserdata(L, sizeof(conn_data));

	/* fill in structure */
	luasql_setmeta (L, LUASQL_CONNECTION_OCI8);
    conn->env = env;
    conn->connecting = 0;
    conn->closed = 1;
    conn->auto_commit = 0;
    conn->cur_counter = 0;
    conn->srvhp = NULL;
    conn->svchp = NULL;
    conn->errhp = NULL;
    conn->authp = NULL;

    strncpy(conn->sourcename, sourcename, sizeof(conn->sourcename));
    strncpy(conn->username, username, sizeof(conn->username));
    strncpy(conn->password, password, sizeof(conn->password));

	/* error handler */
	ASSERT_OCI (L, OCIHandleAlloc((dvoid *) env->envhp,
		(dvoid **) &(conn->errhp),
		(ub4) OCI_HTYPE_ERROR, (size_t) 0, (dvoid **) 0), env->errhp);
	/* login */
	ASSERT_OCI (L, OCILogon(env->envhp, conn->errhp, &(conn->svchp),
		(CONST text*) username, strlen(username),
		(CONST text*) password, strlen(password),
		(CONST text*) sourcename, strlen(sourcename)), conn->errhp);

	conn->closed = 0;
	env->conn_counter++;

	return 1;
}


static void
async_completed (void *p) {
    conn_data *conn = (conn_data *) p;
    conn->connecting = 0;
}


#define OCI_THR_CHECK(exp) { sword s = exp; if (s) pthread_exit((void *) s); }

/*
** Async connects to a data source.
*/
static void *
async_open (void *p) {
    conn_data *conn = (conn_data *) p;
    env_data *env = conn->env;

    pthread_cleanup_push(async_completed, p);
    pthread_cleanup_push(pthread_mutex_unlock, (void *) &conn->env->mtx);

    pthread_mutex_lock(&conn->env->mtx);

    OCI_THR_CHECK (OCIHandleAlloc((dvoid *) env->envhp,
        (dvoid **) &(conn->errhp),
        (ub4) OCI_HTYPE_ERROR, (size_t) 0, (dvoid **) 0));

    OCI_THR_CHECK (OCIHandleAlloc((dvoid *) env->envhp,
        (dvoid **) &(conn->srvhp),
        (ub4) OCI_HTYPE_SERVER, (size_t) 0, (dvoid **) 0));

    OCI_THR_CHECK (OCIHandleAlloc((dvoid *) env->envhp,
        (dvoid **) &(conn->svchp),
        (ub4) OCI_HTYPE_SVCCTX, (size_t) 0, (dvoid **) 0));

    OCI_THR_CHECK(OCIServerAttach(conn->srvhp, conn->errhp,
        (text *) conn->sourcename, (sb4) strlen(conn->sourcename), OCI_DEFAULT));

    OCI_THR_CHECK (OCIAttrSet((dvoid *) conn->svchp, OCI_HTYPE_SVCCTX,
        (dvoid *) conn->srvhp, (ub4)0, OCI_ATTR_SERVER, conn->errhp));

    OCI_THR_CHECK (OCIHandleAlloc((dvoid *)env->envhp,
        (dvoid **) &conn->authp, OCI_HTYPE_SESSION, 0 , (dvoid **) 0));

    OCI_THR_CHECK (OCIAttrSet((dvoid *)conn->authp, OCI_HTYPE_SESSION,
        (dvoid *) conn->username,    (ub4) strlen(conn->username), OCI_ATTR_USERNAME, conn->errhp));

    OCI_THR_CHECK (OCIAttrSet((dvoid *)conn->authp, OCI_HTYPE_SESSION,
        (dvoid *) conn->password,    (ub4) strlen(conn->password), OCI_ATTR_PASSWORD, conn->errhp));

    OCI_THR_CHECK (OCISessionBegin(conn->svchp, conn->errhp, conn->authp,
        OCI_CRED_RDBMS, (ub4) OCI_DEFAULT));

    OCI_THR_CHECK (OCIAttrSet(conn->svchp, OCI_HTYPE_SVCCTX,
        conn->authp, 0, OCI_ATTR_SESSION, conn->errhp));

    /* set nonblocking */
    OCI_THR_CHECK (OCIAttrSet ((dvoid *) conn->srvhp, (ub4) OCI_HTYPE_SERVER,
        (dvoid *) 0, (ub4) 0,
        (ub4) OCI_ATTR_NONBLOCKING_MODE, conn->errhp));

    pthread_cleanup_pop(1);
    pthread_cleanup_pop(1);

    return (void *) OCI_SUCCESS;
}


/*
** Connects to a data source asynchronous.
*/
static int
env_connect_async (lua_State *L) {
    env_data *env = getenvironment (L);

    const char *sourcename = luaL_checkstring(L, 2);
    const char *username = luaL_checkstring(L, 3);
    const char *password = luaL_checkstring(L, 4);

    sword status;

    /* Alloc connection object */
    conn_data *conn;

    if (lua_gettop(L) > 1 && lua_isuserdata(L, -1)) {
        conn = (conn_data *)luaL_checkudata (L, -1, LUASQL_CONNECTION_OCI8);
        if (conn == NULL) {
            return luaL_error (L, LUASQL_PREFIX"connection handle expected");
        }
    } else {
        conn = (conn_data *)lua_newuserdata(L, sizeof(conn_data));

        /* fill in structure */
        luasql_setmeta (L, LUASQL_CONNECTION_OCI8);
        conn->env = env;
        conn->connecting = 1;
        conn->closed = 1;
        conn->auto_commit = 0;
        conn->cur_counter = 0;
        conn->srvhp = NULL;
        conn->svchp = NULL;
        conn->errhp = NULL;
        conn->authp = NULL;

        strncpy(conn->sourcename, sourcename, sizeof(conn->sourcename));
        strncpy(conn->username, username, sizeof(conn->username));
        strncpy(conn->password, password, sizeof(conn->password));

        pthread_create(&conn->tid, NULL, async_open, conn);

        lua_pushinteger (L, OCI_STILL_EXECUTING);

        return 2;
    }

    if (conn->connecting) {
        lua_pushvalue (L, -1);
        lua_pushinteger (L, OCI_STILL_EXECUTING);
        return 2;
    }

    pthread_join(conn->tid, (void **) &status);

    ASSERT_OCI (L, status, conn->errhp);

    conn->closed = 0;
    conn->tid = 0;
    env->conn_counter++;

    lua_pushvalue (L, -1);

    return 1;
}


/*
** Close environment object.
*/
static int
env_close (lua_State *L) {
    env_data *env = (env_data *)luaL_checkudata (L, 1, LUASQL_ENVIRONMENT_OCI8);
    luaL_argcheck (L, env != NULL, 1, LUASQL_PREFIX"environment expected");

    if (env->closed) {
        lua_pushboolean (L, 0);
        return 1;
    }

    if (env->conn_counter > 0)
        return luaL_error (L, LUASQL_PREFIX"there are open connections");

    env->closed = 1;

    if (env->envhp)
        OCIHandleFree ((dvoid *)env->envhp, OCI_HTYPE_ENV);
    if (env->errhp)
        OCIHandleFree ((dvoid *)env->errhp, OCI_HTYPE_ERROR);

    lua_pushboolean (L, 1);

    return 1;
}


/*
** Creates an Environment and returns it.
*/
static int
create_environment (lua_State *L) {
    env_data *env = (env_data *)lua_newuserdata(L, sizeof(env_data));
    luasql_setmeta (L, LUASQL_ENVIRONMENT_OCI8);

    /* fill in structure */
    env->closed = 0;
    env->conn_counter = 0;
    env->envhp = NULL;
    env->errhp = NULL;

    if (OCIEnvCreate ( &(env->envhp), (ub4)OCI_THREADED, (dvoid *)0,
            (dvoid * (*)(dvoid *, size_t)) 0,
            (dvoid * (*)(dvoid *, dvoid *, size_t)) 0,
            (void (*)(dvoid *, dvoid *)) 0,
            (size_t) 0,
            (dvoid **) 0))
        return luaL_error (L, LUASQL_PREFIX"couldn't create environment");

    /* error handler */
    ASSERT_OCI (L, OCIHandleAlloc((dvoid *) env->envhp,
        (dvoid **) &(env->errhp),
        (ub4) OCI_HTYPE_ERROR, (size_t) 0, (dvoid **) 0), NULL);

    pthread_mutex_init(&env->mtx, NULL);

    return 1;
}


/*
** Create metatables for each class of object.
*/
static void
create_metatables (lua_State *L) {
    struct luaL_Reg environment_methods[] = {
        {"__gc", env_close}, /* Should this method be changed? */
        {"close", env_close},
        {"connect", env_connect},
        {"connect_async", env_connect_async},
        {NULL, NULL},
    };

    struct luaL_Reg connection_methods[] = {
        {"__gc", conn_close}, /* Should this method be changed? */
        {"abort", conn_abort},
        {"reset", conn_reset},
        {"close", conn_close},
        {"execute", conn_execute},
        {"commit", conn_commit},
        {"rollback", conn_rollback},
        {"setautocommit", conn_setautocommit},
        {NULL, NULL},
    };

    struct luaL_Reg cursor_methods[] = {
        {"__gc", cur_close}, /* Should this method be changed? */
        {"close", cur_close},
        {"getcolnames", cur_getcolnames},
        {"getcoltypes", cur_getcoltypes},
        {"getcolumns", cur_getcolumns},
        {"fetch", cur_fetch},
        {"numrows", cur_numrows},
        {NULL, NULL},
    };

    luasql_createmeta (L, LUASQL_ENVIRONMENT_OCI8, environment_methods);
    luasql_createmeta (L, LUASQL_CONNECTION_OCI8, connection_methods);
    luasql_createmeta (L, LUASQL_CURSOR_OCI8, cursor_methods);
    lua_pop (L, 3);
}


static void
inject_consts(lua_State *L) {
    lua_createtable(L, 0 /* narr */, 3 /* nrec */);    /* oci.* */

    lua_pushinteger(L, OCI_SUCCESS);
    lua_setfield(L, -2, "OCI_SUCCESS");

    lua_pushinteger(L, OCI_SUCCESS_WITH_INFO);
    lua_setfield(L, -2, "OCI_SUCCESS_WITH_INFO");

    lua_pushinteger(L, OCI_STILL_EXECUTING);
    lua_setfield(L, -2, "OCI_STILL_EXECUTING");

    lua_getglobal(L, "package");
    lua_getfield(L, -1, "loaded");
    lua_pushvalue(L, -3);
    lua_setfield(L, -2, "oci");
    lua_pop(L, 2);

    lua_setglobal(L, "oci");
}


/*
** Creates the metatables for the objects and registers the
** driver open method.
*/
LUASQL_API int
luaopen_luasql_oci8 (lua_State *L) {
    struct luaL_Reg driver[] = {
        {"oci8", create_environment},
        {NULL, NULL},
    };
    inject_consts (L);
    create_metatables (L);
    lua_newtable (L);
    luaL_setfuncs (L, driver, 0);
    luasql_set_info (L);
    return 1;
}
