/*
** $Id: luasql.c,v 1.28 2009/02/11 12:08:50 tomas Exp $
** See Copyright Notice in license.html
*/

#include <string.h>

#include "lua.h"
#include "lauxlib.h"


#include "luasql.h"

#if !defined(lua_pushliteral)
#define lua_pushliteral(L, s) \
	lua_pushstring(L, "" s, (sizeof(s)/sizeof(char))-1)
#endif


typedef struct { short  closed; } pseudo_data;

/*
** Return the name of the object's metatable.
** This function is used by `tostring'.
*/
static int luasql_tostring (lua_State *L) {
	char buff[100];
	pseudo_data *obj = (pseudo_data *)lua_touserdata (L, 1);
	if (obj->closed)
		strcpy (buff, "closed");
	else
		sprintf (buff, "%p", (void *)obj);
	lua_pushfstring (L, "%s (%s)", lua_tostring(L,lua_upvalueindex(1)), buff);
	return 1;
}


#if !defined LUA_VERSION_NUM || LUA_VERSION_NUM < 502
/*
** Adapted from Lua 5.2.0
*/
void luaL_setfuncs (lua_State *L, const luaL_Reg *l, int nup) {
	luaL_checkstack(L, nup+1, "too many upvalues");
	for (; l->name != NULL; l++) {	/* fill the table with given functions */
		int i;
		lua_pushstring(L, l->name);
		for (i = 0; i < nup; i++)	/* copy upvalues to the top */
			lua_pushvalue(L, -(nup + 1));
		lua_pushcclosure(L, l->func, nup);	/* closure with those upvalues */
		lua_settable(L, -(nup + 3));
	}
	lua_pop(L, nup);	/* remove upvalues */
}
#endif

/*
** Create a metatable and leave it on top of the stack.
*/
LUASQL_API int luasql_createmeta (lua_State *L, const char *name, const luaL_Reg *methods) {
	if (!luaL_newmetatable (L, name))
		return 0;

	/* define methods */
	luaL_setfuncs (L, methods, 0);

	/* define metamethods */
	lua_pushliteral (L, "__index");
	lua_pushvalue (L, -2);
	lua_settable (L, -3);

	lua_pushliteral (L, "__tostring");
	lua_pushstring (L, name);
	lua_pushcclosure (L, luasql_tostring, 1);
	lua_settable (L, -3);

	lua_pushliteral (L, "__metatable");
	lua_pushliteral (L, LUASQL_PREFIX"you're not allowed to get this metatable");
	lua_settable (L, -3);

	return 1;
}


/*
** Define the metatable for the object on top of the stack
*/
LUASQL_API void luasql_setmeta (lua_State *L, const char *name) {
	luaL_getmetatable (L, name);
	lua_setmetatable (L, -2);
}


/*
** Assumes the table is on top of the stack.
*/
LUASQL_API void luasql_set_info (lua_State *L) {
	lua_pushliteral (L, "_COPYRIGHT");
	lua_pushliteral (L, "Copyright (C) 2003-2017 Kepler Project");
	lua_settable (L, -3);
	lua_pushliteral (L, "_DESCRIPTION");
	lua_pushliteral (L, "LuaOCI is a simple interface from Lua to a Oracle");
	lua_settable (L, -3);
	lua_pushliteral (L, "_VERSION");
	lua_pushliteral (L, "LuaOCI "LUAOCI_VERSION_NUMBER" (for "LUA_VERSION")");
	lua_settable (L, -3);
}
