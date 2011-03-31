#   include <mysql.h>
#   include <sqlite3.h>
#   include <stdio.h>
#   include <stdlib.h>
#   include <string.h>
#   include <time.h>
#include <stdarg.h>
#ifndef _RADB_H_
#define _RADB_H_

#define RADB_SQLITE3 1
#define RADB_MYSQL 2


typedef struct
{
    unsigned    inUse;
    void        *handle;
} radbChild;

typedef struct
{
    radbChild *children;
    unsigned count;
    /*
     * rumble_readerwriter rrw;
     */
} radbPool;

typedef struct {
    unsigned dbType;
    radbPool pool;
    void* handle;
} radbMaster;


typedef struct
{
    void        *state;
    void        *db;
    unsigned    result;
    char        buffer[1024];
    radbMaster* master;
} radbObject;

typedef struct
{
    enum { STRING = 1, NUMBER = 2 }           type;
    unsigned    size;
    void        *data;
} radbItem;
typedef struct
{
    radbItem      *column;
    unsigned    items;
} radbResult;


/*$2
 -----------------------------------------------------------------------------------------------------------------------
    Fixed prototypes
 -----------------------------------------------------------------------------------------------------------------------
 */
  
int radb_do(radbMaster* radbm, const char *statement, ...);
radbObject *radb_prepare(radbMaster* radbm, const char *statement, ...);
void        radb_free_result(radbResult *result);
#   define radb_free(a)  radb_free_result(a)
radbResult    *radb_fetch_row(void *state);
radbResult    *radb_step(radbObject *dbo);
void        radb_cleanup(radbObject *dbo);
const char* radb_last_error(radbObject* dbo);
#   define radb_as(a, b) *((a *) b)
#define radb_run radb_do


/*$2
 -----------------------------------------------------------------------------------------------------------------------
    Model-specific definitions
 -----------------------------------------------------------------------------------------------------------------------
 */
#ifdef _SQLITE3_H_
radbMaster* radb_init_sqlite(const char* file);
void        radb_prepare_sqlite(radbObject *dbo, const char *statement, va_list vl);
radbResult    *radb_fetch_row_sqlite(void *state);
#endif

#   ifdef MYSQL_CLIENT
radbMaster* radb_init_mysql(unsigned threads, const char* host, const char* user, const char* pass, const char* db, unsigned port);
void        radb_prepare_mysql(radbObject *dbo, const char *statement, va_list vl);
radbResult    *radb_fetch_row_mysql(void *state);
void radb_release_handle_mysql(radbPool *pool, void* handle);
void* radb_get_handle_mysql(radbPool *pool);
#      define RUMBLE_DB_RESULT    0
#   else
#      define RUMBLE_DB_RESULT    100
#   endif


#endif