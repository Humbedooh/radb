/*$I0 */
#include <mysql.h>
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <stdarg.h>
#ifndef _RADB_H_
#   define _RADB_H_
#   define RADB_EMPTY      0
#   define RADB_SQLITE3    1
#   define RADB_MYSQL      2
#   define RADB_PARSED     1
#   define RADB_PREPARED   2
#   define RADB_BOUND      3
#   define RADB_EXECUTED   4
#   define RADB_FETCH      5
/*#   define RADB_DEBUG*/
typedef struct
{
    unsigned    inUse;
    void        *handle;
} radbChild;
typedef struct
{
    radbChild   *children;
    unsigned    count;
} radbPool;
typedef struct
{
    unsigned    dbType;
    radbPool    pool;
    void        *handle;
} radbMaster;

/*$1
 ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    radbItem: A struct holding exactly one value from one column as a union.
 ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 */

typedef struct
{
    enum { STRING = 1, NUMBER = 2 }           type;
    unsigned    size;
    union
    {
        char        string[256];
        uint32_t    uint32;
        int32_t     int32;
        int64_t     int64;
        uint64_t    uint64;
        double      _double;
        float       _float;
    } data;
} radbItem;

/*$1
 ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    radbResult: A result object holding the currently fetched row of data
 ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 */

typedef struct
{
    radbItem    *column;
    unsigned    items;
    void        *bindings;
} radbResult;

/*$1
 ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    radbObject: An object holding the current SQL statement and its status
 ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 */

typedef struct
{
    void        *state;
    void        *db;
    unsigned    status;
    char        buffer[1024];
    radbResult  *result;
    radbMaster  *master;
    void        *inputBindings;
} radbObject;

/*$2
 -----------------------------------------------------------------------------------------------------------------------
    Fixed prototypes
 -----------------------------------------------------------------------------------------------------------------------
 */

int         radb_do(radbMaster *radbm, const char *statement, ...);
radbObject  *radb_prepare(radbMaster *radbm, const char *statement, ...);
void        radb_free_result(radbResult *result);
radbResult  *radb_fetch_row(void *state);
radbResult  *radb_step(radbObject *dbo);
void        radb_cleanup(radbObject *dbo);
const char  *radb_last_error(radbObject *dbo);
#   define radb_free(a)    radb_free_result(a)
#   define radb_run        radb_do

/*$2
 -----------------------------------------------------------------------------------------------------------------------
    Model-specific definitions
 -----------------------------------------------------------------------------------------------------------------------
 */

#   ifdef _SQLITE3_H_
radbMaster  *radb_init_sqlite(const char *file);
void        radb_prepare_sqlite(radbObject *dbo, const char *statement, va_list vl);
radbResult  *radb_fetch_row_sqlite(radbObject *dbo);
#   endif
#   ifdef MYSQL_CLIENT
radbMaster  *radb_init_mysql(unsigned threads, const char *host, const char *user, const char *pass, const char *db, unsigned port);
void        radb_prepare_mysql(radbObject *dbo, const char *statement, va_list vl);
radbResult  *radb_fetch_row_mysql(radbObject *dbo);
void        radb_release_handle_mysql(radbPool *pool, void *handle);
void        *radb_get_handle_mysql(radbPool *pool);
#      define RUMBLE_DB_RESULT    0
#   else
#      define RUMBLE_DB_RESULT    100
#   endif
#endif
