/*$6
 +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 */

#include "radb.h"

/*
 =======================================================================================================================
 =======================================================================================================================
 */
const char *radb_last_error(radbObject *dbo) {
    if (dbo->state == 0) {
        return ("");
    }

#ifdef _SQLITE3_H_
    if (dbo->master->dbType == RADB_SQLITE3) {
        return (sqlite3_errmsg((sqlite3 *) dbo->db));
    }
#endif
#ifdef MYSQL_CLIENT
    if (dbo->master->dbType == RADB_MYSQL) {
        return (mysql_error((MYSQL *) dbo->db));
    }
#endif
    return ("");
}

/*
 =======================================================================================================================
 =======================================================================================================================
 */
radbResult *radb_step(radbObject *dbo) {
    if (!dbo) return (0);
    if (dbo->state == 0) {
        fprintf(stderr, "[RADB] Can't step: Statement wasn't prepared properly!\r\n");
        return (0);
    }

#ifdef _SQLITE3_H_
    if (dbo->master->dbType == RADB_SQLITE3) {
        if (sqlite3_step((sqlite3_stmt *) dbo->state) == SQLITE_ROW)
        {
#   ifdef RADB_DEBUG
            printf("[RADB] SQLITE says there be dragons in %p!\r\n", dbo->state);
#   endif
            return (radb_fetch_row_sqlite(dbo));
        }
    }
#endif
#ifdef MYSQL_CLIENT
    if (dbo->master->dbType == RADB_MYSQL) {
        if (dbo->status <= RADB_BOUND)
        {
#   ifdef RADB_DEBUG
            printf("[RADB] Executing statement\r\n");
#   endif
            mysql_stmt_execute((MYSQL_STMT *) dbo->state);
            dbo->status = RADB_EXECUTED;
#   ifdef RADB_DEBUG
            printf("[RADB] dbo->staus is: %u\r\n", dbo->status);
#   endif
        }

        return (radb_fetch_row_mysql(dbo));
    }
#endif
    return (0);
}

/*
 =======================================================================================================================
 =======================================================================================================================
 */
void radb_cleanup(radbObject *dbo)
{
#ifdef RADB_DEBUG
    printf("Cleaning up\r\n");
#endif
#ifdef _SQLITE3_H_
    if (dbo->master->dbType == RADB_SQLITE3) {
        if (dbo->state) sqlite3_finalize((sqlite3_stmt *) dbo->state);
    }
#endif
#ifdef MYSQL_CLIENT
    if (dbo->master->dbType == RADB_MYSQL)
    {
#   ifdef RADB_DEBUG
        printf("Closing state\r\n");
#   endif
        if (dbo->state) mysql_stmt_close((MYSQL_STMT *) dbo->state);
#   ifdef RADB_DEBUG
        printf("Releasing handle\r\n");
#   endif
        radb_release_handle_mysql(&dbo->master->pool, dbo->db);
    }

#   ifdef RADB_DEBUG
    printf("Calling radb_free_result\r\n");
#   endif
    radb_free_result(dbo->result);
    if (dbo->inputBindings) free(dbo->inputBindings);
    free((radbObject *) dbo);
#endif
}

/*
 =======================================================================================================================
 =======================================================================================================================
 */
int radb_do(radbMaster *radbm, const char *statement, ...) {

    /*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/
    va_list     args;
    radbObject  *dbo = calloc(1, sizeof(radbObject));
    int         rc = 0;
    /*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/

    if (!radbm) {
        printf("[RADB] Received a null-pointer as radbm!\r\n");
        return (0);
    }

#ifdef RADB_DEBUG
    printf("[RADB] Running RADB->do\r\n");
#endif
    dbo->result = 0;
    dbo->master = radbm;
    dbo->inputBindings = 0;
    va_start(args, statement);
#ifdef _SQLITE3_H_
    if (radbm->dbType == RADB_SQLITE3) {
        dbo->db = radbm->handle;
        radb_prepare_sqlite(dbo, statement, args);
#   ifdef RADB_DEBUG
        printf("[RADB] Stepping\r\n");
#   endif
        rc = (sqlite3_step((sqlite3_stmt *) dbo->state) == SQLITE_ROW) ? 1 : 0;
    }
#endif
#ifdef MYSQL_CLIENT
    if (radbm->dbType == RADB_MYSQL) {
        dbo->db = radb_get_handle_mysql(&radbm->pool);
        radb_prepare_mysql(dbo, statement, args);
#   ifdef RADB_DEBUG
        printf("[RADB] Stepping\r\n");
#   endif
        if (dbo->status == RADB_BOUND) {
            rc = mysql_stmt_execute((MYSQL_STMT *) dbo->state);
            if (!rc) rc = mysql_stmt_affected_rows((MYSQL_STMT *) dbo->state);
            if (!rc) rc = mysql_stmt_num_rows((MYSQL_STMT *) dbo->state);
        } else {
            fprintf(stderr, "[RADB] Couldn't execute the SQL statement!\r\n");
            rc = -1;
        }
    }
#endif
    va_end(args);
    radb_cleanup(dbo);
#ifdef RADB_DEBUG
    printf("[RADB] Step returned %d\r\n", rc);
#endif
    return (rc);
}

/*
 =======================================================================================================================
 =======================================================================================================================
 */
radbObject *radb_prepare(radbMaster *radbm, const char *statement, ...) {

    /*~~~~~~~~~~~~~*/
    va_list     args;
    radbObject  *dbo;
    /*~~~~~~~~~~~~~*/

    if (!radbm) {
        printf("[RADB] Received a null-pointer as radbm!\r\n");
        return (0);
    }

    dbo = calloc(1, sizeof(radbObject));
#ifdef RADB_DEBUG
    printf("[RADB] pre-preparation of: %s\r\n", statement);
#endif
    dbo->master = radbm;
    dbo->result = malloc(sizeof(radbResult));
    dbo->result->items = 0;
    dbo->result->column = 0;
    dbo->result->bindings = 0;
    dbo->inputBindings = 0;
    va_start(args, statement);
#ifdef _SQLITE3_H_
    if (radbm->dbType == RADB_SQLITE3) {
        dbo->db = radbm->handle;
        radb_prepare_sqlite(dbo, statement, args);
    }
#endif
#ifdef MYSQL_CLIENT
    else if (radbm->dbType == RADB_MYSQL) {
        dbo->db = radb_get_handle_mysql(&radbm->pool);
        radb_prepare_mysql(dbo, statement, args);
    }
#endif
    va_end(args);
    dbo->status = RADB_EXECUTED;
    return (dbo);
}

/*
 =======================================================================================================================
 =======================================================================================================================
 */
void radb_free_result(radbResult *result)
{
#ifdef RADB_DEBUG
    printf("freeing up result data\r\n");
#endif
    if (!result) return;
    if (result->column) free(result->column);
    if (result->bindings) free(result->bindings);
    result->column = 0;
    result->bindings = 0;
    free(result);
#ifdef RADB_DEBUG
    printf("done!!\r\n");
#endif
}

#ifdef MYSQL_CLIENT /* Only compile if mysql support is enabled */

/*
 =======================================================================================================================
 =======================================================================================================================
 */
radbMaster *radb_init_mysql(unsigned threads, const char *host, const char *user, const char *pass, const char *db, unsigned port) {

    /*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/
    unsigned    i,
                ok = 1;
    my_bool     yes = 1;
    MYSQL       *m;
    radbMaster  *radbm = malloc(sizeof(radbMaster));
    /*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/

    radbm->dbType = RADB_MYSQL;
    radbm->pool.count = threads;
    radbm->pool.children = calloc(threads, sizeof(radbChild));
    for (i = 0; i < threads; i++) {
        radbm->pool.children[i].handle = mysql_init(0);
        m = (MYSQL *) radbm->pool.children[i].handle;
        mysql_options(m, MYSQL_OPT_RECONNECT, &yes);
        if (!mysql_real_connect(m, host, user, pass, db, port, 0, 0)) {
            fprintf(stderr, "Failed to connect to database: Error: %s", mysql_error(m));
            ok = 0;
            break;
        }
    }

    if (!ok) return (0);
    return (radbm);
}

/*
 =======================================================================================================================
 =======================================================================================================================
 */
void *radb_get_handle_mysql(radbPool *pool) {

    /*~~~~~~*/
    int i,
        x = 5;
    /*~~~~~~*/

    while (x != 0) {
        for (i = 0; i < pool->count; i++) {
            if (pool->children[i].inUse == 0) {
                pool->children[i].inUse = 1;
                return (pool->children[i].handle);
            }
        }

        x--;
    }

    return (0);
}

/*
 =======================================================================================================================
 =======================================================================================================================
 */
void radb_release_handle_mysql(radbPool *pool, void *handle) {

    /*~~*/
    int i;
    /*~~*/

    for (i = 0; i < pool->count; i++) {
        if (pool->children[i].handle == handle) {
            pool->children[i].inUse = 0;
            break;
        }
    }
}

/*
 =======================================================================================================================
 =======================================================================================================================
 */
void radb_prepare_mysql(radbObject *dbo, const char *statement, va_list vl) {

    /*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/
    char                    *sql,
                            b;
    const char              *p,
                            *op;
    char                    injects[32];
    void                    *O;
    int                     rc;
    size_t                  len = 0,
                            strl = 0;
    int                     at = 0,
                            params,
                            i;
    unsigned long           str_len[100];
    unsigned char           object[1024];
    int                     used = 0;
    int                     ss = 0;
    MYSQL_BIND              *bindings;
    unsigned int            d_uint;
    signed int              d_sint;
    signed long long int    d_lint;
    double                  d_double;
    MYSQL_RES               *meta;
    /*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/

    dbo->status = 0;
#   ifdef RADB_DEBUG
    printf("[RADB] Preparing statement: %s\n", statement);
#   endif
    memset(object, 0, 1024);
    memset(injects, 0, 32);
    sql = (char *) calloc(1, 2048);
    op = statement;
    for (p = strchr(statement, '%'); p != NULL; p = strchr(op, '%')) {
        strl = strlen(op) - strlen(p);
        strncpy((char *) (sql + len), op, strl);
        len += strl;
        if (sscanf((const char *) p, "%%%c", &b)) {
            if (b == '%') {
                strncpy((char *) (sql + len), "%", 1);
                len += 1;
            } else {
                strncpy((char *) (sql + len), "?", 1);
                len += 1;
                injects[at++] = b;
            }

            op = (char *) p + 2;
        }
    }

    strl = strlen(op);
    strncpy((char *) (sql + len), op, strl);
    strl = strlen(sql);
    if (sql[strl - 1] != ';') sql[strl++] = ';';
#   ifdef RADB_DEBUG
    printf("[RADB] Sending statement: %s\n", sql);
#   endif
    dbo->state = mysql_stmt_init((MYSQL *) dbo->db);
    rc = mysql_stmt_prepare((MYSQL_STMT *) dbo->state, sql, strl);
    free(sql);
    if (rc != 0) {
        fprintf(stderr, "[RADB] Mysql: %s\r\n", mysql_error(dbo->db));
        dbo->state = 0;
        return;
    }

    dbo->status = RADB_PREPARED;
    params = mysql_stmt_param_count((MYSQL_STMT *) dbo->state);
    bindings = calloc(sizeof(MYSQL_BIND), params ? params + 1 : 1);
    dbo->inputBindings = bindings;
    for (at = 0; injects[at] != 0; at++) {
        bindings[at].is_null = 0;
        bindings[at].length = 0;
        bindings[at].is_unsigned = 1;
        switch (injects[at])
        {
        case 's':
            bindings[at].buffer_type = MYSQL_TYPE_STRING;
            bindings[at].buffer = (void *) va_arg(vl, const char *);
#   ifdef RADB_DEBUG
            printf("- row %d (%p): %s\r\n", at + 1, bindings[at].buffer, (char *) bindings[at].buffer);
#   endif
            if (!bindings[at].buffer) bindings[at].is_null_value = 1;
            str_len[at] = strlen((const char *) bindings[at].buffer);
            bindings[at].buffer_length = str_len[at];
            bindings[at].length = &str_len[at];
            break;

        case 'u':
            O = &(object[used]);
            d_uint = va_arg(vl, unsigned int);
            bindings[at].buffer_type = MYSQL_TYPE_LONG;
            bindings[at].buffer = (void *) O;
            str_len[at] = sizeof(unsigned int);
            bindings[at].buffer_length = str_len[at];
            bindings[at].length = &str_len[at];
            memcpy(O, &d_uint, str_len[at]);
            used += str_len[at];
#   ifdef RADB_DEBUG
            printf("- row %d (%p): %u\r\n", at + 1, bindings[at].buffer, *((unsigned int *) bindings[at].buffer));
#   endif
            break;

        case 'i':
            bindings[at].is_unsigned = 0;
            O = &(object[used]);
            d_sint = va_arg(vl, signed int);
            ss = sizeof(signed int);
            bindings[at].buffer_type = MYSQL_TYPE_LONG;
            bindings[at].buffer = (void *) O;
            str_len[at] = sizeof(signed int);
            bindings[at].buffer_length = str_len[at];
            bindings[at].length = &str_len[at];
            memcpy(O, &d_uint, str_len[at]);
            used += str_len[at];
            break;

        case 'l':
            bindings[at].is_unsigned = 0;
            bindings[at].buffer_type = MYSQL_TYPE_LONGLONG;
            O = &(object[used]);
            d_lint = va_arg(vl, signed long long int);
            bindings[at].buffer_type = MYSQL_TYPE_LONG;
            bindings[at].buffer = (void *) O;
            str_len[at] = sizeof(signed long long int);
            bindings[at].buffer_length = str_len[at];
            bindings[at].length = &str_len[at];
            memcpy(O, &d_uint, str_len[at]);
            used += str_len[at];
            break;

        case 'f':
            bindings[at].is_unsigned = 0;
            bindings[at].buffer_type = MYSQL_TYPE_DOUBLE;
            O = &(object[used]);
            d_double = va_arg(vl, double);
            bindings[at].buffer = (void *) O;
            str_len[at] = sizeof(double);
            bindings[at].buffer_length = str_len[at];
            bindings[at].length = &str_len[at];
            memcpy(O, &d_uint, str_len[at]);
            used += str_len[at];
            break;

        default:
            break;
        }
    }

#   ifdef RADB_DEBUG
    printf("[RADB] Binding parameters to statement\r\n");
#   endif
    if (mysql_stmt_bind_param((MYSQL_STMT *) dbo->state, bindings)) {
        dbo->state = 0;
        dbo->status = 0;
        fprintf(stderr, "[RADB] Something went wrong :(\r\n");
        free(bindings);
        return;
    }

#   ifdef RADB_DEBUG
    printf("Bound 'em!\r\n");
    fflush(stdout);

    /* Prepare the result set for inserts */
    printf("Checking for meta data\r\n");
#   endif
    meta = mysql_stmt_result_metadata((MYSQL_STMT *) dbo->state);
    if (meta)
    {
#   ifdef RADB_DEBUG
        printf("Found meta at %p\r\n", meta);
#   endif
        dbo->result->items = meta->field_count;
#   ifdef RADB_DEBUG
        printf("[RADB] Preparing %u columns for data\r\n", dbo->result->items);
#   endif
        dbo->result->column = calloc(sizeof(radbItem), dbo->result->items);
        bindings = calloc(sizeof(MYSQL_BIND), dbo->result->items);
        for (i = 0; i < dbo->result->items; i++) {
            memset(dbo->result->column[i].data.string, 0, 256);
            bindings[i].buffer = (void *) dbo->result->column[i].data.string;
            dbo->result->column[i].type = (meta->fields[i].type >= MYSQL_TYPE_VARCHAR) ? 1 : 2;
            bindings[i].buffer_type = meta->fields[i].type;
            bindings[i].buffer_length = meta->fields[i].length + 1;
#   ifdef RADB_DEBUG
            printf("Row %d = %d (max %lu bytes)\r\n", i + 1, meta->fields[i].type, meta->fields[i].length);
#   endif
        }

        mysql_free_result(meta);
        dbo->result->bindings = bindings;
    } else
    {
#   ifdef RADB_DEBUG
        printf("No meta data found\r\n");
#   endif
    }

#   ifdef RADB_DEBUG
    printf("done here!\r\n");
#   endif
    dbo->status = RADB_BOUND;
    fflush(stdout);
}

/*
 =======================================================================================================================
 =======================================================================================================================
 */
radbResult *radb_fetch_row_mysql(radbObject *dbo) {

    /*~~~~~~~*/
    int rc = 0,
        i = 0;
    /*~~~~~~~*/

    if (dbo->status <= RADB_BOUND) {
        printf("[RADB] Error: radb object not initialized properly!\r\n");
        return (0);
    }

#   ifdef RADB_DEBUG
    printf("[RADB] Checking for result set...(%u)\r\n", dbo->status);
#   endif
    if (dbo->status <= RADB_EXECUTED) {
        dbo->status = RADB_FETCH;
#   ifdef RADB_DEBUG
        printf("First fetch: Binding to result set\r\n");
        fflush(stdout);
#   endif
        mysql_stmt_bind_result((MYSQL_STMT *) dbo->state, (MYSQL_BIND *) dbo->result->bindings);
        mysql_stmt_store_result((MYSQL_STMT *) dbo->state);
    }

#   ifdef RADB_DEBUG
    printf("[RADB] Calling stmt_fetch\r\n");
    fflush(stdout);
#   endif
    rc = mysql_stmt_fetch((MYSQL_STMT *) dbo->state);
    if (rc) {
        if (rc == MYSQL_DATA_TRUNCATED) {

            /*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/
            int         x = 0;
            MYSQL_BIND  *bind = (MYSQL_BIND *) dbo->result->bindings;
            /*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/

            for (i = 0; i < dbo->result->items; i++) {
                if (bind[i].error_value) {
                    printf("[RADB] MySQL says row %d was truncated\r\n", i + 1);
                    x++;
                }
            }

            printf("[RADB] Found %d truncated fields\r\n", x);
        }

#   ifdef RADB_DEBUG
        printf("No data fetched, returning null-pointer\r\n");
#   endif
        return (0);
    }

#   ifdef RADB_DEBUG
    printf("Returning result set!\r\n");
#   endif
    return (dbo->result);
}
#endif

/*$I0 */
#include "radb.h"
#ifdef _SQLITE3_H_

/*
 =======================================================================================================================
 =======================================================================================================================
 */
radbMaster *radb_init_sqlite(const char *file) {

    /*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/
    radbMaster  *radbm = malloc(sizeof(radbMaster));
    /*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/

    radbm->dbType = RADB_SQLITE3;
    radbm->pool.count = 0;
    if (sqlite3_open(file, (sqlite3 **) &radbm->handle)) {
        fprintf(stderr, "[RADB] Couldn't open %s: %s\r\n", file, sqlite3_errmsg((sqlite3 *) radbm->handle));
        return (0);
    }

    return (radbm);
}

/*
 =======================================================================================================================
 =======================================================================================================================
 */
void radb_prepare_sqlite(radbObject *dbo, const char *statement, va_list vl) {

    /*~~~~~~~~~~~~~~~~~~~~*/
    char        *sql,
                b;
    const char  *p,
                *op;
    char        injects[32];
    int         rc;
    size_t      len = 0,
                strl = 0;
    int         at = 0;
    /*~~~~~~~~~~~~~~~~~~~~*/

#   ifdef RADB_DEBUG
    printf("[RADB] Preparing: %s\r\n", statement);
#   endif
    memset(injects, 0, 32);
    sql = (char *) calloc(1, 2048);
    op = statement;
    for (p = strchr(statement, '%'); p != NULL; p = strchr(op, '%')) {
        strl = strlen(op) - strlen(p);
        strncpy((char *) (sql + len), op, strl);
        len += strl;
        if (sscanf((const char *) p, "%%%c", &b)) {
            if (b == '%') {
                strncpy((char *) (sql + len), "%", 1);
                len += 1;
            } else {
                strncpy((char *) (sql + len), "?", 1);
                len += 1;
                injects[at++] = b;
            }

            op = (char *) p + 2;
        }
    }

    strl = strlen(op);
    strncpy((char *) (sql + len), op, strl);
    rc = sqlite3_prepare_v2((sqlite3 *) dbo->db, sql, -1, (sqlite3_stmt **) &dbo->state, NULL);
    free(sql);
    if (rc != SQLITE_OK) {
        dbo->state = 0;
        return;
    }

    if (at) {
        for (at = 0; injects[at] != 0; at++) {
            switch (injects[at])
            {
            case 's':   rc = sqlite3_bind_text((sqlite3_stmt *) dbo->state, at + 1, va_arg(vl, const char *), -1, SQLITE_TRANSIENT); break;
            case 'u':   rc = sqlite3_bind_int((sqlite3_stmt *) dbo->state, at + 1, va_arg(vl, unsigned int)); break;
            case 'i':   rc = sqlite3_bind_int((sqlite3_stmt *) dbo->state, at + 1, va_arg(vl, signed int)); break;
            case 'l':   rc = sqlite3_bind_int64((sqlite3_stmt *) dbo->state, at + 1, va_arg(vl, signed long long int)); break;
            case 'f':   rc = sqlite3_bind_double((sqlite3_stmt *) dbo->state, at + 1, va_arg(vl, double)); break;
            default:    break;
            }

            if (rc != SQLITE_OK) {
                printf("SQLite aborted with code %d at item %u!\r\n", rc, at+1);
                dbo->state = 0;
            }
        }
    }
}

/*
 =======================================================================================================================
 =======================================================================================================================
 */
radbResult *radb_fetch_row_sqlite(radbObject *dbo) {

    /*~~~~~~~~~~~~~~*/
    int         count,
                i,
                l;
    radbResult  *res;
    /*~~~~~~~~~~~~~~*/

#   ifdef RADB_DEBUG
    printf("[RADB] Fetching a row from state %p\r\n", dbo->state);
#   endif
    count = sqlite3_column_count((sqlite3_stmt *) dbo->state);
    if (!count) return (0);
    res = malloc(sizeof(radbResult));
    res->column = malloc(count * sizeof(radbItem));
    res->items = count;
    for (i = 0; i < count; i++) {
        l = sqlite3_column_bytes((sqlite3_stmt *) dbo->state, i);
        memset(res->column[i].data.string, 0, l + 1);
        res->column[i].type = 2;
        switch (sqlite3_column_type((sqlite3_stmt *) dbo->state, i))
        {
        case SQLITE_TEXT:
            res->column[i].type = 1;
            memcpy(res->column[i].data.string, sqlite3_column_text((sqlite3_stmt *) dbo->state, i), l);
            break;

        case SQLITE_INTEGER:
            res->column[i].data.int64 = sqlite3_column_int64((sqlite3_stmt *) dbo->state, i);
            break;

        case SQLITE_FLOAT:
            res->column[i].data._double = sqlite3_column_double((sqlite3_stmt *) dbo->state, i);
            break;

        default:
            break;
        }
    }

    return (res);
}
#endif
