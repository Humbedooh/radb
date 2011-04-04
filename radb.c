#include "radb.h"

const char* radb_last_error(radbObject* dbo) {
    if (dbo->state == 0) { return "";}
    #ifdef _SQLITE3_H_
    if (dbo->master->dbType ==RADB_SQLITE3) {
        return sqlite3_errmsg((sqlite3 *) dbo->db);
    }
#endif
    #ifdef MYSQL_CLIENT
    if (dbo->master->dbType ==RADB_MYSQL) {
        return mysql_error((MYSQL*) dbo->db);
    }
#endif
}
/*
 =======================================================================================================================
 =======================================================================================================================
 */
radbResult *radb_step(radbObject *dbo) {
    if (dbo->state == 0) {
        fprintf(stderr, "[RDB] Can't step: Statement wasn't prepared properly!\r\n");
        return (0);
    } else printf("[RDB] Stepping\r\n");
#ifdef _SQLITE3_H_
    if (dbo->master->dbType ==RADB_SQLITE3) {
        if (sqlite3_step((sqlite3_stmt *) dbo->state) == SQLITE_ROW) {
            printf("[RDB] SQLITE says there be dragons in %p!\r\n", dbo->state);
            return (radb_fetch_row_sqlite(dbo));
        }
    }
#endif
#ifdef MYSQL_CLIENT
    if (dbo->master->dbType ==RADB_MYSQL) {
        if (dbo->result == 0) {
            printf("[RADB] Executing statement\r\n");
            mysql_stmt_execute((MYSQL_STMT *) dbo->state);
        }
        return (radb_fetch_row_mysql(dbo->state));
    }
#endif
    return (0);
}

/*
 =======================================================================================================================
 =======================================================================================================================
 */
void radb_cleanup(radbObject *dbo) {
#ifdef _SQLITE3_H_
    if (dbo->master->dbType ==RADB_SQLITE3) {
        if (dbo->state) sqlite3_finalize((sqlite3_stmt *) dbo->state);
    }
#endif
#ifdef MYSQL_CLIENT
    if (dbo->master->dbType ==RADB_MYSQL) {
        if (dbo->state) mysql_stmt_close((MYSQL_STMT *) dbo->state);
        radb_release_handle_mysql(&dbo->master->pool, dbo->db);
    }
    radb_free_result(dbo->result);
    free((radbObject *) dbo);
#endif
}

/*
 =======================================================================================================================
 =======================================================================================================================
 */
int radb_do(radbMaster* radbm, const char *statement, ...) {

    /*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/
    va_list     args;
    radbObject    *dbo = calloc(1, sizeof(radbObject));
    int         rc = 0;
    /*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/

    if (!radbm) { printf("[RADB] Received a null-pointer as radbm!\r\n"); return 0; }
    printf("[RDB] Running RDB->do\r\n");
    dbo->result = 0;
    dbo->master = radbm;
    va_start(args, statement);
#ifdef _SQLITE3_H_
    if (radbm->dbType ==RADB_SQLITE3) {
        dbo->db = radbm->handle;
        radb_prepare_sqlite(dbo, statement, args);
        printf("[RDB] Stepping\r\n");
        rc = (sqlite3_step((sqlite3_stmt *) dbo->state) == SQLITE_ROW) ? 1 : 0;
    }
#endif
#ifdef MYSQL_CLIENT
    if (radbm->dbType ==RADB_MYSQL) {
        dbo->db = radb_get_handle_mysql(&radbm->pool);
        radb_prepare_mysql(dbo, statement, args);
        printf("[RDB] Stepping\r\n");
        if (dbo->state) {
            rc = mysql_stmt_execute((MYSQL_STMT *) dbo->state);
            if (!rc) rc = mysql_stmt_affected_rows((MYSQL_STMT *) dbo->state);
        } else {
            fprintf(stderr, "Couldn't execute the SQL statement!\r\n");
            rc = -1;
        }
    }
#endif
    va_end(args);
    radb_cleanup(dbo);
    printf("[RDB] Step returned %d\r\n", rc);
    return (rc);
}

/*
 =======================================================================================================================
 =======================================================================================================================
 */
radbObject *radb_prepare(radbMaster* radbm, const char *statement, ...) {

    /*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/
    va_list     args;
    radbObject    *dbo = calloc(1, sizeof(radbObject));
    /*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/

    if (!radbm) { printf("[RADB] Received a null-pointer as radbm!\r\n"); return 0; }
    printf("[RDB] pre-preparation of: %s\r\n", statement);
    dbo->result = 0;
    dbo->master = radbm;
    dbo->result = malloc(sizeof(radbResult));
    dbo->result->items = 0;
    dbo->result->column = 0;
    dbo->result->bindings = 0;
    va_start(args, statement);
#ifdef _SQLITE3_H_
    if (radbm->dbType ==RADB_SQLITE3) {
        dbo->db = radbm->handle;
        radb_prepare_sqlite(dbo, statement, args);
    }
#endif
#ifdef MYSQL_CLIENT
    else if (radbm->dbType ==RADB_MYSQL) {
        dbo->db = radb_get_handle_mysql(&radbm->pool);
        radb_prepare_mysql(dbo, statement, args);
    }
#endif
    
    va_end(args);
    return (dbo);
}


void radb_free_result(radbResult *result) {

    /*~~~~~~~~~~*/
    unsigned    i;
    /*~~~~~~~~~~*/

    if (result->column) free(result->column);
    if (result->bindings) free(result->bindings);
    free(result);
}



/*$I0 */
#include "radb.h"
#ifdef MYSQL_CLIENT /* Only compile if mysql support is enabled */

radbMaster* radb_init_mysql(unsigned threads, const char* host, const char* user, const char* pass, const char* db, unsigned port) {
    unsigned i, ok=1;
    my_bool     yes = 1;
    MYSQL* m;
    radbMaster* radbm = malloc(sizeof(radbMaster));
    radbm->dbType = RADB_MYSQL;
    radbm->pool.count = threads;
    radbm->pool.children = calloc(threads, sizeof(radbChild));
    for (i=0;i<threads;i++) {
        radbm->pool.children[i].handle = mysql_init(0);
        m = (MYSQL*) radbm->pool.children[i].handle;
        mysql_options(m, MYSQL_OPT_RECONNECT, &yes);
        if (!mysql_real_connect(m, host, user, pass, db, port, 0, 0)) {
            fprintf(stderr, "Failed to connect to database: Error: %s", mysql_error(m));
            ok = 0;
            break;
        }
    }
    if (!ok) return 0;
    return radbm;
}
/*
 =======================================================================================================================
 =======================================================================================================================
 */
void *radb_get_handle_mysql(radbPool *pool) {
    int i,x=5;
    while (x != 0) {
        for (i=0;i<pool->count;i++) {
            if (pool->children[i].inUse == 0) { 
                pool->children[i].inUse = 1;
                return pool->children[i].handle;
            }
        }
        x--;
    }
    return (0);
}

void radb_release_handle_mysql(radbPool *pool, void* handle) {
    int i;
    for (i=0;i<pool->count;i++) {
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
                            params,i;
    unsigned long           *str_len;
    unsigned char           object[1024];
    int                     used = 0;
    int                     ss = 0;
    MYSQL_BIND              *bindings;
    unsigned int            d_uint;
    signed int              d_sint;
    signed long long int    d_lint;
    double                  d_double;
    MYSQL_RES* meta;
    /*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/

    printf("[RDB] Preparing statement: %s\n", statement);
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
    if (sql[strl-1] != ';') sql[strl++] = ';';
    printf("[RADB] Sending statement: %s\n", sql);
    dbo->state = mysql_stmt_init((MYSQL *) dbo->db);
    rc = mysql_stmt_prepare((MYSQL_STMT *) dbo->state, sql, strl);
    free(sql);
    if (rc != 0) {
        fprintf(stderr, "[RDB] Mysql: %s\r\n", mysql_error(dbo->db));
        dbo->state = 0;
        return;
    }

    params = mysql_stmt_param_count((MYSQL_STMT *) dbo->state);
    bindings = calloc(sizeof(MYSQL_BIND), params ? params + 1 : 1);
    for (at = 0; injects[at] != 0; at++) {
        bindings[at].is_null = 0;
        bindings[at].length = 0;
        bindings[at].is_unsigned = 1;
        switch (injects[at])
        {
        case 's':
            str_len = malloc(sizeof(unsigned long));
            bindings[at].buffer_type = MYSQL_TYPE_STRING;
            bindings[at].buffer = (void *) va_arg(vl, const char *);
            *str_len = strlen((const char *) bindings[at].buffer);
            bindings[at].buffer_length = *str_len;
            break;

        case 'u':
            O = &(object[used]);
            d_uint = va_arg(vl, unsigned int);
            ss = sizeof(unsigned int);
            memcpy(O, &d_uint, ss);
            bindings[at].buffer_type = MYSQL_TYPE_LONG;
            bindings[at].buffer = (void *) O;
            used += ss;
            break;

        case 'i':
            bindings[at].is_unsigned = 0;
            O = &(object[used]);
            d_sint = va_arg(vl, signed int);
            ss = sizeof(signed int);
            memcpy(O, &d_sint, ss);
            bindings[at].buffer_type = MYSQL_TYPE_LONG;
            bindings[at].buffer = (void *) O;
            used += ss;
            break;

        case 'l':
            bindings[at].is_unsigned = 0;
            bindings[at].buffer_type = MYSQL_TYPE_LONGLONG;
            ss = sizeof(signed long long int);
            O = &(object[used]);
            d_lint = va_arg(vl, signed long long int);
            memcpy(O, &d_lint, ss);
            bindings[at].buffer_type = MYSQL_TYPE_LONG;
            bindings[at].buffer = (void *) O;
            used += ss;
            break;

        case 'f':
            bindings[at].is_unsigned = 0;
            bindings[at].buffer_type = MYSQL_TYPE_DOUBLE;
            O = &(object[used]);
            d_double = va_arg(vl, double);
            ss = sizeof(double);
            memcpy(O, &d_double, ss);
            bindings[at].buffer = (void *) O;
            used += ss;
            break;

        default:
            break;
        }

        if (mysql_stmt_bind_param((MYSQL_STMT *) dbo->state, bindings)) {
            dbo->state = 0;
        }
    }
    meta = mysql_stmt_result_metadata((MYSQL_STMT *) dbo->state);
    dbo->result->items = meta->field_count;

    printf("Meta says %u columns\r\n", dbo->result->items);
    dbo->result->column = calloc(sizeof(radbItem), dbo->result->items);
    bindings = calloc(sizeof(MYSQL_BIND), dbo->result->items);
    
    for (i = 0; i < dbo->result->items; i++) {
        memset(dbo->result->column[i].data.string, 0, 255);
        bindings[i].buffer = dbo->result->column[i].data.string;
        dbo->result->column[i].type = (meta->fields[i].type == MYSQL_TYPE_STRING) ? 1 : 2;
        bindings[i].buffer_type = meta->fields[i].type;
    }
    dbo->result->bindings = bindings;
    mysql_free_result(meta);
    printf("done here!\r\n");
    dbo->result->stored = 0;
    fflush(stdout);
}

/*
 =======================================================================================================================
 =======================================================================================================================
 */
radbResult *radb_fetch_row_mysql(void *state) {

    /*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/
    radbObject    *dbo = (radbObject *) state;
    /*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/
    printf("[RADB] Fetching a result %u\r\n", dbo->result->stored);
    if (dbo->result->stored != 1) {
        dbo->result->stored = 1;

        printf("Binding to result set\r\n");
        fflush(stdout);
        mysql_stmt_bind_result((MYSQL_STMT *) dbo->state, (MYSQL_BIND*)dbo->result->bindings);
        mysql_stmt_store_result((MYSQL_STMT *) dbo->state);

    }
    if (mysql_stmt_fetch((MYSQL_STMT *) dbo->state)) {
        return 0;
    }

    return (dbo->result);
}
#endif


/*$I0 */
#include "radb.h"
#ifdef _SQLITE3_H_


radbMaster* radb_init_sqlite(const char* file) {
    unsigned i, ok=1;
    radbMaster* radbm = malloc(sizeof(radbMaster));
    radbm->dbType = RADB_SQLITE3;
    radbm->pool.count = 0;
     if (sqlite3_open(file, (sqlite3 **) &radbm->handle)) {
         fprintf(stderr, "[RADB] Couldn't open %s: %s\r\n", file, sqlite3_errmsg((sqlite3 *) radbm->handle));
         return 0;
     }
    return radbm;
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

    printf("[RDB] Preparing: %s\r\n", statement);
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
            dbo->state = 0;
        }
    }
}

/*
 =======================================================================================================================
 =======================================================================================================================
 */
radbResult *radb_fetch_row_sqlite(void *state) {

    /*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/
    int                     count,
                            i,
                            l;
    signed long long int    *p;
    double                  *d;
    radbObject                *dbo = (radbObject *) state;
    radbResult                *res;
    /*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/

    printf("[RDB] Fetching a row from state %p\r\n", dbo->state);
    count = sqlite3_column_count((sqlite3_stmt *) dbo->state);
    if (!count) return (0);
    res = malloc(sizeof(radbResult));
    res->column = malloc(count * sizeof(radbItem));
    res->items = count;
    for (i = 0; i < count; i++) {
        l = sqlite3_column_bytes((sqlite3_stmt *) dbo->state, i);
        memset( res->column[i].data.string, 0, l+1);
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
