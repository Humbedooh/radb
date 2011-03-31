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
                            params;
    unsigned long           *str_len;
    unsigned char           object[1024];
    int                     used = 0;
    int                     ss = 0;
    MYSQL_BIND              *bindings;
    unsigned int            d_uint;
    signed int              d_sint;
    signed long long int    d_lint;
    double                  d_double;
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
    dbo->state = mysql_stmt_init((MYSQL *) dbo->db);
    rc = mysql_stmt_prepare((MYSQL_STMT *) dbo->state, sql, strl);
    free(sql);
    if (rc) {
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
}

/*
 =======================================================================================================================
 =======================================================================================================================
 */
radbResult *radb_fetch_row_mysql(void *state) {

    /*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/
    int         count,
                i;
    radbObject    *dbo = (radbObject *) state;
    MYSQL_RES   *meta;
    MYSQL_BIND  *bindings;
    radbResult    *res = malloc(sizeof(radbResult));
    /*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/

    meta = mysql_stmt_result_metadata((MYSQL_STMT *) dbo->state);
    count = mysql_stmt_field_count((MYSQL_STMT *) dbo->state);
    bindings = calloc(sizeof(MYSQL_BIND), count);
    res->column = malloc(count * sizeof(radbItem));
    res->items = count;
    for (i = 0; i < count; i++) {
        res->column[i].data = calloc(1, meta->fields[i].length);
        bindings[i].buffer = res->column[i].data;
        res->column[i].type = (meta->fields[i].type == MYSQL_TYPE_STRING) ? 1 : 2;
        bindings[i].buffer_type = meta->fields[i].type;
    }

    mysql_stmt_bind_result((MYSQL_STMT *) dbo->state, bindings);
    mysql_stmt_store_result((MYSQL_STMT *) dbo->state);
    mysql_free_result(meta);
    free(bindings);
    return (res);
}
#endif
