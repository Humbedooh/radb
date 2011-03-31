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
        res->column[i].data = calloc(1, l + 1);
        res->column[i].size = l;
        res->column[i].type = 2;
        switch (sqlite3_column_type((sqlite3_stmt *) dbo->state, i))
        {
        case SQLITE_TEXT:
            res->column[i].type = 1;
            memcpy(res->column[i].data, sqlite3_column_text((sqlite3_stmt *) dbo->state, i), l);
            break;

        case SQLITE_INTEGER:
            p = res->column[i].data;
            *p = sqlite3_column_int64((sqlite3_stmt *) dbo->state, i);
            break;

        case SQLITE_FLOAT:
            d = res->column[i].data;
            *d = sqlite3_column_double((sqlite3_stmt *) dbo->state, i);
            break;

        default:
            break;
        }
    }

    return (res);
}
#endif
