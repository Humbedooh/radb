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
            dbo->result = 1;
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
            mysql_stmt_execute((MYSQL_STMT *) dbo->state);
            rc = mysql_stmt_affected_rows((MYSQL_STMT *) dbo->state);
        } else {
            fprintf(stderr, "Couldn't execute the SQL statement!\r\n");
            rc = 0;
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

    for (i = 0; i < result->items; i++) {
        free(result->column[i].data);
    }

    if (result->column) free(result->column);
    free(result);
}
