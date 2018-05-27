#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>

EXEC SQL INCLUDE sqltypes;
EXEC SQL INCLUDE sqlca;
EXEC SQL INCLUDE "informixesql.h";

EXEC SQL define CDC_MAJ_VER         1;
EXEC SQL define CDC_MIN_VER         1;
EXEC SQL define FULLROWLOG_OFF      0;
EXEC SQL define FULLROWLOG_ON       1;

#define MAX_SQL_STMT_LEN            8192

int
free_prepares(void)
{
    EXEC SQL FREE informixcdc_opntxns_ins;
    EXEC SQL FREE informixcdc_opntxns_upd;
    EXEC SQL FREE informixcdc_opntxns_del;

    return 0;
}

int
prepare_savepoints(void)
{
    EXEC SQL PREPARE informixcdc_opntxns_ins FROM
        "insert into informixcdc_opntxns (id, transaction_id, seq_number) \
            values (?, ?, ?)";

    if (SQLCODE != 0) {
        goto except;
    }

    EXEC SQL PREPARE informixcdc_opntxns_upd FROM
        "update informixcdc_opntxns \
         set seq_number = ? \
         where id = ? and transaction_id = ?";

    if (SQLCODE != 0) {
        goto except;
    }

    EXEC SQL PREPARE informixcdc_opntxns_del FROM
        "delete from informixcdc_opntxns \
         where id = ? and transaction_id = ?";

    if (SQLCODE != 0) {
        goto except;
    }

    EXEC SQL PREPARE informixcdc_lsttxn_ins FROM
        "insert into informixcdc_lsttxn (id, seq_number) \
            values (?, ?)";

    if (SQLCODE != 0) {
        goto except;
    }

    EXEC SQL PREPARE informixcdc_lsttxn_upd FROM
        "update informixcdc_lsttxn \
         set seq_number = ? \
         where id = ?";

    if (SQLCODE != 0) {
        goto except;
    }
    assert(SQLCODE == 0);
    goto finally;
except:
    assert(SQLCODE != 0);
finally:
    return SQLCODE;
}

int
set_connection(conn_name)
EXEC SQL BEGIN DECLARE SECTION;
PARAMETER char* conn_name;
EXEC SQL END DECLARE SECTION;
{
    EXEC SQL SET CONNECTION :conn_name;

    return SQLCODE;
}

bigint
query_restart_seq_number(id)
EXEC SQL BEGIN DECLARE SECTION;
PARAMETER int id;
EXEC SQL END DECLARE SECTION;
{
    EXEC SQL BEGIN DECLARE SECTION;
    bigint restart_seq_number;
    EXEC SQL END DECLARE SECTION;

    EXEC SQL PREPARE restart_seq_number FROM
        "select nvl(min(seq_number), 0) from informixcdc_opntxns where id = ?";

    if (SQLCODE != 0) {
        goto except;
    }

    EXEC SQL EXECUTE restart_seq_number
             INTO :restart_seq_number USING :id;

    if (SQLCODE != 0) {
        goto except;
    }
    assert(SQLCODE == 0);
    goto finally;
except:
    assert(SQLCODE != 0);
    restart_seq_number = SQLCODE;
finally:
    EXEC SQL FREE restart_seq_number;
    return restart_seq_number;
}

bigint
query_last_seq_number(id)
EXEC SQL BEGIN DECLARE SECTION;
PARAMETER int id;
EXEC SQL END DECLARE SECTION;
{
    EXEC SQL BEGIN DECLARE SECTION;
    bigint last_seq_number;
    EXEC SQL END DECLARE SECTION;

    EXEC SQL PREPARE last_seq_number FROM
        "select seq_number from informixcdc_lsttxn where id = ?";

    if (SQLCODE != 0) {
        goto except;
    }

    EXEC SQL EXECUTE last_seq_number INTO :last_seq_number USING :id;

    if (SQLCODE == SQLNOTFOUND) {
        last_seq_number = 0;
    }
    else if (SQLCODE != 0) {
        goto except;
    }
    assert(last_seq_number >= 0);
    assert(SQLCODE == 0);
    goto finally;
except:
    last_seq_number = SQLCODE;
finally:
    EXEC SQL FREE last_seq_number;
    return last_seq_number;
}

int
upsert_opntxns(id, seq_number, transaction_id)
EXEC SQL BEGIN DECLARE SECTION;
PARAMETER int id;
PARAMETER bigint seq_number;
PARAMETER int transaction_id;
EXEC SQL END DECLARE SECTION;
{
    EXEC SQL EXECUTE informixcdc_opntxns_ins
             USING :id, :transaction_id, :seq_number;

    if (SQLCODE == -239 || SQLCODE == -268) {
        EXEC SQL EXECUTE informixcdc_opntxns_upd
                 USING :seq_number, :id, :transaction_id;
    }
    return SQLCODE;
}

int
delete_opntxns(id, transaction_id)
EXEC SQL BEGIN DECLARE SECTION;
PARAMETER int id;
PARAMETER int transaction_id;
EXEC SQL END DECLARE SECTION;
{
    EXEC SQL EXECUTE informixcdc_opntxns_del
             USING :id, :transaction_id;

    return SQLCODE;
}

int
upsert_lsttxn(id, seq_number)
EXEC SQL BEGIN DECLARE SECTION;
PARAMETER int id;
PARAMETER bigint seq_number;
EXEC SQL END DECLARE SECTION;
{
    EXEC SQL EXECUTE informixcdc_lsttxn_upd
             USING :seq_number, :id;

    if (sqlca.sqlerrd[2] == 0) {
        EXEC SQL EXECUTE informixcdc_lsttxn_ins
                 USING :id, :seq_number;
    }

    return SQLCODE;
}

int
cdc_connect(conn_string, conn_dbservername, conn_name, conn_user, conn_passwd,
            timeout, max_records)
EXEC SQL BEGIN DECLARE SECTION;
    PARAMETER char* conn_string;
    PARAMETER char* conn_dbservername;
    PARAMETER char* conn_name;
    PARAMETER char* conn_user;
    PARAMETER char* conn_passwd;
    PARAMETER int timeout;
    PARAMETER int max_records;
EXEC SQL END DECLARE SECTION;
{
    EXEC SQL BEGIN DECLARE SECTION;
    integer session_id;
    EXEC SQL END DECLARE SECTION;

    if (conn_user && conn_passwd) {
        EXEC SQL CONNECT TO :conn_string
                         AS :conn_name
                         USER :conn_user
                         USING :conn_passwd;
    }
    else {
        EXEC SQL CONNECT TO :conn_string
                         AS :conn_name;
    }

    if (SQLCODE != 0) {
        EXEC SQL DISCONNECT CURRENT;
        return SQLCODE;
    }

    EXEC SQL EXECUTE FUNCTION informix.cdc_opensess(
        :conn_dbservername, 0, :timeout, :max_records, CDC_MAJ_VER, CDC_MIN_VER
    ) INTO :session_id;

    if (SQLCODE != 0) {
        return SQLCODE;
    }

    return session_id;
}

int
cdc_enable(table, columns, session_id, tabid)
EXEC SQL BEGIN DECLARE SECTION;
    PARAMETER char* table;
    PARAMETER char* columns;
    PARAMETER int session_id;
    PARAMETER int tabid;
EXEC SQL end DECLARE SECTION;
{
    EXEC SQL BEGIN DECLARE SECTION;
    int retval;
    EXEC SQL end DECLARE SECTION;

    EXEC SQL EXECUTE FUNCTION informix.cdc_set_fullrowlogging(
        :table, FULLROWLOG_ON
    ) INTO :retval;

    if (SQLCODE != 0) {
        return SQLCODE;
    }
    else if (retval < 0) {
        return retval;
    }

    EXEC SQL EXECUTE FUNCTION informix.cdc_startcapture(
        :session_id, 0, :table, :columns, :tabid
    ) INTO :retval;

    if (SQLCODE != 0) {
        return SQLCODE;
    }
    else if (retval < 0) {
        return retval;
    }

    return 0;
}

int
cdc_activate(session_id, seq_number)
EXEC SQL BEGIN DECLARE SECTION;
    PARAMETER int session_id;
    PARAMETER bigint seq_number;
EXEC SQL END DECLARE SECTION;
{
    EXEC SQL BEGIN DECLARE SECTION;
    int retval;
    EXEC SQL END DECLARE SECTION;

    EXEC SQL EXECUTE FUNCTION informix.cdc_activatesess(
        :session_id, :seq_number
    ) INTO :retval;

    if (SQLCODE != 0) {
        return SQLCODE;
    }
    if (retval < 0) {
        return retval;
    }

    return 0;
}

int
commtx_savepoint(int id, bigint seq_number, int transaction_id)
{
    int sqlcode;
    EXEC SQL BEGIN WORK;

    if (SQLCODE != 0) {
        return SQLCODE;
    }

    sqlcode = delete_opntxns(id, transaction_id);

    if (SQLCODE != 0) {
        return SQLCODE;
    }

    sqlcode = upsert_lsttxn(id, seq_number);

    if (SQLCODE != 0) {
        goto except;
    }

    EXEC SQL COMMIT WORK;

    if (SQLCODE != 0) {
        goto except;
    }
    assert(SQLCODE == 0);
    goto finally;
except:
    sqlcode = SQLCODE;
    EXEC SQL ROLLBACK WORK;
finally:
    return sqlcode;
}

int
rbtx_savepoint(int id, int transaction_id)
{
    return delete_opntxns(id, transaction_id);
}

int
add_tabschema(int tabid, int var_len_cols, char* cols_def, table_t *table)
{
    EXEC SQL BEGIN DECLARE SECTION;
    char sql[MAX_SQL_STMT_LEN+1];
    EXEC SQL END DECLARE SECTION;

    ifx_sqlda_t *sqlda = NULL;
    int col;
    int rc;

    table->num_cols = 0;

    sprintf(sql, "create temp table t_informixcdc (%s) with no log", cols_def);

    EXEC SQL EXECUTE IMMEDIATE :sql;

    if (SQLCODE != 0) {
        goto except;
    }

    EXEC SQL PREPARE informixcdc FROM "select * from t_informixcdc";

    if (SQLCODE != 0) {
        goto except;
    }

    EXEC SQL DESCRIBE informixcdc INTO sqlda;

    if (SQLCODE != 0) {
        goto except;
    }

    // can't find the header that defines rtypsize
    for (col=0; col < sqlda->sqld; col++) {
        table->columns[col].col_type = sqlda->sqlvar[col].sqltype;
        table->columns[col].col_size =
            rtypsize(sqlda->sqlvar[col].sqltype, sqlda->sqlvar[col].sqllen);
        table->columns[col].col_xid = sqlda->sqlvar[col].sqlxid;
        table->columns[col].col_name =
            malloc(strlen(sqlda->sqlvar[col].sqlname)+1);
        if (! table->columns[col].col_name) {
            goto except;
        }
        strcpy(table->columns[col].col_name, sqlda->sqlvar[col].sqlname);
        table->num_cols++;
    }
    table->num_var_cols = var_len_cols;

    EXEC SQL EXECUTE IMMEDIATE "drop table t_informixcdc";

    // don't raise an error if we can't drop the temp table,
    // the next attempt to add a table will fail with error

    assert(SQLCODE == 0);
    rc = SQLCODE;
    goto finally;
except:
    if (tabid < MAX_CDC_TABS) {
        for (col=0; col < table->num_cols; col++) {
            free(table->columns[col].col_name);
        }
    }
    rc = -1;
finally:
    free(sqlda);
    EXEC SQL FREE informixcdc;
    return rc;
}

int
cdc_lo_read(int session_id, char *buffer, int read_sz, int *read_err)
{
    return ifx_lo_read(session_id, buffer, read_sz, read_err);
}
