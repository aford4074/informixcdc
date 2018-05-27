#ifndef _INFORMIXESQL_H
#define _INFORMIXESQL_H

#define TABLENAME_LEN      (255 + 1 + 255 + 1 + 255 + 1 + 255)
#define COLARG_LEN         1024
#define MAX_CDC_TABS               64
#define MAX_CDC_COLS               64

typedef struct {
    int col_type;
    int col_xid;
    int col_size;
    char *col_name;
} column_t;

typedef struct {
    char tabname[TABLENAME_LEN+1];
    int num_cols;
    int num_var_cols;
    column_t columns[MAX_CDC_COLS];
} table_t;

int free_prepares(void);
int prepare_savepoints(void);
int set_connection(char* conn_name);
bigint query_restart_seq_number(int id);
bigint query_last_seq_number(int id);
int upsert_opntxns(int id, bigint seq_number, int transaction_id);
int delete_opntxns(int id, int transaction_id);
int upsert_lsttxn(int id, bigint seq_number);
int cdc_connect(char* conn_string, char* conn_dbservername, char* conn_name,
                char* conn_user, char* conn_passwd, int timeout,
                int max_records);
int cdc_enable(char* table, char* columns, int session_id, int tabid);
int cdc_activate(int session_id, bigint seq_number);
int commtx_savepoint(int id, bigint seq_number, int transaction_id);
int rbtx_savepoint(int id, int transaction_id);
int cdc_lo_read(int session_id, char *buffer, int read_sz, int *read_err);
int add_tabschema(int tabid, int var_len_cols, char* sql, table_t *table);

#endif
