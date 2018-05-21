
/* InformixCdc objects */

#include <Python.h>
#include "structmember.h"

/* From the InformixDB module:
 * Python and Informix both have a datetime.h, the Informix header is
 * included above because it comes first in the include path. We manually
 * include the Python one here (needs a few preprocessor tricks...)
 */
#include "datetime.h"
#define DATETIME_INCLUDE datetime.h
#define PYDTINC2(f) #f
#define PYDTINC1(f) PYDTINC2(f)
#include PYDTINC1(PYTHON_INCLUDE/DATETIME_INCLUDE)

EXEC SQL INCLUDE sqltypes;

#define DEFAULT_TIMEOUT 60
#define DEFAULT_MAX_RECORDS 100
#define DEFAULT_SYSCDCDB "syscdcv1"
#define CONNNAME_LEN 50
#define CONNSTRING_LEN 512

/*
 * for reading the CDC SBLOB via ifx_lo_read()
 * the buffer must be large enough to hold at least 1 full CDC record
 * make this configurable at runtime in the future
 */
#define LO_BYTES_PER_READ 65536
#define DATABUFFER_SIZE LO_BYTES_PER_READ * 2  // must be large enough to hold 1 record

#define MAX_CDC_TABS 64
#define MAX_CDC_COLS 64
#define MAX_SQL_STMT_LEN 8192

#define PACKET_SCHEME 66

#define HEADER_SZ_OFFSET 0
#define PAYLOAD_SZ_OFFSET (HEADER_SZ_OFFSET + 4)
#define PACKET_SCHEME_OFFSET (PAYLOAD_SZ_OFFSET + 4)
#define RECORD_NUMBER_OFFSET (PACKET_SCHEME_OFFSET + 4)
#define RECORD_HEADER_OFFSET (RECORD_NUMBER_OFFSET + 4)
#define CHANGE_HEADER_SZ 20

/*
 * Informix CDC Record types
*/

#define CDC_REC_BEGINTX            1
#define CDC_REC_COMMTX             2
#define CDC_REC_RBTX               3
#define CDC_REC_INSERT            40
#define CDC_REC_DELETE            41
#define CDC_REC_UPDBEF            42
#define CDC_REC_UPDAFT            43
#define CDC_REC_DISCARD           62
#define CDC_REC_TRUNCATE         119
#define CDC_REC_TABSCHEM         200
#define CDC_REC_TIMEOUT          201
#define CDC_REC_ERROR            202

EXEC SQL define TABLENAME_LEN 768;
EXEC SQL define COLARG_LEN 1024;
EXEC SQL define CDC_MAJ_VER 1;
EXEC SQL define CDC_MIN_VER 1;
EXEC SQL define FULLROWLOG_OFF 0;
EXEC SQL define FULLROWLOG_ON 1;

/*
 * Structs for maintaining copies of sqlda structs
*/

typedef struct {
    int col_type;
    int col_xid;
    int col_size;
    char *col_name;
} column_t;

typedef struct {
    int num_cols;
    int num_var_cols;
    column_t column[MAX_CDC_COLS];
} columns_t;

#define PY_DEFAULT_ARGUMENT_INIT(name, value, ret) \
    PyObject *name = NULL; \
    static PyObject *default_##name = NULL; \
    if (! default_##name) { \
        default_##name = value; \
        if (! default_##name) { \
            PyErr_SetString(PyExc_RuntimeError, \
                            "Can not create default value for " #name); \
            return ret; \
        } \
    }

#define PY_DEFAULT_ARGUMENT_SET(name) if (! name) name = default_##name; \
    Py_INCREF(name)

static PyObject *ErrorObject;

/*
 * The InformixCdcObject
*/

typedef struct {
    PyObject_HEAD
    char name[CONNNAME_LEN];
    int is_connected;
    $integer session_id;
    PyObject *dbservername;
    int timeout;
    int max_records;
    PyObject *syscdcdb;
    char *lo_buffer;
    char *next_record_start;
    //int partial_record_bytes;
    int bytes_in_buffer;
    int endianness;
    int next_table_id;
    columns_t tab_cols[MAX_CDC_TABS];
} InformixCdcObject;

/*
 * Function prototypes for extracting data from CDC Records
*/

static int2 ld2(const char *p);
static int4 ld4(const char *p);
static bigint ld8(const char *p);
static double lddbl(const char *p, const int endianness);
static float ldfloat(const char *p, const int endianness);
static int get_platform_endianness(void);
static char* fmt_utc_time(char *buf, time_t t);

static void cdc_extract_header(const char *record, int *header_sz, int *payload_sz,
                               int *packet_scheme, int *record_number);
static PyObject* cdc_extract_record(InformixCdcObject *self, int payload_sz,
                                    int packet_scheme, int record_number);
static int cdc_extract_tabschema(InformixCdcObject *self, int payload_sz, PyObject *py_dict);
static int cdc_extract_timeout(InformixCdcObject *self, int payload_sz, PyObject *py_dict);
static int cdc_add_tabschema(InformixCdcObject *self, int payload_sz);
static int cdc_extract_iud(InformixCdcObject *self, int payload_sz, PyObject *py_dict);
static PyObject *cdc_extract_columns_to_list(InformixCdcObject *self, int tabid);

static PyTypeObject InformixCdc_Type;

#define InformixCdcObject_Check(v)      (Py_TYPE(v) == &InformixCdc_Type)

static void
InformixCdc_dealloc(InformixCdcObject* self)
{
    Py_XDECREF(self->dbservername);
    Py_XDECREF(self->syscdcdb);
    PyMem_Free(self->lo_buffer);

    for (int tabid=0; tabid < self->next_table_id; tabid++) {
        for (int col=0; col < self->tab_cols[tabid].num_cols; col++) {
            PyMem_Free(self->tab_cols[tabid].column[col].col_name);
        }
    }

    Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyObject *
InformixCdc_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    InformixCdcObject *self = NULL;

    self = (InformixCdcObject *)type->tp_alloc(type, 0);
    if (self == NULL) {
        goto except;
    }

    self->name[0] = '\0';
    self->is_connected = 0;
    self->session_id = -1;

    self->dbservername = PyString_FromString("");
    if (self->dbservername == NULL) {
        goto except;
    }

    self->timeout = -1;
    self->max_records = -1;

    self->syscdcdb = PyString_FromString("");
    if (self->syscdcdb == NULL) {
        goto except;
    }

    self->lo_buffer = NULL;
    self->next_record_start = NULL;
    //self->partial_record_bytes = 0;
    self->bytes_in_buffer = 0;
    self->endianness = 0;
    self->next_table_id = 0;

    assert(! PyErr_Occurred());
    assert(self);
    goto finally;
except:
    assert(PyErr_Occurred());
    Py_XDECREF(self);
    self = NULL;
finally:
    return (PyObject *)self;
}

static int
InformixCdc_init(InformixCdcObject *self, PyObject *args, PyObject *kwds)
{
    int ret = -1;
    PyObject *dbservername = NULL;
    PyObject *tmp;
    PY_DEFAULT_ARGUMENT_INIT(syscdcdb, PyString_FromString(DEFAULT_SYSCDCDB), -1);
    self->timeout = DEFAULT_TIMEOUT;
    self->max_records = DEFAULT_MAX_RECORDS;

    static const char *kwlist[] = { "dbservername", "timeout", "max_records",
                                    "syscdcdb", NULL };
    if (! PyArg_ParseTupleAndKeywords(args, kwds, "S|iiS:init",
                                      (char**)(kwlist),
                                      &dbservername, &self->timeout,
                                      &self->max_records, &syscdcdb)) {
        return -1;
    }

    PY_DEFAULT_ARGUMENT_SET(syscdcdb);

    sprintf(self->name, "cdc%p", self);

    tmp = self->dbservername;
    Py_INCREF(dbservername);
    self->dbservername = dbservername;
    Py_DECREF(tmp);

    tmp = self->syscdcdb;
    Py_INCREF(syscdcdb);
    self->syscdcdb = syscdcdb;
    Py_DECREF(tmp);

    if (self->lo_buffer == NULL) {
        self->lo_buffer = PyMem_Malloc(DATABUFFER_SIZE);
        if (! self->lo_buffer) {
            PyErr_SetString(PyExc_MemoryError, "cannot allocate lo_buffer");
            goto except;
        }
    }

    self->next_record_start = self->lo_buffer;
    //self->partial_record_bytes = 0;
    self->bytes_in_buffer = 0;
    self->endianness = get_platform_endianness();
    self->next_table_id = 0;

    ret = 0;
    assert(! PyErr_Occurred());
    goto finally;
except:
    assert(PyErr_Occurred());
    ret = -1;
finally:
    return ret;
}

static PyMemberDef InformixCdc_members[] = {
    {
        "dbservername",
        T_OBJECT_EX,
        offsetof(InformixCdcObject, dbservername),
        READONLY,
        PyDoc_STR("dbservername of Informix instance to capture CDC"),
    },
    {
        "timeout",
        T_INT,
        offsetof(InformixCdcObject, timeout),
        READONLY,
        PyDoc_STR("timeout in seconds to block while waiting for CDC events"),
    },
    {
        "max_records",
        T_INT,
        offsetof(InformixCdcObject, max_records),
        READONLY,
        PyDoc_STR("max records CDC can return in one event message"),
    },
    {
        "syscdcdb",
        T_OBJECT_EX,
        offsetof(InformixCdcObject, syscdcdb),
        READONLY,
        PyDoc_STR("syscdc database name"),
    },
    {
        NULL
    }
};

static PyObject *
InformixCdc_getis_connected(InformixCdcObject *self, void *closure)
{
    PyObject *is_connected = NULL;

    is_connected = PyBool_FromLong(self->is_connected);
    if (is_connected == NULL) {
        goto except;
    }

    assert(! PyErr_Occurred());
    assert(is_connected);
    goto finally;
except:
    assert(PyErr_Occurred());
    Py_XDECREF(is_connected);
    is_connected = NULL;
finally:
    return is_connected;
}

static PyObject *
InformixCdc_getsession_id(InformixCdcObject *self, void *closure)
{
    PyObject *session_id = NULL;

    session_id = PyInt_FromLong(self->session_id);
    if (session_id == NULL) {
        goto except;
    }

    assert(! PyErr_Occurred());
    assert(session_id);
    goto finally;
except:
    assert(PyErr_Occurred());
    Py_XDECREF(session_id);
    session_id = NULL;
finally:
    return session_id;
}

static PyGetSetDef InformixCdc_getseters[] = {
    {
        "is_connected",
        (getter)InformixCdc_getis_connected,
        NULL,
        PyDoc_STR("status of connection to CDC database server"),
        NULL
    },
    {
        "session_id",
        (getter)InformixCdc_getsession_id,
        NULL,
        PyDoc_STR("session id returned by Informix for CDC"),
        NULL
    },
    {
        NULL
    }
};

static PyObject *
InformixCdc_connect(InformixCdcObject *self, PyObject *args, PyObject *kwds)
{
    PyObject *ret = NULL;
    EXEC SQL BEGIN DECLARE SECTION;
    char conn_string[CONNSTRING_LEN];
    char *conn_dbservername;
    char *conn_name = self->name;
    char *conn_user;
    char *conn_passwd;
    long timeout = self->timeout;
    long max_records = self->max_records;
    $integer session_id;
    EXEC SQL END DECLARE SECTION;
    const char* conn_syscdcdb;
    PyObject *py_conn_user = NULL;
    PyObject *py_conn_passwd = NULL;

    static const char *kwlist[] = { "user", "passwd", NULL };
    if (! PyArg_ParseTupleAndKeywords(args, kwds, "|SS:connect",
                                      (char**)kwlist, &py_conn_user,
                                      &py_conn_passwd)) {
        return NULL;
    }

    if (py_conn_user == NULL) {
        Py_INCREF(Py_None);
        py_conn_user = Py_None;
    }

    if (py_conn_passwd == NULL) {
        Py_INCREF(Py_None);
        py_conn_passwd = Py_None;
    }

    conn_dbservername = PyString_AsString(self->dbservername);
    if (conn_dbservername == NULL) {
        goto except;
    }

    conn_syscdcdb = PyString_AsString(self->syscdcdb);
    if (conn_syscdcdb == NULL) {
        goto except;
    }

    sprintf(conn_string, "%s@%s", conn_syscdcdb, conn_dbservername);

    if (py_conn_user != Py_None && py_conn_passwd != Py_None) {
        conn_user = PyString_AsString(py_conn_user);
        if (conn_user == NULL ) {
            goto except;
        }
        conn_passwd = PyString_AsString(py_conn_passwd);
        if (conn_passwd == NULL) {
            goto except;
        }
        EXEC SQL CONNECT TO :conn_string AS :conn_name USER :conn_user USING :conn_passwd;
    }
    else {
        EXEC SQL CONNECT TO :conn_string AS :conn_name;
    }

    if (SQLCODE != 0) {
        ret = PyInt_FromLong(SQLCODE);
        if (ret == NULL) {
            goto except;
        }
        goto finally;
    }

    self->is_connected = 1;

    EXEC SQL EXECUTE FUNCTION informix.cdc_opensess(
        :conn_dbservername, 0, :timeout, :max_records, CDC_MAJ_VER, CDC_MIN_VER
    ) INTO :session_id;

    if (SQLCODE != 0) {
        ret = PyInt_FromLong(SQLCODE);
        if (ret == NULL) {
            goto except;
        }
        goto finally;
    }
    else if (session_id < 0) {
        ret = PyInt_FromLong(session_id);
        if (ret == NULL) {
            goto except;
        }
        goto finally;
    }

    self->session_id = session_id;

    ret = PyInt_FromLong(0);
    assert(! PyErr_Occurred());
    assert(ret);
    goto finally;
except:
    assert(PyErr_Occurred());
    Py_XDECREF(ret);
    ret = NULL;
finally:
    Py_DECREF(py_conn_user);
    Py_DECREF(py_conn_passwd);
    return ret;
}

static PyObject *
InformixCdc_enable(InformixCdcObject *self, PyObject *args, PyObject *kwds)
{
    PyObject *ret = NULL;
    EXEC SQL BEGIN DECLARE SECTION;
    char *cdc_table = NULL;
    char *cdc_columns = NULL;
    int session_id = self->session_id;
    int table_id = self->next_table_id;
    int retval;
    EXEC SQL END DECLARE SECTION;

    PyObject *py_cdc_table = NULL;
    PyObject *py_cdc_columns = NULL;

    if (table_id >= MAX_CDC_TABS) {
        PyErr_SetString(PyExc_IndexError, "max Informix CDC tables reached");
        return NULL;
    }

    static const char *kwlist[] = {"table", "columns", NULL };

    if (! PyArg_ParseTupleAndKeywords(args, kwds, "SS:enable",
                                      (char**)kwlist, &py_cdc_table,
                                      &py_cdc_columns)) {
        return NULL;
    }

    cdc_table = PyString_AsString(py_cdc_table);
    if (cdc_table == NULL ) {
        goto except;
    }

    cdc_columns = PyString_AsString(py_cdc_columns);
    if (cdc_columns == NULL) {
        goto except;
    }

    EXEC SQL EXECUTE FUNCTION informix.cdc_set_fullrowlogging(
        :cdc_table, FULLROWLOG_ON
    ) INTO :retval;

    if (SQLCODE != 0) {
        ret = PyInt_FromLong(SQLCODE);
        goto finally;
    }
    else if (retval < 0) {
        ret = PyInt_FromLong(retval);
        goto finally;
    }

    EXEC SQL EXECUTE FUNCTION informix.cdc_startcapture(
        :session_id, 0, :cdc_table, :cdc_columns, :table_id
    ) INTO :retval;

    if (SQLCODE != 0) {
        ret = PyInt_FromLong(SQLCODE);
        if (ret == NULL) {
            goto except;
        }
        goto finally;
    }
    else if (retval < 0) {
        ret = PyInt_FromLong(retval);
        if (ret == NULL) {
            goto except;
        }
        goto finally;
    }

    self->next_table_id += 1;
    ret = PyInt_FromLong(0);
    if (ret == NULL) {
        goto except;
    }
    assert(! PyErr_Occurred());
    assert(ret);
    goto finally;
except:
    assert(PyErr_Occurred());
    Py_XDECREF(ret);
    ret = NULL;
finally:
    Py_DECREF(py_cdc_table);
    Py_DECREF(py_cdc_columns);
    return ret;

}

static PyObject *
InformixCdc_activate(InformixCdcObject *self)
{
    PyObject *ret = NULL;
    EXEC SQL BEGIN DECLARE SECTION;
    $integer session_id = self->session_id;
    bigint seq_number = 0;
    $integer retval = -1;
    EXEC SQL END DECLARE SECTION;

    EXEC SQL EXECUTE FUNCTION informix.cdc_activatesess(
        :session_id, :seq_number
    ) INTO :retval;

    if (retval < 0) {
        ret = PyInt_FromLong(retval);
        if (ret == NULL) {
            goto except;
        }
        goto finally;
    }

    ret = PyInt_FromLong(SQLCODE);
    if (ret == NULL) {
        goto except;
    }
    assert(! PyErr_Occurred());
    assert(ret);
    goto finally;
except:
    assert(PyErr_Occurred());
    Py_XDECREF(ret);
    ret = NULL;
finally:
    return ret;
}

static PyObject *
InformixCdc_fetchone(InformixCdcObject *self)
{
    PyObject *py_dict = NULL;
    int bytes_read;
    mint lo_read_err = 0;
    int4 header_sz;
    int4 payload_sz;
    int4 packet_scheme;
    int4 record_number;
    int4 record_sz;
    int rc;

    while (1) {
        if (self->bytes_in_buffer >= 16) {
            cdc_extract_header(self->next_record_start, &header_sz,
                               &payload_sz, &packet_scheme, &record_number);

            record_sz = header_sz + payload_sz;
            if (self->bytes_in_buffer >= record_sz) {
                py_dict = cdc_extract_record(self, payload_sz,
                                             packet_scheme, record_number);
                if (py_dict == NULL) {
                    goto except;
                }

                if (record_number == CDC_REC_TABSCHEM) {
                    rc = cdc_add_tabschema(self, payload_sz);
                    if (rc != 0) {
                        PyErr_SetString(PyExc_IndexError,
                                        "cannot add Informix CDC table scheme");
                        goto except;
                    }
                }

                self->next_record_start += record_sz;
                self->bytes_in_buffer -= record_sz;

                // we've read and extracted a full record, break out of the loop
                break;
            }
        }

        // there isn't a full record in the buffer if we are here
        memcpy(self->lo_buffer, self->next_record_start, self->bytes_in_buffer);
        bytes_read = ifx_lo_read(self->session_id,
                                 &self->lo_buffer[self->bytes_in_buffer],
                                 LO_BYTES_PER_READ, &lo_read_err);
        if (bytes_read <= 0 || lo_read_err < 0) {
            PyErr_SetString(PyExc_IOError, "read from Informix CDC SBLOB failed");
            goto except;
        }
        self->next_record_start = self->lo_buffer;
        self->bytes_in_buffer += bytes_read;
    }
    assert(! PyErr_Occurred());
    assert(py_dict);
    goto finally;
except:
    assert(PyErr_Occurred());
    Py_XDECREF(py_dict);
    py_dict = NULL;
finally:
    return py_dict;
}

/*
static PyObject *
InformixCdc_fetchone_old(InformixCdcObject *self)
{
    PyObject *py_dict = NULL;
    int bytes_read;
    mint lo_read_err = 0;
    int4 header_sz;
    int4 payload_sz;
    int4 packet_scheme;
    int4 record_number;
    int4 record_sz;
    int rc;

    while (1) {
        // if a partial record or we've got nothing in the buffer,
        // we need to read from the SLOB
        // if a partial record we need to copy the partial to the head of lo_buffer
        if (self->partial_record_bytes > 0 || self->bytes_in_buffer == 0) {
            if (self->partial_record_bytes > 0) {
                memcpy(self->lo_buffer, self->next_record_start, self->partial_record_bytes);
                bytes_read = ifx_lo_read(self->session_id,
                                         &self->lo_buffer[self->partial_record_bytes],
                                         LO_BYTES_PER_READ, &lo_read_err);
                self->partial_record_bytes = 0;
            }
            else {
                bytes_read = ifx_lo_read(self->session_id,
                                         self->lo_buffer,
                                         LO_BYTES_PER_READ, &lo_read_err);
            }

            if (bytes_read <= 0 || lo_read_err < 0) {
                PyErr_SetString(PyExc_IOError, "read from Informix CDC SBLOB failed");
                goto except;
            }

            self->next_record_start = self->lo_buffer;
            self->bytes_in_buffer += bytes_read;
        }

        if (self->bytes_in_buffer >= 16) {
            // we have enough to extract the record size
            cdc_extract_header(self->next_record_start, &header_sz,
                               &payload_sz, &packet_scheme, &record_number);

            record_sz = header_sz + payload_sz;
            if (self->bytes_in_buffer >= record_sz) {
                py_dict = cdc_extract_record(
                    self, payload_sz, packet_scheme, record_number
                );
                if (py_dict == NULL) {
                    goto except;
                }

                if (record_number == CDC_REC_TABSCHEM) {
                    rc = cdc_add_tabschema(self, payload_sz);
                    if (rc != 0) {
                        PyErr_SetString(PyExc_IndexError,
                                        "cannot add Informix CDC table scheme");
                        goto except;
                    }
                }

                self->next_record_start += record_sz;
                self->bytes_in_buffer -= record_sz;
                break;
            }
            else {
                self->partial_record_bytes = self->bytes_in_buffer;
            }
        }
        else {
            self->partial_record_bytes = self->bytes_in_buffer;
        }
    }
    assert(! PyErr_Occurred());
    assert(py_dict);
    goto finally;
except:
    assert(PyErr_Occurred());
    Py_XDECREF(py_dict);
    py_dict = NULL;
finally:
    return py_dict;
}
*/


static PyObject *
InformixCdc_iter(InformixCdcObject *self)
{
    Py_INCREF(self);
    return (PyObject *)self;
}

static PyObject *
InformixCdc_iternext(InformixCdcObject *self)
{
    PyObject *py_buffer = InformixCdc_fetchone(self);
    if (py_buffer == NULL) {
        goto except;
    }
    assert(! PyErr_Occurred());
    assert(py_buffer);
    goto finally;
except:
    assert(PyErr_Occurred());
    Py_XDECREF(py_buffer);
    py_buffer = NULL;
finally:
    return py_buffer;
}

static PyMethodDef InformixCdc_methods[] = {
    {
        "connect",
        (PyCFunction)InformixCdc_connect,
        METH_VARARGS | METH_KEYWORDS,
        PyDoc_STR("connect() -> None")
    },
    {
        "enable",
        (PyCFunction)InformixCdc_enable,
        METH_VARARGS | METH_KEYWORDS,
        PyDoc_STR("enable() -> None")
    },
    {
        "activate",
        (PyCFunction)InformixCdc_activate,
        METH_NOARGS,
        PyDoc_STR("activate() -> None")
    },
    {   /* sentinel */
        NULL,
        NULL
    }
};

static PyTypeObject InformixCdc_Type = {
    /* The ob_type field must be initialized in the module init function
     * to be portable to Windows without using C++. */
    PyVarObject_HEAD_INIT(NULL, 0)
    "informixcdcmodule.InformixCdc",             /*tp_name*/
    sizeof(InformixCdcObject),          /*tp_basicsize*/
    0,                          /*tp_itemsize*/
    /* methods */
    (destructor)InformixCdc_dealloc, /*tp_dealloc*/
    0,                          /*tp_print*/
    0,                        /*tp_getattr*/
    0,                        /*tp_setattr*/
    0,                          /*tp_compare*/
    0,                          /*tp_repr*/
    0,                          /*tp_as_number*/
    0,                          /*tp_as_sequence*/
    0,                          /*tp_as_mapping*/
    0,                          /*tp_hash*/
    0,                      /*tp_call*/
    0,                      /*tp_str*/
    0,                      /*tp_getattro*/
    0,                      /*tp_setattro*/
    0,                      /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,     /*tp_flags*/
    "InformixCdc Objects",                      /*tp_doc*/
    0,                      /*tp_traverse*/
    0,                      /*tp_clear*/
    0,                      /*tp_richcompare*/
    0,                      /*tp_weaklistoffset*/
    (getiterfunc)InformixCdc_iter,                      /*tp_iter*/
    (iternextfunc)InformixCdc_iternext,                      /*tp_iternext*/
    InformixCdc_methods,                      /*tp_methods*/
    InformixCdc_members,                      /*tp_members*/
    InformixCdc_getseters,                      /*tp_getset*/
    0,                      /*tp_base*/
    0,                      /*tp_dict*/
    0,                      /*tp_descr_get*/
    0,                      /*tp_descr_set*/
    0,                      /*tp_dictoffset*/
    (initproc)InformixCdc_init,                      /*tp_init*/
    0,                      /*tp_alloc*/
    InformixCdc_new,                      /*tp_new*/
    0,                      /*tp_free*/
    0,                      /*tp_is_gc*/
};
/* --------------------------------------------------------------------- */

/* ---------- */

static PyTypeObject Str_Type = {
    /* The ob_type field must be initialized in the module init function
     * to be portable to Windows without using C++. */
    PyVarObject_HEAD_INIT(NULL, 0)
    "informixcdcmodule.Str",             /*tp_name*/
    0,                          /*tp_basicsize*/
    0,                          /*tp_itemsize*/
    /* methods */
    0,                          /*tp_dealloc*/
    0,                          /*tp_print*/
    0,                          /*tp_getattr*/
    0,                          /*tp_setattr*/
    0,                          /*tp_compare*/
    0,                          /*tp_repr*/
    0,                          /*tp_as_number*/
    0,                          /*tp_as_sequence*/
    0,                          /*tp_as_mapping*/
    0,                          /*tp_hash*/
    0,                          /*tp_call*/
    0,                          /*tp_str*/
    0,                          /*tp_getattro*/
    0,                          /*tp_setattro*/
    0,                          /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE, /*tp_flags*/
    0,                          /*tp_doc*/
    0,                          /*tp_traverse*/
    0,                          /*tp_clear*/
    0,                          /*tp_richcompare*/
    0,                          /*tp_weaklistoffset*/
    0,                          /*tp_iter*/
    0,                          /*tp_iternext*/
    0,                          /*tp_methods*/
    0,                          /*tp_members*/
    0,                          /*tp_getset*/
    0, /* see initxx */         /*tp_base*/
    0,                          /*tp_dict*/
    0,                          /*tp_descr_get*/
    0,                          /*tp_descr_set*/
    0,                          /*tp_dictoffset*/
    0,                          /*tp_init*/
    0,                          /*tp_alloc*/
    0,                          /*tp_new*/
    0,                          /*tp_free*/
    0,                          /*tp_is_gc*/
};

/* ---------- */

static PyObject *
null_richcompare(PyObject *self, PyObject *other, int op)
{
    Py_INCREF(Py_NotImplemented);
    return Py_NotImplemented;
}

static PyTypeObject Null_Type = {
    /* The ob_type field must be initialized in the module init function
     * to be portable to Windows without using C++. */
    PyVarObject_HEAD_INIT(NULL, 0)
    "informixcdcmodule.Null",            /*tp_name*/
    0,                          /*tp_basicsize*/
    0,                          /*tp_itemsize*/
    /* methods */
    0,                          /*tp_dealloc*/
    0,                          /*tp_print*/
    0,                          /*tp_getattr*/
    0,                          /*tp_setattr*/
    0,                          /*tp_compare*/
    0,                          /*tp_repr*/
    0,                          /*tp_as_number*/
    0,                          /*tp_as_sequence*/
    0,                          /*tp_as_mapping*/
    0,                          /*tp_hash*/
    0,                          /*tp_call*/
    0,                          /*tp_str*/
    0,                          /*tp_getattro*/
    0,                          /*tp_setattro*/
    0,                          /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE, /*tp_flags*/
    0,                          /*tp_doc*/
    0,                          /*tp_traverse*/
    0,                          /*tp_clear*/
    null_richcompare,           /*tp_richcompare*/
    0,                          /*tp_weaklistoffset*/
    0,                          /*tp_iter*/
    0,                          /*tp_iternext*/
    0,                          /*tp_methods*/
    0,                          /*tp_members*/
    0,                          /*tp_getset*/
    0, /* see initxx */         /*tp_base*/
    0,                          /*tp_dict*/
    0,                          /*tp_descr_get*/
    0,                          /*tp_descr_set*/
    0,                          /*tp_dictoffset*/
    0,                          /*tp_init*/
    0,                          /*tp_alloc*/
    0, /* see initxx */         /*tp_new*/
    0,                          /*tp_free*/
    0,                          /*tp_is_gc*/
};


/* ---------- */

static PyObject *
informixcdc_connect(PyObject *self, PyObject *args, PyObject *kwds) {
    Py_INCREF(Py_None);
    return Py_None;
}

/* List of functions defined in the module */

static PyMethodDef informixcdc_methods[] = {
    {
        "connect",
        (PyCFunction)informixcdc_connect,
        METH_NOARGS,
        PyDoc_STR("connect() -> None")
    },
    {   /* sentinel */
        NULL,
        NULL
    }
};

PyDoc_STRVAR(module_doc,
"This is a module for Informix Change Data Capture.");

PyMODINIT_FUNC
init_informixcdc(void)
{
    PyObject *m;

    PyDateTime_IMPORT;

    /* Due to cross platform compiler issues the slots must be filled
     * here. It's required for portability to Windows without requiring
     * C++. */
    Null_Type.tp_base = &PyBaseObject_Type;
    Null_Type.tp_new = PyType_GenericNew;
    Str_Type.tp_base = &PyUnicode_Type;

    /* Finalize the type object including setting type of the new type
     * object; doing it here is required for portability, too. */
    if (PyType_Ready(&InformixCdc_Type) < 0)
        return;

    /* Create the module and add the functions */
    m = Py_InitModule3("_informixcdc", informixcdc_methods, module_doc);
    if (m == NULL) {
        return;
    }

    /* Add some symbolic constants to the module */
    if (ErrorObject == NULL) {
        ErrorObject = PyErr_NewException("informixcdc.error", NULL, NULL);
        if (ErrorObject == NULL) {
            return;
        }
    }
    Py_INCREF(ErrorObject);
    PyModule_AddObject(m, "error", ErrorObject);

    /* Add Str */
    if (PyType_Ready(&Str_Type) < 0) {
        return;
    }
    PyModule_AddObject(m, "Str", (PyObject *)&Str_Type);

    /* Add Null */
    if (PyType_Ready(&Null_Type) < 0) {
        return;
    }
    PyModule_AddObject(m, "Null", (PyObject *)&Null_Type);

    Py_INCREF(&InformixCdc_Type);
    PyModule_AddObject(m, "InformixCdc", (PyObject *)&InformixCdc_Type);
}

#define BYTESHIFT 8
#define BYTEMASK 0xFF
#define IS_LITTLE_ENDIAN 0
#define IS_BIG_ENDIAN 1

static int2
ld2 (const char *p)
{
    int2 rtn = (((p)[0] << BYTESHIFT) + ((p)[1] & BYTEMASK));
    return rtn;
}

static int4
ld4 (const char *p)
{
    int4 rtn = ((((((
        ((int4)p[0] << BYTESHIFT) +
        (p[1] & BYTEMASK)) << BYTESHIFT) +
        (p[2] & BYTEMASK)) << BYTESHIFT) +
        (p[3] & BYTEMASK)));
    return rtn;
}

static bigint
ld8(const char *p)
{
    bigint rtn = ((((((((((((((
        ((bigint)p[0] << BYTESHIFT) +
        ((bigint)p[1] & BYTEMASK)) << BYTESHIFT) +
        ((bigint)p[2] & BYTEMASK)) << BYTESHIFT) +
        ((bigint)p[3] & BYTEMASK)) << BYTESHIFT) +
        ((bigint)p[4] & BYTEMASK)) << BYTESHIFT) +
        ((bigint)p[5] & BYTEMASK)) << BYTESHIFT) +
        ((bigint)p[6] & BYTEMASK)) << BYTESHIFT) +
        ((bigint)p[7] & BYTEMASK)));
    return rtn;
}

static double
lddbl(const char *p, const int endianness)
{
    double fval;

    if (endianness == IS_LITTLE_ENDIAN) {
        char *f;
        int c;
        f = (char *)&fval + sizeof(double) - 1;
        c = sizeof(double);
        do {
            *f-- = *p++;
        } while (--c);
    }
    else {
        memcpy((char*)&fval, p, sizeof(double));
    }

    return(fval);
}

float
ldfloat(const char *p, const int endianness)
{
    float fval;

    if (endianness == IS_LITTLE_ENDIAN) {
        char *f;
        int c;
        f = (char *)&fval + sizeof(float) - 1;
        c = sizeof(float);
        do {
            *f-- = *p++;
        } while (--c);
    }
    else {
        memcpy((char*)&fval, p, sizeof(float));
    }

    return(fval);
}

static int
get_platform_endianness()
{
    unsigned int i = 1;
    char *c = (char*)&i;
    if (*c) {
        return IS_LITTLE_ENDIAN;
    }
    else {
        return IS_BIG_ENDIAN;
    }
};

static char *
fmt_utc_time(char *buf, time_t t)
{
    struct tm *ltime = localtime(&t);

    sprintf(buf, "%04u-%02u-%02u %02u:%02u:%02u",
            (1900 + ltime->tm_year)%10000, ltime->tm_mon + 1, ltime->tm_mday,
            ltime->tm_hour, ltime->tm_min, ltime->tm_sec);
    return buf;
}

static void
cdc_extract_header(const char *record, int *header_sz, int *payload_sz,
                   int *packet_scheme, int *record_number) {
    *header_sz = ld4((char*)record + HEADER_SZ_OFFSET);
    *payload_sz = ld4((char*)record + PAYLOAD_SZ_OFFSET);
    *packet_scheme = ld4((char*)record + PACKET_SCHEME_OFFSET);
    *record_number = ld4((char*)record + RECORD_NUMBER_OFFSET);
}

static PyObject*
cdc_extract_record(InformixCdcObject *self, int payload_sz,
                   int packet_scheme, int record_number)
{
    char record_type[17];
    PyObject *py_dict = NULL;
    PyObject *py_record_type = NULL;
    int rc = -1;

    if (packet_scheme != PACKET_SCHEME) {
        PyErr_SetString(PyExc_BufferError, "invalid Informix CDC packet scheme");
        goto except;
    }

    py_dict = PyDict_New();
    if (py_dict == NULL) {
        goto except;
    }

    switch (record_number) {
        case CDC_REC_BEGINTX:
            strcpy(record_type, "CDC_REC_BEGINTX");
            rc = 0;
            break;
        case CDC_REC_COMMTX:
            strcpy(record_type, "CDC_REC_COMMTX");
            rc = 0;
            break;
        case CDC_REC_INSERT:
            strcpy(record_type, "CDC_REC_INSERT");
            rc = cdc_extract_iud(self, payload_sz, py_dict);
            break;
        case CDC_REC_DELETE:
            strcpy(record_type, "CDC_REC_DELETE");
            rc = cdc_extract_iud(self, payload_sz, py_dict);
            break;
        case CDC_REC_UPDBEF:
            strcpy(record_type, "CDC_REC_UPDBEF");
            rc = cdc_extract_iud(self, payload_sz, py_dict);
            break;
        case CDC_REC_UPDAFT:
            strcpy(record_type, "CDC_REC_UPDAFT");
            rc = cdc_extract_iud(self, payload_sz, py_dict);
            break;
        case CDC_REC_RBTX:
            strcpy(record_type, "CDC_REC_RBTX");
            rc = 0;
            break;
        case CDC_REC_DISCARD:
            strcpy(record_type, "CDC_REC_DISCARD");
            rc = 0;
            break;
        case CDC_REC_TRUNCATE:
            strcpy(record_type, "CDC_REC_TRUNCATE");
            rc = 0;
            break;
        case CDC_REC_TABSCHEM:
            strcpy(record_type, "CDC_REC_TABSCHEM");
            rc = cdc_extract_tabschema(self, payload_sz, py_dict);
            break;
        case CDC_REC_TIMEOUT:
            strcpy(record_type, "CDC_REC_TIMEOUT");
            rc = cdc_extract_timeout(self, payload_sz, py_dict);
            break;
        case CDC_REC_ERROR:
            strcpy(record_type, "CDC_REC_ERROR");
            rc = 0;
            break;
        default:
            strcpy(record_type, "CDC_REC_UNKNOWN");
            rc = 0;
    }

    if (rc != 0) {
        if (! PyErr_Occurred()) {
            PyErr_SetString(PyExc_RuntimeError, "error extracting CDC record");
        }
        goto except;
    }

    py_record_type = PyString_FromString(record_type);
    if (py_record_type == NULL) {
        goto except;
    }

    if (PyDict_SetItemString(py_dict, "record_type", py_record_type) != 0) {
        goto except;
    };

    assert(! PyErr_Occurred());
    assert(py_record_type);
    assert(py_dict);
    Py_DECREF(py_record_type);
    goto finally;
except:
    assert(PyErr_Occurred());
    Py_XDECREF(py_dict);
    Py_XDECREF(py_record_type);
    py_dict = NULL;
finally:
    return py_dict;
}

static int
cdc_extract_tabschema(InformixCdcObject *self, int payload_sz, PyObject *py_dict)
{
    int ret = -1;
    char *rec = self->next_record_start+RECORD_HEADER_OFFSET;
    PyObject *py_tabid = NULL;
    PyObject *py_flags = NULL;
    PyObject *py_fix_len_sz = NULL;
    PyObject *py_fix_len_cols = NULL;
    PyObject *py_var_len_cols = NULL;
    PyObject *py_cols_desc = NULL;

    py_tabid = PyInt_FromLong(ld4(rec));
    if (py_tabid == NULL) {
        goto except;
    }
    py_flags = PyInt_FromLong(ld4(rec+4));
    if (py_flags == NULL) {
        goto except;
    }
    py_fix_len_sz = PyInt_FromLong(ld4(rec+8));
    if (py_fix_len_sz == NULL) {
        goto except;
    }
    py_fix_len_cols = PyInt_FromLong(ld4(rec+12));
    if (py_fix_len_cols == NULL) {
        goto except;
    }
    py_var_len_cols = PyInt_FromLong(ld4(rec+16));
    if (py_var_len_cols == NULL) {
        goto except;
    }
    py_cols_desc = PyString_FromStringAndSize(rec+20, payload_sz);
    if (py_cols_desc == NULL) {
        goto except;
    }

    if (PyDict_SetItemString(py_dict, "tabid", py_tabid) != 0) {
        goto except;
    }
    if (PyDict_SetItemString(py_dict, "flags", py_flags) != 0) {
        goto except;
    }
    if (PyDict_SetItemString(py_dict, "fix_len_sz", py_fix_len_sz) != 0) {
        goto except;
    }
    if (PyDict_SetItemString(py_dict, "fix_len_cols", py_fix_len_cols) != 0) {
        goto except;
    }
    if (PyDict_SetItemString(py_dict, "var_len_cols", py_var_len_cols) != 0) {
        goto except;
    }
    if (PyDict_SetItemString(py_dict, "cols_desc", py_cols_desc) != 0) {
        goto except;
    }

    ret = 0;
    assert(! PyErr_Occurred());
    assert(py_tabid);
    assert(py_flags);
    assert(py_fix_len_sz);
    assert(py_fix_len_cols);
    assert(py_var_len_cols);
    assert(py_cols_desc);
    Py_DECREF(py_tabid);
    Py_DECREF(py_flags);
    Py_DECREF(py_fix_len_sz);
    Py_DECREF(py_fix_len_cols);
    Py_DECREF(py_var_len_cols);
    Py_DECREF(py_cols_desc);
    goto finally;
except:
    assert(PyErr_Occurred());
    Py_XDECREF(py_tabid);
    Py_XDECREF(py_flags);
    Py_XDECREF(py_fix_len_sz);
    Py_XDECREF(py_fix_len_cols);
    Py_XDECREF(py_var_len_cols);
    Py_XDECREF(py_cols_desc);
    ret = -1;
finally:
    return ret;
}

static int
cdc_extract_iud(InformixCdcObject *self, int payload_sz, PyObject *py_dict)
{
    int ret = -1;
    char *rec = self->next_record_start+RECORD_HEADER_OFFSET;
    int tabid;
    PyObject *py_seq_number = NULL;
    PyObject *py_transaction_id = NULL;
    PyObject *py_tabid = NULL;
    PyObject *py_flags = NULL;
    PyObject *py_list = NULL;

    py_seq_number = PyLong_FromLong(ld8(rec));
    if (py_seq_number == NULL) {
        goto except;
    }
    py_transaction_id = PyInt_FromLong(ld8(rec+8));
    if (py_transaction_id == NULL) {
        goto except;
    }

    tabid = ld4(rec+12);
    py_tabid = PyInt_FromLong(tabid);
    if (py_tabid == NULL) {
        goto except;
    }

    py_flags = PyInt_FromLong(ld4(rec+16));
    if (py_flags == NULL) {
        goto except;
    }
    py_list = cdc_extract_columns_to_list(self, tabid);
    if (py_list == NULL) {
        goto except;
    }

    if (PyDict_SetItemString(py_dict, "seq_number", py_seq_number) != 0) {
        goto except;
    }
    if (PyDict_SetItemString(py_dict, "transaction_id", py_transaction_id) != 0) {
        goto except;
    }
    if (PyDict_SetItemString(py_dict, "tabid", py_tabid) != 0) {
        goto except;
    }
    if (PyDict_SetItemString(py_dict, "flags", py_flags) != 0) {
        goto except;
    }
    if (PyDict_SetItemString(py_dict, "columns", py_list) != 0) {
        goto except;
    }

    ret = 0;
    assert(! PyErr_Occurred());
    assert(py_seq_number);
    assert(py_transaction_id);
    assert(py_tabid);
    assert(py_flags);
    assert(py_list);
    Py_DECREF(py_seq_number);
    Py_DECREF(py_transaction_id);
    Py_DECREF(py_tabid);
    Py_DECREF(py_flags);
    Py_DECREF(py_list);
    goto finally;
except:
    assert(PyErr_Occurred());
    Py_XDECREF(py_seq_number);
    Py_XDECREF(py_transaction_id);
    Py_XDECREF(py_tabid);
    Py_XDECREF(py_flags);
    Py_XDECREF(py_list);
    ret = -1;
finally:
    return ret;
}

static int
cdc_add_tabschema(InformixCdcObject *self, int payload_sz)
{
    int ret = -1;
    EXEC SQL BEGIN DECLARE SECTION;
    char sql[MAX_SQL_STMT_LEN];
    EXEC SQL END DECLARE SECTION;

    char *rec = self->next_record_start+RECORD_HEADER_OFFSET;
    int tabid = ld4(rec);
    int var_len_cols = ld4(rec + 16);
    ifx_sqlda_t *sqlda = NULL;
    columns_t *columns;

    if (tabid >= MAX_CDC_TABS) {
        PyErr_SetString(PyExc_IndexError, "max Informix CDC tables reached");
        goto except;
    }

    columns = &self->tab_cols[tabid];
    columns->num_cols = 0;

    sprintf(sql, "create temp table t_informixcdc (%s) with no log",
            rec+20);

    EXEC SQL EXECUTE IMMEDIATE :sql;

    if (SQLCODE != 0) {
        PyErr_SetString(PyExc_Exception, "cannot create CDC temp table");
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

    for (int col=0; col < sqlda->sqld; col++) {
        columns->column[col].col_type = sqlda->sqlvar[col].sqltype;
        columns->column[col].col_size =
            rtypsize(sqlda->sqlvar[col].sqltype, sqlda->sqlvar[col].sqllen);
        columns->column[col].col_xid = sqlda->sqlvar[col].sqlxid;
        columns->column[col].col_name =
            PyMem_Malloc(strlen(sqlda->sqlvar[col].sqlname)+1);
        if (! columns->column[col].col_name) {
            PyErr_SetString(PyExc_MemoryError, "cannot allocate column name");
            goto except;
        }
        strcpy(columns->column[col].col_name, sqlda->sqlvar[col].sqlname);
        columns->num_cols++;
    }
    columns->num_var_cols = var_len_cols;

    EXEC SQL FREE informixcdc;

    if (SQLCODE != 0) {
        goto except;
    }

    EXEC SQL EXECUTE IMMEDIATE "drop table t_informixcdc";

    // don't raise an error if we can't drop the temp table,
    // the next attempt to add a table will fail with error

    ret = 0;
    assert(! PyErr_Occurred());
    goto finally;
except:
    assert(PyErr_Occurred());
    if (tabid < MAX_CDC_TABS) {
        columns = &self->tab_cols[tabid];
        for (int col=0; col < columns->num_cols; col++) {
            PyMem_Free(columns->column[col].col_name);
        }
    }
    ret = -1;
finally:
    free(sqlda);
    return ret;
}

static int
cdc_extract_timeout(InformixCdcObject *self, int payload_sz, PyObject *py_dict)
{
    int ret = -1;
    char *rec = self->next_record_start+RECORD_HEADER_OFFSET;
    PyObject *py_seq_number = NULL;

    py_seq_number = PyLong_FromLong(ld8(rec));
    if (py_seq_number == NULL) {
        goto except;
    }

    if (PyDict_SetItemString(py_dict, "seq_number", py_seq_number) != 0) {
        goto except;
    }

    ret = 0;
    assert(! PyErr_Occurred());
    assert(py_seq_number);
    Py_DECREF(py_seq_number);
    goto finally;
except:
    assert(PyErr_Occurred());
    Py_XDECREF(py_seq_number);
    ret = -1;
finally:
    return ret;
}

#define INT8_LO_OFFSET          2
#define INT8_HI_OFFSET          6
#define BOOL_COL_LEN			2
#define VARCHAR_LEN_OFFSET      1
#define LVARCHAR_LEN_OFFSET     3
#define ERRSTR_LEN              50

static PyObject *
cdc_extract_columns_to_list(InformixCdcObject *self, int tabid)
{
    char *rec = self->next_record_start+RECORD_HEADER_OFFSET;
    char *col;
    char ch_int8[21];
    char ch_decimal[35];
    short c_smallint;
    int c_integer;
    float c_float;
    double c_double;
    dec_t c_decimal;
    dtime_t c_datetime;
    char *varchar_len_arr;
    int varchar_len_arr_idx = 0;
    int varchar_len;
    int col_len;
    columns_t *columns;
    bigint c_bigint;
    ifx_int8_t c_int8;
    short mdy_date[3];
    int advance_col;
    int rc;
    char err_str[ERRSTR_LEN];
    PyObject *py_list = NULL;
    PyObject *py_dict = NULL;
    PyObject *py_name = NULL;
    PyObject *py_value = NULL;

    if (tabid >= self->next_table_id) {
        PyErr_SetString(PyExc_IndexError, "invalid internel CDC table id");
        goto except;
    }

    py_list = PyList_New(0);
    if (py_list == NULL) {
        goto except;
    }

    columns = &self->tab_cols[tabid];
    varchar_len_arr = rec + CHANGE_HEADER_SZ;
    col = varchar_len_arr + columns->num_var_cols * 4;
    for (int col_idx=0; col_idx < columns->num_cols; col_idx++) {
        py_dict = PyDict_New();
        if (py_dict == NULL) {
            goto except;
        }

        advance_col = 1;
        switch (MASKNONULL(columns->column[col_idx].col_type)) {
            case SQLINT8:
            case SQLSERIAL8:
                // IS THIS THE RIGHT WAY?
                c_int8.sign = ld2(col);
                c_int8.data[0] = ld4(col+INT8_LO_OFFSET);
                c_int8.data[1] = ld4(col+INT8_HI_OFFSET);

                if (risnull(CINT8TYPE, (char*)&c_int8)) {
                    Py_INCREF(Py_None);
                    py_value = Py_None;
                }
                else {
                    rc = ifx_int8toasc(&c_int8, ch_int8, 20);
                    if (rc != 0) {
                        PyErr_SetString(PyExc_ValueError,
                                        "cannot convert INT8 to ascii");
                        goto except;
                    }
                    py_value = PyLong_FromString(ch_int8, NULL, 10);
                    if (py_value == NULL) {
                        snprintf(err_str, sizeof(err_str),
                                 "INT8: PyLong_FromString failed: %s", ch_int8);
                        PyErr_SetString(PyExc_ValueError, err_str);
                        goto except;
                    }
                }
                break;

            case SQLSERIAL:
            case SQLINT:
                c_integer = ld4(col);
                if (risnull(CINTTYPE, (char*)&c_integer)) {
                    Py_INCREF(Py_None);
                    py_value = Py_None;
                }
                else {
                    py_value = PyInt_FromLong(c_integer);
                    if (py_value == NULL) {
                        snprintf(err_str, sizeof(err_str),
                                 "INT: PyInt_FromLong failed: %d", c_integer);
                        PyErr_SetString(PyExc_ValueError, err_str);
                        goto except;
                    }
                }
                break;

            case SQLDATE:
                c_integer = ld4(col);
                if (risnull(CINTTYPE, (char*)&c_integer)) {
                    Py_INCREF(Py_None);
                    py_value = Py_None;
                }
                else {
                    rc = rjulmdy(c_integer, mdy_date);
                    if (rc < 0) {
                        snprintf(err_str, sizeof(err_str),
                                 "DATE: rjulmdy failed: %d", c_integer);
                        PyErr_SetString(PyExc_ValueError, err_str);
                        goto except;
                    }
                    py_value = PyDate_FromDate(
                        mdy_date[2], mdy_date[0], mdy_date[1]);
                    if (py_value == NULL) {
                        snprintf(err_str, sizeof(err_str),
                                 "DATE: PyDate_FromDate failed: %d", c_integer);
                        PyErr_SetString(PyExc_ValueError, err_str);
                        goto except;
                    }
                }
                break;

            case SQLBOOL:
                /* boolean stream has 2 bytes, first byte indicate nullness
                 * second byte indicate 1 (true) or 0 (false)
                */
                if (*col == 1) {
                    py_value = Py_None;
                }
                else {
                    py_value = *(col+1) ? Py_True : Py_False;
                }
                Py_INCREF(py_value);
                advance_col = 0;
                col += BOOL_COL_LEN;
                break;

            case SQLCHAR:
                if (risnull(CCHARTYPE, col)) {
                    Py_INCREF(Py_None);
                    py_value = Py_None;
                }
                else {
                    py_value = PyString_FromString(col);
                    if (py_value == NULL) {
                        snprintf(err_str, sizeof(err_str),
                                 "CHAR: PyString_FromString failed: %s", col);
                        PyErr_SetString(PyExc_ValueError, err_str);
                        goto except;
                    }
                }
                break;

            case SQLNVCHAR:
            case SQLVCHAR:
                varchar_len = ld4(varchar_len_arr + 4 * varchar_len_arr_idx++);
                col_len = varchar_len - VARCHAR_LEN_OFFSET;
                col += VARCHAR_LEN_OFFSET;
                if (risnull(CVCHARTYPE, col)) {
                    Py_INCREF(Py_None);
                    py_value = Py_None;
                }
                else {
                    py_value = PyString_FromStringAndSize(col, col_len);
                    if (py_value == NULL) {
                        snprintf(err_str, sizeof(err_str),
                                 "VARCHAR: PyString_FromStringAndSize failed: %d %s",
                                 col_len, col);
                        PyErr_SetString(PyExc_ValueError, err_str);
                        goto except;
                    }
                }
                advance_col = 0;
                col += col_len;
                break;

            case SQLLVARCHAR:
                varchar_len = ld4(varchar_len_arr + 4 * varchar_len_arr_idx++);
                col_len = varchar_len - LVARCHAR_LEN_OFFSET;
                col += LVARCHAR_LEN_OFFSET;
                if (risnull(CLVCHARTYPE, col)) {
                    Py_INCREF(Py_None);
                    py_value = Py_None;
                }
                else {
                    py_value = PyString_FromStringAndSize(col, col_len);
                    if (py_value == NULL) {
                        snprintf(err_str, sizeof(err_str),
                                 "LVARCHAR: PyString_FromStringAndSize failed: %d %s",
                                 col_len, col);
                        PyErr_SetString(PyExc_ValueError, err_str);
                        goto except;
                    }
                }
                advance_col = 0;
                col += col_len;
                break;

            case SQLINFXBIGINT:
                c_bigint = ld8(col);
                if (risnull(CBIGINTTYPE, (char*)&c_bigint)) {
                    Py_INCREF(Py_None);
                    py_value = Py_None;
                }
                else {
                    py_value = PyLong_FromLong(c_bigint);
                    if (py_value == NULL) {
                        snprintf(err_str, sizeof(err_str),
                                 "BIGINT: PyLong_FromLong failed: %ld", c_bigint);
                        PyErr_SetString(PyExc_ValueError, err_str);
                        goto except;
                    }
                }
                break;

            case SQLFLOAT :
                c_double = lddbl(col, self->endianness);
                if (risnull(CDOUBLETYPE, (char*)&c_double)) {
                    Py_INCREF(Py_None);
                    py_value = Py_None;
                }
                else {
                    py_value = PyFloat_FromDouble(c_double);
                    if (py_value == NULL) {
                        snprintf(err_str, sizeof(err_str),
                                 "FLOAT: PyFloat_FromDouble failed: %lf", c_double);
                        PyErr_SetString(PyExc_ValueError, err_str);
                        goto except;
                    }
                }
                break;

            case SQLSMFLOAT:
                c_float = ldfloat(col, self->endianness);
                if (risnull(CFLOATTYPE, (char*)&c_float)) {
                    Py_INCREF(Py_None);
                    py_value = Py_None;
                }
                else {
                    py_value = PyFloat_FromDouble(c_float);
                    if (py_value == NULL) {
                        snprintf(err_str, sizeof(err_str),
                                 "SMFLOAT: PyFloat_FromDouble failed: %f", c_float);
                        PyErr_SetString(PyExc_ValueError, err_str);
                        goto except;
                    }
                }
                break;

            case SQLSMINT:
                c_smallint = ld2(col);
                if (risnull(CSHORTTYPE, (char*)&c_smallint)) {
                    Py_INCREF(Py_None);
                    py_value = Py_None;
                }
                else {
                    py_value = PyInt_FromLong(c_smallint);
                    if (py_value == NULL) {
                        snprintf(err_str, sizeof(err_str),
                                 "SMINT: PyInt_FromLong failed: %hd", c_smallint);
                        PyErr_SetString(PyExc_ValueError, err_str);
                        goto except;
                    }
                }
                break;

            case SQLMONEY:
            case SQLDECIMAL:
                lddecimal(col, columns->column[col_idx].col_size, &c_decimal);
                if (risnull(CDECIMALTYPE, (char*)&c_decimal)) {
                    Py_INCREF(Py_None);
                    py_value = Py_None;
                }
                else {
                    rc = dectoasc(&c_decimal, ch_decimal, 34,
                                  columns->column[col_idx].col_size);
                    if (rc != 0) {
                        PyErr_SetString(PyExc_ValueError, "cannot extract decimal");
                        goto except;
                    }
                    ch_decimal[34] = '\0';
                    py_value = PyString_FromString(ch_decimal);
                    if (py_value == NULL) {
                        snprintf(err_str, sizeof(err_str),
                                 "DECIMAL: PyString_FromString failed: %s",
                                 ch_decimal);
                        PyErr_SetString(PyExc_ValueError, err_str);
                        goto except;
                    }
                }
                break;

            case SQLDTIME:
            case SQLINTERVAL:
                lddecimal(col, columns->column[col_idx].col_size, &(c_datetime.dt_dec));
                if (risnull(CDTIMETYPE, (char*)&(c_datetime.dt_dec))) {
                    Py_INCREF(Py_None);
                    py_value = Py_None;
                }
                else {
                    rc = dectoasc(&c_datetime.dt_dec, ch_decimal, 34,
                                  columns->column[col_idx].col_size);
                    if (rc != 0) {
                        PyErr_SetString(PyExc_ValueError, "cannot extract datetime");
                        goto except;
                    }
                    ch_decimal[34] = '\0';
                    py_value = PyString_FromString(ch_decimal);
                    if (py_value == NULL) {
                        snprintf(err_str, sizeof(err_str),
                                 "DTIME: PyString_FromString failed: %s",
                                 ch_decimal);
                        PyErr_SetString(PyExc_ValueError, err_str);
                        goto except;
                    }
                }
                break;

            default:
                PyErr_SetString(PyExc_ValueError, "unsupported data type");
                goto except;
                break;
        }

        py_name = PyString_FromString(columns->column[col_idx].col_name);
        if (py_name == NULL) {
            goto except;
        }

        if (PyDict_SetItemString(py_dict, "name", py_name) != 0) {
            goto except;
        }
        if (PyDict_SetItemString(py_dict, "value", py_value) != 0) {
            goto except;
        }
        if (PyList_Append(py_list, py_dict) != 0) {
            goto except;
        }

        if (advance_col) {
            col += columns->column[col_idx].col_size;
        }
    }

    assert(! PyErr_Occurred());
    assert(py_name);
    assert(py_value);
    assert(py_dict);
    assert(py_list);
    Py_DECREF(py_name);
    Py_DECREF(py_value);
    Py_DECREF(py_dict);
    goto finally;
except:
    assert(PyErr_Occurred());
    Py_XDECREF(py_name);
    Py_XDECREF(py_value);
    Py_XDECREF(py_dict);
    Py_XDECREF(py_list);
    py_list = NULL;
finally:
    return py_list;
}
