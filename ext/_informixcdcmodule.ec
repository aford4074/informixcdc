
/* InformixCdc objects */

#include <Python.h>
#include "structmember.h"

#define DEFAULT_TIMEOUT 60L
#define DEFAULT_MAX_RECORDS 100L
#define DEFAULT_SYSCDCDB "syscdcv1"
EXEC SQL define TABLENAME_LEN 768;
EXEC SQL define COLARG_LEN 1024;

static PyObject *ErrorObject;

typedef struct {
    PyObject_HEAD
    char name[30];
    int is_connected;
    $integer session_id;
    PyObject *dbservername;
    int timeout;
    int max_records;
    PyObject *syscdcdb;
} InformixCdcObject;

static PyTypeObject InformixCdc_Type;

#define InformixCdcObject_Check(v)      (Py_TYPE(v) == &InformixCdc_Type)

static void
InformixCdc_dealloc(InformixCdcObject* self)
{
    Py_XDECREF(self->dbservername);
    Py_XDECREF(self->syscdcdb);
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyObject *
InformixCdc_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    InformixCdcObject *self;

    self = (InformixCdcObject *)type->tp_alloc(type, 0);
    if (self != NULL) {
        self->name[0] = '\0';
        self->is_connected = 0;
        self->session_id = -1;

        self->dbservername = PyString_FromString("");
        if (self->dbservername == NULL) {
            Py_DECREF(self);
            return NULL;
        }

        self->timeout = -1;
        self->max_records = -1;

        self->syscdcdb = PyString_FromString("");
        if (self->syscdcdb == NULL) {
            Py_DECREF(self);
            return NULL;
        }
    }

    return (PyObject *)self;
}

static int
InformixCdc_init(InformixCdcObject *self, PyObject *args, PyObject *kwds)
{
    PyObject *dbservername = NULL;
    PyObject *syscdcdb = NULL;
    PyObject *tmp;

    static char *kwlist[] = {"dbservername", "timeout", "max_records",
                             "syscdcdb", NULL };

    if (! PyArg_ParseTupleAndKeywords(args, kwds, "S|iiS:init", kwlist,
                                      &dbservername, &self->timeout,
                                      &self->max_records, &syscdcdb)) {
        return -1;
    }

    sprintf(self->name, "cdc%p", self);

    tmp = self->dbservername;
    Py_INCREF(dbservername);
    self->dbservername = dbservername;
    Py_DECREF(tmp);

    if (self->timeout <= 0) {
        self->timeout = 30;
    }

    if (self->max_records <= 0) {
        self->max_records = 100;
    }

    if (! syscdcdb) {
        syscdcdb = PyString_FromString(DEFAULT_SYSCDCDB);
    }
    tmp = self->syscdcdb;
    Py_INCREF(syscdcdb);
    self->syscdcdb = syscdcdb;
    Py_DECREF(tmp);

    return 0;
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
    PyObject *is_connected;

    is_connected = PyBool_FromLong(self->is_connected);
    if (is_connected == NULL) {
        return NULL;
    }

    //Py_INCREF(is_connected);
    return is_connected;
}

static PyObject *
InformixCdc_getsession_id(InformixCdcObject *self, void *closure)
{
    PyObject *session_id;

    session_id = PyInt_FromLong(self->session_id);
    if (session_id == NULL) {
        return NULL;
    }

    //Py_INCREF(session_id);
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
    EXEC SQL BEGIN DECLARE SECTION;
    char conn_string[255];
    char* conn_dbservername = NULL;
    char *conn_name = NULL;
    char *conn_user = NULL;
    char *conn_passwd = NULL;
    long timeout;
    long max_records;
    $integer session_id;
    EXEC SQL END DECLARE SECTION;

    char* conn_syscdcdb = NULL;

    PyObject *sqlcode;
    PyObject *py_conn_user = NULL;
    PyObject *py_conn_passwd = NULL;

    static char *kwlist[] = {"user", "passwd", NULL };

    if (! PyArg_ParseTupleAndKeywords(args, kwds, "|SS:connect", kwlist,
                                      &py_conn_user, &py_conn_passwd)) {
        return NULL;
    }

    if (py_conn_user && py_conn_user != Py_None) {
        conn_user = PyString_AsString(py_conn_user);
        if (conn_user == NULL ) {
            return NULL;
        }
    }

    if (py_conn_passwd && py_conn_passwd != Py_None) {
        conn_passwd = PyString_AsString(py_conn_passwd);
        if (conn_passwd == NULL) {
            return NULL;
        }
    }

    conn_dbservername = PyString_AsString(self->dbservername);
    if (conn_dbservername == NULL) {
        return NULL;
    }

    conn_syscdcdb = PyString_AsString(self->syscdcdb);
    if (conn_syscdcdb == NULL) {
        return NULL;
    }

    conn_name = self->name;
    sprintf(conn_string, "%s@%s", conn_syscdcdb, conn_dbservername);

    if (conn_user && conn_passwd) {
        EXEC SQL CONNECT TO :conn_string AS :conn_name USER :conn_user USING :conn_passwd;
    }
    else {
        EXEC SQL CONNECT TO :conn_string AS :conn_name;
    }

    if (SQLCODE == 0) {
        self->is_connected = 1;
    }
    else {
        sqlcode = PyInt_FromLong(SQLCODE);
        //Py_INCREF(sqlcode);
        return sqlcode;
    }

    timeout = self->timeout;
    max_records = self->max_records;

    EXEC SQL EXECUTE FUNCTION informix.cdc_opensess(
        :conn_dbservername, 0, :timeout, :max_records, 1, 1
    ) INTO :session_id;

    if (SQLCODE != 0) {
        sqlcode = PyInt_FromLong(SQLCODE);
    }
    else if (session_id < 0) {
        sqlcode = PyInt_FromLong(session_id);
    }
    else {
        self->session_id = session_id;
        sqlcode = PyInt_FromLong(0L);
    }

    //Py_INCREF(sqlcode);
    return sqlcode;
}

static PyObject *
InformixCdc_enable(InformixCdcObject *self, PyObject *args, PyObject *kwds)
{
    EXEC SQL BEGIN DECLARE SECTION;
    char *cdc_table = NULL;
    char *cdc_columns = NULL;
    $integer session_id;
    $integer table_id = 0;
    $integer retval;
    EXEC SQL END DECLARE SECTION;

    PyObject *sqlcode;
    PyObject *py_cdc_table = NULL;
    PyObject *py_cdc_columns = NULL;

    static char *kwlist[] = {"table", "columns", NULL };

    if (! PyArg_ParseTupleAndKeywords(args, kwds, "SS:enable", kwlist,
                                      &py_cdc_table, &py_cdc_columns)) {
        return NULL;
    }

    cdc_table = PyString_AsString(py_cdc_table);
    if (cdc_table == NULL ) {
        return NULL;
    }

    cdc_columns = PyString_AsString(py_cdc_columns);
    if (cdc_columns == NULL) {
        return NULL;
    }

    EXEC SQL EXECUTE FUNCTION informix.cdc_set_fullrowlogging(
        :cdc_table, 1)
    INTO :retval;

    if (SQLCODE != 0) {
        sqlcode = PyInt_FromLong(SQLCODE);
        //Py_INCREF(sqlcode);
        return sqlcode;
    }
    else if (retval < 0) {
        sqlcode = PyInt_FromLong(retval);
        //Py_INCREF(sqlcode);
        return sqlcode;
    }

    session_id = self->session_id;

    EXEC SQL EXECUTE FUNCTION informix.cdc_startcapture(
        :session_id, 0, :cdc_table, :cdc_columns, :table_id
    ) INTO :retval;

    if (SQLCODE != 0) {
        sqlcode = PyInt_FromLong(SQLCODE);
    }
    else if (retval < 0) {
        sqlcode = PyInt_FromLong(retval);
    }
    else {
        sqlcode = PyInt_FromLong(0L);
    }

    //Py_INCREF(sqlcode);
    return sqlcode;
}

static PyObject *
InformixCdc_activate(InformixCdcObject *self)
{
    EXEC SQL BEGIN DECLARE SECTION;
    $integer session_id;
    bigint lsn = 0;
    $integer retval;
    EXEC SQL END DECLARE SECTION;

    PyObject *sqlcode;

    session_id = self->session_id;

    EXEC SQL EXECUTE FUNCTION informix.cdc_activatesess(
        :session_id, :lsn
    ) INTO :retval;

    if (SQLCODE != 0) {
        sqlcode = PyInt_FromLong(SQLCODE);
    }
    else if (retval < 0) {
        sqlcode = PyInt_FromLong(retval);
    }
    else {
        sqlcode = PyInt_FromLong(0L);
    }

    //Py_INCREF(sqlcode);
    return sqlcode;
}

static PyObject *
InformixCdc_read(InformixCdcObject *self)
{
    int bytes_read;
    int lo_read_err = 0;
    char *buffer = NULL;
    PyObject *py_buffer = NULL;

    buffer = PyMem_Malloc(1024);
    if (! buffer) {
        return PyErr_NoMemory();
    }

    bytes_read = ifx_lo_read(self->session_id, buffer, 1024, &lo_read_err);
    if (bytes_read < 0 || lo_read_err < 0) {
        PyMem_Free(buffer);
        return NULL;
    }

    py_buffer = PyString_FromStringAndSize(buffer, bytes_read);
    if (py_buffer == NULL) {
        PyMem_Free(buffer);
        return NULL;
    }

    PyMem_Free(buffer);
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
    {
        "read",
        (PyCFunction)InformixCdc_read,
        METH_NOARGS,
        PyDoc_STR("read() -> None")
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
    0,                      /*tp_iter*/
    0,                      /*tp_iternext*/
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
    if (m == NULL)
        return;

    /* Add some symbolic constants to the module */
    if (ErrorObject == NULL) {
        ErrorObject = PyErr_NewException("informixcdc.error", NULL, NULL);
        if (ErrorObject == NULL)
            return;
    }
    Py_INCREF(ErrorObject);
    PyModule_AddObject(m, "error", ErrorObject);

    /* Add Str */
    if (PyType_Ready(&Str_Type) < 0)
        return;
    PyModule_AddObject(m, "Str", (PyObject *)&Str_Type);

    /* Add Null */
    if (PyType_Ready(&Null_Type) < 0)
        return;
    PyModule_AddObject(m, "Null", (PyObject *)&Null_Type);

    Py_INCREF(&InformixCdc_Type);
    PyModule_AddObject(m, "InformixCdc", (PyObject *)&InformixCdc_Type);
}
