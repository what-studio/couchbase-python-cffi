import sys

from couchbase_ffi._cinit import get_handle
from couchbase_ffi.result import ValueResult, Result
from couchbase_ffi._rtconfig import pycbc_exc_lcb, PyCBC
from couchbase_ffi.bufmanager import BufManager
from couchbase_ffi._strutil import from_cstring

ffi, C = get_handle()


def mres2vres(mres):
    return mres[None]


def buf2str(v, n):
    return from_cstring(ffi.cast('const char*', v), n)


class ViewResultBase(Result):
    COMMAND_DECL  = NotImplemented
    HANDLE_DECL = NotImplemented
    ROWCB_DECL = NotImplemented

    def __init__(self):
        self._c_command = ffi.new(self.COMMAND_DECL)
        self._c_handle = ffi.new(self.HANDLE_DECL)
        self._parent = None
        self.rows = []
        self._rows_per_call = 0
        self._bound_cb = ffi.callback(self.ROWCB_DECL, self._on_single_row)
        self.done = False
        self.value = None
        self.http_status = 0

    def _schedule(self, parent, mres):
        self._init_command()
        self._parent = parent
        rc = self._query(parent, mres)
        if rc:
            raise pycbc_exc_lcb(rc)

    def _init_command(self):
        raise NotImplementedError

    def _query(self, parent, mres):
        # should return rc
        raise NotImplementedError

    def _handle_done(self, resp, mres):
        self._c_handle = None
        if resp.rc:
            if resp.rc == C.LCB_HTTP_ERROR:
                try:
                    raise PyCBC.exc_http(self.value)
                except:
                    mres._add_err(sys.exc_info())
            else:
                mres._add_bad_rc(resp.rc, self)

        self._handle_resp(resp, mres)

        if resp.htresp:
            if not self.value and resp.htresp.nbody:
                self.value = buf2str(resp.htresp.body, resp.htresp.nbody)
            self.http_status = resp.htresp.htstatus

        if self._parent._is_async:
            self._invoke_async(mres, is_final=True)

        self.done = True

        if self._parent._is_async:
            try:
                mres._maybe_throw()
                self._invoke_async(mres, is_final=True)
            except:
                mres.errback(mres, *sys.exc_info())
            finally:
                del self._parent

    def _handle_resp(self, resp, mres):
        raise NotImplementedError

    @property
    def rows_per_call(self):
        return self._rows_per_call
    @rows_per_call.setter
    def rows_per_call(self, val):
        self._rows_per_call = int(val)

    def _should_call(self, is_final):
        if is_final:
            return True
        return -1 < self._rows_per_call < len(self.rows)

    def _invoke_async(self, mres, is_final=False):
        # if (rd->type == LCBEX_VROW_ROW) {
        #     return vres->rows_per_call > -1 &&
        #     PyList_GET_SIZE(vres->rows) > vres->rows_per_call;
        # } else {
        #     return PyList_GET_SIZE(vres->rows);
        # }
        if not self._should_call(is_final=is_final):
            return

        cb = mres.callback
        if cb:
            cb(mres)
            self.rows = []

    def _on_single_row(self, instance, cbtype, resp):
        mres = ffi.from_handle(resp.cookie)
        if resp.rflags & C.LCB_RESP_F_FINAL:
            self._handle_done(resp, mres)
            return

        if resp.rc != C.LCB_SUCCESS:
            mres._add_bad_rc(resp.rc, self)
            return

        row = self._process_resp(resp, mres)

        if row is not None:
            # So now that we have a row..
            self.rows.append(row)

        if self._parent._is_async:
            self._invoke_async(mres)

    def _process_resp(self, resp, mres):
        raise NotImplementedError

    def fetch(self, mres):
        C.lcb_wait(self._parent._lcbh)
        ret = self.rows
        self.rows = []
        mres._maybe_throw()
        return ret


class ViewResult(ViewResultBase):
    COMMAND_DECL = 'lcb_CMDVIEWQUERY*'
    HANDLE_DECL = 'lcb_VIEWHANDLE*'
    ROWCB_DECL = 'void(lcb_t,int,const lcb_RESPVIEWQUERY*)'

    def __init__(self, ddoc, view, options, flags):
        super(ViewResult, self).__init__()
        self._ddoc = ddoc
        self._view = view
        self._options = options
        self._flags = flags

    @property
    def key(self):
        return 'VIEW[{0}/{1}]'.format(self._ddoc, self._view)

    def _init_command(self):
        cmd = self._c_command

        bm = BufManager(ffi)
        urlopts = ffi.NULL
        pypost = None

        if self._options:
            in_uri, in_post = self._options._long_query_encoded
            # Note, encoded means URI/JSON encoded; not charset
            urlopts = bm.new_cstr(in_uri)
            if in_post and in_post != '{}':
                pypost = in_post

        C.lcb_view_query_initcmd(
            cmd, bm.new_cstr(self._ddoc),
            bm.new_cstr(self._view), urlopts, self._bound_cb)

        if pypost:
            cmd.postdata, cmd.npostdata = bm.new_cbuf(pypost)

        cmd.cmdflags = self._flags
        cmd.handle = self._c_handle

    def _query(self, parent, mres):
        return C.lcb_view_query(parent._lcbh, mres._cdata, self._c_command)

    def _handle_resp(self, resp, mres):
        if resp.nvalue:
            self.value = buf2str(resp.value, resp.nvalue)
            try:
                self.value = PyCBC.json_decode(self.value)
            except:
                pass

    def _process_resp(self, resp, mres):
        row = {}
        if resp.nkey:
            row['key'] = PyCBC.json_decode(buf2str(resp.key, resp.nkey))
        if resp.nvalue:
            row['value'] = PyCBC.json_decode(buf2str(resp.value, resp.nvalue))
        if resp.docid:
            # Document ID is always a simple string, so no need to decode
            row['id'] = buf2str(resp.docid, resp.ndocid)
        if resp.docresp:
            py_doc = ValueResult()
            l_doc = resp.docresp
            row['__DOCRESULT__'] = py_doc
            py_doc.key = row['id']
            py_doc.flags = l_doc.itmflags
            py_doc.cas = l_doc.cas
            py_doc.rc = l_doc.rc
            if not resp.docresp.rc:
                buf = bytes(ffi.buffer(l_doc.value, l_doc.nvalue))
                try:
                    tc = self._parent._tc
                    py_doc.value = tc.decode_value(buf, py_doc.flags)
                except:
                    py_doc.value = buf[::]
        return row
