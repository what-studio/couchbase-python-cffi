from couchbase_ffi._cinit import get_handle
from couchbase_ffi._rtconfig import pycbc_exc_lcb, PyCBC
from couchbase_ffi._strutil import from_cstring
from couchbase_ffi.bufmanager import BufManager
from couchbase_ffi.result import Result


ffi, C = get_handle()


class _N1QLParams(object):
    def __init__(self):
        self._lp = ffi.gc(C.lcb_n1p_new(), C.lcb_n1p_free)

    def setquery(self, query=None, type=C.LCB_N1P_QUERY_STATEMENT):
        bm = BufManager(ffi)
        query = bm.new_cbuf(query)
        rc = C.lcb_n1p_setquery(self._lp, bm.new_cstr(query), len(query), type)
        if rc:
            raise pycbc_exc_lcb(rc)

    def setoption(self, option, value):
        bm = BufManager(ffi)
        option = bm.new_cbuf(option)
        value = bm.new_cbuf(value)
        rc = C.lcb_n1p_setopt(self._lp, option, len(option), value, len(value))
        if rc:
            raise pycbc_exc_lcb(rc)

    def set_namedarg(self, arg, value):
        bm = BufManager(ffi)
        arg = bm.new_cbuf(arg)
        value = bm.new_cbuf(value)
        rc = C.lcb_n1p_namedparam(self._lp, arg, len(arg), value, len(value))
        if rc:
            raise pycbc_exc_lcb(rc)

    def add_posarg(self, arg):
        bm = BufManager(ffi)
        arg = bm.new_cbuf(arg)
        rc = C.lcb_n1p_posparam(self._lp, arg, len(arg))
        if rc:
            raise pycbc_exc_lcb(rc)

    def clear(self):
        C.lcb_n1p_reset(self._lp)


ROWCB_DECL = 'void(lcb_t,int,const lcb_RESPN1QL*)'


def buf2str(v, n):
    return from_cstring(ffi.cast('const char*', v), n)


class N1QLResult(Result):
    def __init__(self, params, prepare=0, cross_bucket=0):
        self._c_command = ffi.new('lcb_CMDN1QL*')
        self._c_handle = ffi.new('lcb_N1QLHANDLE*')
        self._params = params
        self._prepare = prepare
        self._cross_bucket = cross_bucket
        self._parent = None
        self.rows = []

        # self._rows_per_call = 0
        self._bound_cb = ffi.callback(ROWCB_DECL, self._on_single_row)
        self.done = False
        self.value = None
        self.http_status = 0

    @property
    def key(self):
        return 'VIEW[{0}/{1}]'.format(self._ddoc, self._view)

    @property
    def rows_per_call(self):
        return self._rows_per_call
    @rows_per_call.setter
    def rows_per_call(self, val):
        self._rows_per_call = int(val)

    def _schedule(self, parent, mres):
        bm = BufManager(ffi)
        urlopts = ffi.NULL
        pypost = None
        cmd = self._c_command

        C._Cb_n1ql_query_initcmd(cmd, bm.new_cstr(self._params),
                                 len(self._params), self._bound_cb)

        # if pypost:
        #     cmd.postdata, cmd.npostdata = bm.new_cbuf(pypost)

        cmd.handle = self._c_handle

        self._parent = parent
        rc = C.lcb_n1ql_query(parent._lcbh, mres._cdata, self._c_command)
        if rc:
            raise pycbc_exc_lcb(rc)

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

    def _should_call(self, is_final):
        if is_final:
            return True
        return -1 < self._rows_per_call < len(self.rows)

    def _invoke_async(self, mres, is_final=False):
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

        if resp.nrow:
            row = PyCBC.json_decode(buf2str(resp.row, resp.nrow))
            # So now that we have a row..
            self.rows.append(row)
        if self._parent._is_async:
            self._invoke_async(mres)

    def fetch(self, mres):
        C.lcb_wait(self._parent._lcbh)
        ret = self.rows
        self.rows = []
        mres._maybe_throw()
        return ret