from couchbase_ffi._cinit import get_handle
from couchbase_ffi._rtconfig import pycbc_exc_lcb, PyCBC
from couchbase_ffi.bufmanager import BufManager
from couchbase_ffi.view import buf2str, ViewResultBase

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


class N1QLResult(ViewResultBase):
    COMMAND_DECL = 'lcb_CMDN1QL*'
    HANDLE_DECL = 'lcb_N1QLHANDLE*'
    ROWCB_DECL = 'void(lcb_t,int,const lcb_RESPN1QL*)'

    def __init__(self, params, prepare=0, cross_bucket=0):
        super(N1QLResult, self).__init__()
        self._params = params
        self._prepare = prepare
        self._cross_bucket = cross_bucket

    def _init_command(self):
        bm = BufManager(ffi)
        cmd = self._c_command
        C._Cb_n1ql_query_initcmd(cmd, bm.new_cstr(self._params),
                                 len(self._params), self._bound_cb)
        cmd.handle = self._c_handle

    def _query(self, parent, mres):
        return C.lcb_n1ql_query(parent._lcbh, mres._cdata, self._c_command)

    def _handle_resp(self, resp, mres):
        pass

    def _process_resp(self, resp, mres):
        if resp.nrow:
            return PyCBC.json_decode(buf2str(resp.row, resp.nrow))