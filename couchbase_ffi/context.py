import types

from couchbase.exceptions import ValueFormatError, ArgumentError, CouchbaseError
from couchbase.user_constants import *

from couchbase_ffi.result import (OperationResult, Result, ValueResult)

ffi = None
C = None

class Options(dict):
    pass


class CommandContext(object):
    def __init__(self, conn, **kwargs):
        self._basename = self.STRUCTNAME
        self._conn = conn
        self.quiet = kwargs.get('quiet', False)
        self.posargs = kwargs.get('posargs', tuple())

    def make_struct_pointers(self, ncmds):
        fmtstr_l = "{0} [{1}]"
        fmtstr_pp = "{0} *[{1}]"

        self._cmdlist = ffi.new(fmtstr_l.format(self._basename, ncmds))
        self._cmdpp = ffi.new(fmtstr_pp.format(self._basename, ncmds))
        self._cdata = []
        self._ncmds = ncmds

    def process_single_command(self, req, koptions):
        raise NotImplentedError()

    @staticmethod
    def extract_cas(req, options):
        req.cas = options.get('cas', 0)

    @staticmethod
    def extract_ttl(req, options):
        try:
            ttl = options.get('ttl', 0)
            if ttl < 0:
                raise ArgumentError.pyexc("TTL cannot be negative", obj=ttl)

            req.exptime = ttl
            return ttl

        except CouchbaseError:
            raise

        except Exception as e:
            raise ArgumentError.pyexc(inner=e, obj=options.get('ttl', 0))

    @property
    def STRUCTNAME(self):
        raise NotImplementedError("Struct name must be specified in subclass")

    def convert_to_koptions(self, options):
        raise ArgumentError.pyexc(obj=options, message="Options expected")

    def _build_single(self, kv, kviter, ix):
        try:
            cur = kviter.next()
        except AttributeError:
            cur = kviter.__next__()

        if isinstance(kv, dict):
            key, koptions = cur
            if not isinstance(koptions, Options):
                if isinstance(koptions, OperationResult):
                    koptions = Options(cas=koptions.cas)
                else:
                    koptions = self.convert_to_koptions(koptions)

        else:
            key = cur
            koptions = None

            if isinstance(key, Result):
                if isinstance(key, OperationResult):
                    koptions = Options(cas=key.cas)
                key = key.key

            if koptions is None:
                koptions = Options()

        cmd = self._cmdlist[ix]
        self._cmdpp[ix] = ffi.addressof(cmd)

        try:
            k_enc = self._conn._tc.encode_key(key)
            if not k_enc:
                raise ValueFormatError.pyexc("Key is empty", obj=key)

            s_key = ffi.new('char[]', k_enc)

        except CouchbaseError:
            raise
        except Exception as e:
            raise ValueFormatError.pyexc(obj=key, inner=e)

        self._cdata.append(s_key)

        req = cmd.v.v0
        req.key = s_key
        req.nkey = len(k_enc)

        self.process_single_command(req, koptions)

    _disallowed_types = (
        basestring, bytearray, int, long, float, types.NoneType, bool)

    @classmethod
    def _verify_iter(self, kv):
        if isinstance(kv, (dict, list, tuple)):
            return

        if isinstance(kv, self._disallowed_types):
            raise ArgumentError.pyexc(obj=kv,
                                      message="Bad sequence type")

        if not hasattr(kv, '__iter__'):
            raise ArgumentError.pyexc(obj=kv,
                                      message="Object is not iterable")

        try:
            len(kv)
        except TypeError:
            raise ArgumentError.pyexc(obj=kv,
                                      message="Iterator must have length")

    def build(self, kv):
        self._verify_iter(kv)
        if not len(kv):
            raise ArgumentError.pyexc(obj=kv, message="No items in container")

        self.make_struct_pointers(len(kv))
        curix = 0


        if isinstance(kv, dict):
            kviter = kv.iteritems()
        else:
            kviter = iter(kv)

        while True:
            try:
                self._build_single(kv, kviter, curix)
                curix += 1

            except StopIteration:
                break

        if curix != self._ncmds:
            raise ArgumentError.pyexc(message="Bad iterator", obj=kv)
        return self._cmdpp

    def args(self):
        return (self._ncmds, self._cmdpp)


class StoreCommandContext(CommandContext):
    STRUCTNAME = 'lcb_store_cmd_t'

    def __init__(self, parent, mode, kv, cas=0, ttl=0, format=None, **kwargs):
        super(StoreCommandContext, self).__init__(parent, **kwargs)

        if format is None:
            if mode in (C.LCB_APPEND, C.LCB_PREPEND):
                format = FMT_UTF8
            else:
                format = parent.default_format

        self._koptions = Options({
            'cas': cas,
            'ttl': ttl,
            'format': format
        })

        self._mode = mode
        self.build(kv)

    def convert_to_koptions(self, koptions):
        return Options(value=koptions)

    def process_single_command(self, req, koptions):
        _tmp = koptions
        koptions = self._koptions.copy()
        koptions.update(_tmp)
        try:
            v_enc, flags = self._conn._tc.encode_value(koptions['value'],
                                                       koptions['format'])
            if not isinstance(v_enc, bytes):
                raise ValueFormatError.pyexc("Value was not bytes", obj=v_enc)

            s_val = ffi.new('char[]', v_enc)

        except CouchbaseError:
            raise

        except Exception as ex:
            raise ValueFormatError.pyexc(str(ex))

        self._cdata.append(s_val)

        req.bytes = s_val
        req.nbytes = len(v_enc)

        self.extract_cas(req, koptions)
        self.extract_ttl(req, koptions)

        try:
            req.flags = flags
        except Exception as e:
            raise ValueFormatError.pyexc(obj=flags, inner=e)

        req.operation = self._mode

class GetCommandContext(CommandContext):

    STRUCTNAME = 'lcb_get_cmd_t'
    _is_lock = False

    def __init__(self, parent, kv, ttl=0, **kwargs):
        super(GetCommandContext, self).__init__(parent, **kwargs)
        self._koptions = Options({
            'ttl': ttl
        })

        self.build(kv)
        self.no_format = kwargs.get('no_format', False)

    def process_single_command(self, req, koptions):
        options = koptions.copy()
        options.update(self._koptions)

        ttl = self.extract_ttl(req, options)

        if self._is_lock:
            req.lock = 1
            if not ttl:
                raise ArgumentError.pyexc("Lock must have TTL")

class LockCommandContext(GetCommandContext):
    _is_lock = True


class RemoveCommandContext(CommandContext):

    STRUCTNAME = 'lcb_remove_cmd_t'

    def __init__(self, parent, kv, cas=0, **kwargs):
        super(RemoveCommandContext, self).__init__(parent, **kwargs)
        self._koptions = {
            'cas': cas
        }

        self.build(kv)

    def convert_to_koptions(self, options):
        return Options(cas=options)

    def process_single_command(self, req, koptions):
        options = self._koptions.copy()
        options.update(koptions)
        self.extract_cas(req, options)


class ArithCommandContext(CommandContext):
    STRUCTNAME = 'lcb_arithmetic_cmd_t'

    @property
    def is_incr(self):
        raise NotImplementedError()

    def __init__(self, parent, kv, **kwargs):
        super(ArithCommandContext, self).__init__(parent, **kwargs)

        self._koptions = {
            'initial': kwargs.get('initial', None),
            'ttl': kwargs.get('ttl', 0),
            'amount': kwargs.get('amount', 1)
        }

        self.build(kv)

    def process_single_command(self, req, koptions):
        options = koptions.copy()
        options.update(self._koptions)

        initial = options.get('initial', None)
        if initial is not None:
            req.create = 1
            req.initial = initial
        else:
            req.create = 0
            req.initial = 0

        if self.is_incr:
            req.delta = options['amount']
        else:
            req.delta = -options['amount']

        self.extract_ttl(req, options)


class DecrCommandContext(ArithCommandContext):
    is_incr = False


class IncrCommandContext(ArithCommandContext):
    is_incr = True


class UnlockCommandContext(CommandContext):
    STRUCTNAME = 'lcb_unlock_cmd_t'

    def __init__(self, parent, kv, **kwargs):
        super(UnlockCommandContext, self).__init__(parent, **kwargs)

        if self.posargs:
            kwargs['cas'] = self.posargs[0]

        self._koptions = {
            'cas': kwargs.get('cas', 0)
        }

        self.build(kv)

    def convert_to_koptions(self, input):
        try:
            input + 0
            return Options(cas=input)
        except:
            raise ArgumentError.pyexc("Invalid CAS", obj=input)

    def process_single_command(self, req, koptions):
        options = self._koptions.copy()
        options.update(koptions)

        if not options.get('cas', 0):
            raise ArgumentError.pyexc("Must have CAS for unlock")

        req.cas = options['cas']


class TouchCommandContext(CommandContext):
    STRUCTNAME = 'lcb_touch_cmd_t'

    def __init__(self, parent, kv, **kwargs):
        super(TouchCommandContext, self).__init__(parent, **kwargs)

        self._koptions = {
            'ttl': kwargs.get('ttl', 0)
        }

        self.build(kv)

    def convert_to_koptions(self, input):
        try:
            input + 0
            return Options(ttl=input)
        except:
            raise ArgumentError.pyexc("Invalid TTL", obj=input)

    def process_single_command(self, req, koptions):
        options = self._koptions.copy()
        options.update(koptions)

        self.extract_ttl(req, options)
