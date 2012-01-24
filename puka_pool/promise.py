import logging
log = logging.getLogger('cluster')

MINOR=0   # an event, mostly ignored
MAJOR=1   # should by default be returned by wait()
FINAL=2   # must be returned by wait() and is the last one

class PromiseCollection(object):
    def __init__(self):
        self._promises = {}
        self.promise_number = 1
        self._dirty = {}

    def new(self, **kwargs):
        number, self.promise_number = self.promise_number, self.promise_number+1
        promise = Promise(number, **kwargs)
        self._promises[number] = promise
        return promise

    def run(self, number, **kwargs):
        r = self._dirty[number].run(**kwargs)
        if not self._dirty[number].results:
            del self._dirty[number]
        return r

    def emit(self, number, result):
        if number not in self._promises:
            log.info('promise %s not found, %r %r' % (number, event, args))
            return

        promise = self._promises[number]
        self._dirty[number] = promise
        promise.emit(result)
        if result.kind is FINAL:
            del self._promises[number]

    def rollback_emit(self, number, filter_fun):
        promise = self._promises[number].rollback_emit(filter_fun)

class Promise(object):
    def __init__(self, number):
        self.number = number
        self.results = []

    def emit(self, result):
        self.results.append( result )

    def run(self, kind=MAJOR):
        while self.results:
            result = self.results.pop(0)

            if getattr(self, result.event, None):
                getattr(self, result.event)(self, result)
            if result.kind >= kind:
                if self.major_callback:
                    self.major_callback(self, result)
                return result

    def rollback_emit(self, filter_fun):
        self.results = filter(lambda a:not filter_fun(a), self.results)

class Result(dict):
    event = None
    is_error = False
    kind = None

    def __init__(self, event, kind, **kwargs):
        self.kind = kind
        kwargs = kwargs.copy()
        kwargs['event'] = event
        self.update( kwargs )
        for k, v in kwargs.items():
            setattr(self, k, v)
