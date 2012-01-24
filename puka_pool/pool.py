import puka
import time
import collections
import itertools
import select
import socket
import errno
import logging
import uuid
import functools

from .promise import Result, MINOR, MAJOR, FINAL, PromiseCollection

log = logging.getLogger('cluster')

try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict


Message = collections.namedtuple('Message', ['msg_id', 'args', 'kwargs'])
Consumer = collections.namedtuple('Consumer', ['con_id', 'args', 'kwargs'])
Received = collections.namedtuple('Received', [])

class _Pool:
    def __init__(self):
        self.timers = {}
        self.promises = PromiseCollection()

    def add_timer(self, key, timeout, callback):
        self.timers[key] = (time.time() + timeout / 1000.0, callback)

    def del_timer(self, key):
        if key in self.timers:
            del self.timers[key]

    def next_timer(self, min_when=None):
        now = time.time()
        if min_when is None:
            min_when = now + 30.0
        for key in self.timers.keys():
            (when, callback) = self.timers[key]
            if when - now <= 0:
                callback()
                del self.timers[key]
                # quit instantly
                min_when = now
            else:
                min_when = min(min_when, when)
        return max(0.0, min_when - now)

    def _loop(self, should_break, quit_after=None):
        while True:
            nodes = [n for n in self.nodes if n.client]
            for n in nodes:
                n.client.run_any_callbacks()
            r = should_break()
            if r:
                return r

            rfds = nodes
            wfds = []
            do_break = False
            for n in nodes:
                try:
                    if n.client.needs_write():
                        wfds.append( n )
                except socket.error, e:
                    if e.errno is not errno.ECONNREFUSED: raise
                    n.mark_dead()
                    do_break = True
            if do_break:
                continue

            r, w, e = select.select(rfds, wfds, rfds, self.next_timer(quit_after))
            for n in set(itertools.chain(r, e)):
                try:
                    n.client.on_read()
                except socket.error, e:
                    if e.errno is not errno.ECONNREFUSED: raise
                    n.mark_dead()
                    do_break = True
                if not n.client or not n.client.sd:
                    do_break = True
            if do_break:
                continue

            for n in w:
                if not n.client:
                    continue
                try:
                    n.client.on_write()
                except socket.error, e:
                    if e.errno is not errno.ECONNREFUSED: raise
                    n.mark_dead()

            if (not r and not e and not w
                and quit_after
                and time.time() >= quit_after):
                # timeout
                return None

    def loop(self, timeout=None):
        quit_after = time.time() + timeout if timeout is not None else None
        while True:
            l = self._loop(lambda: bool(self.promises._dirty),
                           quit_after)
            if l is None:
                # Timeout
                return None
            while self.promises._dirty:
                for number in self.promises._dirty.keys():
                    self.promises.run(number)

    def wait(self, promise, timeout=None, kind=MAJOR):
        quit_after = time.time() + timeout if timeout is not None else None
        number = promise.number
        while True:
            l = self._loop(lambda: number in self.promises._dirty,
                           quit_after)
            if l is None:
                # Timeout
                return None
            r = self.promises.run(number, kind=kind)
            if r:
                return r

    def emit(self, number, result):
        self.promises.emit(number, result)

def promise_decorator(method):
    @functools.wraps(method)
    def wrapper(*args, **kwargs):
        callback = kwargs.get('callback')
        if callback: del kwargs['callback']
        p = method(*args, **kwargs)
        p.major_callback = callback
        return p
    return wrapper

class AtLeastOnePool(_Pool):
    max_timeout = 10000
    def __init__(self):
        self.nodes = []
        self.unsent_messages = []
        self.messages = OrderedDict()
        self.consumes_id = set()
        self.consumes = {}
        _Pool.__init__(self)

    def on_setup_user(self, client, setup_done):
        self.on_setup(client, setup_done)
    def on_setup(self, client, setup_done):
        setup_done()

    def on_setup_system(self, client, setup_done):
        setup_done()
        # after OPEN
        self.maybe_flush()
        self.maybe_consume()

    def add_node(self, amqp_url):
        self.nodes.append( Node(self, amqp_url, self.max_timeout) )

    def del_node(self, amqp_url):
        node = [n for n in self.nodes if n.amqp_url == amqp_url][0]
        self.nodes.remove(node)
        node.close()

    @promise_decorator
    def publish(self, *args, **kwargs):
        p = self.promises.new()
        msg_id = p.number
        self.unsent_messages.append(msg_id)
        self.messages[msg_id] = Message(msg_id, args, kwargs)
        self.maybe_flush()
        return p

    def maybe_flush(self):
        nodes = [n for n in self.nodes if n.status is OPEN]
        if not nodes or not self.unsent_messages:
            return
        node = nodes[0]
        for msg_id in self.unsent_messages:
            self.emit(msg_id, Result('publish', MINOR, node=node.amqp_url))
        node.publish([self.messages[msg_id] \
                          for msg_id in self.unsent_messages])
        self.unsent_messages = []

    def on_nack(self, list_of_msg_id, node):
        for msg_id in list_of_msg_id:
            self.emit(msg_id, Result('nack', MINOR, node=node.amqp_url))
        um = list_of_msg_id[:]
        um.extend(self.unsent_messages)
        self.unsent_messages = um
        self.maybe_flush()

    def on_permanent_nack(self, msg_id, node, amqp_result):
        self.emit(msg_id, Result('nack', FINAL, node=node.amqp_url,
                                 amqp_result=amqp_result, is_error=True))
        del self.messages[msg_id]

    def on_ack(self, msg_id, node):
        self.emit(msg_id, Result('ack', FINAL, node=node.amqp_url))
        msg = self.messages[msg_id]
        del self.messages[msg_id]

    def maybe_consume(self):
        nodes = [n for n in self.nodes if n.status is OPEN]
        for node in nodes:
            d = self.consumes_id - node.consumes_id
            if d:
                node.consume( [self.consumes[con_id] for con_id in d] )

    @promise_decorator
    def consume(self, *args, **kwargs):
        p = self.promises.new()
        self.consumes_id.add(p.number)
        self.consumes[p.number] = Consumer(p.number, args, kwargs)
        self.maybe_consume()
        return p

    def on_consume(self, con_id, node, amqp_result):
        def do_ack():
            if node.client:
                node.client.basic_ack(amqp_result)
        consumer = self.consumes[con_id]
        if amqp_result.is_error:
            if isinstance(amqp_result.exception, puka.ConnectionBroken):
                self.emit(con_id, Result('cancel', MINOR, node=node.amqp_url))
                self.promises.rollback_emit(con_id, lambda r:r.event == 'message')
            else:
                raise amqp_result.exception
        else:
            self.emit(con_id, Result('message', MAJOR, node=node.amqp_url,
                                     amqp_result=amqp_result,
                                 ack=do_ack \
                                         if not consumer.kwargs.get('no_ack') \
                                         else None))

    def ack(self, result):
        nodes = [n for n in self.nodes \
                     if n.amqp_url is result.amqp_url and n.status is OPEN]
        if not nodes: return
        node = nodes[0]
        if node: pass
        self.emit(msg_id, Result('ack', FINAL, node=node.amqp_url))
        msg = self.messages[msg_id]
        del self.messages[msg_id]

DEAD=0
CONNECTING=1
SETTINGUP=2
OPEN=3
WAITING=4

class Node:
    def __init__(self, cluster, amqp_url, max_timeout):
        self.status = DEAD
        self.cluster = cluster
        self.amqp_url = amqp_url
        self.max_timeout = max_timeout
        self.timeout = 2
        self.client = None
        self.do_connect()

    def fileno(self):
        return self.client.fileno()

    def do_connect(self):
        self.status = CONNECTING
        self.messages = OrderedDict()
        self.consumes_id = {}
        self.client = puka.Client(str(self.amqp_url), pubacks=True)
        self.client.connect(callback=self.on_connect)
        log.info('#%s connecting... ' % (self.amqp_url,))

    def on_connect(self, _promise, result):
        if result.is_error:
            self.mark_dead()
            return
        assert result.name == 'connection.start'
        self.status = SETTINGUP
        self.timeout = 2
        log.info('#%s connected' % (self.amqp_url,))
        self.cluster.on_setup_user(self.client, self.on_setup_done1)

    def on_setup_done1(self):
        self.cluster.on_setup_system(self.client, self.on_setup_done2)

    def on_setup_done2(self):
        self.status = OPEN
        log.info('#%s connected and setup, ready to go' % (self.amqp_url,))

    def mark_dead(self):
        # We can be marked dead both in on_read as in on_write, so
        # need to handle multiple entries here:
        if self.status == WAITING:
            return
        self.status = WAITING
        if self.client.sd:
            self.client.close()
        self.client = None

        self.cluster.on_nack(self.messages.keys(), self)
        self.messages.clear()

        to, self.timeout = self.timeout, min(self.timeout**2,
                                             self.max_timeout)
        self.cluster.add_timer(self, to, self.do_connect)
        log.info('#%s dead, try again in %sms ' %
                     (self.amqp_url, to))

    def close(self):
        self.status = DEAD
        if self.client:
            self.client.close()
            self.client = None
        self.cluster.del_timer(self)
        self.cluster = None

    def publish(self, list_of_msg):
        for msg in list_of_msg:
            self._publish(msg)

    def _publish(self, msg):
        self.messages[msg.msg_id] = msg
        def on_publish(_promise, result):
            self.on_publish(msg.msg_id, result)
        pub_kwargs = msg.kwargs.copy()
        pub_kwargs['callback'] = on_publish
        self.client.basic_publish(*msg.args, **pub_kwargs)

    def on_publish(self, msg_id, result):
        if result.is_error and isinstance(result.exception, puka.ConnectionBroken):
            if msg_id in self.messages:
                del self.messages[msg_id]
                self.cluster.on_nack([msg_id], self)
            return

        del self.messages[msg_id]
        if not result.is_error:
            self.cluster.on_ack(msg_id, self)
        else:
            self.cluster.on_permanent_nack(msg_id, self, result)

    def consume(self, consumes):
        for consume in consumes:
            self._consume(consume)

    def _consume(self, consume):
        self.consumes_id[ ] = consume.con_id
        con_kwargs = consume.kwargs.copy()
        def on_consume(_promise, result):
            self.on_consume(consume.con_id, result)
        con_kwargs['callback'] = on_consume
        t = self.client.basic_consume(*consume.args, **con_kwargs)

    def on_consume(self, con_id, result):
        if result.is_error:
            if isinstance(amqp_result.exception, puka.ConnectionBroken):
                self.promises.rollback_emit(con_id, lambda r:r.event == 'message')
                self.emit(con_id, Result('cancel', MINOR, node=node.amqp_url))
            else:
                raise amqp_result.exception
        else:
            self.cluster.on_consume(con_id, self, result)

