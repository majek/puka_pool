import asyncore
import errno
import os
import socket
import logging
import threading
import select

log = logging.getLogger('proxy')

_map = {}

class FixedDispatcher(asyncore.dispatcher):
    def handle_error(self):
        # Default error handling is just pathetic. Just raise the
        # fracking exception, is it so hard?
        log.error('Dispatcher error', exc_info=True)
        raise

class JustRecvSock(FixedDispatcher):
    def handle_read(self):
        self.recv(4096)

class Sock(FixedDispatcher):
    write_buffer = ''
    socks = None
    def __init__(self, *args, **kwargs):
        FixedDispatcher.__init__(self, *args, **kwargs)
        self.left_port = self.getsockname()[1]
        self.right_port = self.getpeername()[1]

    def readable(self):
        return not self.other.write_buffer

    def handle_read(self):
        self.other.write_buffer += self.recv(4096*4)

    def handle_write(self):
        sent = self.send(self.write_buffer)
        self.write_buffer = self.write_buffer[sent:]

    def handle_close(self):
        log.info(' [-] %i -> %i (closed)' % (self.left_port, self.right_port))
        self.close()
        if self.socks:
            self.socks.remove( self )
        if self.other.socks:
            self.other.socks.remove( self.other )
        if self.other.other:
            self.other.close()
            self.other = None

class Server(FixedDispatcher):
    def __init__(self, dst_port, src_port=0, map=None):
        self.dst_port = dst_port
        self.map = map
        FixedDispatcher.__init__(self, map=self.map)
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.set_reuse_addr()
        self.bind(('127.0.0.1', src_port))
        self.src_port = self.getsockname()[1]
        log.info(' [*] Proxying %i ==> %i' % \
                     (self.src_port, self.dst_port))
        self.listen(5)
        self.socks = []

    def handle_accept(self):
        pair = self.accept()
        if not pair:
            return
        left, addr = pair
        try:
            right = socket.create_connection(('127.0.0.1', self.dst_port))
        except socket.error, e:
            if e.errno is not errno.ECONNREFUSED: raise
            log.info(' [!] %i -> %i ==> %i refused' % \
                         (addr[1], self.src_port, self.dst_port))
            left.close()
        else:
            log.info(' [+] %i -> %i ==> %i -> %i' % \
                         (addr[1], self.src_port,
                          right.getsockname()[1], self.dst_port))
            a, b = Sock(left, map=self.map), Sock(right, map=self.map)
            a.other, b.other = b, a
            a.socks = self.socks
            self.socks.append( a )

    def close(self):
        while self.socks:
            self.socks[0].handle_close()
        log.info(' [*] Closed %i ==> %i' % \
                     (self.src_port, self.dst_port))
        self.shutdown(True)
        asyncore.dispatcher.close(self)


# server1 = Server(src_port=49620, dst_port=820, map=_map)
# server2 = Server(src_port=49621, dst_port=80, map=_map)
# asyncore.loop(map=self.map)


class AsyncoreRunner(threading.Thread):
    def __init__(self, map):
        self.map = map
        read, write = socket.socketpair()
        self.X = JustRecvSock(read, map=self.map)
        self.write = write
        self.exit = False
        threading.Thread.__init__(self)

    def ping(self):
        self.write.send('x')

    def run(self):
        log.debug(' [ ] Proxy thread started')
        while not self.exit:
            try:
                asyncore.loop(map=self.map, count=1, use_poll=False)
            except select.error, e:
                # Sometimes there is a race between 'ping' and closing
                # a socket. Select can return 9/EBADF in such case.
                if e[0] is not errno.EBADF: raise
        log.debug(' [ ] Proxy thread stopped')

    def close(self):
        self.exit = True
        self.write.close()
        self.join()



class AmqpProxy(object):
    def __init__(self, ar):
        self.ar = ar
        self.srv = Server(dst_port=5672, map=self.ar.map)
        self.port = self.srv.src_port
        self.ar.ping()

    def disable(self):
        if self.srv:
            self.srv.close()
            self.srv = None
        self.ar.ping()

    def enable(self):
        self.srv = Server(src_port=self.port, dst_port=5672, map=self.ar.map)
        self.ar.ping()

    def __str__(self):
        return 'amqp://127.0.0.1:%s' % (self.port,)

class Proxy(object):
    def __init__(self):
        self._map = {}
        self.ar = AsyncoreRunner(self._map)
        self.ar.start()

    def close(self):
        self.ar.close()
        self.ar = None

    def AmqpProxy(self):
        return AmqpProxy(ar=self.ar)

if __name__ == '__main__':
    import time
    time.sleep(1)

    x = AmqpProxy()
    print x
    time.sleep(1)
    x.close()
    time.sleep(1)
    ar.close()

