import functools
import uuid

from puka_pool import test_proxy
import puka_pool


# import unittest_backport as unittest
import unittest

_proxy = None
token = str(uuid.uuid4())

def CreateQueuePool(meta, queue_name):
    class _Pool(meta):
        def on_setup(self, client, on_setup_done):
            class Foo:
                def __init__(self):
                    client.queue_declare(queue=queue_name,
                                         auto_delete=True, durable=False,
                                         callback=self.foo1)
                def foo1(self, _promise, result):
                    assert result.is_error is False
                    on_setup_done()
            Foo()
    return _Pool()


class ProxyTest(unittest.TestCase):
    def tearDown(self):
        if not getattr(self, 'amqp_proxies', None):
            return
        for amqp_proxy in self.amqp_proxies:
            amqp_proxy.disable()

def with_proxy(number=1):
    def foo(method):
        @functools.wraps(method)
        def wrapper(self):
            self.amqp_proxies = [_proxy.AmqpProxy() for i in range(number)]
            return method(self, *self.amqp_proxies)
        return wrapper
    return foo


class TestBasic(ProxyTest):
    @with_proxy()
    def test_simple_publish(self, amqp_url):
        pool = CreateQueuePool(puka_pool.AtLeastOnePool, 'q')
        p = pool.publish(exchange='', routing_key='q', body=token)
        pool.add_node(amqp_url)
        result = pool.wait(p)
        self.assertEqual(result.event, 'ack')
        self.assertEqual(result.node, amqp_url)

    @with_proxy(2)
    def test_one_dead_publish(self, amqp_url1, amqp_url2):
        pool = CreateQueuePool(puka_pool.AtLeastOnePool, 'q')
        p = pool.publish(exchange='', routing_key='q', body=token)

        amqp_url1.disable()
        amqp_url2.disable()

        pool.add_node(amqp_url1)
        pool.add_node(amqp_url2)
        result = pool.wait(p, timeout=0.01)
        self.assertEqual(result, None)

        amqp_url2.enable()
        result = pool.wait(p)
        self.assertEqual(result.event, 'ack')
        self.assertEqual(result.node, amqp_url2)

    @with_proxy()
    def test_minor(self, amqp_url):
        pool = CreateQueuePool(puka_pool.AtLeastOnePool, 'q')
        pool.add_node(amqp_url)
        p = pool.publish(exchange='', routing_key='q', body=token)
        result = pool.wait(p, kind=puka_pool.MINOR)
        self.assertEqual(result.event, 'publish')
        self.assertEqual(result.node, amqp_url)
        result = pool.wait(p, kind=puka_pool.MINOR)
        self.assertEqual(result.event, 'ack')
        self.assertEqual(result.node, amqp_url)

    @with_proxy()
    def test_permanent_nack(self, amqp_url):
        pool = puka_pool.AtLeastOnePool()
        pool.add_node(amqp_url)

        p1 = pool.publish(exchange='doesnt exist', routing_key='q', body=token)
        p2 = pool.publish(exchange='doesnt exist', routing_key='q', body=token)
        for p in [p1, p2]:
            result = pool.wait(p)
            self.assertEqual(result.event, 'nack')
            self.assertEqual(result.is_error, True)
            self.assertEqual(result.node, amqp_url)
            self.assertEqual(result.amqp_result.is_error, True)

    @with_proxy()
    def test_consume(self, amqp_url):
        pool = CreateQueuePool(puka_pool.AtLeastOnePool, 'q1')
        pool.add_node(amqp_url)

        p = pool.publish(exchange='', routing_key='q1', body=token+'a')
        pool.wait(p)

        amqp_url.disable()
        consume_promise = pool.consume(queue='q1', no_ack=True)
        amqp_url.enable()

        p = pool.publish(exchange='', routing_key='q1', body=token+'b')
        pool.wait(p)

        result = pool.wait(consume_promise)
        self.assertEqual(result.event, 'message')
        self.assertEqual(result.is_error, False)
        self.assertEqual(result.amqp_result['body'], token+'a')

        result = pool.wait(consume_promise)
        self.assertEqual(result.event, 'message')
        self.assertEqual(result.is_error, False)
        self.assertEqual(result.amqp_result['body'], token+'b')


if __name__ == '__main__':
    _proxy = test_proxy.Proxy()
    try:
        unittest.main()
    finally:
        _proxy.close()

