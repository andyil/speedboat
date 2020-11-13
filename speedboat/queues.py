import boto3
from speedboat.lazythreadpool import LazyThreadPool
import threading


class MessageSender:

    def __init__(self, queue_url):
        self.queue_url = queue_url
        self.ltp = LazyThreadPool()
        self.local = threading.local()

    def send_10(self, batch):
        if not hasattr(self.local, 'client'):
            self.local.client = boto3.client('sqs')

        sqs = self.local.client
        entries = [{'Id': str(i), 'MessageBody': elem} for i, elem in enumerate(batch)]

        sqs.send_message_batch(self.queue_url, entries)

    def batch_10(self, messages):
        current = []
        for m in messages:
            current.append(m)
            if len(current) == 10:
                yield current
                current = []

        if current:
            yield current

    def send_all(self, messages):
        for batch in self.batch_10(messages):
            yield self.ltp.do_all(self.send_10, batch)





def send_messages(queue_url, messages):
    ms = MessageSender(queue_url)
    return ms.send_all(messages)

for x in send_messages('https://sqs.eu-central-1.amazonaws.com/175267656368/x1', [str(x) for x in range(100)]):
    print(x)
