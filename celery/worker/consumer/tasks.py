"""Worker Task Consumer Bootstep."""
from __future__ import absolute_import, unicode_literals
from kombu.common import QoS, ignore_errors
from celery import bootsteps
from celery.utils.log import get_logger
from .mingle import Mingle

__all__ = ['Tasks']
logger = get_logger(__name__)
debug = logger.debug


class Tasks(bootsteps.StartStopStep):
    """Bootstep starting the task message consumer."""

    requires = (Mingle,)

    def __init__(self, c, **kwargs):
        c.task_consumer = c.qos = None
        c.additional_task_consumers = []
        super(Tasks, self).__init__(c, **kwargs)

    def start(self, c):
        """Start task consumer."""
        c.update_strategies()

        # - RabbitMQ 3.3 completely redefines how basic_qos works..
        # This will detect if the new qos smenatics is in effect,
        # and if so make sure the 'apply_global' flag is set on qos updates.
        qos_global = not c.connection.qos_semantics_matches_spec

        # set initial prefetch count
        c.connection.default_channel.basic_qos(
            0, c.initial_prefetch_count, qos_global,
        )
        for connection in c.additional_connections:
            connection.default_channel.basic_qos(
                0, c.initial_prefetch_count, qos_global
            )

        c.task_consumer = c.app.amqp.TaskConsumer(
            c.connection, on_decode_error=c.on_decode_error,
        )
        c.additional_task_consumers = [c.app.amqp.TaskConsumer(
            connection, on_decode_error=c.on_decode_error,
        ) for connection in c.additional_connections]

        def set_prefetch_count(prefetch_count):
            c.task_consumer.qos(prefetch_count=prefetch_count,
                                apply_global=qos_global)
            for task_consumer in c.additional_task_consumers:
                task_consumer.qos(prefetch_count=prefetch_count,
                                  apply_global=qos_global)
        c.qos = QoS(set_prefetch_count, c.initial_prefetch_count)

    def stop(self, c):
        """Stop task consumers."""
        if c.task_consumer:
            debug('Canceling task consumer...')
            ignore_errors(c, c.task_consumer.cancel)

        # Stop additional task consumers

        debug("Cancelling additional task consumers...")
        for task_consumer in c.additional_task_consumers:
            ignore_errors(c, task_consumer.cancel)

    def shutdown(self, c):
        """Shutdown task consumer."""
        if c.task_consumer:
            self.stop(c)
            debug('Closing primary consumer channel...')
            ignore_errors(c, c.task_consumer.close)
            c.task_consumer = None
        for task_consumer in c.additional_task_consumers:
            debug('Closing additional consumer channels...')
            ignore_errors(c, task_consumer.close)
        c.additional_task_consumers = []

    def info(self, c):
        """Return task consumer info."""
        return {'prefetch_count': c.qos.value if c.qos else 'N/A'}
