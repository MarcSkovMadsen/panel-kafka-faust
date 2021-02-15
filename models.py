from panel.io.callbacks import PeriodicCallback
import param
import panel as pn
pn.extension(comms="vscode")
import toml
from confluent_kafka import KafkaError, KafkaException, Producer as _Producer, Consumer as _Consumer
from confluent_kafka.admin import AdminClient as _AdminClient, NewTopic
import sys

import logging

class Component(param.Parameterized):
    editor = param.Parameter(precedence=-1, constant=True)
    view = param.Parameter(precedence=-1, constant=True)

    _editor_display_treshold = 0
    _view_display_treshold = 1
    _widgets = None
    _name = "Component"

    def __init__(self, **params):
        super().__init__(**params)

        self.editor = self._create_editor_view()
        self.view = self._create_view()

    def _create_editor_view(self):
        with param.edit_constant(self):
            return pn.Param(self, widgets=self._widgets, show_name=False, sizing_mode="stretch_width", display_threshold=self._editor_display_treshold)
    def _create_view(self):
        with param.edit_constant(self):
            return pn.Param(self, widgets=self._widgets, show_name=False, sizing_mode="stretch_width", display_threshold=self._view_display_treshold)

    def __str__(self):
        return self.name

    def __repr__(self):
        return self.name

class LogStream(Component):
    value = param.String(precedence=1)
    latest = param.String(precedence=0, label="Message")

    _widgets = {
        "value": {"widget_type": pn.widgets.TextAreaInput, "sizing_mode": "stretch_both"}
    }
    _name = "Log"


    def flush(self):
        pass

    def write(self, message):
        self.latest = message
        self.value = message + self.value


stream = LogStream()

def get_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    handler = logging.StreamHandler(stream=stream)
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(formatter)

    logger.addHandler(handler)
    return logger

logger = get_logger(__name__)


class KafkaConfig(Component):
    bootstrap_servers = param.String(precedence=1)
    sasl_mechanisms = param.String(precedence=1)
    security_protocol = param.String(precedence=1)
    sasl_username = param.String(precedence=1)
    sasl_password = param.String(precedence=1)
    group_id = param.String('python_example_group_1', precedence=1)
    auto_offset_reset = param.ObjectSelector('earliest', objects=["earliest", "latest"], precedence=1)
    replicas = param.Integer(3)

    _widgets = {
            "sasl_password": pn.widgets.PasswordInput
        }
    _name = "Kafka Configuration"

    def __init__(self, **params):
        super().__init__(**params)

        self._producer = None
        self._admin_client = None
        self._consumer = None

    @classmethod
    def create_from_toml(cls, file: str, key="Kafka"):
        _conf=toml.load(file)
        if key:
            _conf=_conf[key]
        return cls(
            bootstrap_servers = _conf['bootstrap.servers'],
            sasl_mechanisms = _conf['sasl.mechanisms'],
            security_protocol = _conf['security.protocol'],
            sasl_username = _conf['sasl.username'],
            sasl_password = _conf['sasl.password'],
            group_id = _conf['group.id'],
            auto_offset_reset = _conf['auto.offset.reset'],
            replicas = _conf['replicas'],
        )

    def to_producer_dict(self):
        return {
            'bootstrap.servers': self.bootstrap_servers,
            'sasl.mechanisms': self.sasl_mechanisms,
            'security.protocol': self.security_protocol,
            'sasl.username': self.sasl_username,
            'sasl.password': self.sasl_password,
        }

    def to_consumer_dict(self):
        conf = self.to_producer_dict()
        conf['group.id'] = self.group_id
        conf['auto.offset.reset'] = self.auto_offset_reset
        return conf


    @param.depends(
        "bootstrap_servers",
        "sasl_mechanisms",
        "security_protocol",
        "sasl_username",
        "sasl_password",
        watch=True
    )
    def _clear_all(self):
        self._producer = None
        self._consumer = None
        self._admin_client = None

    @param.depends(
        "group_id", "auto_offset_reset",
        watch=True
    )
    def _clear_consumer(self):
        self._consumer = None

    @property
    def producer(self):
        if not self._producer:
            self._producer = _Producer(self.to_producer_dict())
        return self._producer

    @property
    def consumer(self):
        if not self._consumer:
            self._consumer = _Consumer(self.to_consumer_dict())
        return self._consumer

    @property
    def admin_client(self):
        if not self._admin_client:
            self._admin_client = _AdminClient(self.to_producer_dict())
        return self._admin_client


class Topic(Component):
    topic = param.String(default="test-topic", label="Name", doc="""The name of the Topic""", precedence=1)
    num_partitions = param.Integer(1, bounds=(1,None), precedence=1)
    replication_factor = param.Integer(3, bounds=(1, None), precedence=1)

    create = param.Action()
    delete = param.Action()

    config = param.ClassSelector(class_=KafkaConfig, precedence=-1)

    _widgets = {
        "create": {"button_type": "primary"},
        "delete": {"button_type": "danger"}
    }

    def __init__(self, **params):
        super().__init__(**params)

        self.create=self._create
        self.delete=self._delete

    def _create(self, *events):
        admin = self.config.admin_client
        fs = admin.create_topics([NewTopic(
            self.topic,
            num_partitions=1,
            replication_factor=3
        )])
        for topic, f in fs.items():
            try:
                f.result()  # The result itself is None
                logger.info("Topic {} created".format(topic))
            except KafkaException as e:
                # Continue if error code TOPIC_ALREADY_EXISTS, which may be true
                # Otherwise fail fast
                if e.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                    logger.info("Cannot create Topic {} as it already exists".format(topic))
                else:
                    logger.error("Failed to create topic {}: {}".format(topic, e))

    def _delete(self, *events):
        admin = self.config.admin_client
        fs = admin.delete_topics([self.topic])
        for topic, f in fs.items():
            try:
                f.result()  # The result itself is None
                logger.info("Topic {} deleted".format(topic))
            except KafkaException as e:
                # Continue if error code TOPIC_ALREADY_EXISTS, which may be true
                # Otherwise fail fast
                if e.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                    logger.info("Cannot create Topic {} as it already exists".format(topic))
                else:
                    logger.error("Failed to create topic {}: {}".format(topic, e))

class Producer(Component):
    key = param.String("alice", precedence=1)
    value = param.String("hello world", precedence=1)

    produce = param.Action(precedence=0)
    poll = param.Action(precedence=0)
    delivered_records = param.Integer(precedence=0, constant=True)

    topic = param.ClassSelector(class_=Topic, precedence=-1)
    config = param.ClassSelector(class_=KafkaConfig, precedence=-1)

    _name = "Producer"
    _widgets = {
        "produce": {"button_type": "primary"}
    }

    def __init__(self, **params ):
        super().__init__(**params)

        self.produce = self._produce
        self.poll = self._poll

    def _acked(self, err, msg):
        """Delivery report handler called on
        successful or failed delivery of message"""
        if err is not None:
            logger.error("Failed to deliver message: {}".format(err))
        else:
            with param.edit_constant(self):
                self.delivered_records += 1
            logger.info("Produced record to topic {} partition [{}] @ offset {}"
                  .format(msg.topic(), msg.partition(), msg.offset()))

    def _poll(self, *events):
        self.config.producer.poll(0)

    def _produce(self, *events):
        key = self.key
        value = self.value
        logger.info("Producing record: {}\t{}".format(key, value))

        producer = self.config.producer
        producer.produce(topic.topic, key=key, value=value, on_delivery=self._acked)
        # p.poll() serves delivery reports (on_delivery)
        # from previous produce() calls.
        def poll():
            print("poll")
            result = producer.poll(0)
            if result == 0:
                pn.state.add_periodic_callback(poll, count=1, timeout=1000)

        poll()

class Consumer(Component):
    topic = param.ClassSelector(class_=Topic, precedence=-1)
    config = param.ClassSelector(class_=KafkaConfig, precedence=-1)

    message = param.Parameter()
    key = param.String()
    value = param.String()

    consumed_records  = param.Integer(precedence=0, constant=True)

    subscribe = param.Action()
    poll = param.Action()

    _name = "Consumer"
    _widgets = {
        "poll": {"button_type": "primary"}
    }

    def __init__(self, **params ):
        super().__init__(**params)

        self.subscribe = self._subscribe
        self.poll = self._poll

    def _subscribe(self, *events):
        consumer = self.config.consumer
        consumer.subscribe([self.topic.topic])

    def _poll(self, *events):
        consumer = self.config.consumer
        msg = consumer.poll(1.0)

        if msg is None:
            # No message available within timeout.
            # Initial message consumption may take up to
            # `session.timeout.ms` for the consumer group to
            # rebalance and start consuming
            pass

        elif msg.error():
            logger.error('error: {}'.format(msg.error()))
        else:
            # Check for Kafka message
            record_key = msg.key().decode("utf-8")
            record_value = msg.value().decode("utf-8")
            self.consumed_records += 1
            self.message = msg
            self.key = record_key
            self.value = record_value

            logger.info("Consumed record with key {} and value {}, \
                and updated total count to {}"
                .format(record_key, record_value, self.consumed_records))


    def _create_editor(self):
        with param.edit_constant(self):
            return pn.Param(self, widgets=self._widgets, show_name=False, sizing_mode="stretch_width", display_threshold=self._editor_display_treshold, expand=True, expand_button=True)

def create_app():
    from awesome_panel_extensions.frameworks.fast.templates import FastListTemplate

    config = KafkaConfig.create_from_toml("config.toml")
    topic = Topic(config=config)
    producer = Producer(config=config, topic=topic)
    consumer = Consumer(config=config, topic=topic)

    server_button = pn.widgets.Button(name="Start Server")
    def start_stop_server(*events):
        if start_stop_server.server == None:
            template = FastListTemplate(title="Holoviz Panel meets Confluent Kafka and Faust", main=[app])
            start_stop_server.server = template.show()
            server_button.button_type="primary"
            server_button.name = "Start Server"
        else:
            start_stop_server.server.stop()
            start_stop_server.server = None
            server_button.button_type="danger"
            server_button.name = "Stop Server"
    start_stop_server.server = None
    server_button.on_click(start_stop_server)

    logos = pn.Column(
        pn.pane.HTML(
            "<img src='http://blog.holoviz.org/images/panel_logo.png' style='height:100px;display:inline-block'></img>"
            "<img src='https://www.indellient.com/wp-content/uploads/2020/10/20201021_Introduction-to-Apache-Kafka_BLOG-FEATURED-IMAGE.jpg' style='height:100px;display:inline-block'></img>"
            "<img src='https://assets.confluent.io/m/1661ef5e4ff82d3d/original/20200122-PNG-web-dev-logo-denim.png' style='height:100px;display:inline-block'></img>"
            "<img src='https://raw.githubusercontent.com/robinhood/faust/8ee5e209322d9edf5bdb79b992ef986be2de4bb4/artwork/banner-alt1.png' style='height:100px;display:inline-block'></img>",
            sizing_mode="stretch_width"
            )
    )

    app = pn.Column(
        pn.Tabs(
            ("Home", logos),
            ("Configuration", config.view),
            ("Topic", topic.view),
            ("Producer", producer.editor),
            ("Consumer", consumer.editor),
            ("Log", stream.view),
            ("Server", server_button),
            sizing_mode="stretch_both"
        ),
        pn.layout.Divider(sizing_mode="stretch_width"),
        stream.param.latest,
        sizing_mode="stretch_both"
    )
    return app


