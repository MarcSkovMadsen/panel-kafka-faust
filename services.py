import toml
path = "config.toml"
_conf=toml.load(path)["kafka"]
from confluent_kafka import KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
import sys
import param

CONFIG = {
    'bootstrap.servers': _conf['bootstrap.servers'],
    'sasl.mechanisms': _conf['sasl.mechanisms'],
    'security.protocol': _conf['security.protocol'],
    'sasl.username': _conf['sasl.username'],
    'sasl.password': _conf['sasl.password'],
}

class Config(param.Parameterized):
    bootstrap_servers = param.String()
    sasl_mechanisms = param.String()
    security_protocol = param.String()
    sasl_username = param.String()
    sasl_password = param.String()

    @classmethod
    def create_from_toml(cls, file: str):


class Topic(param.Parameterized):

def create_admin_client(config=CONFIG):
    return AdminClient(config)

def create_topic(topic, config=CONFIG):
    """
        Create a topic if needed
        Examples of additional admin API functionality:
        https://github.com/confluentinc/confluent-kafka-python/blob/master/examples/adminapi.py
    """
    a = create_admin_client(config=config)
    fs = a.create_topics([NewTopic(
         topic,
         num_partitions=1,
         replication_factor=3
    )])
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print("Topic {} created".format(topic))
        except Exception as e:
            # Continue if error code TOPIC_ALREADY_EXISTS, which may be true
            # Otherwise fail fast
            if e.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                print("Failed to create topic {}: {}".format(topic, e))
                sys.exit(1)

def delete_topic(topic, config=CONFIG):
    a = create_admin_client(config=config)
    a.delete_topics()