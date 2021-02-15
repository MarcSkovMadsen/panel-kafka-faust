# %%
import faust
from models import KafkaConfig
from aiokafka.helpers import create_ssl_context
import pathlib

CONFIG_FILE = pathlib.Path(__file__).parent.parent / "config.toml"
# %%
_config = KafkaConfig.create_from_toml(str(CONFIG_FILE))
broker = "kafka://" + _config.bootstrap_servers
_ssl_context = create_ssl_context()
broker_credentials = faust.SASLCredentials(
    username=_config.sasl_username,
    password=_config.sasl_password,
    ssl_context=_ssl_context,
)
replicas=_config.replicas