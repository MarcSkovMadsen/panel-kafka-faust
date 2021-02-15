import param
import panel as pn
pn.extension(comm="vscode")
from models import KafkaConfig, Topic
from confluent_kafka.admin import AdminClient, NewTopic

class KafkaConfigurationEditor(param.Parameterized):
    config = param.ClassSelector(class_=KafkaConfig, instantiate=True)
    view = param.Parameter(constant=True)

    def __init__(self, **params):
        params["config"]=params.get("config", KafkaConfig())
        super().__init__(**params)

        self._create_view()

    def _create_view(self):
        widgets = {
            "sasl_password": pn.widgets.PasswordInput
        }
        view = pn.Column(
            pn.pane.Markdown("## Kafka Configuration", margin=(0, 15)),
            pn.Param(self.config, widgets=widgets, show_name=False, margin=(0,5), sizing_mode="stretch_width"),
            sizing_mode="stretch_width"
        )
        with param.edit_constant(self):
            self.view = view

class TopicEditor(param.Parameterized):
    topic = param.ClassSelector(class_=Topic)
    admin = param.ClassSelector(class_=Admin)
