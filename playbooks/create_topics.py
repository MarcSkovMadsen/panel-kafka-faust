# %%
from models import KafkaConfig, Topic
# %%
config = KafkaConfig.create_from_toml("config.toml")
# %%
def recreate_topic(name):
    _topic = Topic(config=config)
    _topic.topic = name
    _topic.delete()
    _topic.create()

topics = [
    "hello-world-__assignor-__leader",
    "greetings",
    "page_views",
    "page_views-__assignor-__leader",
    "page_views-__assignor-__leader",
    "playbooks.page_views.count_page_views-page_views-PageView.id-repartition",
    "playbooks.page_views-page_views-changelog",
]
for topic in topics:
    recreate_topic(topic)