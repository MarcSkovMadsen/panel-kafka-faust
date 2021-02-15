"""Simple Hello World Example

Source: https://faust.readthedocs.io/en/latest/playbooks/quickstart.html
Faust CLI: faust -A playbooks.hello_world worker -l info
Faust Send: faust -A playbooks.hello_world send greetings "Hello Kafka topic"
"""

# %%
import faust
from .config import broker, broker_credentials, replicas
# %%
app = faust.App(
    'hello-world',
    broker=broker,
    broker_credentials=broker_credentials,
    value_serializer='raw',
)
greetings_topic = app.topic('greetings', replicas=replicas)
# %%
@app.agent(greetings_topic)
async def greet(greetings):
    async for greeting in greetings:
        print(greeting)
# %%
