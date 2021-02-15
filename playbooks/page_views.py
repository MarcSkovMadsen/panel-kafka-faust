"""Simple Page Views Example

Source: https://faust.readthedocs.io/en/latest/playbooks/pageviews.html
Faust CLI: faust -A playbooks.page_views worker -l info
Faust Send: faust -A page_views send page_views '{"id": "foo", "user": "bar"}'
"""
# faust -A page_views worker -l info
import faust
from .config import broker, broker_credentials, replicas

app = faust.App(
    'page_views',
    broker=broker,
    broker_credentials=broker_credentials,
    topic_partitions=4,
)

class PageView(faust.Record):
    id: str
    user: str

page_view_topic = app.topic('page_views', value_type=PageView, replicas=replicas)
page_views = app.Table('page_views', default=int)

@app.agent(page_view_topic)
async def count_page_views(views):
    async for view in views.group_by(PageView.id):
        page_views[view.id] += 1