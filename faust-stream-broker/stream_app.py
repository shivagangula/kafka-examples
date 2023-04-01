# commands :
# . kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic consortium_topic --from-beginning
# faust -A stream_app send consortium_topic '{"transaction_id": "vict896", "valid_vmep_url":"https://retoolapi.dev/UCxDJm/test?id=1"}'
# faust -A stream_app  worker -l info --web-port=6068
# . kafka-topics.sh --zookeeper localhost:2181 --delete --topic consortium_topic

import faust
import requests
from . import combine_vmeps_data

app = faust.App(
    'consortium_faust_app',
    broker='kafka://localhost:9092',
    topic_partitions=10,
)

## ------------------------ CALL VMEPs ------------------------------------
class VMEPData(faust.Record):
    transaction_id: str
    valid_vmep_url: str

consortium_topic = app.topic('consortium_topic', value_type=VMEPData)

async def get_score(res):
    try:
        data = await res.json()
        score = data[0].get("score")
        return score
    except Exception as e:
        return 0

@app.agent(consortium_topic, concurrency= 1000 )
async def call_vmep(messages_data):
    async for msg in messages_data:
        vmep_url = msg.valid_vmep_url
        async with app.http_client.get(vmep_url) as response:
            score = await get_score( response )
            print ( score )
            await combine_vmeps_data.cast(VMEPResponse(score = score, transaction_id= msg.transaction_id) )




             
