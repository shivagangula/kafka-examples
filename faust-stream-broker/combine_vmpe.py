
import faust
import requests

app = faust.App(
    'consortium_faust_app2',
    broker='kafka://localhost:9092',
    topic_partitions=10,
)

## update stream
class VMEPResponse(faust.Record):
    transaction_id: str
    score: str

vmep_response_topic = app.topic('vmep_response_topic', value_type=VMEPResponse, partitions = 20)
consortium_scores = app.Table('consortium_scores', default=int)

@app.agent(vmep_response_topic, concurrency=1 )
async def combine_vmeps_data(messages_data):
    async for messages in messages_data.group_by(VMEPResponse.transaction_id):
        consortium_scores[messages.transaction_id] =+ messages.score 
        print (consortium_scores[messages.transaction_id] )
        print ( messages.score )