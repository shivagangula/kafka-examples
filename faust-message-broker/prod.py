# import faust
# import requests



# URL = "https://random-data-api.com/api/v2/beers"


# # app = faust.App('faust-app', broker='kafka://localhost')
# # topic = app.topic('hello-topic')
# # @app.agent(topic)
# # async def call_api(urls):
# #     async for url in urls:
# #         print ( requests.get(url).json().get("brand") )


# # @app.timer(interval=1.0)
# # async def example_sender(app):
# #     await call_api.send(
# #         value=URL,
# #     )

# # if __name__ == '__main__':
# #     app.main()


# app = faust.App('stream-example')

# @app.agent()
# async def myagent(stream):
#     """Example agent."""
#     async for value in stream:
#         print(f'MYAGENT RECEIVED -- {value!r}')
#         yield value

# if __name__ == '__main__':
#     app.main()

import faust
app= faust.App('agents-demo')
greetings_topic = app.topic('greetings', value_type=str, value_serializer='raw')
# Producer
@app.timer(interval=1.0)
async def send_greeting():
    await greetings_topic.send(value='Hello, World!')