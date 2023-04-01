import faust

app= faust.App('agents-demo')
greetings_topic = app.topic('greetings', value_type=str, value_serializer='raw')

@app.agent(greetings_topic)
async def greet(stream):
    async for greeting in stream:
        print(greeting)
if __name__ == '__main__':
    app.main()