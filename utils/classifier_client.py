import jsonrpclib

URL = "http://localhost:6060"

client = jsonrpclib.Server(URL)

# TODO: extend the classfier to accept json input
# and extract feature here
def classify(text):
    topic = client.classify(text)
    print('{} -> {}'.format(text, topic))
    return topic

if __name__ == '__main__':
    tests = [
        "Trump's approval rating is highest in 11 months",
        "NFL cheerleader files complaint alleging gender discrimination",
        "US government is officially investigating Facebook",
        "Treat your child to these fun Easter Basket gift ideas",
        "Kevin Smith has lost more weight since heart attack"
    ]

    for title in tests:
        classify(title)