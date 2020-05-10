from builtins import BaseException, print, str

from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import socket
import json
import time

# authorization tokens
access_token = "1045756888510541824-1zbS5Pi4hL6pYUwHuO41pwNcK4CNvm"
access_token_secret = "yer1Vmp2te61Wat52olEpmptjYqLumSZ3ry9XpyKTJ7oH"
consumer_key = "WAlQxKnCMdIFRsx1NyS7bdAEV"
consumer_secret = "cJNYNLYHhb0McLJaQ3sN9uWiAR6DmFDhzmN6CXNUCCIPOezM18"

auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

class StreamListener(StreamListener):

    def __init__(self, socket):
        self.socket = socket

    def on_data(self, data):
        try:
            msg = json.loads(data)
            print(msg['text'].encode('utf-8'))
            self.socket.send(msg['text'].encode('utf-8'))
            return True
        except BaseException as e:
            print("Encountered error in on_data: %s" % str(e))
        return True

    def on_error(self, status_code):
        print("Encountered streaming error (", status_code, ")")
        return True

def readData(socket):
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, StreamListener(socket))
    tags = ["layoffs","covid","corona","wfh"]
    stream.filter(track=tags)

if __name__ == "__main__":
    s = socket.socket()
    host = "localhost"
    port = 6000
    s.bind((host, port))
    print("Listening on the port: %s" % str(port))
    s.listen(5)
    socket, addr = s.accept()
    print("Received the request from: " + str(addr))
    time.sleep(5)
    readData(socket)