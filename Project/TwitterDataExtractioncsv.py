import tweepy

# authorization tokens
access_token = "1045756888510541824-1zbS5Pi4hL6pYUwHuO41pwNcK4CNvm"
access_token_secret = "yer1Vmp2te61Wat52olEpmptjYqLumSZ3ry9XpyKTJ7oH"
consumer_key = "WAlQxKnCMdIFRsx1NyS7bdAEV"
consumer_secret = "cJNYNLYHhb0McLJaQ3sN9uWiAR6DmFDhzmN6CXNUCCIPOezM18"

# StreamListener class inherits from tweepy.StreamListener and overrides on_status/on_error methods.
class StreamListener(tweepy.StreamListener):
    def on_status(self, status):
        print(status.id_str)
        # if "retweeted_status" attribute exists, flag this tweet as a retweet.
        is_retweet = hasattr(status, "retweeted_status")

        # check if text has been truncated
        if hasattr(status,"extended_tweet"):
            text = status.extended_tweet["full_text"]
        else:
            text = status.text

        # check if this is a quote tweet.
        is_quote = hasattr(status, "quoted_status")
        quoted_text = ""
        if is_quote:
            # check if quoted tweet's text has been truncated before recording it
            if hasattr(status.quoted_status,"extended_tweet"):
                quoted_text = status.quoted_status.extended_tweet["full_text"]
            else:
                quoted_text = status.quoted_status.text

        # remove characters that might cause problems with csv encoding
        remove_characters = [",","\n"]
        for c in remove_characters:
            text.replace(c," ")
            quoted_text.replace(c, " ")

        with open("/home/lohitha/Desktop/BigDataProgramming/COVID19_Data2.csv", "a", encoding='utf-8') as f:
            f.write("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n" % (status.id_str,status.created_at,status.user.screen_name,status.user.location,status.user.verified,status.user.followers_count,status.user.lang,is_retweet,is_quote,text,quoted_text))

    def on_error(self, status_code):
        print("Encountered streaming error (", status_code, ")")
        sys.exit()

if __name__ == "__main__":
    # complete authorization and initialize API endpoint
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth)

    # initialize stream
    streamListener = StreamListener()
    stream = tweepy.Stream(auth=api.auth, listener=streamListener,tweet_mode='extended')
    with open("/home/lohitha/Desktop/BigDataProgramming/COVID19_Data2.csv", "w", encoding='utf-8') as f:
        f.write("tweet_id,created_at,user,location,verified,followers_count,language,is_retweet,is_quote,text,quoted_text\n")
    tags = ["covid19","covid-19","COVID-19","COVID19","corona","CORONA"]
    stream.filter(track=tags)

