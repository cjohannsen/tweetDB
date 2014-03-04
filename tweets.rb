require 'tweetstream'
require 'mongo'

#include Mongo classes
include Mongo

#variables
words = "mapr, hadoop, bigdata"
max_tweets = 20

#Tweetstream configuration
TweetStream.configure do |config|
  config.consumer_key       = 'consumer_key'
  config.consumer_secret    = 'consumer_secret'
  config.oauth_token        = 'oauth_token'
  config.oauth_token_secret = 'oauth_token_secret'
  config.auth_method        = :oauth
end

#MongoDB configuration
db = MongoClient.new("localhost", 27017).db("tweetsDB")
tweets = db.collection("tweets")

reduce = "function (key, score) {   var sum = 0;   score.forEach(function(doc){ sum += 1; });   return { count:sum }; }"
map = "function () {   emit(this.user.screen_name, { count:1 }); }"

client = TweetStream::Client.new.track(words) do |status|
  #puts "[#{status.user.screen_name}] #{status.text}"
  data = status.to_h
  tweets.insert(data)
  #mapreduce job
  results = tweets.map_reduce(map, reduce, { out: "results" })
  #display most active users
  user = results.find({"$where" => "this.value.count > #{max_tweets}"}).to_a
  user.each { |x| 
      puts x["_id"] 
      puts x["value"]["count"]
      }
end
