import pickle

from reddit_api import Reddit_API
api_keys = pickle.load()

def get_post_exposure(name, key_fp = 'plugins/monkey.pkl'):
    api_keys = pickle.load(open(key_fp))
    reddit = Reddit_API()
    reddit.connect_reddit(pickle_name = 'monkey')