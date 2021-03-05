import pickle

# https://stackoverflow.com/questions/41923674/how-to-identify-if-a-byte-string-is-a-pickled-object
def is_pickle_stream(stream):
    try:
        pickle.loads(stream)
        return True
    except:
        return False
