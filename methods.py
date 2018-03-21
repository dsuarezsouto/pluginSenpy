from sklearn.base import BaseEstimator, TransformerMixin
from nltk.tokenize import sent_tokenize, word_tokenize
from nltk.stem import PorterStemmer
from sklearn.base import BaseEstimator, TransformerMixin
from nltk import pos_tag
from collections import Counter 

from sklearn.pipeline import Pipeline, FeatureUnion
from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer, TfidfTransformer
from sklearn.feature_extraction import DictVectorizer
from sklearn.preprocessing import FunctionTransformer
from sklearn.decomposition import NMF, LatentDirichletAllocation

from sklearn.linear_model import LogisticRegression
from nltk.tokenize import TweetTokenizer
from nltk.corpus import stopwords
import re
import string
import pandas as pd

def custom_tokenizerClassifier(words):
    """Preprocessing tokens as seen in the lexical notebook"""
    
    emoji_pattern = re.compile("["
        u"\U0001F600-\U0001F64F"  # emoticons
        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
        u"\U0001F680-\U0001F6FF"  # transport & map symbols
        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                           "]+", flags=re.UNICODE)
    urls = re.compile(r'.http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+')
    ht = re.compile(r'http.')
    bar = re.compile(r'//*')
    pr = ["rt","@","http","https","'s",'...', 'english', 'translation','):', '. .', '..']
    #tknzr = TweetTokenizer(strip_handles=True, reduce_len=True)
    tknzr = TweetTokenizer(strip_handles=False, reduce_len=True)
    #tokens = word_tokenize(words.lower())
    tokens = tknzr.tokenize(words.lower())
    porter = PorterStemmer()
    lemmas = [porter.stem(t) for t in tokens]
    # Clean stop-words
    stoplist = stopwords.words('spanish')
    lemmas_clean = [w for w in lemmas if w not in stoplist]
    # Clean punctuation
    punctuation = set(string.punctuation)
    lemmas_punct = [w for w in lemmas_clean if  w not in punctuation]
    # Clean emojis,urls,bars,etc
    lemmas_clean = [w for w in lemmas_punct if not w.startswith('@') if w not in pr 
            if not bar.search(w) if not ht.search(w)
            if not w.isdigit()]
    
    return lemmas_clean
def custom_tokenizerThemes(words):
    """Preprocessing tokens as seen in the lexical notebook"""
    
    emoji_pattern = re.compile("["
        u"\U0001F600-\U0001F64F"  # emoticons
        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
        u"\U0001F680-\U0001F6FF"  # transport & map symbols
        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                           "]+", flags=re.UNICODE)
    urls = re.compile(r'.http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+')
    ht = re.compile(r'http.')
    bar = re.compile(r'//*')
    pr = ["rt","@","http","https","'s",'...', 'english', 'translation','):', '. .', '..']
    #tknzr = TweetTokenizer(strip_handles=True, reduce_len=True)
    tknzr = TweetTokenizer(strip_handles=False, reduce_len=True)
    #tokens = word_tokenize(words.lower())
    tokens = tknzr.tokenize(words.lower())
    porter = PorterStemmer()
    lemmas = [porter.stem(t) for t in tokens]
    # Clean stop-words
    stoplist = stopwords.words('spanish')
    lemmas_clean = [w for w in lemmas if w not in stoplist]
    # Clean punctuation
    punctuation = set(string.punctuation)
    lemmas_punct = [w for w in lemmas_clean if w=='?' or w not in punctuation]
    # Clean emojis,urls,bars,etc
    lemmas_clean = [w for w in lemmas_punct if w!="insomnio" if not w.startswith('@') if w not in pr 
            if not bar.search(w) if not ht.search(w)
            if not w.isdigit()]
    
    return lemmas_clean
class LexicalStats (BaseEstimator, TransformerMixin):
    """Extract lexical features from each document"""
    
    def number_sentences(self, doc):
        sentences = sent_tokenize(doc, language='spanish')
        return len(sentences)

    def fit(self, x, y=None):
        return self

    def transform(self, docs):
        return [{'length': len(doc),
                 'num_sentences': self.number_sentences(doc)}
                for doc in docs]


class PosStats(BaseEstimator, TransformerMixin):
    """Obtain number of tokens with POS categories"""

    def stats(self, doc):
        tokens = custom_tokenizerClassifier(doc)
        tagged = pos_tag(doc, tagset='universal')
        counts = Counter(tag for word,tag in tagged)
        total = sum(counts.values())
        #copy tags so that we return always the same number of features
        pos_features = {'NOUN': 0, 'ADJ': 0, 'VERB': 0, 'ADV': 0, 'CONJ': 0, 
                        'ADP': 0, 'PRON':0, 'NUM': 0}
        
        pos_dic = dict((tag, float(count)/total) for tag,count in counts.items())
        for k in pos_dic:
            if k in pos_features:
                pos_features[k] = pos_dic[k]
        return pos_features
    
    def transform(self, docs, y=None):
        return [self.stats(tweet) for tweet in docs]
    
    def fit(self, docs, y=None):
        """Returns `self` unless something different happens in train and test"""
        return self



class TopicTopWords(BaseEstimator, TransformerMixin):

    def __init__(self,filesWords):
        self.filesWords=filesWords
    
    def fit(self, X, y=None):
        return self
    
    def loadWords(self, file):
        df=pd.read_csv(file, encoding='utf-8', delimiter=",", header=0)
        df=df.set_index("words")
        dic=[]
        dic=dict((word,prob.values[0]) for word,prob in df.iterrows())
    
        return dic
    
    def topWords(self, tweet, resultlist):
    

        topWords_a=self.loadWords(self.filesWords[0])
        topWords_b=self.loadWords(self.filesWords[1])
        topWords_c=self.loadWords(self.filesWords[2])
        topWords_d=self.loadWords(self.filesWords[3])
        topWords_e=self.loadWords(self.filesWords[4])
        #allhtdict = dict((ht, 0) for ht in listallht)
        #sent = tknzrwhu.tokenize(str(tweet))       
        prob_a=0
        prob_b=0
        prob_c=0
        prob_d=0
        prob_e=0
        for term in tweet:
            if term in topWords_a.keys():
                prob_a+=topWords_a.get(term)
            if term in topWords_b.keys():
                prob_b+=topWords_b.get(term)
            if term in topWords_c.keys():
                prob_c+=topWords_c.get(term)
            if term in topWords_d.keys():
                prob_d+=topWords_d.get(term)
            if term in topWords_e.keys():
                prob_e+=topWords_e.get(term)
        theme_dict={"a":prob_a,"b":prob_b,"c":prob_c,"d":prob_d,"e":prob_e}
        #print(tweet)
        #print(theme_dict)
        resultlist.append(theme_dict)
        
        return (resultlist)


    def transform(self, data):
        #print("Entra en hashtags")
        #dataproc = preProcess(data)
        #lista = []
        listaresultado = []
        for tweet in data:
            tweet_processed=custom_tokenizerThemes(tweet)
        #for tweet in dataproc:
        #    lista = self.allHashtags(tweet, lista)
        #for tweet in dataproc:
        #    listaresultado = self.Hashtags(tweet, lista, listaresultado)
            listaresultado=self.topWords(tweet_processed,listaresultado)
        #print(listaresultado)
        return listaresultado