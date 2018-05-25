from senpy.plugins import AnalysisPlugin, SentimentPlugin#, ShelfMixin
from senpy.models import Response, Entry, Sentiment, Results

from sklearn.base import BaseEstimator, TransformerMixin
from nltk.stem import PorterStemmer
from nltk import word_tokenize
from nltk.tokenize import TweetTokenizer
from nltk.corpus import stopwords
import re
import string
import os
import numpy as np
import pandas as pd
from methods import *



class DreamPlugin(AnalysisPlugin):

    '''Plugin to detect users with insomnia'''
    author = "Daniel Suárez Souto"
    version = '1'

    def activate(self, *args, **kwargs):

        self.local_path =  os.path.dirname(os.path.abspath(__file__))
        # Dataset con los tweets de usuarios de insomnio y no insomnio
        self.dataset = self.local_path + self.dataset
        # Dataset con tweets de cada uno de los temas de insomnio
        self.themeCatalogue = self.local_path + self.themeCatalogue

        self.fileWords_a = self.local_path + self.fileWords_a
        self.fileWords_b = self.local_path + self.fileWords_b
        self.fileWords_c = self.local_path + self.fileWords_c
        self.fileWords_d = self.local_path + self.fileWords_d
        self.fileWords_e = self.local_path + self.fileWords_e
        # Array con todos los archivos para pasarselo al objeto TopicTopWords
        filesWords= [self.fileWords_a,self.fileWords_b,self.fileWords_c,self.fileWords_d,self.fileWords_e]


        #Load Dataset Users
        df = pd.read_csv(self.dataset)
        #Load Dataset of Themes
        df_themes=pd.read_csv(self.themeCatalogue)
        # Encode categorical variables
        df.loc[df["Sleep Group"]=="y","Sleep Group"] = 1
        df.loc[df["Sleep Group"]=="n","Sleep Group"] = 0
        df['Sleep Group'] = df['Sleep Group'].astype(np.int64)
        X = df['text'].values
        y = df['Sleep Group'].values

        df_themes.loc[df_themes["Themes"]=="a","Themes"] = 0
        df_themes.loc[df_themes["Themes"]=="b","Themes"] = 1
        df_themes.loc[df_themes["Themes"]=="c","Themes"] = 2
        df_themes.loc[df_themes["Themes"]=="d","Themes"] = 3
        df_themes.loc[df_themes["Themes"]=="e","Themes"] = 4
        df_themes['Themes'] = df_themes['Themes'].astype(np.int64)
        X_themes = df_themes['text'].values
        y_themes = df_themes['Themes'].values

        self._pipelineClassifier = Pipeline([
             ('features', FeatureUnion([
                    ('lexical_stats', Pipeline([
                                ('stats', LexicalStats()),
                                ('vectors', DictVectorizer())
                            ])),
                    ('words', TfidfVectorizer(tokenizer=custom_tokenizerClassifier)),
                    ('ngrams',  Pipeline([
                                ('count_vectorizer',  CountVectorizer(ngram_range = (1, 3), encoding = 'ISO-8859-1', 
                                        tokenizer=custom_tokenizerClassifier)),
                                ('tfidf_transformer', TfidfTransformer())
                            ])),
                    ('pos_stats', Pipeline([ #Assigning a grammatical category
                                ('pos_stats', PosStats()),
                                ('vectors', DictVectorizer())
                            ])),
                    # Topics of the Docs
                    ('lda', Pipeline([ 
                                ('count', CountVectorizer(tokenizer=custom_tokenizerClassifier)),
                                ('lda',  LatentDirichletAllocation(n_topics=4, max_iter=5,
                                                       learning_method='online', 
                                                       learning_offset=50.,
                                                       random_state=0))
                            ])),
                ])),
                # Machine Learning
                ('clf', LogisticRegression(penalty='l1',tol=0.1,C=1,n_jobs=-1))  # classifier
        ])

        self._pipelineClassifier.fit(X,y)

        self._pipelineTheme = Pipeline([
            ('featuresTheme', FeatureUnion([
                    ('lexical_statsTheme', Pipeline([
                                ('statsTheme', LexicalStats()),
                                ('vectorsTheme', DictVectorizer())
                            ])),
                    ('wordsTheme', TfidfVectorizer(tokenizer=custom_tokenizerThemes)),
                    ('ngramsTheme',  Pipeline([
                                ('count_vectorizer',  CountVectorizer(ngram_range = (1, 3), encoding = 'ISO-8859-1', 
                                        tokenizer=custom_tokenizerThemes)),
                                ('tfidf_transformer', TfidfTransformer())
                            ])),
                    ('pos_statsTheme', Pipeline([ #Assigning a grammatical category
                                ('pos_statsTheme', PosStats()),
                                ('vectorsTheme', DictVectorizer())
                            ])),
                    # Topics of the Docs
                    ('label-lda', Pipeline([
                                ('topWords', TopicTopWords(filesWords=filesWords)),
                                ('vect', DictVectorizer())
                            ])),
                ])),
            # Machine Learning
            ('clfTheme',LogisticRegression(penalty='l2',tol=0.01,C=14,n_jobs=-1))  # classifier

        ])

        self._pipelineTheme.fit(X_themes,y_themes)


    def deactivate(self, *args, **kwargs):
        pass

    def analyse_entry(self, entry, params):

        text = entry["nif:isString"]
        
        print(text)

        #wordlist = custom_tokenizer(text)
        #print(text)

        #text = ""

        #for i in wordlist:
        #    text = text + i

        value = self._pipelineClassifier.predict([text])

        prediction = value[0]
        print("PREDICTIOn {}".format(prediction))
        if (prediction == 1):
            is_insomniac = True
            theme= self._pipelineTheme.predict([text])
            theme=theme[0]
            print("THEME: {}".format(theme))
            # Decodificamos el theme
            if theme==0:
                themeText= "Solo el trastorno de sueño"
            elif theme==1:
                themeText= "Trastorno de sueño y causas"
            elif theme==2:
                themeText="Trastorno de sueño y solicita ayuda"
            elif theme==3:
                themeText="Trastorno de sueño y la actividad que hace durante la noche"
            elif theme==4:
                themeText= "Trastorno del sueño e indica la medida que toma para intentar evitarlo"
            else:
                themeText="ERROR AL CATALOGAR EL TEMA"
            entity = {'@id':'Entity0','text':text,'is_insomniac':is_insomniac,'theme':str(theme),'themeText':themeText}
        else:
            is_insomniac = False
            entity = {'@id':'Entity0','text':text,'is_insomniac':is_insomniac}


        #No poner en foaf:predator por que foaf predator no existe como etiqueda dentro de la ontologia foaf
        #entity = {'@id':'Entity0','text':text,'is_insomniac':is_insomniac}

        #,'foaf:accountName':params['input'],'prov:wasGeneratedBy':self.id}

        #entry.entities = []
        #entry.entities.append(entity)        
        #yield entry
        yield entity