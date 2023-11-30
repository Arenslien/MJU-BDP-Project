# Library 
import pandas as pd
import numpy as np
from nltk.corpus import stopwords
import nltk
nltk.download('stopwords')
import re
from sklearn.feature_extraction.text import TfidfVectorizer 
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.metrics.pairwise import euclidean_distances

from keras.preprocessing.text import Tokenizer
from keras.preprocessing.sequence import pad_sequences

# Sample corpus
documents = [
    'Machine learning is the study of computer algorithms that improve automatically through experience.\
    Machine learning algorithms build a mathematical model based on sample data, known as training data.\
    The discipline of machine learning employs various approaches to teach computers to accomplish tasks \
    where no fully satisfactory algorithm is available.',
    'Machine learning is closely related to computational statistics, which focuses on making predictions using computers.\
    The study of mathematical optimization delivers methods, theory and application domains to the field of machine learning.',
    'Machine learning involves computers discovering how they can perform tasks without being explicitly programmed to do so. \
    It involves computers learning from data provided so that they carry out certain tasks.',
    'Machine learning approaches are traditionally divided into three broad categories, depending on the nature of the "signal"\
    or "feedback" available to the learning system: Supervised, Unsupervised and Reinforcement',
    'Software engineering is the systematic application of engineering approaches to the development of software.\
    Software engineering is a computing discipline.',
    'A software engineer creates programs based on logic for the computer to execute. A software engineer has to be more concerned\
    about the correctness of the program in all the cases. Meanwhile, a data scientist is comfortable with uncertainty and variability.\
    Developing a machine learning application is more iterative and explorative process than software engineering.'
]

documents_df=pd.DataFrame(documents,columns=["documents"])

# removing special characters and stop words from the text
stop_words_l=stopwords.words('english')
documents_df['documents_cleaned']=documents_df["documents"].apply(lambda x: " ".join(re.sub(r'[^a-zA-Z]',' ',w).lower() for w in x.split() if re.sub(r'[^a-zA-Z]',' ',w).lower() not in stop_words_l))

# TF-IDF VECTORISER 설정
tfidfvectoriser=TfidfVectorizer()
# print(documents_df.documents_cleaned)
tfidfvectoriser.fit(documents_df.documents_cleaned)
# for value in documents_df.documents_cleaned:
#     print(len(value))
#     print(value)
tfidf_vectors=tfidfvectoriser.transform(documents_df.documents_cleaned)
print(tfidf_vectors)
print(type(tfidf_vectors[0, 0]))

# 토크나이저 설정
tokenizer = Tokenizer()
tokenizer.fit_on_texts(documents_df.documents_cleaned)
tokenized_documents = tokenizer.texts_to_sequences(documents_df.documents_cleaned)
tokenized_paded_documents = pad_sequences(tokenized_documents,maxlen=64,padding='post')
vocab_size = len(tokenizer.word_index)+1

# reading Glove word embeddings into a dictionary with "word" as key and values as word vectors
embedding_dict = dict()

print()

with open('./glove.840B.300d.txt') as file:
    for line in file:
        word_vector = line.split()
        word = word_vector[0]
        try:
            word_vector_arr = np.asarray(word_vector[1:], dtype='float32')
            embedding_dict[word] = word_vector_arr
        except (ValueError):
            print(word_vector)
            
print(f"embedding dict size: {len(embedding_dict)}")

# creating embedding matrix, every row is a vector representation from the vocabulary indexed by the tokenizer index.
# Abstract의 tokenized 값 * 300 사이즈
embedding_matrix=np.zeros((vocab_size,300)) # GLVOE 차원 맞게 100 --> 300으로 수정

for word,i in tokenizer.word_index.items():
    # print(word) # doucment의 모든 word
    embedding_vector = embedding_dict.get(word)
    if embedding_vector is not None:
        embedding_matrix[i] = embedding_vector # 벡터 행 쌓기
        
# calculating average of word vectors of a document weighted by tf-idf
document_embeddings=np.zeros((len(tokenized_paded_documents),300)) # GLOVE 차원 맞게 100 --> 300으로 수정
words=tfidfvectoriser.get_feature_names_out()

# instead of creating document-word embeddings, directly creating document embeddings
for i in range(documents_df.shape[0]):
    for j in range(len(words)):
        document_embeddings[i] += embedding_matrix[tokenizer.word_index[words[j]]] * tfidf_vectors[i, j]

pairwise_similarities=cosine_similarity(document_embeddings)
pairwise_differences=euclidean_distances(document_embeddings)
print(pairwise_similarities)
print(pairwise_differences)