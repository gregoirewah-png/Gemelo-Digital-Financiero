"""Recuperación local TF-IDF. La generación opcional ocurre después en Groq."""
import logging
import os
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from src.analytics.knowledge_store import load_file_corpus,read_knowledge

LOG=logging.getLogger(__name__)
STOPWORDS='a al algo algunas algunos ante antes como con contra cual cuando de del desde donde el ella ellas ellos en entre era eres es esa esas ese esos esta estas este esto estos fue ha hay la las le les lo los me mi mis muy no nos o para pero por porque que quien se ser si sin sobre son su sus te tiene tu tus un una unas uno unos y ya yo'.split()


class KnowledgeIndex:
    def __init__(self,corpus):
        self.version=corpus['version'];self.chunks=corpus['chunks']
        texts=[c['title']+' '+c['heading']+' '+c['body'] for c in self.chunks]
        self.vectorizer=TfidfVectorizer(strip_accents='unicode',lowercase=True,ngram_range=(1,2),stop_words=STOPWORDS,sublinear_tf=True)
        self.matrix=self.vectorizer.fit_transform(texts)

    def retrieve(self,question,limit=3,min_score=.10):
        if not question or not question.strip():return []
        vector=self.vectorizer.transform([question[:4000]])
        if not vector.nnz:return []
        scores=cosine_similarity(vector,self.matrix).ravel()
        indexes=sorted(range(len(scores)),key=lambda i:(-scores[i],self.chunks[i]['chunk_id']))
        results=[];documents=set()
        for i in indexes:
            if scores[i]<min_score:break
            chunk=self.chunks[i]
            if chunk['document_id'] in documents:continue
            results.append({**chunk,'score':round(float(scores[i]),6),'corpus_version':self.version})
            documents.add(chunk['document_id'])
            if len(results)>=limit:break
        return results


_cache={}


def retrieve_knowledge(question):
    if os.getenv('RAG_ENABLED','true').lower() not in ['true','1','yes']:return []
    try:corpus=read_knowledge()
    except Exception:
        LOG.debug('Corpus SQL no disponible; se usa el corpus curado de la imagen')
        corpus=load_file_corpus()
    version=corpus['version']
    if version not in _cache:
        _cache.clear();_cache[version]=KnowledgeIndex(corpus)
    limit=min(max(int(os.getenv('RAG_TOP_K','3')),1),4)
    threshold=min(max(float(os.getenv('RAG_MIN_SCORE','.10')),0.01),1.)
    return _cache[version].retrieve(question,limit,threshold)
