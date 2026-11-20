"""Corpus curado, versionado y separado de las cifras personales Gold."""
import hashlib
import json
from pathlib import Path
from urllib.parse import urlsplit
from psycopg2.extras import Json,execute_values,RealDictCursor
from src.common.config import ROOT,db_connect


def load_file_corpus(folder=None):
    folder=Path(folder or ROOT/'knowledge').resolve()
    catalog=json.loads((folder/'catalog.json').read_text())
    documents=[];chunks=[];seen=set()
    for spec in catalog['documents']:
        document_id=spec['document_id']
        if document_id in seen:
            raise ValueError('Identificador de conocimiento duplicado')
        seen.add(document_id)
        source=(folder/spec['file']).resolve()
        if not source.is_relative_to(folder) or source.suffix!='.md':
            raise ValueError('La referencia debe pertenecer al corpus de conocimiento')
        url=spec.get('source_url')
        if url:
            parsed=urlsplit(url)
            host=parsed.hostname or ''
            if parsed.scheme!='https' or not any(host==domain or host.endswith('.'+domain) for domain in ['condusef.gob.mx','banxico.org.mx','gob.mx']):
                raise ValueError('Fuente externa fuera del catálogo curado')
        content=source.read_text()
        if not content.strip() or len(content)>20000:
            raise ValueError('Documento de conocimiento vacío o excesivo')
        digest=hashlib.sha256(content.encode()).hexdigest()
        document={**spec,'content_hash':digest}
        documents.append(document)
        heading=spec['title'];paragraphs=[];sequence=0
        def flush():
            nonlocal sequence
            if not paragraphs:return
            sequence+=1
            body='\n\n'.join(paragraphs)
            chunks.append({'chunk_id':f'{document_id}:{sequence:02d}','document_id':document_id,'title':spec['title'],
                           'heading':heading,'body':body,'source_url':url,'publisher':spec['publisher'],
                           'reviewed_at':spec['reviewed_at'],'content_hash':hashlib.sha256(body.encode()).hexdigest()})
            paragraphs.clear()
        for paragraph in content.split('\n\n'):
            if paragraph.startswith('#'):
                lines=paragraph.splitlines()
                flush();heading=lines[0].lstrip('# ').strip()
                paragraph='\n'.join(lines[1:]).strip()
            if not paragraph.strip():continue
            if sum(map(len,paragraphs))+len(paragraph)>1200:flush()
            paragraphs.append(paragraph.strip())
        flush()
    if not chunks:raise ValueError('El corpus no contiene fragmentos')
    canonical=json.dumps({'documents':documents,'chunks':chunks},sort_keys=True,ensure_ascii=False)
    version=hashlib.sha256(canonical.encode()).hexdigest()
    return {'version':version,'documents':documents,'chunks':chunks}


def publish_knowledge():
    corpus=load_file_corpus()
    version=corpus['version']
    with db_connect() as conn,conn.cursor() as q:
        q.execute('SELECT pg_advisory_xact_lock(712032)')
        q.execute('INSERT INTO knowledge.corpora(version,document_count,chunk_count) VALUES (%s,%s,%s) ON CONFLICT DO NOTHING',
                  (version,len(corpus['documents']),len(corpus['chunks'])))
        execute_values(q,'INSERT INTO knowledge.documents(corpus_version,document_id,title,source_url,publisher,reviewed_at,content_hash) VALUES %s ON CONFLICT DO NOTHING',
                       [(version,d['document_id'],d['title'],d['source_url'],d['publisher'],d['reviewed_at'],d['content_hash']) for d in corpus['documents']])
        execute_values(q,'INSERT INTO knowledge.chunks(corpus_version,chunk_id,document_id,heading,body,content_hash) VALUES %s ON CONFLICT DO NOTHING',
                       [(version,c['chunk_id'],c['document_id'],c['heading'],c['body'],c['content_hash']) for c in corpus['chunks']])
        q.execute('INSERT INTO knowledge.active_corpus(singleton,version) VALUES (true,%s) ON CONFLICT(singleton) DO UPDATE SET version=excluded.version',(version,))
    return {'version':version,'documents':len(corpus['documents']),'chunks':len(corpus['chunks'])}


def read_knowledge():
    with db_connect(reader=True) as conn,conn.cursor(cursor_factory=RealDictCursor) as q:
        q.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ')
        q.execute('SELECT version FROM knowledge.active_corpus')
        row=q.fetchone()
        if not row:raise ValueError('No hay un corpus publicado')
        version=row['version']
        q.execute('SELECT c.chunk_id,c.document_id,c.heading,c.body,c.content_hash,d.title,d.source_url,d.publisher,d.reviewed_at '
                  'FROM knowledge.chunks c JOIN knowledge.documents d USING(corpus_version,document_id) '
                  'WHERE c.corpus_version=%s ORDER BY c.chunk_id',(version,))
        chunks=[dict(item) for item in q.fetchall()]
    return {'version':version,'chunks':chunks}
