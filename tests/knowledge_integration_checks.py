"""Valida corpus SQL, base derivada y ausencia de cambios en Gold. Sin Groq."""
import hashlib
import json
from psycopg2 import errors
from src.analytics.knowledge_store import publish_knowledge, read_knowledge
from src.analytics.executive_metrics import executive_snapshot, business_findings
from src.common.config import db_connect
from src.ml.agent_service import query_gold
from src.ml.knowledge_rag import retrieve_knowledge


def gold_digest():
    with db_connect(reader=True) as c,c.cursor() as q:
        rows=[]
        for view,sort in [('publication','run_id'),('monthly_cashflow','period_month'),
                          ('credit_card_anomalies','cust_id'),('personal_profiles','profile_id')]:
            q.execute('SELECT * FROM serving.'+view+' ORDER BY '+sort)
            rows.append(q.fetchall())
    return hashlib.sha256(json.dumps(rows,default=str,sort_keys=True).encode()).hexdigest()


def check():
    before=gold_digest()
    first=publish_knowledge();second=publish_knowledge()
    assert first==second
    read=read_knowledge()
    assert read['version']==first['version'] and len(read['chunks'])==first['chunks']
    hits=retrieve_knowledge('¿Qué es un fondo de emergencia?')
    assert hits[0]['document_id']=='emergencia' and hits[0]['corpus_version']==first['version']
    with db_connect(reader=True) as c,c.cursor() as q:
        q.execute('SELECT COUNT(*) FROM serving.credit_cards');cards=q.fetchone()[0]
        q.execute('SELECT COUNT(*),COUNT(DISTINCT cust_id),COUNT(isolation_score),COUNT(*) FILTER(WHERE is_anomaly) FROM serving.credit_card_anomalies')
        count,unique,scores,flags=q.fetchone()
        assert count==unique==cards and count>0
        q.execute('SELECT COUNT(*) FROM serving.credit_card_anomalies a FULL JOIN serving.credit_cards c USING(source_run_id,cust_id) WHERE a.cust_id IS NULL OR c.cust_id IS NULL')
        assert q.fetchone()[0]==0
        q.execute('SELECT COUNT(*) FROM serving.credit_card_anomalies WHERE isolation_score IS NOT NULL AND abs(anomaly_score+isolation_score)>1e-10')
        assert q.fetchone()[0]==0
        q.execute('SELECT COUNT(*) FROM knowledge.corpora WHERE version=%s',(first['version'],))
        assert q.fetchone()[0]==1
    try:
        with db_connect(reader=True) as c,c.cursor() as q:
            q.execute("UPDATE knowledge.active_corpus SET singleton=true")
        raise AssertionError('El lector pudo escribir conocimiento')
    except (errors.InsufficientPrivilege,errors.ReadOnlySqlTransaction):pass
    assert query_gold('dataset2','anomalies')['rows']
    snapshot=executive_snapshot();findings=business_findings(snapshot)
    assert findings['customers']==cards and findings['anomalies']==flags
    assert gold_digest()==before
    print(json.dumps({'knowledge':first,'IF_rows':count,'IF_scores':scores,'IF_flags':flags,
                      'gold_unchanged':True,'reader_is_read_only':True,'executive_snapshot_consistent':True},ensure_ascii=False))


if __name__=='__main__':check()
