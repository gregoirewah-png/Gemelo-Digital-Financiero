"""Recuperación reproducible, rechazo de rutas externas y prioridad de cifras verificadas."""
import json
from types import SimpleNamespace
import pytest
from src.analytics.knowledge_store import load_file_corpus
from src.ml.knowledge_rag import KnowledgeIndex
import src.ml.personal_agent as service
from tests.test_personal_agent import snapshot, agent  # fixtures con perfil ficticio


def test_retrieval_relevant_and_abstains_outside_corpus():
    corpus = load_file_corpus()
    index = KnowledgeIndex(corpus)
    cases = [('¿Qué es el CAT?', 'cat'), ('¿Qué es un fondo de emergencia?', 'emergencia'),
             ('¿Qué significa pagar el mínimo de mi tarjeta?', 'pago_minimo')]
    for question, document in cases:
        result = index.retrieve(question)
        assert result and result[0]['document_id'] == document
        assert result[0]['corpus_version'] == corpus['version']
        assert len({r['document_id'] for r in result}) == len(result)
    assert not index.retrieve('astronomía galaxias supernovas')
    assert not index.retrieve(' ')
    assert load_file_corpus()['version'] == corpus['version']


def test_corpus_rejects_escape_and_untrusted_source(tmp_path):
    (tmp_path/'ok.md').write_text('# Tema\n\nReferencia educativa.')
    spec = {'document_id':'a', 'file':'../fuera.md', 'title':'A', 'publisher':'Guía',
            'source_url':None, 'reviewed_at':'2026-11-19'}
    catalog = tmp_path/'catalog.json'
    catalog.write_text(json.dumps({'documents':[spec]}))
    with pytest.raises(ValueError, match='pertenecer'):
        load_file_corpus(tmp_path)
    spec.update(file='ok.md', source_url='https://condusef.gob.mx.example.org/engaño')
    catalog.write_text(json.dumps({'documents':[spec]}))
    with pytest.raises(ValueError, match='fuera'):
        load_file_corpus(tmp_path)
    spec['source_url'] = None
    catalog.write_text(json.dumps({'documents':[spec]}))
    before = load_file_corpus(tmp_path)['version']
    (tmp_path/'ok.md').write_text('# Tema\n\nReferencia educativa revisada.')
    assert load_file_corpus(tmp_path)['version'] != before


def test_rag_offline_has_visible_source_and_keeps_profile_evidence(agent, monkeypatch):
    index = KnowledgeIndex(load_file_corpus())
    monkeypatch.setattr(service, 'retrieve_knowledge', index.retrieve)
    answer = agent.generar_respuesta('¿Qué es el CAT?')
    assert 'no está disponible' in answer
    assert agent.last_result['kind'] == 'knowledge'
    assert agent.last_result['sources'][0]['document_id'] == 'cat'
    assert agent.last_evidence[0]['kind'] == 'personal_profile'
    assert any(e['kind'] == 'knowledge' and e['content_hash'] for e in agent.last_evidence)


def test_generated_response_receives_corpus_separate_from_verified_gold(agent, monkeypatch):
    index = KnowledgeIndex(load_file_corpus())
    monkeypatch.setattr(service, 'retrieve_knowledge', index.retrieve)
    def create(**kwargs):
        system = kwargs['messages'][0]['content']
        assert 'DATOS VERIFICADOS' in system and 'REFERENCIAS RECUPERADAS (SOLO CONSULTA)' in system
        assert '9000' in system and 'Nunca sustituyen las cifras' in system
        assert 'banxico.org.mx' in system
        return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content='Una explicación sencilla.'))])
    agent.client = SimpleNamespace(chat=SimpleNamespace(completions=SimpleNamespace(create=create)))
    assert agent.generar_respuesta('¿Qué es el CAT?') == 'Una explicación sencilla.'


def test_rag_does_not_publish_fabricated_interest_examples(agent, monkeypatch):
    index=KnowledgeIndex(load_file_corpus())
    monkeypatch.setattr(service,'retrieve_knowledge',index.retrieve)
    def create(**kwargs):
        return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content='Compara un crédito al 12% con otro al 10% y comisión de 2%.'))])
    agent.client=SimpleNamespace(chat=SimpleNamespace(completions=SimpleNamespace(create=create)))
    answer=agent.generar_respuesta('¿Qué es el CAT?')
    assert '12%' not in answer and 'guía' in answer
    assert len(agent.last_result['sources'])==1
