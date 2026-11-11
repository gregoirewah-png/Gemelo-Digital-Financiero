from types import SimpleNamespace
import pytest
import src.ml.agent_service as service


def test_agent_reads_gold_before_prompt(monkeypatch):
    observed=[]
    def query(*args,**kwargs):
        observed.append(args)
        return {'publication':{'run_id':'verified_run'},'rows':[{'months':60,'income':12345}]}
    monkeypatch.setattr(service,'query_gold',query)
    agent=service.AgenteFinancieroGroq()
    def create(**kwargs):
        assert 'verified_run' in kwargs['messages'][0]['content']
        assert '12345' in kwargs['messages'][0]['content']
        return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(tool_calls=None,content='Respuesta con Gold'))])
    agent.client=SimpleNamespace(chat=SimpleNamespace(completions=SimpleNamespace(create=create)))
    assert agent.generar_respuesta('¿Mis ingresos?')=='Respuesta con Gold'
    assert len(observed)==2


def test_unavailable_gold_is_reported(monkeypatch):
    def broken(*args,**kwargs):
        raise ValueError('No hay Gold publicado')
    monkeypatch.setattr(service,'query_gold',broken)
    assert 'No hay Gold publicado' in service.AgenteFinancieroGroq().generar_respuesta('¿Saldo?')


def test_invalid_dataset_cannot_become_sql():
    with pytest.raises(ValueError):
        service.query_gold('dataset1; DROP TABLE gold.monthly_cashflow')
