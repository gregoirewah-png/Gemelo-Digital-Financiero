from pathlib import Path
from streamlit.testing.v1 import AppTest
ROOT=Path(__file__).resolve().parents[1]


def test_user_view_contains_only_chat_controls():
    app=AppTest.from_file(str(ROOT/'src/dashboard/app.py')).run(timeout=30)
    assert not app.exception
    assert len(app.chat_input)==1
    assert not app.selectbox and not app.number_input


def test_operations_login():
    app=AppTest.from_file(str(ROOT/'src/dashboard/operations.py')).run(timeout=30)
    assert not app.exception
    assert len(app.text_input)==2
    assert not app.tabs


def test_authenticated_operations_view():
    app=AppTest.from_file(str(ROOT/'src/dashboard/operations.py'))
    app.session_state['operations_authenticated']=True
    app.run(timeout=30)
    assert not app.exception
    assert len(app.tabs)==6


def test_login_handles_unicode_and_accepts_configured_credentials():
    import os
    app=AppTest.from_file(str(ROOT/'src/dashboard/operations.py')).run(timeout=30)
    app.text_input[0].set_value('José')
    app.text_input[1].set_value('contraseña')
    app.button[0].click().run(timeout=30)
    assert not app.exception and app.error
    app.text_input[0].set_value(os.environ['OPERATIONS_USER'])
    app.text_input[1].set_value(os.environ['OPERATIONS_PASSWORD'])
    app.button[0].click().run(timeout=30)
    assert not app.exception
    assert app.session_state['operations_authenticated'] is True


def test_streamlit_theme_has_readable_text_on_both_surfaces():
    import tomllib
    import streamlit as st

    theme = tomllib.loads((ROOT/'.streamlit/config.toml').read_text())['theme']
    assert st.get_option('theme.base') == 'light'
    assert st.get_option('theme.textColor') == theme['textColor']

    def luminance(color):
        channels = [int(color[index:index+2],16)/255 for index in (1,3,5)]
        linear = [value/12.92 if value <= .04045 else ((value+.055)/1.055)**2.4
                  for value in channels]
        return sum(value*weight for value,weight in zip(linear,(.2126,.7152,.0722)))

    text = luminance(theme['textColor'])
    for name in ['backgroundColor','secondaryBackgroundColor']:
        background = luminance(theme[name])
        contrast = (max(text,background)+.05)/(min(text,background)+.05)
        assert contrast >= 4.5


def test_user_expense_shortcut_returns_chart_without_external_ai():
    app=AppTest.from_file(str(ROOT/'src/dashboard/app.py')).run(timeout=30)
    app.button[3].click().run(timeout=30)
    assert not app.exception
    answer=app.session_state['messages'][-1]
    assert 'tres gastos principales' in answer['content']
    assert answer['result']['kind']=='categories'
    assert app.get('plotly_chart')
