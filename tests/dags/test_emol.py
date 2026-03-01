import pytest
import pendulum
from airflow.models import DagBag
from airflow.exceptions import AirflowSkipException

# Evitar errores de parseo por variables no definidas en el entorno local
import os
os.environ["AIRFLOW_VAR_MP_TICKET"] = "test_ticket"

@pytest.fixture()
def dagbag():
    return DagBag(dag_folder="src/dags", include_examples=False)

@pytest.fixture()
def emol_dag(dagbag):
    return dagbag.get_dag(dag_id="emol")

def get_python_function(task_id):
    """Helper to extract the original python callable from the emol dag definition"""
    import sys
    import importlib.util
    
    spec = importlib.util.spec_from_file_location("emol_dag_module", "src/dags/emol.py")
    emol_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(emol_module)
    
    import inspect
    
    closure_vars = {}
    
    def trace_calls(frame, event, arg):
        if event == "return" and frame.f_code.co_name == "scrapper":
            closure_vars.update(frame.f_locals)
        return trace_calls

    old_trace = sys.gettrace()
    sys.settrace(trace_calls)
    try:
        emol_module.scrapper()
    finally:
        sys.settrace(old_trace)
        
    func = closure_vars.get(task_id)
    if func and hasattr(func, 'function'):
        return func.function
    return func


def test_no_import_errors(dagbag):
    assert len(dagbag.import_errors) == 0, f"Errors found: {dagbag.import_errors}"

def test_emol_dag_loaded(emol_dag):
    assert emol_dag is not None
    assert len(emol_dag.tasks) == 4
    
    task_ids = [task.task_id for task in emol_dag.tasks]
    assert "extract_news_links" in task_ids
    assert "flatten_links" in task_ids
    assert "extract_news_data" in task_ids
    assert "save_data" in task_ids

# --- Unit Tests for Tasks ---

def test_extract_news_links_success(mocker, emol_dag):
    mock_response_1 = mocker.Mock()
    mock_response_1.json.return_value = {
        "hits": {
            "hits": [
                {
                    "_source": {
                        "id": "123",
                        "permalink": "http://emol.com/test",
                        "fechaPublicacion": "2024-01-01T12:00:00",
                        "fechaModificacion": "2024-01-01T12:30:00"
                    }
                }
            ]
        }
    }
    mock_response_2 = mocker.Mock()
    mock_response_2.json.return_value = {
        "hits": {
            "hits": [
                {
                    "_source": {
                        "id": "124",
                        "permalink": "http://emol.com/test2",
                        "fechaPublicacion": "2023-12-31T23:59:59",
                        "fechaModificacion": "2023-12-31T23:59:59"
                    }
                }
            ]
        }
    }
    
    mocker.patch("src.dags.emol.Fetcher.get", side_effect=[mock_response_1, mock_response_2])

    endpoint = {"categoria": "nacional", "url": "http://dummy.com"}
    context = {"logical_date": pendulum.datetime(2024, 1, 2, tz="UTC")}

    extract_news_links = get_python_function("extract_news_links")
    
    result = extract_news_links(endpoint=endpoint, **context)

    assert len(result) == 1
    assert result[0]["id"] == "123"
    assert result[0]["categoria"] == "nacional"
    assert result[0]["link"] == "https://emol.com/test"

def test_extract_news_links_empty(mocker, emol_dag):
    mock_response = mocker.Mock()
    mock_response.json.return_value = {"hits": {"hits": []}}
    mocker.patch("src.dags.emol.Fetcher.get", return_value=mock_response)

    endpoint = {"categoria": "nacional", "url": "http://dummy.com"}
    context = {"logical_date": pendulum.datetime(2024, 1, 2, tz="UTC")}

    extract_news_links = get_python_function("extract_news_links")

    with pytest.raises(AirflowSkipException):
        extract_news_links(endpoint=endpoint, **context)

def test_flatten_links(emol_dag):
    links = [[{"id": 1}], [{"id": 2}]]
    flatten_links = get_python_function("flatten_links")

    result = flatten_links(links)
    assert len(result) == 2
    assert result[0]["id"] == 1
    assert result[1]["id"] == 2

def test_extract_news_data_success(mocker, emol_dag):
    mock_page = mocker.Mock()
    mock_title = mocker.Mock()
    mock_title.text = "Test Titulo"
    
    mock_bajada = mocker.Mock()
    mock_bajada.text = "Test Bajada"
    
    mock_texto = mocker.Mock()
    mock_texto.get_all_text.return_value = "Test Texto"

    def css_side_effect(selector):
        if selector == "h1#cuDetalle_cuTitular_tituloNoticia":
            return [mock_title]
        elif selector == "h2#cuDetalle_cuTitular_bajadaNoticia":
            return [mock_bajada]
        elif selector == "div#cuDetalle_cuTexto_textoNoticia > div":
            return [mock_texto] * 2
        return []

    mock_page.css.side_effect = css_side_effect
    mocker.patch("src.dags.emol.Fetcher.get", return_value=mock_page)

    input_data = {
        "id": "123",
        "link": "https://emol.com/test",
        "categoria": "nacional",
        "fecha_publicacion": "2024-01-01",
        "fecha_modificacion": "2024-01-01"
    }

    extract_news_data = get_python_function("extract_news_data")
    
    result = extract_news_data(link_data=input_data)

    assert result["id"] == "123"
    assert result["titulo"] == "Test Titulo"
    assert result["bajada"] == "Test Bajada"
    assert result["noticia"] == "Test Texto\nTest Texto"

def test_save_data_success(mocker, emol_dag):
    mock_conn = mocker.Mock()
    mock_cursor = mocker.Mock()
    mock_conn.cursor.return_value = mock_cursor
    mocker.patch("airflow.providers.postgres.hooks.postgres.PostgresHook.get_conn", return_value=mock_conn)

    data = {
        "id": "123", "categoria": "n", "titulo": "t", "bajada": "b", 
        "noticia": "n", "fecha_publicacion": "f", "fecha_modificacion": "fm"
    }

    save_data = get_python_function("save_data")
    
    result = save_data(data=data)

    assert result == 0
    assert mock_cursor.execute.called
    assert mock_conn.commit.called


