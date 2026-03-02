import pytest
import pendulum
from airflow.models import DagBag
from airflow.exceptions import AirflowSkipException

import os
os.environ.setdefault("AIRFLOW_VAR_CMF_APIKEY", "test_api_key")


@pytest.fixture()
def dagbag():
    return DagBag(dag_folder="src/dags", include_examples=False)


@pytest.fixture()
def cmf_diario_dag(dagbag):
    return dagbag.get_dag(dag_id="cmf_diario")


def get_python_function(task_id):
    """Helper para extraer la función Python original desde el módulo del DAG cmf_diario."""
    import sys
    import importlib.util

    spec = importlib.util.spec_from_file_location("cmf_diario_module", "src/dags/cmf_diario.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    closure_vars = {}

    def trace_calls(frame, event, arg):
        if event == "return" and frame.f_code.co_name == "scrapper":
            closure_vars.update(frame.f_locals)
        return trace_calls

    old_trace = sys.gettrace()
    sys.settrace(trace_calls)
    try:
        module.scrapper()
    finally:
        sys.settrace(old_trace)

    func = closure_vars.get(task_id)
    if func and hasattr(func, "function"):
        return func.function
    return func


# --- Pruebas de carga del DAG ---

def test_no_import_errors(dagbag):
    assert len(dagbag.import_errors) == 0, f"Errores al importar: {dagbag.import_errors}"


def test_cmf_diario_dag_loaded(cmf_diario_dag):
    assert cmf_diario_dag is not None

    task_ids = [task.task_id for task in cmf_diario_dag.tasks]
    assert "extract_indicadores" in task_ids
    assert "flatten_indicadores" in task_ids
    assert "upsert_indicadores" in task_ids


def test_cmf_diario_dag_schedule(cmf_diario_dag):
    """El DAG diario debe ejecutarse con cron diario a medianoche."""
    assert cmf_diario_dag.schedule == "@daily"


# --- Pruebas unitarias de extract_indicadores ---

def test_extract_indicadores_dolar_success(mocker):
    """Extracción exitosa del dólar: verifica transformación de valor y estructura de retorno."""
    mock_response = mocker.Mock()
    mock_response.json.return_value = {
        "Dolares": [
            {"Valor": "1.050,25", "Fecha": "2026-01-15"}
        ]
    }
    mocker.patch("src.dags.cmf_diario.Fetcher.get", return_value=mock_response)
    mocker.patch("src.dags.cmf_diario.Variable.get", return_value="fake_api_key")

    extract_indicadores = get_python_function("extract_indicadores")
    context = {"logical_date": pendulum.datetime(2026, 1, 15, tz="UTC")}

    result = extract_indicadores(indicador="dolar", **context)

    assert len(result) == 1
    assert result[0]["indicador"] == "dolar"
    # "1.050,25" -> quitar punto -> "1050,25" -> coma a punto -> "105025"
    # Nota: el DAG hace .replace(".", "").replace(",", ".") en ese orden
    assert result[0]["valor"] == "1050.25"
    assert result[0]["fecha_valor"] == "2026-01-15"


def test_extract_indicadores_euro_success(mocker):
    """Extracción exitosa del euro."""
    mock_response = mocker.Mock()
    mock_response.json.return_value = {
        "Euros": [
            {"Valor": "1.200,50", "Fecha": "2026-01-15"}
        ]
    }
    mocker.patch("src.dags.cmf_diario.Fetcher.get", return_value=mock_response)
    mocker.patch("src.dags.cmf_diario.Variable.get", return_value="fake_api_key")

    extract_indicadores = get_python_function("extract_indicadores")
    context = {"logical_date": pendulum.datetime(2026, 1, 15, tz="UTC")}

    result = extract_indicadores(indicador="euro", **context)

    assert len(result) == 1
    assert result[0]["indicador"] == "euro"
    assert result[0]["valor"] == "1200.50"
    assert result[0]["fecha_valor"] == "2026-01-15"


def test_extract_indicadores_uf_success(mocker):
    """Extracción exitosa de la UF con múltiples días (la UF puede retornar varios días de una vez)."""
    mock_response = mocker.Mock()
    mock_response.json.return_value = {
        "UFs": [
            {"Valor": "38000,00", "Fecha": "2026-01-15"},
            {"Valor": "38050,00", "Fecha": "2026-01-16"},
        ]
    }
    mocker.patch("src.dags.cmf_diario.Fetcher.get", return_value=mock_response)
    mocker.patch("src.dags.cmf_diario.Variable.get", return_value="fake_api_key")

    extract_indicadores = get_python_function("extract_indicadores")
    context = {"logical_date": pendulum.datetime(2026, 1, 15, tz="UTC")}

    result = extract_indicadores(indicador="uf", **context)

    assert len(result) == 2
    assert result[0]["indicador"] == "uf"
    assert result[1]["indicador"] == "uf"


def test_extract_indicadores_no_api_key(mocker):
    """Sin API Key debe lanzar AirflowSkipException."""
    mocker.patch("src.dags.cmf_diario.Variable.get", return_value=None)

    extract_indicadores = get_python_function("extract_indicadores")
    context = {"logical_date": pendulum.datetime(2026, 1, 15, tz="UTC")}

    with pytest.raises(AirflowSkipException):
        extract_indicadores(indicador="dolar", **context)


def test_extract_indicadores_api_error_response(mocker):
    """Si la API retorna un CodigoError, debe lanzar AirflowSkipException."""
    mock_response = mocker.Mock()
    mock_response.json.return_value = {
        "CodigoError": "404",
        "Descripcion": "No se encontró el recurso"
    }
    mocker.patch("src.dags.cmf_diario.Fetcher.get", return_value=mock_response)
    mocker.patch("src.dags.cmf_diario.Variable.get", return_value="fake_api_key")

    extract_indicadores = get_python_function("extract_indicadores")
    context = {"logical_date": pendulum.datetime(2026, 1, 15, tz="UTC")}

    with pytest.raises(AirflowSkipException):
        extract_indicadores(indicador="dolar", **context)


def test_extract_indicadores_empty_values(mocker):
    """Si la API retorna lista vacía de valores (ej. feriado), debe lanzar AirflowSkipException."""
    mock_response = mocker.Mock()
    mock_response.json.return_value = {"Dolares": []}
    mocker.patch("src.dags.cmf_diario.Fetcher.get", return_value=mock_response)
    mocker.patch("src.dags.cmf_diario.Variable.get", return_value="fake_api_key")

    extract_indicadores = get_python_function("extract_indicadores")
    context = {"logical_date": pendulum.datetime(2026, 1, 15, tz="UTC")}

    with pytest.raises(AirflowSkipException):
        extract_indicadores(indicador="dolar", **context)


def test_extract_indicadores_fetcher_exception(mocker):
    """Si Fetcher lanza una excepción de red, debe capturarla y lanzar AirflowSkipException."""
    mocker.patch("src.dags.cmf_diario.Fetcher.get", side_effect=ConnectionError("timeout"))
    mocker.patch("src.dags.cmf_diario.Variable.get", return_value="fake_api_key")

    extract_indicadores = get_python_function("extract_indicadores")
    context = {"logical_date": pendulum.datetime(2026, 1, 15, tz="UTC")}

    with pytest.raises(AirflowSkipException):
        extract_indicadores(indicador="dolar", **context)


def test_extract_indicadores_builds_correct_endpoint(mocker):
    """Verifica que el endpoint construido incluye año, mes y día correctamente."""
    mock_response = mocker.Mock()
    mock_response.json.return_value = {
        "Dolares": [{"Valor": "900,00", "Fecha": "2026-03-02"}]
    }
    mock_get = mocker.patch("src.dags.cmf_diario.Fetcher.get", return_value=mock_response)
    mocker.patch("src.dags.cmf_diario.Variable.get", return_value="fake_api_key")

    extract_indicadores = get_python_function("extract_indicadores")
    context = {"logical_date": pendulum.datetime(2026, 3, 2, tz="UTC")}

    extract_indicadores(indicador="dolar", **context)

    call_args = mock_get.call_args
    called_url = call_args[0][0]
    assert "dolar" in called_url
    assert "2026" in called_url
    assert "03" in called_url
    assert "02" in called_url


# --- Pruebas unitarias de flatten_indicadores ---

def test_flatten_indicadores_multiple_lists():
    """Verifica que flatten aplana correctamente listas de distintos indicadores."""
    flatten_indicadores = get_python_function("flatten_indicadores")

    input_data = [
        [{"indicador": "dolar", "valor": "900", "fecha_valor": "2026-01-15"}],
        [{"indicador": "euro", "valor": "1000", "fecha_valor": "2026-01-15"}],
        [
            {"indicador": "uf", "valor": "38000", "fecha_valor": "2026-01-15"},
            {"indicador": "uf", "valor": "38050", "fecha_valor": "2026-01-16"},
        ],
    ]

    result = flatten_indicadores(input_data)

    assert len(result) == 4
    assert result[0]["indicador"] == "dolar"
    assert result[1]["indicador"] == "euro"
    assert result[2]["indicador"] == "uf"
    assert result[3]["indicador"] == "uf"


def test_flatten_indicadores_empty_input():
    """Aplana correctamente una lista de listas vacías."""
    flatten_indicadores = get_python_function("flatten_indicadores")

    result = flatten_indicadores([[], [], []])

    assert result == []


def test_flatten_indicadores_single_list():
    """Un solo indicador con un valor retorna una lista de un elemento."""
    flatten_indicadores = get_python_function("flatten_indicadores")

    input_data = [[{"indicador": "dolar", "valor": "900", "fecha_valor": "2026-01-15"}]]

    result = flatten_indicadores(input_data)

    assert len(result) == 1
    assert result[0]["indicador"] == "dolar"
