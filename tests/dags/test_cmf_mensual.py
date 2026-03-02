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
def cmf_mensual_dag(dagbag):
    return dagbag.get_dag(dag_id="cmf_mensual")


def get_python_function(task_id):
    """Helper para extraer la función Python original desde el módulo del DAG cmf_mensual."""
    import sys
    import importlib.util

    spec = importlib.util.spec_from_file_location("cmf_mensual_module", "src/dags/cmf_mensual.py")
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


def test_cmf_mensual_dag_loaded(cmf_mensual_dag):
    assert cmf_mensual_dag is not None

    task_ids = [task.task_id for task in cmf_mensual_dag.tasks]
    assert "extract_indicadores" in task_ids
    assert "flatten_indicadores" in task_ids
    assert "upsert_indicadores" in task_ids


def test_cmf_mensual_dag_schedule(cmf_mensual_dag):
    """El DAG mensual debe ejecutarse con schedule @monthly."""
    assert cmf_mensual_dag.schedule == "@monthly"


# --- Pruebas unitarias de extract_indicadores ---

def test_extract_indicadores_utm_success(mocker):
    """Extracción exitosa de la UTM: verifica la transformación de valor y estructura de retorno."""
    mock_response = mocker.Mock()
    mock_response.json.return_value = {
        "UTMs": [
            {"Valor": "66.711", "Fecha": "2026-01"}
        ]
    }
    mocker.patch("src.dags.cmf_mensual.Fetcher.get", return_value=mock_response)
    mocker.patch("src.dags.cmf_mensual.Variable.get", return_value="fake_api_key")

    extract_indicadores = get_python_function("extract_indicadores")
    context = {"logical_date": pendulum.datetime(2026, 1, 1, tz="UTC")}

    result = extract_indicadores(indicador="utm", **context)

    assert len(result) == 1
    assert result[0]["indicador"] == "utm"
    # "66.711" -> quitar punto -> "66711" -> no hay coma -> "66711"
    assert result[0]["valor"] == "66711"
    assert result[0]["fecha_valor"] == "2026-01"


def test_extract_indicadores_ipc_success(mocker):
    """Extracción exitosa del IPC."""
    mock_response = mocker.Mock()
    mock_response.json.return_value = {
        "IPCs": [
            {"Valor": "0,4", "Fecha": "2026-01"}
        ]
    }
    mocker.patch("src.dags.cmf_mensual.Fetcher.get", return_value=mock_response)
    mocker.patch("src.dags.cmf_mensual.Variable.get", return_value="fake_api_key")

    extract_indicadores = get_python_function("extract_indicadores")
    context = {"logical_date": pendulum.datetime(2026, 1, 1, tz="UTC")}

    result = extract_indicadores(indicador="ipc", **context)

    assert len(result) == 1
    assert result[0]["indicador"] == "ipc"
    # "0,4" -> sin puntos -> "0,4" -> coma a punto -> "0.4"
    assert result[0]["valor"] == "0.4"
    assert result[0]["fecha_valor"] == "2026-01"


def test_extract_indicadores_no_api_key(mocker):
    """Sin API Key debe lanzar AirflowSkipException."""
    mocker.patch("src.dags.cmf_mensual.Variable.get", return_value=None)

    extract_indicadores = get_python_function("extract_indicadores")
    context = {"logical_date": pendulum.datetime(2026, 1, 1, tz="UTC")}

    with pytest.raises(AirflowSkipException):
        extract_indicadores(indicador="utm", **context)


def test_extract_indicadores_api_error_response(mocker):
    """Si la API retorna un CodigoError, debe lanzar AirflowSkipException."""
    mock_response = mocker.Mock()
    mock_response.json.return_value = {
        "CodigoError": "403",
        "Descripcion": "Token inválido"
    }
    mocker.patch("src.dags.cmf_mensual.Fetcher.get", return_value=mock_response)
    mocker.patch("src.dags.cmf_mensual.Variable.get", return_value="fake_api_key")

    extract_indicadores = get_python_function("extract_indicadores")
    context = {"logical_date": pendulum.datetime(2026, 1, 1, tz="UTC")}

    with pytest.raises(AirflowSkipException):
        extract_indicadores(indicador="utm", **context)


def test_extract_indicadores_empty_values(mocker):
    """Si la API retorna lista vacía debe lanzar AirflowSkipException."""
    mock_response = mocker.Mock()
    mock_response.json.return_value = {"UTMs": []}
    mocker.patch("src.dags.cmf_mensual.Fetcher.get", return_value=mock_response)
    mocker.patch("src.dags.cmf_mensual.Variable.get", return_value="fake_api_key")

    extract_indicadores = get_python_function("extract_indicadores")
    context = {"logical_date": pendulum.datetime(2026, 1, 1, tz="UTC")}

    with pytest.raises(AirflowSkipException):
        extract_indicadores(indicador="utm", **context)


def test_extract_indicadores_fetcher_exception(mocker):
    """Si Fetcher lanza una excepción de red, debe capturarla y lanzar AirflowSkipException."""
    mocker.patch("src.dags.cmf_mensual.Fetcher.get", side_effect=ConnectionError("timeout"))
    mocker.patch("src.dags.cmf_mensual.Variable.get", return_value="fake_api_key")

    extract_indicadores = get_python_function("extract_indicadores")
    context = {"logical_date": pendulum.datetime(2026, 1, 1, tz="UTC")}

    with pytest.raises(AirflowSkipException):
        extract_indicadores(indicador="utm", **context)


def test_extract_indicadores_builds_correct_endpoint(mocker):
    """Verifica que el endpoint mensual incluye año y mes (sin día)."""
    mock_response = mocker.Mock()
    mock_response.json.return_value = {
        "UTMs": [{"Valor": "66.000", "Fecha": "2026-03"}]
    }
    mock_get = mocker.patch("src.dags.cmf_mensual.Fetcher.get", return_value=mock_response)
    mocker.patch("src.dags.cmf_mensual.Variable.get", return_value="fake_api_key")

    extract_indicadores = get_python_function("extract_indicadores")
    context = {"logical_date": pendulum.datetime(2026, 3, 1, tz="UTC")}

    extract_indicadores(indicador="utm", **context)

    called_url = mock_get.call_args[0][0]
    assert "utm" in called_url
    assert "2026" in called_url
    assert "03" in called_url
    # El endpoint mensual NO debe incluir el segmento de días
    assert "dias" not in called_url


# --- Pruebas unitarias de flatten_indicadores ---

def test_flatten_indicadores_multiple_lists():
    """Verifica que flatten aplana correctamente listas de UTM e IPC."""
    flatten_indicadores = get_python_function("flatten_indicadores")

    input_data = [
        [{"indicador": "utm", "valor": "66711", "fecha_valor": "2026-01"}],
        [{"indicador": "ipc", "valor": "0.4", "fecha_valor": "2026-01"}],
    ]

    result = flatten_indicadores(input_data)

    assert len(result) == 2
    assert result[0]["indicador"] == "utm"
    assert result[1]["indicador"] == "ipc"


def test_flatten_indicadores_empty_input():
    """Aplana correctamente una lista de listas vacías."""
    flatten_indicadores = get_python_function("flatten_indicadores")

    result = flatten_indicadores([[], []])

    assert result == []


def test_flatten_indicadores_single_list():
    """Un solo indicador con un valor retorna una lista de un elemento."""
    flatten_indicadores = get_python_function("flatten_indicadores")

    input_data = [[{"indicador": "utm", "valor": "66711", "fecha_valor": "2026-01"}]]

    result = flatten_indicadores(input_data)

    assert len(result) == 1
    assert result[0]["indicador"] == "utm"
