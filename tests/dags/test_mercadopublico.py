import pytest
import pendulum
import time
from airflow.models import DagBag
from airflow.exceptions import AirflowSkipException

# Evitar errores de parseo por variables no definidas en el entorno local
import os
os.environ["AIRFLOW_VAR_MP_TICKET"] = "test_ticket"

@pytest.fixture()
def dagbag():
    return DagBag(dag_folder="src/dags", include_examples=False)

@pytest.fixture()
def mp_dag(dagbag):
    return dagbag.get_dag(dag_id="mercadopublico")

def get_python_function(task_id):
    import sys
    import importlib.util
    import inspect
    
    spec = importlib.util.spec_from_file_location("mp_dag_module", "src/dags/mercadopublico.py")
    mp_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mp_module)
    
    closure_vars = {}
    
    def trace_calls(frame, event, arg):
        if event == "return" and frame.f_code.co_name == "scrapper":
            closure_vars.update(frame.f_locals)
        return trace_calls

    old_trace = sys.gettrace()
    sys.settrace(trace_calls)
    try:
        mp_module.scrapper()
    finally:
        sys.settrace(old_trace)
        
    func = closure_vars.get(task_id)
    if func and hasattr(func, 'function'):
        return func.function
    return func


def test_no_import_errors(dagbag):
    assert len(dagbag.import_errors) == 0, f"Errors found: {dagbag.import_errors}"

def test_mp_dag_loaded(mp_dag):
    assert mp_dag is not None
    
    task_ids = [task.task_id for task in mp_dag.tasks]
    assert "extract_licitaciones" in task_ids
    assert "extract_licitaciones_detalle" in task_ids
    assert "extract_licitacion_items" in task_ids
    assert "upsert_licitacion" in task_ids
    assert "upsert_licitacion_items" in task_ids

# --- Unit Tests for Tasks ---

def test_extract_licitaciones_success(mocker):
    # Mocking Fetcher.get
    mock_response = mocker.Mock()
    mock_response.json.return_value = {
        "Cantidad": 2,
        "Listado": [
            {"CodigoExterno": "123-45", "CodigoEstado": 8}, # Adjudicada
            {"CodigoExterno": "999-99", "CodigoEstado": 5}, # Publicada (Debe ignorarse)
        ]
    }
    mocker.patch("src.dags.mercadopublico.Fetcher.get", return_value=mock_response)
    mocker.patch("src.dags.mercadopublico.Variable.get", return_value="fake_ticket")

    extract_licitaciones = get_python_function("extract_licitaciones")
    context = {"logical_date": pendulum.datetime(2024, 1, 2, tz="UTC")}
    
    result = extract_licitaciones(**context)

    # Solo debe retornar los códigos en estado 8
    assert len(result) == 1
    assert result[0] == "123-45"

def test_extract_licitaciones_no_ticket(mocker):
    mocker.patch("src.dags.mercadopublico.Variable.get", return_value=None)
    
    extract_licitaciones = get_python_function("extract_licitaciones")
    context = {"logical_date": pendulum.datetime(2024, 1, 2, tz="UTC")}

    with pytest.raises(AirflowSkipException):
        extract_licitaciones(**context)

    def test_extract_licitaciones_detalle_success(mocker):
        mocker.patch("src.dags.mercadopublico.time.sleep") # Saltar espera en tests
        mock_response = mocker.Mock()
        mock_response.json.return_value = {
            "Listado": [{
                "CodigoExterno": "123-45",
                "Nombre": "Compra Lápices",
                "Descripcion": "Para oficina",
                "Estado": "Adjudicada",
                "CodigoTipo": 1, "Tipo": "L1", "TipoConvocatoria": 1,
                "Estimacion": 1, "MontoEstimado": 1000, "Moneda": "CLP",
                "Modalidad": 1, "UnidadTiempoDuracionContrato": 1, "TiempoDuracionContrato": 12,
                "Comprador": {
                    "CodigoOrganismo": "O1", "NombreOrganismo": "Org1", "RutUnidad": "1-9",
                    "CodigoUnidad": "U1", "NombreUnidad": "Unid1", "DireccionUnidad": "Dir1",
                    "ComunaUnidad": "C1", "RegionUnidad": "R1", "RutUsuario": "2-7",
                    "CodigoUsuario": "Us1", "NombreUsuario": "User1", "CargoUsuario": "Jefe"
                },
                "Fechas": {
                    "FechaCreacion": "2024-01-01", "FechaCierre": "2024-01-02",
                    "FechaInicio": "2024-01-03", "FechaFinal": "2024-01-04"
                },
                "Items": {
                    "Cantidad": 1,
                    "Listado": [{"Correlativo": 1, "NombreProducto": "Lápiz Bic"}]
                }
            }]
        }
        mocker.patch("src.dags.mercadopublico.Fetcher.get", return_value=mock_response)
        mocker.patch("src.dags.mercadopublico.Variable.get", return_value="fake_ticket")

        extract_licitaciones_detalle = get_python_function("extract_licitaciones_detalle")

        result = extract_licitaciones_detalle(codigo="123-45")

        assert result["codigo"] == "123-45"
        assert result["nombre"] == "Compra Lápices"
        assert result["codigo_comprador"] == "O1"
        assert result["fecha_creacion"] == "2024-01-01"
        assert len(result["listado_items"]) == 1

    def test_extract_licitacion_items(mocker):
        input_licitaciones = [
            {
                "codigo": "123-45",
                "listado_items": [
                    {
                        "Correlativo": 1, "CodigoProducto": "P1", "Cantidad": 10,
                        "Categoria": "Oficina", "CodigoCategoria": 1, "Descripcion": "Lápiz",
                        "UnidadMedida": "Caja", "NombreProducto": "Bic",
                        "Adjudicacion": {"RutProveedor": "3-5", "NombreProveedor": "Libreria", "MontoUnitario": 100}
                    },
                    {
                        "Correlativo": 2, "CodigoProducto": "P2", "Cantidad": 5,
                        "Categoria": "Oficina", "CodigoCategoria": 1, "Descripcion": "Goma",
                        "UnidadMedida": "Unidad", "NombreProducto": "Staedtler",
                        "Adjudicacion": {"RutProveedor": "3-5", "NombreProveedor": "Libreria", "MontoUnitario": 50}
                    }
                ]
            }
        ]

        extract_licitacion_items = get_python_function("extract_licitacion_items")
        
        result = extract_licitacion_items(licitaciones=input_licitaciones)

        # El DAG está configurado explícitamente para extraer solo el *primer item* [:1]
        assert len(result) == 1
        assert result[0]["correlativo"] == 1
        assert result[0]["codigo_licitacion"] == "123-45"
        assert result[0]["monto_unitario"] == 100
