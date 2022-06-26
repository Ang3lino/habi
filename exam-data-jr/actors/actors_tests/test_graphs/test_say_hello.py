from actors.jobs.say_hello import say_hello_job
from actors.jobs.jobs import launcher


def test_say_hello():
    """
    This is an example test for a Dagster job.

    For hints on how to test your Dagster graphs, see our documentation tutorial on Testing:
    https://docs.dagster.io/concepts/testing
    """
    result = say_hello_job.execute_in_process()
    assert result.success
    assert result.output_for_node("hello") == "Hello, Dagster!"

def test_launch():
    run_config = {
        "resources": {
            "excel": {
                "config": {
                    "excel_name": "Matriz_de_adyacencia_data_team.xlsx"
                }
            },
            "mysql": {
                "config": {
                    "username": 'root',
                    "password": 'root',
                    "hostname": 'localhost',
                    "db_name": 'actors'
                }
            }
        }
    }
    res = launcher.execute_in_process(run_config=run_config)
    assert res.success