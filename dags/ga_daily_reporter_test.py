from galileo import unit_testing


def test_dag_import():
    """Test that the DAG file can be successfully imported.
    This tests that the DAG can be parsed, but does not run it in an Airflow
    environment. This is a recommended sanity check by the official Airflow
    docs: https://airflow.incubator.apache.org/tutorial.html#testing
    """
    if __package__ is None:
        import sys
        from os import path
        sys.path.append( path.dirname( path.dirname( path.abspath(__file__) ) ) )
        import ga_daily_reporter as module
    else:
        from . import ga_daily_reporter as module
    unit_testing.assert_has_valid_dag(module)


if __name__ == '__main__':
    test_dag_import()