from novacommerce.etl.validate_pipeline import Check
from novacommerce.etl.orders_cdc_pipeline import SELECT_CHANGES, UPSERT
import pytest

@pytest.mark.parametrize('observed,limit,expected', [(0,0,True),(1,0,False),(4,5,True),(6,5,False)])
def test_quality_threshold(observed,limit,expected):
    assert Check('test',observed,limit).passed is expected

def test_quality_defaults_to_error():
    assert Check('sample',0,0).severity == 'ERROR'

def test_change_queue_locks_rows():
    assert 'SKIP LOCKED' in SELECT_CHANGES

def test_staging_load_is_upsert():
    assert 'ON CONFLICT (order_id)' in UPSERT
