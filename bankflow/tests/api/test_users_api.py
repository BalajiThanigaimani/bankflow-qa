from bankflow.framework.api.client import ApiClient

def test_get_users():
    response = ApiClient().get_users()
    assert response.status_code == 200
    assert isinstance(response.json(), list)
