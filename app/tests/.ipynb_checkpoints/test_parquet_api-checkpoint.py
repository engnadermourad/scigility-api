import pytest
from fastapi.testclient import TestClient
from app.main import app
import os

client = TestClient(app)

# Set these values or load them from env for flexibility
# TEST_USER = os.getenv("TEST_USER", "testuser")
# TEST_PASSWORD = os.getenv("TEST_PASSWORD", "testpass")
S3_PARQUET_PATH = "s3a://nmourmx-scigility/Silver/client_parquet/"

# @pytest.fixture(scope="module")
# def get_jwt_token():
#     """Obtain a JWT token using the login endpoint."""
#     response = client.post("/login", data={
#         "username": TEST_USER,
#         "password": TEST_PASSWORD
#     })
#     assert response.status_code == 200
#     token = response.json().get("access_token")
#     assert token is not None
#     return token

@pytest.fixture
def auth_headers(get_jwt_token):
    return {
        "Authorization": f"Bearer {get_jwt_token}"
    }

def test_health_check():
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"message": "live stream API running."}

def test_full_parquet(auth_headers):
    response = client.get("/full", params={"path": S3_PARQUET_PATH}, headers=auth_headers)
    assert response.status_code == 200
    json_data = response.json()
    assert "dataset" in json_data
    assert "row_count" in json_data
    assert "data" in json_data
    assert isinstance(json_data["data"], list)
