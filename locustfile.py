"""Optional local load scenario; run with ``pip install -e '.[load]'`` first."""

from uuid import uuid4

from locust import HttpUser, between, task


class PaymentUser(HttpUser):
    wait_time = between(0.05, 0.2)

    @task(4)
    def create_payment(self) -> None:
        key = str(uuid4())
        with self.client.post(
            "/api/v1/payments",
            headers={"Idempotency-Key": key},
            json={
                "merchant_id": "locust-merchant",
                "amount": 100,
                "currency": "INR",
                "payment_method": {"type": "card", "token": "test_card"},
            },
            name="POST /api/v1/payments",
            catch_response=True,
        ) as response:
            if response.status_code not in {201, 402, 503}:
                response.failure(f"unexpected status {response.status_code}")

    @task(1)
    def list_payments(self) -> None:
        self.client.get("/api/v1/payments", params={"merchant_id": "locust-merchant"}, name="GET /api/v1/payments")
