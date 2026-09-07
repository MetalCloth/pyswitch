import json
from contextlib import asynccontextmanager
from uuid import UUID, uuid4

from fastapi import FastAPI, Header, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field, field_validator
from fastapi.exceptions import RequestValidationError

from .config import Settings
from .domain import Payment, PaymentStatus
from .providers.base import ProviderError
from .providers.mock import MockAdyenProvider, MockRazorpayProvider, MockStripeProvider
from .service import PaymentInput, PaymentService
from .store import InMemoryPaymentStore


class PaymentMethod(BaseModel):
    type: str
    token: str = Field(min_length=1)


class CreatePaymentRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    merchant_id: str = Field(min_length=1, max_length=100)
    amount: int = Field(gt=0)
    currency: str = Field(min_length=3, max_length=3)
    payment_method: PaymentMethod

    @field_validator("currency")
    @classmethod
    def uppercase_currency(cls, value: str) -> str:
        return value.upper()


class PaymentResponse(BaseModel):
    id: UUID
    merchant_id: str
    amount: int
    currency: str
    status: PaymentStatus
    created_at: str
    request_id: str


def _payment_response(payment: Payment, request_id: str) -> PaymentResponse:
    return PaymentResponse(id=payment.id, merchant_id=payment.merchant_id, amount=payment.amount, currency=payment.currency, status=payment.status, created_at=payment.created_at.isoformat(), request_id=request_id)


def create_app(settings: Settings | None = None, service: PaymentService | None = None) -> FastAPI:
    settings = settings or Settings.from_env()
    if service is None:
        service = PaymentService([MockStripeProvider(), MockAdyenProvider(), MockRazorpayProvider()], InMemoryPaymentStore())

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        yield

    app = FastAPI(title="PySwitch", version="0.1.0", lifespan=lifespan)
    app.state.service = service
    app.state.settings = settings

    @app.middleware("http")
    async def request_id(request: Request, call_next):
        request.state.request_id = request.headers.get("X-Request-ID", str(uuid4()))
        response = await call_next(request)
        response.headers["X-Request-ID"] = request.state.request_id
        return response

    @app.exception_handler(ProviderError)
    async def provider_error(request: Request, exc: ProviderError):
        return JSONResponse(status_code=402 if exc.code == "PAYMENT_DECLINED" else 503, content={"error": {"code": exc.code, "message": str(exc), "request_id": request.state.request_id}})

    @app.exception_handler(LookupError)
    async def lookup_error(request: Request, exc: LookupError):
        return JSONResponse(status_code=503, content={"error": {"code": "PROVIDER_UNAVAILABLE", "message": str(exc), "request_id": request.state.request_id}})

    @app.exception_handler(RequestValidationError)
    async def validation_error(request: Request, exc: RequestValidationError):
        return JSONResponse(status_code=422, content={"error": {"code": "VALIDATION_ERROR", "message": "Request validation failed", "request_id": request.state.request_id}})

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    @app.get("/ready")
    async def ready(request: Request):
        providers = request.app.state.service.providers
        return {"status": "ready", "providers": {p.name: await p.health_check() for p in providers}}

    @app.post("/api/v1/payments", response_model=PaymentResponse, status_code=201)
    async def create_payment(payload: CreatePaymentRequest, request: Request, idempotency_key: str | None = Header(default=None, alias="Idempotency-Key")):
        if not idempotency_key:
            return JSONResponse(status_code=400, content={"error": {"code": "VALIDATION_ERROR", "message": "Idempotency-Key header is required", "request_id": request.state.request_id}})
        if payload.currency not in request.app.state.settings.supported_currencies:
            return JSONResponse(status_code=422, content={"error": {"code": "VALIDATION_ERROR", "message": "Unsupported currency", "request_id": request.state.request_id}})
        if payload.payment_method.type != "card" or not payload.payment_method.token.startswith("test_"):
            return JSONResponse(status_code=422, content={"error": {"code": "VALIDATION_ERROR", "message": "A synthetic payment token is required", "request_id": request.state.request_id}})
        payment = await request.app.state.service.create(PaymentInput(payload.merchant_id, payload.amount, payload.currency, payload.payment_method.type, payload.payment_method.token, idempotency_key))
        return _payment_response(payment, request.state.request_id)

    @app.get("/api/v1/payments/{payment_id}", response_model=PaymentResponse)
    async def get_payment(payment_id: UUID, request: Request):
        payment = await request.app.state.service.store.get(payment_id)
        if not payment:
            return JSONResponse(status_code=404, content={"error": {"code": "INTERNAL_ERROR", "message": "Payment not found", "request_id": request.state.request_id}})
        return _payment_response(payment, request.state.request_id)

    @app.get("/api/v1/payments", response_model=list[PaymentResponse])
    async def list_payments(request: Request, merchant_id: str | None = Query(default=None)):
        return [_payment_response(p, request.state.request_id) for p in await request.app.state.service.store.list(merchant_id)]

    @app.get("/api/v1/providers")
    async def providers(request: Request):
        return [{"name": p.name, "healthy": await p.health_check()} for p in request.app.state.service.providers]

    return app


app = create_app()
