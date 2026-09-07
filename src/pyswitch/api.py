from contextlib import asynccontextmanager
from uuid import UUID, uuid4

from fastapi import Depends, FastAPI, Header, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field, field_validator
from fastapi.exceptions import RequestValidationError

from .config import Settings
from .domain import Payment, PaymentStatus
from .idempotency import IdempotencyUnavailable
from .rate_limit import InMemoryTokenBucketLimiter, RateLimitUnavailable, RateLimiter
from .providers.base import ProviderError
from .providers.mock import MockAdyenProvider, MockRazorpayProvider, MockStripeProvider
from .service import IdempotencyConflict, PaymentInput, PaymentService, RefundError
from .store import InMemoryPaymentStore


class AdminUnauthorized(Exception):
    pass


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


class ProviderConfigRequest(BaseModel):
    success_rate: float | None = Field(default=None, ge=0, le=1)
    min_latency_ms: int | None = Field(default=None, ge=0)
    max_latency_ms: int | None = Field(default=None, ge=0)
    timeout_probability: float | None = Field(default=None, ge=0, le=1)
    server_error_probability: float | None = Field(default=None, ge=0, le=1)
    decline_probability: float | None = Field(default=None, ge=0, le=1)


class RefundRequest(BaseModel):
    amount: int | None = Field(default=None, gt=0)


class RefundResponse(BaseModel):
    id: UUID
    payment_id: UUID
    amount: int
    request_id: str


def _provider_config(provider) -> dict[str, object]:
    config = provider.config
    return {
        "success_rate": config.success_rate,
        "min_latency_ms": config.min_latency_ms,
        "max_latency_ms": config.max_latency_ms,
        "timeout_probability": config.timeout_probability,
        "server_error_probability": config.server_error_probability,
        "decline_probability": config.decline_probability,
        "forced_failure": config.forced_failure,
    }


def _payment_response(payment: Payment, request_id: str) -> PaymentResponse:
    return PaymentResponse(id=payment.id, merchant_id=payment.merchant_id, amount=payment.amount, currency=payment.currency, status=payment.status, created_at=payment.created_at.isoformat(), request_id=request_id)


def create_app(settings: Settings | None = None, service: PaymentService | None = None, rate_limiter: RateLimiter | None = None) -> FastAPI:
    settings = settings or Settings.from_env()
    if service is None:
        service = PaymentService([MockStripeProvider(), MockAdyenProvider(), MockRazorpayProvider()], InMemoryPaymentStore())
    rate_limiter = rate_limiter or InMemoryTokenBucketLimiter(capacity=settings.rate_limit_capacity, refill_per_second=settings.rate_limit_refill_per_second)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        yield

    app = FastAPI(title="PySwitch", version="0.1.0", lifespan=lifespan)
    app.state.service = service
    app.state.settings = settings
    app.state.rate_limiter = rate_limiter

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

    @app.exception_handler(AdminUnauthorized)
    async def admin_unauthorized(request: Request, exc: AdminUnauthorized):
        return JSONResponse(status_code=401, content={"error": {"code": "VALIDATION_ERROR", "message": "Admin credentials are required", "request_id": request.state.request_id}})

    @app.exception_handler(IdempotencyConflict)
    async def idempotency_conflict(request: Request, exc: IdempotencyConflict):
        return JSONResponse(status_code=409, content={"error": {"code": exc.code, "message": str(exc), "request_id": request.state.request_id}})

    @app.exception_handler(IdempotencyUnavailable)
    async def idempotency_unavailable(request: Request, exc: IdempotencyUnavailable):
        return JSONResponse(status_code=503, content={"error": {"code": exc.code, "message": "Idempotency coordination is unavailable", "request_id": request.state.request_id}})

    @app.exception_handler(RateLimitUnavailable)
    async def rate_limit_unavailable(request: Request, exc: RateLimitUnavailable):
        return JSONResponse(status_code=503, content={"error": {"code": exc.code, "message": "Rate limiting is unavailable", "request_id": request.state.request_id}})

    @app.exception_handler(RefundError)
    async def refund_error(request: Request, exc: RefundError):
        status_code = 503 if exc.code == "PROVIDER_UNAVAILABLE" else 422
        return JSONResponse(status_code=status_code, content={"error": {"code": exc.code, "message": str(exc), "request_id": request.state.request_id}})

    async def require_admin(request: Request, x_admin_token: str | None = Header(default=None, alias="X-Admin-Token")):
        if x_admin_token != request.app.state.settings.admin_token:
            raise AdminUnauthorized

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    @app.get("/ready")
    async def ready(request: Request):
        providers = request.app.state.service.providers
        return {"status": "ready", "providers": {p.name: await p.health_check() for p in providers}}

    @app.post("/api/v1/payments", response_model=PaymentResponse, status_code=201)
    async def create_payment(payload: CreatePaymentRequest, request: Request, idempotency_key: str | None = Header(default=None, alias="Idempotency-Key")):
        limit = await request.app.state.rate_limiter.consume(payload.merchant_id)
        rate_headers = {
            "X-RateLimit-Limit": str(limit.limit),
            "X-RateLimit-Remaining": str(limit.remaining),
        }
        if not limit.allowed:
            rate_headers["Retry-After"] = str(limit.retry_after_seconds)
            return JSONResponse(status_code=429, headers=rate_headers, content={"error": {"code": "RATE_LIMITED", "message": "Merchant rate limit exceeded", "request_id": request.state.request_id}})
        if not idempotency_key:
            return JSONResponse(status_code=400, headers=rate_headers, content={"error": {"code": "VALIDATION_ERROR", "message": "Idempotency-Key header is required", "request_id": request.state.request_id}})
        if payload.currency not in request.app.state.settings.supported_currencies:
            return JSONResponse(status_code=422, headers=rate_headers, content={"error": {"code": "VALIDATION_ERROR", "message": "Unsupported currency", "request_id": request.state.request_id}})
        if payload.payment_method.type != "card" or not payload.payment_method.token.startswith("test_"):
            return JSONResponse(status_code=422, headers=rate_headers, content={"error": {"code": "VALIDATION_ERROR", "message": "A synthetic payment token is required", "request_id": request.state.request_id}})
        payment = await request.app.state.service.create(PaymentInput(payload.merchant_id, payload.amount, payload.currency, payload.payment_method.type, payload.payment_method.token, idempotency_key))
        response = _payment_response(payment, request.state.request_id)
        return JSONResponse(status_code=201, headers=rate_headers, content=response.model_dump(mode="json"))

    @app.get("/api/v1/payments/{payment_id}", response_model=PaymentResponse)
    async def get_payment(payment_id: UUID, request: Request):
        payment = await request.app.state.service.store.get(payment_id)
        if not payment:
            return JSONResponse(status_code=404, content={"error": {"code": "INTERNAL_ERROR", "message": "Payment not found", "request_id": request.state.request_id}})
        return _payment_response(payment, request.state.request_id)

    @app.get("/api/v1/payments", response_model=list[PaymentResponse])
    async def list_payments(request: Request, merchant_id: str | None = Query(default=None)):
        return [_payment_response(p, request.state.request_id) for p in await request.app.state.service.store.list(merchant_id)]

    @app.post("/api/v1/payments/{payment_id}/refund", response_model=RefundResponse)
    async def refund_payment(payment_id: UUID, payload: RefundRequest, request: Request):
        refund = await request.app.state.service.refund(payment_id, payload.amount)
        return RefundResponse(id=refund.id, payment_id=refund.payment_id, amount=refund.amount, request_id=request.state.request_id)

    @app.get("/api/v1/providers")
    async def providers(request: Request):
        service = request.app.state.service
        return [{"name": p.name, "healthy": await p.health_check(), "config": _provider_config(p), "circuit_state": service.circuits[p.name].state, "consecutive_failures": service.circuits[p.name].consecutive_failures} for p in service.providers]

    @app.get("/api/v1/providers/{provider_name}")
    async def provider_detail(provider_name: str, request: Request):
        provider = next((p for p in request.app.state.service.providers if p.name == provider_name), None)
        if provider is None:
            return JSONResponse(status_code=404, content={"error": {"code": "VALIDATION_ERROR", "message": "Unknown provider", "request_id": request.state.request_id}})
        circuit = request.app.state.service.circuits[provider.name]
        return {"name": provider.name, "healthy": await provider.health_check(), "config": _provider_config(provider), "circuit_state": circuit.state, "consecutive_failures": circuit.consecutive_failures}

    @app.put("/api/v1/admin/providers/{provider_name}/config")
    async def configure_provider(provider_name: str, payload: ProviderConfigRequest, request: Request, _: None = Depends(require_admin)):
        provider = next((p for p in request.app.state.service.providers if p.name == provider_name), None)
        if provider is None:
            return JSONResponse(status_code=404, content={"error": {"code": "VALIDATION_ERROR", "message": "Unknown provider", "request_id": request.state.request_id}})
        changes = payload.model_dump(exclude_none=True)
        min_latency_ms = changes.get("min_latency_ms", provider.config.min_latency_ms)
        max_latency_ms = changes.get("max_latency_ms", provider.config.max_latency_ms)
        if max_latency_ms < min_latency_ms:
            return JSONResponse(status_code=422, content={"error": {"code": "VALIDATION_ERROR", "message": "max_latency_ms must be at least min_latency_ms", "request_id": request.state.request_id}})
        provider.configure(**changes)
        return {"name": provider.name, "config": _provider_config(provider)}

    @app.post("/api/v1/admin/providers/{provider_name}/fail")
    async def fail_provider(provider_name: str, request: Request, _: None = Depends(require_admin)):
        provider = next((p for p in request.app.state.service.providers if p.name == provider_name), None)
        if provider is None:
            return JSONResponse(status_code=404, content={"error": {"code": "VALIDATION_ERROR", "message": "Unknown provider", "request_id": request.state.request_id}})
        provider.configure(forced_failure=True)
        return {"name": provider.name, "healthy": False}

    @app.post("/api/v1/admin/providers/{provider_name}/recover")
    async def recover_provider(provider_name: str, request: Request, _: None = Depends(require_admin)):
        provider = next((p for p in request.app.state.service.providers if p.name == provider_name), None)
        if provider is None:
            return JSONResponse(status_code=404, content={"error": {"code": "VALIDATION_ERROR", "message": "Unknown provider", "request_id": request.state.request_id}})
        provider.configure(forced_failure=False)
        request.app.state.service.circuits[provider.name].record_success()
        return {"name": provider.name, "healthy": True}

    return app


app = create_app()
