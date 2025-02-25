import logging
import sys
import time
from typing import Any, Dict, Optional

import structlog
from fastapi import FastAPI
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response
from prometheus_client import Counter, Histogram

# Khởi tạo các metric Prometheus
REQUEST_COUNT = Counter(
    "api_requests_total", 
    "Total count of requests by method and path", 
    ["method", "path", "status_code"]
)

REQUEST_LATENCY = Histogram(
    "api_request_duration_seconds", 
    "Request duration in seconds by method and path",
    ["method", "path"]
)

def setup_logging(debug: bool = False):
    """
    Cấu hình logging cho ứng dụng
    """
    # Cấu hình log level
    log_level = logging.DEBUG if debug else logging.INFO
    
    # Cấu hình logging cơ bản của Python
    logging.basicConfig(
        format="%(message)s",
        level=log_level,
        stream=sys.stdout,
    )
    
    # Cấu hình structlog
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer(sort_keys=True),
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    
    # Thiết lập log level cho một số thư viện cụ thể
    for logger_name in [
        "uvicorn", 
        "uvicorn.access", 
        "uvicorn.error",
        "fastapi",
        "asyncpg",
        "py4j",
        "trino",
    ]:
        logging.getLogger(logger_name).setLevel(log_level)
    
    # Giảm log level cho một số thư viện quá verbose
    logging.getLogger("py4j").setLevel(logging.WARNING)
    
    # Trả về logger chính
    return structlog.get_logger()

class LoggingMiddleware(BaseHTTPMiddleware):
    """
    Middleware ghi log cho tất cả các request và response
    """
    async def dispatch(self, request: Request, call_next):
        request_id = str(time.time())
        structlog.contextvars.bind_contextvars(request_id=request_id)
        
        logger = structlog.get_logger()
        logger.info(
            "Request started",
            method=request.method,
            path=request.url.path,
            query_params=str(request.query_params),
            client=request.client.host if request.client else None,
        )
        
        start_time = time.time()
        
        try:
            response = await call_next(request)
            
            # Tính toán thời gian xử lý
            process_time = time.time() - start_time
            
            # Ghi log về response
            logger.info(
                "Request completed",
                method=request.method,
                path=request.url.path,
                status_code=response.status_code,
                processing_time=f"{process_time:.4f}s",
            )
            
            # Cập nhật metric
            REQUEST_COUNT.labels(
                method=request.method, 
                path=request.url.path,
                status_code=response.status_code
            ).inc()
            
            REQUEST_LATENCY.labels(
                method=request.method, 
                path=request.url.path
            ).observe(process_time)
            
            return response
            
        except Exception as e:
            # Tính toán thời gian xử lý
            process_time = time.time() - start_time
            
            # Ghi log về lỗi
            logger.exception(
                "Request failed",
                method=request.method,
                path=request.url.path,
                processing_time=f"{process_time:.4f}s",
                error=str(e),
            )
            
            # Cập nhật metric
            REQUEST_COUNT.labels(
                method=request.method, 
                path=request.url.path,
                status_code=500
            ).inc()
            
            REQUEST_LATENCY.labels(
                method=request.method, 
                path=request.url.path
            ).observe(process_time)
            
            # Ném lỗi để error handler xử lý
            raise

class RequestLoggingContextMiddleware(BaseHTTPMiddleware):
    """
    Middleware thêm thông tin context vào mỗi request để 
    ghi log với structlog
    """
    async def dispatch(self, request: Request, call_next):
        # Tạo ID duy nhất cho request
        request_id = str(int(time.time() * 1000))
        
        # Bind các biến context cho structlog
        structlog.contextvars.clear_contextvars()
        structlog.contextvars.bind_contextvars(
            request_id=request_id,
            method=request.method,
            path=request.url.path,
            client_ip=request.client.host if request.client else None,
        )
        
        # Thực thi request
        response = await call_next(request)
        
        # Thêm status code vào context
        structlog.contextvars.bind_contextvars(
            status_code=response.status_code,
        )
        
        return response

def setup_middleware(app: FastAPI):
    """
    Cài đặt middleware cho logging và monitoring
    """
    app.add_middleware(RequestLoggingContextMiddleware)
    app.add_middleware(LoggingMiddleware)
