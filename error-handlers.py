from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException
from asyncpg.exceptions import PostgresError
from pyspark.sql.utils import AnalysisException, IllegalArgumentException
from trino.exceptions import TrinoQueryError
import structlog

logger = structlog.get_logger(__name__)

class DatabaseConnectionError(Exception):
    """Lỗi kết nối cơ sở dữ liệu"""
    pass

class QueryBuildError(Exception):
    """Lỗi xây dựng truy vấn"""
    pass

class UnknownDataSourceError(Exception):
    """Lỗi khi nguồn dữ liệu không được hỗ trợ"""
    pass

def register_error_handlers(app: FastAPI):
    """Đăng ký các handler xử lý lỗi cho ứng dụng"""
    
    @app.exception_handler(StarletteHTTPException)
    async def http_exception_handler(request: Request, exc: StarletteHTTPException):
        """Xử lý HTTP exceptions"""
        logger.error("HTTP exception", 
                     path=request.url.path, 
                     status_code=exc.status_code, 
                     detail=exc.detail)
        return JSONResponse(
            status_code=exc.status_code,
            content={"error": exc.detail, "type": "http_error"}
        )

    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(request: Request, exc: RequestValidationError):
        """Xử lý lỗi validation từ pydantic"""
        error_details = []
        for error in exc.errors():
            loc = " -> ".join([str(x) for x in error["loc"]])
            error_details.append({
                "location": loc,
                "message": error["msg"],
                "type": error["type"]
            })
        
        logger.warning("Validation error", 
                       path=request.url.path, 
                       errors=error_details)
        
        return JSONResponse(
            status_code=422,
            content={
                "error": "Validation error",
                "type": "validation_error",
                "details": error_details
            }
        )

    @app.exception_handler(DatabaseConnectionError)
    async def db_connection_exception_handler(request: Request, exc: DatabaseConnectionError):
        """Xử lý lỗi kết nối cơ sở dữ liệu"""
        logger.error("Database connection error", 
                     path=request.url.path, 
                     error=str(exc))
        return JSONResponse(
            status_code=503,
            content={
                "error": "Database connection error",
                "type": "database_connection_error",
                "detail": str(exc)
            }
        )

    @app.exception_handler(PostgresError)
    async def postgres_exception_handler(request: Request, exc: PostgresError):
        """Xử lý lỗi từ PostgreSQL"""
        logger.error("PostgreSQL error", 
                     path=request.url.path, 
                     error=str(exc))
        return JSONResponse(
            status_code=500,
            content={
                "error": "Database error",
                "type": "postgres_error",
                "detail": str(exc)
            }
        )

    @app.exception_handler(AnalysisException)
    async def spark_analysis_exception_handler(request: Request, exc: AnalysisException):
        """Xử lý lỗi phân tích truy vấn từ Spark"""
        logger.error("Spark analysis error", 
                     path=request.url.path, 
                     error=str(exc))
        return JSONResponse(
            status_code=400,
            content={
                "error": "Spark query analysis error",
                "type": "spark_analysis_error",
                "detail": str(exc)
            }
        )

    @app.exception_handler(IllegalArgumentException)
    async def spark_argument_exception_handler(request: Request, exc: IllegalArgumentException):
        """Xử lý lỗi tham số không hợp lệ từ Spark"""
        logger.error("Spark argument error", 
                     path=request.url.path, 
                     error=str(exc))
        return JSONResponse(
            status_code=400,
            content={
                "error": "Spark illegal argument error",
                "type": "spark_argument_error",
                "detail": str(exc)
            }
        )

    @app.exception_handler(TrinoQueryError)
    async def trino_query_exception_handler(request: Request, exc: TrinoQueryError):
        """Xử lý lỗi truy vấn từ Trino"""
        logger.error("Trino query error", 
                     path=request.url.path, 
                     error=str(exc))
        return JSONResponse(
            status_code=400,
            content={
                "error": "Trino query error",
                "type": "trino_query_error",
                "detail": str(exc)
            }
        )

    @app.exception_handler(QueryBuildError)
    async def query_build_exception_handler(request: Request, exc: QueryBuildError):
        """Xử lý lỗi xây dựng truy vấn"""
        logger.error("Query build error", 
                     path=request.url.path, 
                     error=str(exc))
        return JSONResponse(
            status_code=400,
            content={
                "error": "Failed to build query",
                "type": "query_build_error",
                "detail": str(exc)
            }
        )

    @app.exception_handler(UnknownDataSourceError)
    async def unknown_datasource_exception_handler(request: Request, exc: UnknownDataSourceError):
        """Xử lý lỗi nguồn dữ liệu không được hỗ trợ"""
        logger.error("Unknown data source error", 
                     path=request.url.path, 
                     error=str(exc))
        return JSONResponse(
            status_code=400,
            content={
                "error": "Unknown data source",
                "type": "unknown_data_source_error",
                "detail": str(exc)
            }
        )

    @app.exception_handler(Exception)
    async def general_exception_handler(request: Request, exc: Exception):
        """Xử lý tất cả các loại lỗi khác"""
        logger.exception("Unhandled exception", 
                         path=request.url.path,
                         error=str(exc))
        return JSONResponse(
            status_code=500,
            content={
                "error": "Internal server error",
                "type": "server_error",
                "detail": str(exc) if app.debug else "An unexpected error occurred"
            }
        )
