import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, status

from dbt_fastapi.routes import dbt_router
from dbt_fastapi.exceptions import DbtFastApiError
from dbt_fastapi.exception_handlers import dbt_error_handler, generic_exception_handler
from dbt_fastapi.config import initialize_dbt_config, get_dbt_config


# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan manager.

    Handles startup and shutdown events:
    - Startup: Initialize and cache dbt configuration
    - Shutdown: Cleanup (if needed)
    """
    # Startup
    logger.info("Application starting up...")
    try:
        # Initialize dbt configuration (discovers and caches paths)
        config = initialize_dbt_config()
        logger.info(
            f"✓ dbt configuration initialized:\n"
            f"  Profiles: {config.dbt_profiles_dir}\n"
            f"  Project: {config.dbt_project_dir}\n"
            f"  Default target: {config.dbt_target_default}"
        )
    except Exception as e:
        logger.error(f"✗ Failed to initialize dbt configuration: {e}")
        raise

    logger.info("Application startup complete")

    # Run application code
    yield

    # Shutdown
    logger.info("Application shutting down...")


app = FastAPI(
    title="dbt FastAPI Wrapper",
    version="0.4.0",
    description="Exposes dbt CLI over HTTP with cached configuration",
    lifespan=lifespan,
)

# Register exception handlers
app.add_exception_handler(DbtFastApiError, dbt_error_handler)
app.add_exception_handler(Exception, generic_exception_handler)


@app.get("/", tags=["info"])
def root():
    """Root endpoint with basic application info."""
    return {"status": "running", "version": app.version, "author": "Lorcan Rae"}


@app.get("/health", tags=["health"])
async def health_check() -> dict[str, str]:
    """
    Health check endpoint for containerised runtime.

    Returns 200 if the application is running.
    This is a simple liveness check.
    """
    return {"status": "healthy"}


@app.get("/readiness", tags=["health"])
async def readiness_check() -> dict[str, str]:
    """
    Readiness check endpoint for containerised runtime.

    Verifies that:
    1. Configuration is initialized
    2. dbt config files exist

    Returns:
        200: Application is ready to serve requests
        503: Application is not ready (configuration issues)
    """
    try:
        config = get_dbt_config()

        # Verify configuration is valid
        config.validate_configuration()

        return {
            "status": "ready",
            "profiles_dir": config.dbt_profiles_dir,
            "project_dir": config.dbt_project_dir,
        }
    except Exception as e:
        logger.error(f"Readiness check failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"status": "not_ready", "error": str(e)},
        )


# Include dbt routes
app.include_router(dbt_router)
