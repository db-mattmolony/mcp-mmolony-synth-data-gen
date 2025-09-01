from pathlib import Path
from mcp.server.fastmcp import FastMCP
from fastapi import FastAPI
from fastapi.responses import FileResponse
from databricks.sdk.core import Config
from databricks import sql
from .prompts import load_prompts
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv() 



cfg = Config()



STATIC_DIR = Path(__file__).parent / "static"

# Create an MCP server
mcp = FastMCP("Custom MCP Server on Databricks Apps for creating")

# Load prompts and tools
load_prompts(mcp)

# Add an addition tool
@mcp.tool()
def add(a: int, b: int) -> int:
    """Add two numbers"""
    return a + b


@mcp.tool()
def create_catalog(catalog_name: str) -> str:
    """
    Create a new catalog in Databricks using a SQL warehouse
    """
    try:
        query = f"CREATE CATALOG IF NOT EXISTS {catalog_name};"
        with sql.connect(
            server_hostname=cfg.host,
            http_path=f"/sql/1.0/warehouses/{os.getenv('DATABRICKS_WAREHOUSE_ID')}",
            credentials_provider=lambda: cfg.authenticate
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query)
                return f"Catalog {catalog_name} created successfully"
    except Exception as e:
        return f"Error querying Databricks Warehouse: {e}"
    
@mcp.tool()
def create_schema(catalog_name: str, schema_name: str) -> str:
    """
    Create a new schema in Databricks catalog using a SQL warehouse
    """
    try:
        query = f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name};"
        with sql.connect(
            server_hostname=cfg.host,
            http_path=f"/sql/1.0/warehouses/{os.getenv('DATABRICKS_WAREHOUSE_ID')}",
            credentials_provider=lambda: cfg.authenticate
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query)
                return f"Schema {schema_name} created successfully"
    except Exception as e:
        return f"Error querying Databricks Warehouse: {e}"
    
    
@mcp.tool()
def create_metadata_tables(catalog_name: str, schema_name: str) -> str:
    """
    Create a new schema in Databricks catalog using a SQL warehouse
    """
    try:
        query = f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name};"
        with sql.connect(
            server_hostname=cfg.host,
            http_path=f"/sql/1.0/warehouses/{os.getenv('DATABRICKS_WAREHOUSE_ID')}",
            credentials_provider=lambda: cfg.authenticate
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query)
                return f"Schema {schema_name} created successfully"
    except Exception as e:
        return f"Error querying Databricks Warehouse: {e}"
    


# Add a dynamic greeting resource
@mcp.resource("greeting://{name}")
def get_greeting(name: str) -> str:
    """Get a personalized greeting"""
    return f"Hello, {name}!"


mcp_app = mcp.streamable_http_app()


# @mcp.prompt("synthetic-data-setup")
# def prompt_synthetic_data_setup() -> str:
#     """How to create synthetic data in Databricks: metadata-first flow."""
#     return """# Databricks MCP – Synthetic Data Metadata Setup

# ## Use Case
# This MCP server enables an LLM to generate synthetic datasets inside Databricks in a structured and governed way.  
# The flow is:
# 1. Create a catalog and schema to house new datasets.  
# 2. Populate metadata tables that describe schemas, tables, and columns.  
# 3. Use this metadata as the foundation for synthetic data generation.

# ## Metadata Tables
# Three core tables are required in every schema:

# 1. **_schema_metadata**  
#    - Stores schema-level information  
#    - Columns: unique schema identifier, schema name, description  

# 2. **_table_metadata**  
#    - Stores table-level information  
#    - Columns: table identifier, schema identifier, table name, table description, column name, column type, column description  

# 3. **_string_categories**  
#    - Stores category values for string-typed columns in `_table_metadata`  
#    - Columns: category identifier, table identifier, column name, category value  

# These tables are used to drive synthetic data creation, ensuring datasets are consistent, reproducible, and self-describing.
# """

mcp_app = mcp.streamable_http_app()


app = FastAPI(
    lifespan=lambda _: mcp.session_manager.run(),
)


@app.get("/", include_in_schema=False)
async def serve_index():
    return FileResponse(STATIC_DIR / "index.html")


app.mount("/", mcp_app)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "custom_server.app:app",
        host="0.0.0.0", 
        port=8000,
        reload=True
    )
