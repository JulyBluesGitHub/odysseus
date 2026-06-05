"""
ChromaDB HTTP bridge for Windows (chromadb 0.6.3).
Wraps chromadb.PersistentClient in a FastAPI server on port 8100.
"""
import os
from pathlib import Path

import uvicorn
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
import chromadb
from chromadb.config import Settings

# ---- Config ----
DATA_DIR = Path(os.environ.get("CHROMA_PERSIST_DIRECTORY", "./chroma_data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)

app = FastAPI(title="ChromaDB Bridge")
client = chromadb.PersistentClient(
    path=str(DATA_DIR),
    settings=Settings(anonymized_telemetry=False),
)

# UUID → name mapping (chromadb HTTP API identifies collections by UUID)
_uuid_to_name: dict[str, str] = {}


def _get_collection(identifier: str):
    """Look up a collection by name or UUID."""
    name = _uuid_to_name.get(identifier, identifier)
    try:
        return client.get_collection(name)
    except Exception:
        raise HTTPException(status_code=404, detail=f"Collection {identifier} not found")


# ---- Models ----
class AddRequest(BaseModel):
    ids: list[str]
    embeddings: list[list[float]] | None = None
    documents: list[str] | None = None
    metadatas: list[dict] | None = None


class QueryRequest(BaseModel):
    query_embeddings: list[list[float]] | None = None
    query_texts: list[str] | None = None
    n_results: int = 10
    where: dict | None = None
    where_document: dict | None = None
    include: list[str] = ["documents", "metadatas", "distances"]


class DeleteRequest(BaseModel):
    ids: list[str] | None = None
    where: dict | None = None


class CreateCollectionRequest(BaseModel):
    name: str
    metadata: dict | None = None
    get_or_create: bool = False
    
    class Config:
        extra = "allow"  # Accept configuration, embedding_function, etc.


# ---- Health (what chromadb.HttpClient.heartbeat() calls) ----
@app.get("/api/v1/heartbeat")
@app.get("/api/v2/heartbeat")
async def heartbeat():
    return {"nanosecond heartbeat": 0}


# ---- Auth (Odysseus expects UserIdentity format) ----
@app.get("/api/v2/auth/identity")
@app.get("/api/v1/auth/identity")
async def auth_identity():
    return {"user_id": "odysseus", "tenant": "default_tenant", "databases": ["default_database"]}


# ---- Tenants ----
@app.get("/api/v2/tenants/{tenant_name}")
@app.get("/api/v1/tenants/{tenant_name}")
async def get_tenant(tenant_name: str):
    return {"name": tenant_name}


# ---- Databases ----
@app.get("/api/v2/tenants/{tenant_name}/databases/{db_name}")
@app.get("/api/v1/tenants/{tenant_name}/databases/{db_name}")
@app.get("/api/v2/databases/{db_name}")
@app.get("/api/v1/databases/{db_name}")
async def get_database(tenant_name: str = "default_tenant", db_name: str = "default_database"):
    return {"id": "00000000-0000-0000-0000-000000000000", "name": db_name, "tenant": tenant_name}


# ---- Collections (also under tenant/database path) ----
@app.post("/api/v1/tenants/{tenant_name}/databases/{db_name}/collections")
@app.post("/api/v2/tenants/{tenant_name}/databases/{db_name}/collections")
async def create_collection_scoped(tenant_name: str, db_name: str, request: Request):
    try:
        body = await request.json()
        name = body.get("name", "")
        metadata = body.get("metadata")
        get_or_create = body.get("get_or_create", False)
        col = client.get_or_create_collection(name=name, metadata=metadata) if get_or_create else client.create_collection(name=name, metadata=metadata)
        _uuid_to_name[str(col.id)] = name
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {
        "id": str(col.id),
        "name": col.name,
        "configuration_json": {"_type": "CollectionConfigurationInternal"},
        "metadata": col.metadata,
        "dimension": None,
        "tenant": tenant_name,
        "database": db_name,
        "version": 0,
        "log_position": 0,
    }


@app.get("/api/v1/tenants/{tenant_name}/databases/{db_name}/collections")
@app.get("/api/v2/tenants/{tenant_name}/databases/{db_name}/collections")
async def list_collections_scoped(tenant_name: str, db_name: str):
    names = client.list_collections()
    result = []
    for name in names:
        try:
            c = client.get_collection(name)
            result.append({
                "id": str(c.id), "name": c.name,
                "configuration_json": {"_type": "CollectionConfigurationInternal"},
                "metadata": c.metadata, "dimension": None, "tenant": tenant_name,
                "database": db_name, "version": 0, "log_position": 0,
            })
        except Exception:
            continue
    return result


# ---- Collections (flat path, also works) ----
@app.post("/api/v1/collections")
@app.post("/api/v2/collections")
async def create_collection(req: CreateCollectionRequest):
    try:
        col = client.get_or_create_collection(name=req.name, metadata=req.metadata) if req.get_or_create else client.create_collection(name=req.name, metadata=req.metadata)
        _uuid_to_name[str(col.id)] = req.name
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {
        "id": str(col.id),
        "name": col.name,
        "configuration_json": {"_type": "CollectionConfigurationInternal"},
        "metadata": col.metadata,
        "dimension": None,
        "tenant": "default_tenant",
        "database": "default_database",
        "version": 0,
        "log_position": 0,
    }


@app.get("/api/v1/collections")
@app.get("/api/v2/collections")
async def list_collections():
    names = client.list_collections()
    result = []
    for name in names:
        try:
            c = client.get_collection(name)
            result.append({
                "id": str(c.id), "name": c.name,
                "configuration_json": {"_type": "CollectionConfigurationInternal"},
                "metadata": c.metadata, "dimension": None, "tenant": "default_tenant",
                "database": "default_database", "version": 0, "log_position": 0,
            })
        except Exception:
            continue
    return result


@app.get("/api/v1/collections/{collection_name}")
@app.get("/api/v2/collections/{collection_name}")
async def get_collection(collection_name: str):
    c = _get_collection(collection_name)
    return {"name": c.name, "id": str(c.id), "metadata": c.metadata, "count": c.count()}


@app.delete("/api/v1/collections/{collection_name}")
@app.delete("/api/v2/collections/{collection_name}")
async def delete_collection(collection_name: str):
    try:
        client.delete_collection(collection_name)
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))
    return {"status": "deleted"}


# ---- Documents ----
@app.post("/api/v1/collections/{collection_name}/add")
@app.post("/api/v2/collections/{collection_name}/add")
async def add_documents(collection_name: str, req: AddRequest):
    c = _get_collection(collection_name)
    c.add(ids=req.ids, embeddings=req.embeddings, documents=req.documents, metadatas=req.metadatas)
    return {"status": "ok"}


@app.post("/api/v1/collections/{collection_name}/query")
@app.post("/api/v2/collections/{collection_name}/query")
async def query_collection(collection_name: str, req: QueryRequest):
    c = _get_collection(collection_name)
    kwargs = dict(n_results=req.n_results, include=req.include)
    if req.where:
        kwargs["where"] = req.where
    if req.where_document:
        kwargs["where_document"] = req.where_document
    if req.query_embeddings:
        kwargs["query_embeddings"] = req.query_embeddings
    elif req.query_texts:
        kwargs["query_texts"] = req.query_texts
    else:
        raise HTTPException(status_code=400, detail="query_embeddings or query_texts required")
    return c.query(**kwargs)


@app.post("/api/v1/collections/{collection_name}/get")
@app.post("/api/v2/collections/{collection_name}/get")
async def get_documents(collection_name: str):
    """Get all documents in a collection (simplified)."""
    c = _get_collection(collection_name)
    return c.get()


@app.post("/api/v1/collections/{collection_name}/delete")
@app.post("/api/v2/collections/{collection_name}/delete")
async def delete_documents(collection_name: str, req: DeleteRequest):
    c = _get_collection(collection_name)
    kwargs = {}
    if req.ids:
        kwargs["ids"] = req.ids
    if req.where:
        kwargs["where"] = req.where
    c.delete(**kwargs)
    return {"status": "ok"}


@app.get("/api/v1/collections/{collection_name}/count")
@app.get("/api/v2/collections/{collection_name}/count")
async def count_documents(collection_name: str):
    c = _get_collection(collection_name)
    return c.count()


# ---- Count (scoped) ----
@app.get("/api/v1/tenants/{tenant_name}/databases/{db_name}/collections/{collection_name}/count")
@app.get("/api/v2/tenants/{tenant_name}/databases/{db_name}/collections/{collection_name}/count")
async def count_documents_scoped(tenant_name: str, db_name: str, collection_name: str):
    c = _get_collection(collection_name)
    return c.count()


# ---- Reset (for testing) ----
@app.post("/api/v1/reset")
@app.post("/api/v2/reset")
async def reset():
    client.reset()
    return {"status": "reset"}


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8100, log_level="warning")
