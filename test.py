from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from azure.storage.blob import (
    BlobServiceClient,
    generate_blob_sas, BlobSasPermissions,
    generate_container_sas, ContainerSasPermissions
)
import requests, asyncio, os
from datetime import datetime, timedelta

# -------------------------
# Config
# -------------------------
AZURE_TRANSLATOR_KEY = os.getenv("AZURE_TRANSLATOR_KEY")
AZURE_TRANSLATOR_ENDPOINT = os.getenv("AZURE_TRANSLATOR_ENDPOINT")  # e.g. https://<region>.api.cognitive.microsofttranslator.com/translator/text/batch/v1.0
AZURE_STORAGE_ACCOUNT = os.getenv("AZURE_STORAGE_ACCOUNT")
AZURE_STORAGE_KEY = os.getenv("AZURE_STORAGE_KEY")
CONTAINER_NAME = "docs"

app = FastAPI()
blob_service_client = BlobServiceClient(
    f"https://{AZURE_STORAGE_ACCOUNT}.blob.core.windows.net",
    credential=AZURE_STORAGE_KEY
)

jobs = {}

# -------------------------
# Models
# -------------------------
class UploadRequest(BaseModel):
    filename: str  # frontend already renames with UUID

class TranslationRequest(BaseModel):
    target_languages: list[str]

# -------------------------
# Helpers
# -------------------------
def generate_upload_sas(filename: str, hours_valid=1):
    """Generate SAS for uploading a file with the given name."""
    expiry = datetime.utcnow() + timedelta(hours=hours_valid)
    sas_token = generate_blob_sas(
        account_name=AZURE_STORAGE_ACCOUNT,
        container_name=CONTAINER_NAME,
        blob_name=filename,
        account_key=AZURE_STORAGE_KEY,
        permission=BlobSasPermissions(write=True, create=True),
        expiry=expiry
    )
    url = f"https://{AZURE_STORAGE_ACCOUNT}.blob.core.windows.net/{CONTAINER_NAME}/{filename}?{sas_token}"
    return url

def generate_download_sas(blob_name: str, hours_valid=1):
    """Generate SAS for downloading a file."""
    expiry = datetime.utcnow() + timedelta(hours=hours_valid)
    sas_token = generate_blob_sas(
        account_name=AZURE_STORAGE_ACCOUNT,
        container_name=CONTAINER_NAME,
        blob_name=blob_name,
        account_key=AZURE_STORAGE_KEY,
        permission=BlobSasPermissions(read=True),
        expiry=expiry
    )
    return f"https://{AZURE_STORAGE_ACCOUNT}.blob.core.windows.net/{CONTAINER_NAME}/{blob_name}?{sas_token}"

def generate_container_sas_url(hours_valid=2):
    """Generate container SAS for Translator job (read+write)."""
    expiry = datetime.utcnow() + timedelta(hours=hours_valid)
    sas_token = generate_container_sas(
        account_name=AZURE_STORAGE_ACCOUNT,
        container_name=CONTAINER_NAME,
        account_key=AZURE_STORAGE_KEY,
        permission=ContainerSasPermissions(read=True, list=True, write=True, create=True),
        expiry=expiry
    )
    return f"https://{AZURE_STORAGE_ACCOUNT}.blob.core.windows.net/{CONTAINER_NAME}?{sas_token}"

def submit_translation_job(languages: list[str]):
    """Start translation job using same container for source+target."""
    container_sas_url = generate_container_sas_url()
    payload = {
        "inputs": [
            {
                "source": { "sourceUrl": container_sas_url },
                "targets": [
                    {"targetUrl": container_sas_url, "language": lang}
                    for lang in languages
                ]
            }
        ]
    }
    headers = {
        "Ocp-Apim-Subscription-Key": AZURE_TRANSLATOR_KEY,
        "Content-Type": "application/json"
    }
    resp = requests.post(f"{AZURE_TRANSLATOR_ENDPOINT}/batches", json=payload, headers=headers)
    if resp.status_code != 202:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    return resp.headers["operation-location"].split("/")[-1]

def get_job_status(job_id: str):
    headers = {"Ocp-Apim-Subscription-Key": AZURE_TRANSLATOR_KEY}
    resp = requests.get(f"{AZURE_TRANSLATOR_ENDPOINT}/batches/{job_id}", headers=headers)
    return resp.json()

async def poll_translation_job(job_id: str):
    try:
        while True:
            status = await asyncio.to_thread(get_job_status, job_id)
            jobs[job_id]["status"] = status.get("status")
            jobs[job_id]["raw_status"] = status
            if status.get("status") in ("Succeeded", "Failed", "Cancelled"):
                break
            await asyncio.sleep(5)
    except Exception as exc:
        jobs[job_id]["status"] = "Error"
        jobs[job_id]["error"] = str(exc)

# -------------------------
# API Endpoints
# -------------------------
@app.post("/request-upload-sas")
def request_upload_sas(req: UploadRequest):
    sas_url = generate_upload_sas(req.filename)
    return {"upload_url": sas_url}

@app.post("/start-translation")
async def start_translation(req: TranslationRequest):
    job_id = submit_translation_job(req.target_languages)
    jobs[job_id] = {"status": "Running"}
    asyncio.create_task(poll_translation_job(job_id))
    return {"job_id": job_id}

@app.get("/check-status/{job_id}")
def check_status(job_id: str):
    return jobs.get(job_id, {"status": "Unknown job_id"})

@app.get("/download/{blob_name}")
def download_file(blob_name: str):
    url = generate_download_sas(blob_name)
    return {"download_url": url}