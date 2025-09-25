from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from azure.storage.blob import (
    BlobServiceClient, 
    generate_blob_sas, BlobSasPermissions,
    generate_container_sas, ContainerSasPermissions
)
import requests, uuid, asyncio, time
from datetime import datetime, timedelta
import os

# -------------------------
# Config
# -------------------------
AZURE_TRANSLATOR_KEY = os.getenv("AZURE_TRANSLATOR_KEY")
AZURE_TRANSLATOR_ENDPOINT = os.getenv("AZURE_TRANSLATOR_ENDPOINT")  # https://<region>.api.cognitive.microsoft.com/translator/text/batch/v1.0
AZURE_STORAGE_ACCOUNT = os.getenv("AZURE_STORAGE_ACCOUNT")
AZURE_STORAGE_KEY = os.getenv("AZURE_STORAGE_KEY")
CONTAINER_NAME = "docs"

app = FastAPI()
blob_service_client = BlobServiceClient(
    f"https://{AZURE_STORAGE_ACCOUNT}.blob.core.windows.net", 
    credential=AZURE_STORAGE_KEY
)

# In-memory job tracker
jobs = {}

# -------------------------
# Request Models
# -------------------------
class UploadRequest(BaseModel):
    filename: str

class TranslationRequest(BaseModel):
    target_languages: list[str]

# -------------------------
# Helpers
# -------------------------

def generate_upload_sas(filename: str, hours_valid=1):
    """Generate SAS for uploading an original file."""
    unique_name = f"{uuid.uuid4().hex}_original_{filename}"
    expiry = datetime.utcnow() + timedelta(hours=hours_valid)
    sas_token = generate_blob_sas(
        account_name=AZURE_STORAGE_ACCOUNT,
        container_name=CONTAINER_NAME,
        blob_name=unique_name,
        account_key=AZURE_STORAGE_KEY,
        permission=BlobSasPermissions(write=True, create=True),
        expiry=expiry
    )
    url = f"https://{AZURE_STORAGE_ACCOUNT}.blob.core.windows.net/{CONTAINER_NAME}/{unique_name}?{sas_token}"
    return unique_name, url

def generate_download_sas(blob_name: str, hours_valid=1):
    """Generate SAS for downloading a translated file."""
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
    """Generate SAS for entire container for Translator API."""
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
    """Submit async translation job to Azure Document Translation."""
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

def rename_translated_blobs(prefix="translate_done."):
    """Rename translated files by prefixing them."""
    container_client = blob_service_client.get_container_client(CONTAINER_NAME)
    renamed = []
    for blob in container_client.list_blobs():
        if not blob.name.startswith(prefix) and not blob.name.endswith(".original"):  # crude filter
            new_name = f"{prefix}{blob.name}"
            src_url = f"https://{AZURE_STORAGE_ACCOUNT}.blob.core.windows.net/{CONTAINER_NAME}/{blob.name}"
            dest_blob = container_client.get_blob_client(new_name)
            dest_blob.start_copy_from_url(src_url)
            # wait until copy success
            while True:
                props = dest_blob.get_blob_properties()
                if props.copy.status == "success":
                    break
                time.sleep(0.5)
            container_client.get_blob_client(blob.name).delete_blob()
            renamed.append({"old": blob.name, "new": new_name})
    return renamed

# def rename_translated_blobs(job_status: dict, prefix="translate_done."):
#     """
#     Rename translated files using language code.
#     Example: fr.translate_done.filename.docx
#     """
#     container_client = blob_service_client.get_container_client(CONTAINER_NAME)
#     renamed = []

#     for doc in job_status.get("documents", []):
#         if doc.get("status") != "Succeeded":
#             continue
#         lang = doc.get("to")
#         translated_path = doc.get("path").split("/")[-1]  # blob name inside container
#         original_name = doc.get("sourcePath").split("/")[-1]

#         # New name format
#         new_name = f"{lang}.{prefix}{original_name}"

#         # Copy → Delete
#         src_url = f"https://{AZURE_STORAGE_ACCOUNT}.blob.core.windows.net/{CONTAINER_NAME}/{translated_path}"
#         dest_blob = container_client.get_blob_client(new_name)
#         dest_blob.start_copy_from_url(src_url)

#         # wait for copy to finish
#         while True:
#             props = dest_blob.get_blob_properties()
#             if props.copy.status == "success":
#                 break
#             time.sleep(0.5)

#         container_client.get_blob_client(translated_path).delete_blob()
#         renamed.append({"old": translated_path, "new": new_name, "lang": lang})

#     return renamed


async def poll_translation_job(job_id: str):
    try:
        while True:
            status = await asyncio.to_thread(get_job_status, job_id)
            jobs[job_id]["status"] = status.get("status")
            jobs[job_id]["raw_status"] = status
            if status.get("status") in ("Succeeded", "Failed", "Cancelled"):
                if status.get("status") == "Succeeded":
                    renamed = await asyncio.to_thread(rename_translated_blobs, "translate_done.")
                    jobs[job_id]["renamed"] = renamed
                break
            await asyncio.sleep(5)
    except Exception as exc:
        jobs[job_id]["status"] = "Error"
        jobs[job_id]["error"] = str(exc)

# async def poll_translation_job(job_id: str):
#     try:
#         while True:
#             status = await asyncio.to_thread(get_job_status, job_id)
#             jobs[job_id]["status"] = status.get("status")
#             jobs[job_id]["raw_status"] = status

#             if status.get("status") in ("Succeeded", "Failed", "Cancelled"):
#                 if status.get("status") == "Succeeded":
#                     renamed = await asyncio.to_thread(rename_translated_blobs, status, "translate_done.")
#                     jobs[job_id]["renamed"] = renamed
#                 break
#             await asyncio.sleep(5)
#     except Exception as exc:
#         jobs[job_id]["status"] = "Error"
#         jobs[job_id]["error"] = str(exc)


# -------------------------
# API Endpoints
# -------------------------

@app.post("/request-upload-sas")
def request_upload_sas(req: UploadRequest):
    blob_name, sas_url = generate_upload_sas(req.filename)
    return {"blob_name": blob_name, "upload_url": sas_url}

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
