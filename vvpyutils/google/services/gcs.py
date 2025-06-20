from dotenv import load_dotenv
from google.cloud import storage


def empty_bucket():
    """Empties the GCS bucket by deleting all blobs in the specified folder."""
    client = storage.Client(project=GCP_PROJECT_ID)
    bucket = client.bucket(GCS_BUCKET_BQ_LZ)

    blobs = bucket.list_blobs(prefix=BUCKET_FOLDER)
    for blob in blobs:
        blob.delete()
        logger.info(
            f"🗑️ Deleted {blob.name} from gs://{GCS_BUCKET_BQ_LZ}/{BUCKET_FOLDER}"
        )
