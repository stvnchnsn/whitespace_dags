from google.cloud import storage


def get_blob(bucket_name, from_fp, to_fp):
    """
    Downloads a file from a Google Cloud Storage bucket to a local path.

    Parameters:
    bucket_name (str): The name of the GCS bucket.
    from_fp (str): The file path in the GCS bucket.
    to_fp (str): The local file path where the file will be downloaded.

    Returns:
    None
    """
    # Create a client object for interacting with Google Cloud Storage
    client = storage.Client()

    # Get the bucket directly by its name
    bucket = client.get_bucket(bucket_name)

    # Get the blob object representing the file in the bucket
    blob = bucket.get_blob(from_fp)

    # Download the file from the bucket to the local file path
    blob.download_to_filename(to_fp)