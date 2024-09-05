import math
import time
import os
import traceback
import boto3


# Function to calculate part size for multipart downloads
def _calculate_part_size(total_size):
    """
    Calculates the optimal part size for a given total file size.
    Returns the part size in bytes.
    """
    # The minimum part size is 5 GB
    MIN_PART_SIZE = 5 * 1024 * 1024 * 1024  # 5 GB in bytes
    # Calculate the ideal part size based on the file size
    ideal_part_size = total_size / 10000
    # Round up to the nearest power of 2 for efficient part handling
    power_of_2 = math.ceil(math.log2(ideal_part_size))
    part_size = int(math.pow(2, power_of_2))
    # Ensure the part size is at least 5 GB
    part_size = max(part_size, MIN_PART_SIZE)
    return part_size


# Function to handle download progress updates
def progress_download_callback(bytes_downloaded, total_bytes):
    """
    Callback to track the progress of the download.
    """
    global bytes_transferred
    global last_update

    bytes_transferred += bytes_downloaded
    timestamp = time.time()

    if timestamp - last_update >= job_update_interval:
        progress = float(bytes_downloaded) / float(total_bytes)
        last_update = timestamp
        print(('action', 'progress_update'), ('num_bytes', bytes_downloaded),
              ('bytes_transferred', bytes_transferred), ('progress', progress))
        return progress
    else:
        # Throttle the update to avoid frequent updates
        pass


# Function to assume an AWS role and return an S3 client
def get_s3_client_with_assume_role(role_arn, session_name):
    """
    Assume an AWS role and return a new S3 client with the assumed role credentials.
    """
    sts_client = boto3.client('sts')
    response = sts_client.assume_role(RoleArn=role_arn, RoleSessionName=session_name)
    credentials = response['Credentials']
    s3_client = boto3.client('s3',
                             aws_access_key_id=credentials['AccessKeyId'],
                             aws_secret_access_key=credentials['SecretAccessKey'],
                             aws_session_token=credentials['SessionToken'])
    return s3_client


# Function to download large S3 files using multipart download
def multi_part_download(s3_client, bucket, object_key, destination_path):
    """
    Download a large file from S3 in multiple parts and reassemble it locally.
    """
    start = time.time()
    print('Starting multi-part download')

    req_kwargs = {
        'Bucket': bucket,
        'Key': object_key
    }

    try:
        # Get the file size from S3
        file_size = s3_client.head_object(**req_kwargs)['ContentLength']
        part_size = _calculate_part_size(file_size)
        num_parts = int(file_size / part_size) + 1

        # Create the destination directory if it doesn't exist
        if not os.path.exists(destination_path):
            print('Creating destination directory:', destination_path)
            os.makedirs(destination_path)

        # Track downloaded parts
        parts_downloaded = []

        for i in range(num_parts):
            # Calculate byte range for current part
            start_byte = i * part_size
            end_byte = min((i + 1) * part_size - 1, file_size - 1)
            byte_range = f'bytes={start_byte}-{end_byte}'

            # Download current part
            part_filename = f'{object_key.split("/")[-1]}.part{i}'
            part_path = os.path.join(destination_path, part_filename)
            print('Downloading:', part_filename, 'Byte Range:', byte_range)

            with open(part_path, 'wb') as f:
                obj = s3_client.get_object(Bucket=bucket, Key=object_key, Range=byte_range)
                total_bytes = int(obj['ContentRange'].split('/')[-1])
                bytes_downloaded = 0
                while True:
                    chunk = obj['Body'].read(1024 * 1024)  # Read 1 MB chunks
                    if not chunk:
                        break
                    f.write(chunk)
                    bytes_downloaded += len(chunk)
                    progress_download_callback(bytes_downloaded, total_bytes)

            # Record successfully downloaded part
            parts_downloaded.append(i)
            print('Downloaded parts:', parts_downloaded)

        # Combine downloaded parts into a single file
        combined_file_path = os.path.join(destination_path, object_key.split('/')[-1])
        print('Combining downloaded parts into:', combined_file_path)

        with open(combined_file_path, 'wb') as f:
            for i in parts_downloaded:
                part_filename = f'{object_key.split("/")[-1]}.part{i}'
                part_path = os.path.join(destination_path, part_filename)
                with open(part_path, 'rb') as part:
                    f.write(part.read())

        # Clean up downloaded part files
        print('Cleaning up downloaded part files')
        for i in parts_downloaded:
            os.remove(os.path.join(destination_path, f'{object_key.split("/")[-1]}.part{i}'))

        # Print time taken to download
        end = time.time()
        print('Download completed in:', end - start, 'seconds')

    except Exception as e:
        print('Error during multipart download:', e, traceback.format_exc())
        raise Exception(f'Error during multipart download: {e}')


# Example usage
if __name__ == "__main__":
    role_arn = "arn:aws:iam::123456789012:role/YourRoleName"
    session_name = "DownloadSession"
    bucket = "your-bucket-name"
    object_key = "your/file/path/in/s3"
    destination_path = "/local/destination/path"

    s3_client = get_s3_client_with_assume_role(role_arn, session_name)
    multi_part_download(s3_client, bucket, object_key, destination_path)
