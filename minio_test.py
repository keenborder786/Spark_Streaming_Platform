from minio import Minio
from minio.error import S3Error

import io
def main():
    # Create a client with the MinIO server playground, its access key
    # and secret key.
    client = Minio(
        "127.0.0.1:9000",
        access_key="user",
        secret_key="password",
        secure=False
    )

    print(client.list_buckets()) 

    result = client.put_object(
    "test", "object", io.BytesIO(b"hello"), 5,
)
    print(
        "created {0} object; etag: {1}, version-id: {2}".format(
            result.object_name, result.etag, result.version_id,
        ),
    )
        


if __name__ == "__main__":
    try:
        main()
    except S3Error as exc:
        print("error occurred.", exc)