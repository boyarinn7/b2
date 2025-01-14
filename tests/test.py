import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

# Конфигурация
ENDPOINT = "https://s3.us-east-005.backblazeb2.com"
BUCKET_NAME = "boyarinnbotbucket"
KEY_ID = "00577030c4f964a0000000002"
APPLICATION_KEY = "K005i/y4ymXbsv4nrAkBIqgFBIGR5RE"


def test_b2_connection():
    try:
        # Создание клиента S3 с параметрами Backblaze B2
        s3_client = boto3.client(
            's3',
            endpoint_url=ENDPOINT,
            aws_access_key_id=KEY_ID,
            aws_secret_access_key=APPLICATION_KEY
        )

        # Список объектов в бакете
        response = s3_client.list_objects_v2(Bucket=BUCKET_NAME)

        if 'Contents' in response:
            print("Объекты в бакете:")
            for obj in response['Contents']:
                print(f" - {obj['Key']}")
        else:
            print("Бакет пуст или отсутствуют права на доступ.")

    except NoCredentialsError:
        print("Ошибка: неверные или отсутствующие учетные данные.")
    except PartialCredentialsError:
        print("Ошибка: указаны не все учетные данные.")
    except Exception as e:
        print(f"Произошла ошибка: {e}")


if __name__ == "__main__":
    test_b2_connection()
