import os
from dotenv import load_dotenv
from fastapi import APIRouter, Query
from pyspark.sql import SparkSession
from fastapi import Depends
from app.core.auth import get_current_user


load_dotenv()

router = APIRouter()

class ParquetPipeline:
    def __init__(self, app_name: str = "ParquetRouterPipeline"):
        aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
        aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")

        if not aws_access_key_id or not aws_secret_access_key:
            raise ValueError("Missing AWS credentials in environment variables")

        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
            .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
            .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
            .getOrCreate()

    def read_parquet(self, path: str):
        return self.spark.read.parquet(path).cache()

    def get_full_content(self, df, name: str):
        data = df.toPandas().to_dict(orient="records")
        return {
            "dataset": name,
            "row_count": len(data),
            "data": data
        }

pipeline = None

@router.on_event("startup")
def init_pipeline():
    global pipeline
    pipeline = ParquetPipeline()


@router.get("/full")
def get_full_parquet(
    path: str = Query(..., description="S3 path to the Parquet dataset"),
    username: str = Depends(get_current_user)
):
    try:
        df = pipeline.read_parquet(path)
        dataset_name = path.strip("/").split("/")[-1].replace("_parquet", "")
        return pipeline.get_full_content(df, dataset_name)
    except Exception as e:
        return {"error": str(e)}






    