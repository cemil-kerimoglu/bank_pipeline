import os
from pyspark.sql import SparkSession
import pyspark


def get_spark_session(app_name: str, spark_conf_path: str = None) -> SparkSession:
    """
    Build and return a SparkSession configured for S3 access via credentials
    stored in .env file.
    """
    builder = SparkSession.builder.appName(app_name)

    # This picks up existing defaults from a spark-defaults.conf file if provided
    if spark_conf_path and os.path.isfile(spark_conf_path):
        with open(spark_conf_path) as conf_file:
            for line in conf_file:
                # Skip comments & blank lines
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                # spark-defaults.conf uses whitespace as delimiter between key & value
                if " " in line:
                    key, value = line.split(None, 1)
                    builder = builder.config(key, value)

    # ------------------------------------------------------------------
    # Gather AWS credentials – support both common naming variants
    # ------------------------------------------------------------------
    access_key  = os.getenv("AWS_ACCESS_KEY_ID") or os.getenv("AWS_ACCESS_KEY")
    secret_key  = os.getenv("AWS_SECRET_ACCESS_KEY") or os.getenv("AWS_ACCESS_SECRET")
    aws_region  = os.getenv("AWS_REGION", "eu-central-1")

    if not (access_key and secret_key):
        raise EnvironmentError(
            "AWS credentials not found in environment variables. "
            "Set either AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY or "
            "AWS_ACCESS_KEY / AWS_ACCESS_SECRET before starting the pipeline."
        )

    builder = builder \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{aws_region}.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.access.key", access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
        .config("spark.sql.parquet.enableVectorizedReader", "true")

    # ------------------------------------------------------------------
    # Ensure Hadoop AWS + AWS SDK JARs are on the class-path
    # ------------------------------------------------------------------
    # Spark downloaded by PyPI (or conda) is the *without-Hadoop* build; the
    # S3AFileSystem implementation therefore lives in the optional hadoop-aws
    # module. We need to pull it (and the shaded AWS SDK) via the built-in Maven
    # resolver so users don’t have to manage jars manually.

    # Environment detection: we use different versions based on PySpark version
    pyspark_version = pyspark.__version__
    is_container = os.path.exists('/.dockerenv') or os.getenv('DOCKER_ENV') == 'true'
    
    # Debug logging
    print(f"DEBUG: PySpark version: {pyspark_version}")
    print(f"DEBUG: Is container: {is_container}")
    print(f"DEBUG: /.dockerenv exists: {os.path.exists('/.dockerenv')}")
    print(f"DEBUG: DOCKER_ENV: {os.getenv('DOCKER_ENV')}")
    
    if pyspark_version.startswith('4.') and not is_container:
        # Local environment with PySpark 4.x - we use working versions
        hadoop_ver = "3.4.0"
        aws_sdk_ver = "1.12.640"
        print("DEBUG: Using LOCAL versions (Hadoop 3.4.0 + AWS SDK 1.12.640)")
    else:
        # Docker environment with PySpark 3.5.x - we use conservative versions
        hadoop_ver = "3.3.4" 
        aws_sdk_ver = "1.12.262"
        print("DEBUG: Using DOCKER versions (Hadoop 3.3.4 + AWS SDK 1.12.262)")

    packages = (
        f"org.apache.hadoop:hadoop-client-runtime:{hadoop_ver},"
        f"org.apache.hadoop:hadoop-aws:{hadoop_ver},"
        f"com.amazonaws:aws-java-sdk-bundle:{aws_sdk_ver}"
    )

    builder = builder.config("spark.jars.packages", packages)

    jvm_open_flag = "--add-opens java.base/javax.security.auth=ALL-UNNAMED"
    builder = builder \
        .config("spark.driver.extraJavaOptions", jvm_open_flag) \
        .config("spark.executor.extraJavaOptions", jvm_open_flag)

    return builder.getOrCreate()
