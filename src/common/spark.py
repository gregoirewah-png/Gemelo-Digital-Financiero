"""Una configuración Spark/Hadoop compatible para todos los jobs."""
import os

from src.common.config import required


def spark_session(name):
    from pyspark.sql import SparkSession
    builder = (
        SparkSession.builder.appName(name)
        .master(os.getenv("SPARK_MASTER", "local[2]"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.snapshotPartitions", "2")
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "1g"))
        .config("spark.hadoop.fs.s3a.endpoint", required("MINIO_ENDPOINT"))
        .config("spark.hadoop.fs.s3a.access.key", required("MINIO_ROOT_USER"))
        .config("spark.hadoop.fs.s3a.secret.key", required("MINIO_ROOT_PASSWORD"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.connection.timeout", "600000")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "600000")
        .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60")
        .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400")
    )
    # La imagen incluye los JAR. Fuera de Docker se resuelven con Ivy.
    if not os.getenv("SPARK_JARS_BUNDLED"):
        builder = builder.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,io.delta:delta-spark_2.12:3.2.0")
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_lake(spark, path):
    """Lee la versión activa Delta y admite las particiones Parquet anteriores."""
    from delta.tables import DeltaTable
    if DeltaTable.isDeltaTable(spark, path):
        return spark.read.format("delta").load(path)
    return spark.read.parquet(path)


def write_lake(frame, path):
    """Publicación ACID de una partición analítica de la corrida."""
    from delta.tables import DeltaTable
    spark = frame.sparkSession
    location = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(path)
    filesystem = location.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    if filesystem.exists(location) and not DeltaTable.isDeltaTable(spark, path):
        DeltaTable.convertToDelta(spark, f"parquet.`{path}`")
    frame.coalesce(1).write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
