from pyspark.sql import SparkSession

from processing.common import config

BIGLAKE_REST_URI = "https://biglake.googleapis.com/iceberg/v1/restcatalog"
KAFKA_LOGIN_HANDLER = "com.google.cloud.hosted.kafka.auth.GcpLoginCallbackHandler"
KAFKA_JAAS = "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;"


def catalog_configs():
    prefix = f"spark.sql.catalog.{config.CATALOG}"
    return {
        prefix: "org.apache.iceberg.spark.SparkCatalog",
        f"{prefix}.type": "rest",
        f"{prefix}.uri": BIGLAKE_REST_URI,
        f"{prefix}.warehouse": f"gs://{config.BUCKET}",
        f"{prefix}.header.x-goog-user-project": config.PROJECT_ID,
        f"{prefix}.rest.auth.type": "org.apache.iceberg.gcp.auth.GoogleAuthManager",
        f"{prefix}.io-impl": "org.apache.iceberg.gcp.gcs.GCSFileIO",
        f"{prefix}.rest-metrics-reporting-enabled": "false",
    }


def spark_configs():
    return {
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.session.timeZone": "UTC",
        **catalog_configs(),
    }


def create_spark(app_name):
    builder = SparkSession.builder.appName(app_name)
    for key, value in spark_configs().items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


def kafka_options():
    """Managed Kafka auth: SASL_SSL + OAUTHBEARER with the Google login handler (uses ADC)."""
    return {
        "kafka.bootstrap.servers": config.KAFKA_BOOTSTRAP,
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "OAUTHBEARER",
        "kafka.sasl.login.callback.handler.class": KAFKA_LOGIN_HANDLER,
        "kafka.sasl.jaas.config": KAFKA_JAAS,
    }


def log_versions(spark):
    scala = spark.sparkContext._jvm.scala.util.Properties.versionNumberString()
    print(f"✔ Spark {spark.version}, Scala {scala}")
