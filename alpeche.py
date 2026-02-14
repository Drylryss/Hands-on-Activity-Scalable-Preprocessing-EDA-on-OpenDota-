import os
import sys

# 1) Force the correct Java & Python paths
os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@17"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Add Java to the system path so Spark can find the 'java' command
os.environ["PATH"] = os.environ["JAVA_HOME"] + "/bin:" + os.environ["PATH"]

import pyspark
from pyspark.sql import SparkSession
# ---------- 1) Stop any existing SparkSession (if any) ----------
try:
    spark.stop()
    print("Stopped existing SparkSession.")
except Exception:
    print("No SparkSession to stop (ok).")

# ---------- 2) Clear PySpark global state (fixes dead Py4J gateway) ----------
try:
    from pyspark.context import SparkContext
    from pyspark.sql import SparkSession

    for k in ["PYSPARK_GATEWAY_PORT", "PYSPARK_GATEWAY_SECRET"]:
        if k in os.environ:
            os.environ.pop(k, None)
            print(f"Cleared env {k}")

    SparkContext._active_spark_context = None
    SparkContext._gateway = None
    SparkContext._jvm = None

    try:
        SparkSession._instantiatedContext = None
        SparkSession._activeSession = None
        SparkSession._defaultSession = None
    except Exception:
        pass

    print("✅ PySpark internals reset")
except Exception as e:
    print("⚠️ Reset step warning:", type(e).__name__, e)

# ---------- 3) Start Spark (stable local mode) ----------
from pyspark.sql import SparkSession

OUTPUT_ROOT = Path.home() / "opendota_processed"
OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

SPARK_LOCAL_DIR = OUTPUT_ROOT / "spark_local"
SPARK_LOCAL_DIR.mkdir(parents=True, exist_ok=True)

spark = (
    SparkSession.builder
    .appName("OpenDota_Notebook")
    .master("local[*]")  # stable; we can change later
    .config("spark.local.dir", str(SPARK_LOCAL_DIR))
    .config("spark.sql.shuffle.partitions", "400")
    .config("spark.sql.adaptive.enabled", "true")
    # IMPORTANT: avoid surprise broadcasts that can destabilize the JVM
    .config("spark.sql.autoBroadcastJoinThreshold", "-1")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("✅ Spark started:", spark.version)
print(" - Master:", spark.sparkContext.master)
print(" - Spark UI:", spark.sparkContext.uiWebUrl)
print("alive_check =", spark.range(1).count())

import os
from pathlib import Path

# 1) Stop if possible
try:
    spark.stop()
    print("Stopped SparkSession.")
except Exception:
    print("No SparkSession to stop (ok).")

# 2) Hard reset PySpark globals (dead gateway fix)
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

for k in ["PYSPARK_GATEWAY_PORT", "PYSPARK_GATEWAY_SECRET"]:
    os.environ.pop(k, None)

SparkContext._active_spark_context = None
SparkContext._gateway = None
SparkContext._jvm = None
SparkSession._instantiatedContext = None
SparkSession._activeSession = None
SparkSession._defaultSession = None

# 3) Start Spark (stable settings)
OUTPUT_ROOT = Path.home() / "opendota_processed"
OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
SPARK_LOCAL_DIR = OUTPUT_ROOT / "spark_local"
SPARK_LOCAL_DIR.mkdir(parents=True, exist_ok=True)

spark = (
    SparkSession.builder
    .appName("OpenDota_Preprocessing_Notebook_Stable")
    .master("local[4]")                 # <-- reduce threads for stability
    .config("spark.ui.enabled", "true")# <-- remove UI port issues while debugging
    .config("spark.local.dir", str(SPARK_LOCAL_DIR))
    .config("spark.sql.shuffle.partitions", "32")
    .config("spark.sql.adaptive.enabled", "false")  # <-- disable AQE for now
    .config("spark.sql.autoBroadcastJoinThreshold", "-1")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
print("✅ Spark started:", spark.version)
print(" - Master:", spark.sparkContext.master)
print(" - Spark UI:", spark.sparkContext.uiWebUrl)
print("alive_check =", spark.range(1).count())