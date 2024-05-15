from dotenv import load_dotenv
import os
import sys
import ftfy


def setup_env():
    load_dotenv()
    SPARK_HOME = os.getenv("SPARK_HOME")
    JAVA_HOME = os.getenv("JAVA_HOME")
    # Set the environment variables
    os.environ["PATH"] = os.pathsep.join([
        os.path.join(SPARK_HOME, "bin"),
        os.environ["PATH"],
    ])
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


def validate_env():
    SPARK_HOME = os.getenv("SPARK_HOME")
    JAVA_HOME = os.getenv("JAVA_HOME")
    # Ensure paths exist
    if not os.path.exists(SPARK_HOME):
        raise ValueError(f"SPARK_HOME path does not exist: {SPARK_HOME}")
    if not os.path.exists(JAVA_HOME):
        raise ValueError(f"JAVA_HOME path does not exist: {JAVA_HOME}")
    
    # check whether PATH env variable contains SPARK_HOME/bin
    if not any(SPARK_HOME in path for path in os.environ["PATH"].split(os.pathsep)):
        raise ValueError(f"PATH does not contain {SPARK_HOME}/bin")
    
    # check whether PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON are set
    if not os.environ["PYSPARK_PYTHON"]:
        raise ValueError("PYSPARK_PYTHON is not set")
    if not os.environ["PYSPARK_DRIVER_PYTHON"]:
        raise ValueError("PYSPARK_DRIVER_PYTHON is not set")


def print_env():
    # Debug prints to verify environment settings
    print(f"SPARK_HOME = {os.environ['SPARK_HOME']}")
    print(f"JAVA_HOME = {os.environ['JAVA_HOME']}")
    print(f"PATH = {os.environ['PATH']}")
    print(f"PYSPARK_PYTHON = {os.environ['PYSPARK_PYTHON']}")
    print(f"PYSPARK_DRIVER_PYTHON = {os.environ['PYSPARK_DRIVER_PYTHON']}") 


# Define the function to fix encoding issues
def fix_encoding(s: str) -> str:
    if s is not None:
        try:
            # Attempt to fix common double-encoding issues
            fixed = s.encode('latin1').decode('utf-8')
            return fixed
        except Exception as e:
            print(f"Error decoding: {e}. Trying ftfy...")
        
        try:
            # Attempt to fix using ftfy
            fixed = ftfy.fix_text(s)
            return fixed
        except Exception as e:
            print(f"ftfy failed: {e}. Returning original string.")
            return s
    return s
