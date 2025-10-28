#!/usr/bin/env python3
"""
Setup verification script for Serverless Spark IoT Framework
Tests all dependencies and configurations
"""

import sys
import platform

def print_header(text):
    print(f"\n{'='*60}")
    print(f"  {text}")
    print(f"{'='*60}\n")

def test_system_info():
    print_header("System Information")
    print(f"Platform: {platform.platform()}")
    print(f"Processor: {platform.processor()}")
    print(f"Python Version: {platform.python_version()}")
    print(f"Architecture: {platform.machine()}")

def test_imports():
    print_header("Testing Package Imports")
    
    results = []
    packages = [
        ("pyspark", "PySpark"),
        ("kafka", "Kafka-Python"),
        ("paho.mqtt.client", "Paho MQTT"),
        ("boto3", "Boto3 (AWS)"),
        ("torch", "PyTorch"),
        ("tensorflow", "TensorFlow"),
        ("numpy", "NumPy"),
        ("pandas", "Pandas"),
        ("sklearn", "Scikit-learn"),
        ("flask", "Flask"),
        ("streamlit", "Streamlit"),
    ]
    
    for module_name, display_name in packages:
        try:
            module = __import__(module_name)
            version = getattr(module, "__version__", "unknown")
            print(f"✅ {display_name:20s} : {version}")
            results.append(True)
        except ImportError as e:
            print(f"❌ {display_name:20s} : NOT INSTALLED")
            results.append(False)
    
    return all(results)

def test_spark():
    print_header("Testing PySpark")
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder \
            .appName("SetupTest") \
            .master("local[2]") \
            .getOrCreate()
        
        df = spark.createDataFrame([(1, "test")], ["id", "value"])
        count = df.count()
        
        spark.stop()
        
        print(f"✅ Spark Session Created Successfully")
        print(f"✅ Test DataFrame Count: {count}")
        return True
    except Exception as e:
        print(f"❌ Spark Test Failed: {e}")
        return False

def test_pytorch_m2():
    print_header("Testing PyTorch M2 Optimization")
    try:
        import torch
        print(f"PyTorch Version: {torch.__version__}")
        print(f"MPS (Metal) Available: {torch.backends.mps.is_available()}")
        print(f"MPS Built: {torch.backends.mps.is_built()}")
        
        if torch.backends.mps.is_available():
            device = torch.device("mps")
            x = torch.randn(3, 3, device=device)
            print(f"✅ Successfully created tensor on MPS device")
            print(f"Tensor shape: {x.shape}")
            return True
        else:
            print("⚠️  MPS not available, using CPU")
            return True
    except Exception as e:
        print(f"❌ PyTorch M2 test failed: {e}")
        return False

def test_tensorflow_m2():
    print_header("Testing TensorFlow M2 Optimization")
    try:
        import tensorflow as tf
        print(f"TensorFlow Version: {tf.__version__}")
        
        # Check for Metal plugin
        gpus = tf.config.list_physical_devices('GPU')
        print(f"GPUs Available: {len(gpus)}")
        
        if gpus:
            for gpu in gpus:
                print(f"  - {gpu}")
            print("✅ TensorFlow Metal GPU acceleration enabled")
        else:
            print("⚠️  No GPU detected, using CPU")
        
        # Simple test
        x = tf.constant([[1.0, 2.0], [3.0, 4.0]])
        print(f"✅ Created TensorFlow tensor: {x.shape}")
        return True
    except Exception as e:
        print(f"❌ TensorFlow test failed: {e}")
        return False

def main():
    print("\n" + "🚀 "*20)
    print("  Serverless Spark IoT Framework - Setup Verification")
    print("�� "*20)
    
    test_system_info()
    
    imports_ok = test_imports()
    spark_ok = test_spark()
    pytorch_ok = test_pytorch_m2()
    tensorflow_ok = test_tensorflow_m2()
    
    print_header("Summary")
    
    if all([imports_ok, spark_ok, pytorch_ok, tensorflow_ok]):
        print("✅ All tests passed! Setup is complete.")
        print("\n🎉 Ready to start building the framework!")
        return 0
    else:
        print("❌ Some tests failed. Please check the output above.")
        print("\nTroubleshooting:")
        print("1. Make sure conda environment is activated: conda activate spark-iot")
        print("2. Try reinstalling packages: conda env update -f environment.yml")
        print("3. Check Docker is running: docker ps")
        return 1

if __name__ == "__main__":
    sys.exit(main())
