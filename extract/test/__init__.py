"""
Tweak path as due to script execution way in Airflow,
can't touch the original code
"""
import sys
import os

abs_path = os.path.dirname(os.path.realpath(__file__))
abs_path = abs_path[: abs_path.find("extract")] + "/extract/saas_usage_ping"
sys.path.append(abs_path)
