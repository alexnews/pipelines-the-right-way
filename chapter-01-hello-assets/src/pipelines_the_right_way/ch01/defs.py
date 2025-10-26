from dagster import Definitions
from .assets import raw_iris, iris_clean, iris_summary

defs = Definitions(assets=[raw_iris, iris_clean, iris_summary])
