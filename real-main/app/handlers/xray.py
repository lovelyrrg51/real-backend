# https://docs.aws.amazon.com/xray/latest/devguide/xray-sdk-python.html
import boto3  # noqa: F401
import botocore  # noqa: F401
import requests  # noqa: F401
from aws_xray_sdk.core import patch_all  # noqa: F401
from aws_xray_sdk.core import xray_recorder  # noqa: F401
