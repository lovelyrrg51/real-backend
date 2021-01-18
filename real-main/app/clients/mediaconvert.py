import os

import boto3

AWS_ACCOUNT_ID = os.environ.get('AWS_ACCOUNT_ID')
MEDIACONVERT_ROLE_ARN = os.environ.get('MEDIACONVERT_ROLE_ARN')
S3_UPLOADS_BUCKET = os.environ.get('S3_UPLOADS_BUCKET')


class MediaConvertClient:

    job_template = 'System-Ott_Hls_Ts_Avc_Aac'

    def __init__(
        self,
        endpoint=None,
        aws_account_id=AWS_ACCOUNT_ID,
        role_arn=MEDIACONVERT_ROLE_ARN,
        uploads_bucket=S3_UPLOADS_BUCKET,
    ):
        assert role_arn, "MediaConvert role ARN is required"
        assert uploads_bucket, "S3 uploads bucket name is required"
        self.role_arn = role_arn
        self.uploads_bucket = uploads_bucket
        aws_region = boto3.Session().region_name
        self.job_template_arn = (
            f'arn:aws:mediaconvert:{aws_region}:{aws_account_id}:jobTemplates/{self.job_template}'
        )
        self.endpoint = endpoint

    @property
    def boto_client(self):
        if not hasattr(self, '_boto_client'):
            self.endpoint = self.endpoint or self.get_endpoint()
            self._boto_client = boto3.client('mediaconvert', endpoint_url=self.endpoint)
        return self._boto_client

    def get_endpoint(self):
        resp = boto3.client('mediaconvert').describe_endpoints(MaxResults=1)
        try:
            return resp['Endpoints'][0]['Url']
        except Exception as err:
            raise Exception(f'Unable to parse response from MediaConvert::DescribeEndpoints: {err}') from err

    def create_job(self, input_s3_key, video_output_s3_key_prefix, image_output_s3_key_prefix):
        input_url = f's3://{self.uploads_bucket}/{input_s3_key}'
        video_output_url_prefix = f's3://{self.uploads_bucket}/{video_output_s3_key_prefix}'
        image_output_url_prefix = f's3://{self.uploads_bucket}/{image_output_s3_key_prefix}'
        job_json = self.get_job_json(input_url, video_output_url_prefix, image_output_url_prefix)
        self.boto_client.create_job(**job_json)

    def get_job_json(self, input_url, video_output_url_prefix, image_output_url_prefix):
        # Minimal job config generated using the AWS console to create a sample job
        return {
            "JobTemplate": self.job_template_arn,
            "Role": self.role_arn,
            "Settings": {
                "OutputGroups": [
                    {
                        "Name": "Apple HLS",
                        "OutputGroupSettings": {"HlsGroupSettings": {"Destination": video_output_url_prefix}},
                    },
                    {
                        "Name": "File Group",
                        "OutputGroupSettings": {
                            "Type": "FILE_GROUP_SETTINGS",
                            "FileGroupSettings": {"Destination": image_output_url_prefix},
                        },
                        "Outputs": [
                            {
                                "ContainerSettings": {"Container": "RAW"},
                                "VideoDescription": {
                                    "ScalingBehavior": "DEFAULT",
                                    "TimecodeInsertion": "DISABLED",
                                    "AntiAlias": "ENABLED",
                                    "Sharpness": 50,
                                    "CodecSettings": {
                                        "Codec": "FRAME_CAPTURE",
                                        "FrameCaptureSettings": {
                                            "FramerateNumerator": 1,
                                            "FramerateDenominator": 5,
                                            "MaxCaptures": 1,
                                            "Quality": 80,
                                        },
                                    },
                                    "DropFrameTimecode": "ENABLED",
                                    "ColorMetadata": "INSERT",
                                },
                            }
                        ],
                    },
                ],
                "Inputs": [
                    {
                        "AudioSelectors": {
                            "Audio Selector 1": {
                                "Offset": 0,
                                "DefaultSelection": "DEFAULT",
                                "ProgramSelection": 1,
                            }
                        },
                        "VideoSelector": {"ColorSpace": "FOLLOW", "Rotate": "AUTO", "AlphaBehavior": "DISCARD"},
                        "FilterEnable": "AUTO",
                        "PsiControl": "USE_PSI",
                        "FilterStrength": 0,
                        "DeblockFilter": "DISABLED",
                        "DenoiseFilter": "DISABLED",
                        "TimecodeSource": "EMBEDDED",
                        "FileInput": input_url,
                    }
                ],
            },
        }
