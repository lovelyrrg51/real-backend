import logging
import os
import urllib

from app import clients, models
from app.logging import LogLevelContext, handler_logging
from app.models.post.enums import PostStatus, PostType
from app.models.post.exceptions import PostException

from . import xray

S3_UPLOADS_BUCKET = os.environ.get('S3_UPLOADS_BUCKET')

logger = logging.getLogger()
xray.patch_all()

secrets_manager_client = clients.SecretsManagerClient()
clients = {
    'appsync': clients.AppSyncClient(),
    'cloudfront': clients.CloudFrontClient(secrets_manager_client.get_cloudfront_key_pair),
    'dynamo': clients.DynamoClient(),
    'mediaconvert': clients.MediaConvertClient(),
    'post_verification': clients.PostVerificationClient(secrets_manager_client.get_post_verification_api_creds),
    's3_uploads': clients.S3Client(S3_UPLOADS_BUCKET),
}

managers = {}
post_manager = managers.get('post') or models.PostManager(clients, managers=managers)


def event_to_extras(event):
    # Seems the boto s3 client deals with non-urlencoded keys to objects everywhere, but
    # apparenttly this falls outside that scope. The event emitter passes us a urlencoded path.
    path = urllib.parse.unquote(event['Records'][0]['s3']['object']['key'])
    return {'s3_key': path}


@handler_logging(event_to_extras=event_to_extras)
def image_post_uploaded(event, context):
    # we suppress INFO logging, except this message
    with LogLevelContext(logger, logging.INFO):
        logger.info('Handling S3 Object Created (image post uploaded) event')

    # Avoid firing on creation of other images (profile photo, album art)
    # Once images are moved to their new path at {userId}/post/{postId}/image/{size}.jpg,
    # the s3 object created event suffix filter should be expaneded to '/image/native.jpg'
    # and this check removed (currently set to '/native.jpg').
    path = urllib.parse.unquote(event['Records'][0]['s3']['object']['key'])
    if 'post' not in path:
        return

    # At this point we have triggered this event because of:
    #   - video post poster images
    #   - image upload for image posts schema version 0
    #   - image upload for image posts schema version 1
    post_id = path.split('/')[2]

    # strongly consistent because we may have just added the post to dynamo
    post = post_manager.get_post(post_id, strongly_consistent=True)
    if not post:
        logger.warning(f'Unable to find post `{post_id}`, ignoring upload')
        return

    if post.type != PostType.IMAGE:
        logger.warning(f'Fired for video post `{post_id}` poster image, ignoring')
        return

    if post.status != PostStatus.PENDING:
        logger.warning(f'Post `{post_id}` is not in PENDING status: `{post.status}`, ignoring upload')
        return

    try:
        post.process_image_upload()
    except Exception as err:
        post.error(str(err))
        if not isinstance(err, PostException):
            raise err
        logger.warning(str(err))


@handler_logging(event_to_extras=event_to_extras)
def video_post_uploaded(event, context):
    # we suppress INFO logging, except this message
    with LogLevelContext(logger, logging.INFO):
        logger.info('Handling S3 Object Created (video post uploaded) event')

    path = urllib.parse.unquote(event['Records'][0]['s3']['object']['key'])
    _, _, post_id, _ = path.split('/')

    # strongly consistent because we may have just added the post to dynamo
    post = post_manager.get_post(post_id, strongly_consistent=True)
    if not post:
        logger.warning(f'Unable to find post `{post_id}`, ignoring upload')
        return

    if post.status != PostStatus.PENDING:
        logger.warning(f'Post `{post_id}` is not in PENDING status: `{post.status}`, ignoring upload')
        return

    size_bytes = event['Records'][0]['s3']['object']['size']
    max_size_bytes = 2 * 1024 * 1024 * 1024  # 2GB as speced via chat
    if size_bytes > max_size_bytes:
        logger.warning(f'Received upload of `{size_bytes}` bytes which exceeds max size for post `{post_id}`')
        post.error()

    try:
        post.start_processing_video_upload()
    except Exception as err:
        post.error(str(err))
        if not isinstance(err, PostException):
            raise err
        logger.warning(str(err))


@handler_logging(event_to_extras=event_to_extras)
def video_post_processed(event, context):
    # we suppress INFO logging, except this message
    with LogLevelContext(logger, logging.INFO):
        logger.info('Handling S3 Object Created (video post processed) event')

    path = urllib.parse.unquote(event['Records'][0]['s3']['object']['key'])
    _, _, post_id, _, _ = path.split('/')

    # strongly consistent because we may have just added the post to dynamo
    post = post_manager.get_post(post_id, strongly_consistent=True)
    if not post:
        logger.warning(f'Unable to find post `{post_id}`, ignoring upload')
        return

    if post.status != PostStatus.PROCESSING:
        logger.warning(f'Post `{post_id}` is not in PROCESSING status: `{post.status}`, ignoring')
        return

    try:
        post.finish_processing_video_upload()
    except Exception as err:
        post.error(str(err))
        if not isinstance(err, PostException):
            raise err
        logger.warning(str(err))
