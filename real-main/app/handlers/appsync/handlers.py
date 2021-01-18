import logging
import os

import pendulum

from app import clients, models
from app.mixins.flag.enums import FlagStatus
from app.mixins.flag.exceptions import FlagException
from app.mixins.view.enums import ViewType
from app.models.album.exceptions import AlbumException
from app.models.appstore.exceptions import AppStoreException
from app.models.block.enums import BlockStatus
from app.models.block.exceptions import BlockException
from app.models.card.exceptions import CardException
from app.models.chat.exceptions import ChatException
from app.models.chat_message.enums import ChatMessageNotificationType
from app.models.chat_message.exceptions import ChatMessageException
from app.models.comment.exceptions import CommentException
from app.models.follower.enums import FollowStatus
from app.models.follower.exceptions import FollowerException
from app.models.like.enums import LikeStatus
from app.models.like.exceptions import LikeException
from app.models.post.enums import PostStatus, PostType
from app.models.post.exceptions import PostException
from app.models.user.enums import UserStatus
from app.models.user.exceptions import UserException
from app.utils import image_size

from .. import xray
from . import routes
from .exceptions import ClientException
from .validation import (
    validate_age_range,
    validate_date_of_birth,
    validate_height,
    validate_height_range,
    validate_location,
    validate_match_genders,
    validate_match_location_radius,
)

S3_UPLOADS_BUCKET = os.environ.get('S3_UPLOADS_BUCKET')
S3_PLACEHOLDER_PHOTOS_BUCKET = os.environ.get('S3_PLACEHOLDER_PHOTOS_BUCKET')

logger = logging.getLogger()
xray.patch_all()

secrets_manager_client = clients.SecretsManagerClient()
clients = {
    'apple': clients.AppleClient(),
    'appstore': clients.AppStoreClient(secrets_manager_client.get_apple_appstore_params),
    'appsync': clients.AppSyncClient(),
    'cloudfront': clients.CloudFrontClient(secrets_manager_client.get_cloudfront_key_pair),
    'cognito': clients.CognitoClient(real_key_pair_getter=secrets_manager_client.get_real_key_pair),
    'dynamo': clients.DynamoClient(),
    'elasticsearch': clients.ElasticSearchClient(),
    'facebook': clients.FacebookClient(),
    'google': clients.GoogleClient(secrets_manager_client.get_google_client_ids),
    'pinpoint': clients.PinpointClient(),
    'post_verification': clients.PostVerificationClient(secrets_manager_client.get_post_verification_api_creds),
    's3_uploads': clients.S3Client(S3_UPLOADS_BUCKET),
    's3_placeholder_photos': clients.S3Client(S3_PLACEHOLDER_PHOTOS_BUCKET),
}

# shared hash table of all managers, enables inter-manager communication
managers = {}
appstore_manager = managers.get('appstore') or models.AppStoreManager(clients, managers=managers)
album_manager = managers.get('album') or models.AlbumManager(clients, managers=managers)
block_manager = managers.get('block') or models.BlockManager(clients, managers=managers)
card_manager = managers.get('card') or models.CardManager(clients, managers=managers)
chat_manager = managers.get('chat') or models.ChatManager(clients, managers=managers)
chat_message_manager = managers.get('chat_message') or models.ChatMessageManager(clients, managers=managers)
comment_manager = managers.get('comment') or models.CommentManager(clients, managers=managers)
follower_manager = managers.get('follower') or models.FollowerManager(clients, managers=managers)
like_manager = managers.get('like') or models.LikeManager(clients, managers=managers)
post_manager = managers.get('post') or models.PostManager(clients, managers=managers)
screen_manager = managers.get('screen') or models.ScreenManager(clients, managers=managers)
user_manager = managers.get('user') or models.UserManager(clients, managers=managers)


def validate_caller(*args, allowed_statuses=None):
    """
    Decorator that inits a caller_user model and verifies the caller has the correct status.
    May be used in two ways:

     -  @validate_caller
        def my_handler(caller_user, ...)

     -  @validate_caller(allowed_statuses=[...])
        def my_handler(caller_user, ...)

    If not specified, `allowed_statuses` defaults to just UserStatus.ACTIVE.
    """

    def outer_wrapper(func):
        def inner_wrapper(caller_user_id, arguments, **kwargs):
            statuses = allowed_statuses or [UserStatus.ACTIVE]
            caller_user = user_manager.get_user(caller_user_id)
            if not caller_user:
                raise ClientException(f'User `{caller_user_id}` does not exist')
            if caller_user.status not in statuses:
                raise ClientException(f'User `{caller_user_id}` is not ' + ' or '.join(statuses))
            return func(caller_user, arguments, **kwargs)

        return inner_wrapper

    if args:
        return outer_wrapper(args[0])
    else:
        return outer_wrapper


def update_last_client(func):
    "Decorator that updates User.lastClient if as needed"

    def wrapper(caller_user, arguments, client=None, **kwargs):
        if caller_user and client:
            caller_user.set_last_client(client)
        return func(caller_user, arguments, client=client, **kwargs)

    return wrapper


def update_last_disable_dating_date(func):
    "Decorator that updates User's last disable dating date if as needed"

    def wrapper(caller_user, arguments, **kwargs):
        if caller_user:
            caller_user.set_last_disable_dating_date()
        return func(caller_user, arguments, **kwargs)

    return wrapper


@routes.register('Mutation.createAnonymousUser')
def create_anonymous_user(caller_user_id, arguments, client=None, **kwargs):
    try:
        user, cognito_tokens = user_manager.create_anonymous_user(caller_user_id)
    except UserException as err:
        raise ClientException(str(err)) from err
    user.set_last_client(client)
    return cognito_tokens


@routes.register('Mutation.createCognitoOnlyUser')
def create_cognito_only_user(caller_user_id, arguments, client=None, **kwargs):
    username = arguments['username']
    full_name = arguments.get('fullName')
    try:
        user = user_manager.create_cognito_only_user(caller_user_id, username, full_name=full_name)
    except UserException as err:
        raise ClientException(str(err)) from err
    user.set_last_client(client)
    return user.serialize(caller_user_id)


@routes.register('Mutation.createAppleUser')
def create_apple_user(caller_user_id, arguments, client=None, **kwargs):
    username = arguments['username']
    full_name = arguments.get('fullName')
    apple_token = arguments['appleIdToken']
    try:
        user = user_manager.create_federated_user(
            'apple', caller_user_id, username, apple_token, full_name=full_name
        )
    except UserException as err:
        raise ClientException(str(err)) from err
    user.set_last_client(client)
    return user.serialize(caller_user_id)


@routes.register('Mutation.createFacebookUser')
def create_facebook_user(caller_user_id, arguments, client=None, **kwargs):
    username = arguments['username']
    full_name = arguments.get('fullName')
    facebook_token = arguments['facebookAccessToken']
    try:
        user = user_manager.create_federated_user(
            'facebook', caller_user_id, username, facebook_token, full_name=full_name
        )
    except UserException as err:
        raise ClientException(str(err)) from err
    user.set_last_client(client)
    return user.serialize(caller_user_id)


@routes.register('Mutation.createGoogleUser')
def create_google_user(caller_user_id, arguments, client=None, **kwargs):
    username = arguments['username']
    full_name = arguments.get('fullName')
    google_id_token = arguments['googleIdToken']
    try:
        user = user_manager.create_federated_user(
            'google', caller_user_id, username, google_id_token, full_name=full_name
        )
    except UserException as err:
        raise ClientException(str(err)) from err
    user.set_last_client(client)
    return user.serialize(caller_user_id)


@routes.register('Mutation.setUserPassword')
@validate_caller
@update_last_client
@update_last_disable_dating_date
def set_user_password(caller_user, arguments, **kwargs):
    encrypted_password = arguments['encryptedPassword']
    try:
        caller_user.set_password(encrypted_password)
    except UserException as err:
        raise ClientException(str(err)) from err
    return caller_user.serialize(caller_user.id)


@routes.register('Mutation.linkAppleLogin')
@validate_caller(allowed_statuses=(UserStatus.ACTIVE, UserStatus.ANONYMOUS))
@update_last_client
@update_last_disable_dating_date
def link_apple_login(caller_user, arguments, **kwargs):
    apple_token = arguments['appleIdToken']
    try:
        caller_user.link_federated_login('apple', apple_token)
    except UserException as err:
        raise ClientException(str(err)) from err
    return caller_user.serialize(caller_user.id)


@routes.register('Mutation.linkFacebookLogin')
@validate_caller(allowed_statuses=(UserStatus.ACTIVE, UserStatus.ANONYMOUS))
@update_last_client
@update_last_disable_dating_date
def link_facebook_login(caller_user, arguments, **kwargs):
    facebook_token = arguments['facebookAccessToken']
    try:
        caller_user.link_federated_login('facebook', facebook_token)
    except UserException as err:
        raise ClientException(str(err)) from err
    return caller_user.serialize(caller_user.id)


@routes.register('Mutation.linkGoogleLogin')
@validate_caller(allowed_statuses=(UserStatus.ACTIVE, UserStatus.ANONYMOUS))
@update_last_client
@update_last_disable_dating_date
def link_google_login(caller_user, arguments, **kwargs):
    google_id_token = arguments['googleIdToken']
    try:
        caller_user.link_federated_login('google', google_id_token)
    except UserException as err:
        raise ClientException(str(err)) from err
    return caller_user.serialize(caller_user.id)


@routes.register('Mutation.startChangeUserEmail')
@validate_caller(allowed_statuses=(UserStatus.ACTIVE, UserStatus.ANONYMOUS))
@update_last_client
@update_last_disable_dating_date
def start_change_user_email(caller_user, arguments, **kwargs):
    email = arguments['email']
    try:
        caller_user.start_change_contact_attribute('email', email)
    except UserException as err:
        raise ClientException(str(err)) from err
    return caller_user.serialize(caller_user.id)


@routes.register('Mutation.finishChangeUserEmail')
@validate_caller(allowed_statuses=(UserStatus.ACTIVE, UserStatus.ANONYMOUS))
@update_last_client
@update_last_disable_dating_date
def finish_change_user_email(caller_user, arguments, **kwargs):
    code = arguments['verificationCode']
    try:
        caller_user.finish_change_contact_attribute('email', code)
    except UserException as err:
        raise ClientException(str(err)) from err
    return caller_user.serialize(caller_user.id)


@routes.register('Mutation.startChangeUserPhoneNumber')
@validate_caller(allowed_statuses=(UserStatus.ACTIVE, UserStatus.ANONYMOUS))
@update_last_client
@update_last_disable_dating_date
def start_change_user_phone_number(caller_user, arguments, **kwargs):
    phone = arguments['phoneNumber']
    try:
        caller_user.start_change_contact_attribute('phone', phone)
    except UserException as err:
        raise ClientException(str(err)) from err
    return caller_user.serialize(caller_user.id)


@routes.register('Mutation.finishChangeUserPhoneNumber')
@validate_caller(allowed_statuses=(UserStatus.ACTIVE, UserStatus.ANONYMOUS))
@update_last_client
@update_last_disable_dating_date
def finish_change_user_phone_number(caller_user, arguments, **kwargs):
    code = arguments['verificationCode']
    try:
        caller_user.finish_change_contact_attribute('phone', code)
    except UserException as err:
        raise ClientException(str(err)) from err
    return caller_user.serialize(caller_user.id)


@routes.register('Mutation.setUserDetails')
@validate_caller
@update_last_client
@update_last_disable_dating_date
def set_user_details(caller_user, arguments, **kwargs):
    username = arguments.get('username')
    full_name = arguments.get('fullName')
    display_name = arguments.get('displayName')
    bio = arguments.get('bio')
    photo_post_id = arguments.get('photoPostId')
    privacy_status = arguments.get('privacyStatus')
    follow_counts_hidden = arguments.get('followCountsHidden')
    view_counts_hidden = arguments.get('viewCountsHidden')
    language_code = arguments.get('languageCode')
    theme_code = arguments.get('themeCode')
    comments_disabled = arguments.get('commentsDisabled')
    likes_disabled = arguments.get('likesDisabled')
    sharing_disabled = arguments.get('sharingDisabled')
    verification_hidden = arguments.get('verificationHidden')
    date_of_birth = arguments.get('dateOfBirth')
    gender = arguments.get('gender')
    location = arguments.get('location')
    height = arguments.get('height')
    match_age_range = arguments.get('matchAgeRange')
    match_genders = arguments.get('matchGenders')
    match_location_radius = arguments.get('matchLocationRadius')
    match_height_range = arguments.get('matchHeightRange')

    args = (
        username,
        full_name,
        display_name,
        bio,
        photo_post_id,
        privacy_status,
        follow_counts_hidden,
        language_code,
        theme_code,
        comments_disabled,
        likes_disabled,
        sharing_disabled,
        verification_hidden,
        view_counts_hidden,
        date_of_birth,
        gender,
        location,
        height,
        match_age_range,
        match_genders,
        match_location_radius,
        match_height_range,
    )
    if all(v is None for v in args):
        raise ClientException('Called without any arguments... probably not what you intended?')

    # are we claiming a new username?
    if username is not None:
        try:
            caller_user.update_username(username)
        except UserException as err:
            raise ClientException(str(err)) from err

    # are we setting a new profile picture?
    if photo_post_id is not None:
        post_id = photo_post_id if photo_post_id != '' else None
        try:
            caller_user.update_photo(post_id)
        except UserException as err:
            raise ClientException(str(err)) from err

    # are we changing our privacy status?
    if privacy_status is not None:
        caller_user.set_privacy_status(privacy_status)

    if location is not None:
        validate_location(location)

    if match_age_range is not None:
        validate_age_range(match_age_range)

    if match_location_radius is not None:
        validate_match_location_radius(match_location_radius, caller_user.subscription_level)

    if match_genders is not None:
        validate_match_genders(match_genders)

    if height is not None:
        validate_height(height)

    if match_height_range is not None:
        validate_height_range(match_height_range)

    if date_of_birth is not None:
        validate_date_of_birth(date_of_birth)

    # update the simple properties
    caller_user.update_details(
        full_name=full_name,
        display_name=display_name,
        bio=bio,
        language_code=language_code,
        theme_code=theme_code,
        follow_counts_hidden=follow_counts_hidden,
        view_counts_hidden=view_counts_hidden,
        comments_disabled=comments_disabled,
        likes_disabled=likes_disabled,
        sharing_disabled=sharing_disabled,
        verification_hidden=verification_hidden,
        date_of_birth=date_of_birth,
        gender=gender,
        location=location,
        height=height,
        match_age_range=match_age_range,
        match_genders=match_genders,
        match_location_radius=match_location_radius,
        match_height_range=match_height_range,
    )
    return caller_user.serialize(caller_user.id)


@routes.register('Mutation.setUserAcceptedEULAVersion')
@validate_caller
@update_last_client
@update_last_disable_dating_date
def set_user_accepted_eula_version(caller_user, arguments, **kwargs):
    version = arguments['version']

    # use the empty string to request deleting
    if version == '':
        version = None

    caller_user.set_accepted_eula_version(version)
    return caller_user.serialize(caller_user.id)


@routes.register('Mutation.setUserAPNSToken')
@validate_caller
@update_last_client
@update_last_disable_dating_date
def set_user_apns_token(caller_user, arguments, **kwargs):
    token = arguments['token']

    # use the empty string to request deleting
    if token == '':
        token = None

    caller_user.set_apns_token(token)
    return caller_user.serialize(caller_user.id)


@routes.register('Mutation.setUserDatingStatus')
@validate_caller
@update_last_client
@update_last_disable_dating_date
def set_user_dating_status(caller_user, arguments, **kwargs):
    status = arguments['status']
    try:
        caller_user.set_dating_status(status)
    except UserException as err:
        raise ClientException(str(err.args[0]), err.args[1] if len(err.args) > 1 else []) from err
    return caller_user.serialize(caller_user.id)


@routes.register('Mutation.resetUser')
def reset_user(caller_user_id, arguments, client=None, **kwargs):
    new_username = arguments.get('newUsername') or None  # treat empty string like null

    # resetUser may be called when user exists in cognito but not in dynamo
    user = user_manager.get_user(caller_user_id)
    if user:
        if user.status not in (UserStatus.ACTIVE, UserStatus.DISABLED):
            raise ClientException(f'Cannot reset user with status `{user.status}`')
        user.reset()

    if new_username:
        # equivalent to calling Mutation.createCognitoOnlyUser()
        try:
            user = user_manager.create_cognito_only_user(caller_user_id, new_username)
        except UserException as err:
            raise ClientException(str(err)) from err
        user.set_last_client(client)

    return user.serialize(caller_user_id) if user else None


@routes.register('Mutation.disableUser')
def disable_user(caller_user_id, arguments, client=None, **kwargs):
    # mark our user as in the process of deleting
    user = user_manager.get_user(caller_user_id)
    if not user:
        raise ClientException(f'User `{caller_user_id}` does not exist')

    user.set_last_client(client)
    user.disable()
    return user.serialize(caller_user_id)


@routes.register('Mutation.deleteUser')
def delete_user(caller_user_id, arguments, client=None, **kwargs):
    user = user_manager.get_user(caller_user_id)
    if not user:
        raise ClientException(f'User `{caller_user_id}` does not exist')

    user.set_last_client(client)
    user.delete()
    return user.serialize(caller_user_id)


@routes.register('Mutation.reportScreenViews')
@validate_caller(allowed_statuses=(UserStatus.ACTIVE, UserStatus.ANONYMOUS))
@update_last_client
@update_last_disable_dating_date
def report_screen_views(caller_user, arguments, **kwargs):
    screens = arguments['screens']
    if len(screens) == 0:
        raise ClientException('A minimum of 1 screen must be reported')
    if len(screens) > 100:
        raise ClientException('A max of 100 screens may be reported at a time')

    viewed_at = pendulum.now('utc')
    screen_manager.record_views(screens, caller_user.id, viewed_at=viewed_at)
    return True


@routes.register('Mutation.grantUserSubscriptionBonus')
@validate_caller
@update_last_client
@update_last_disable_dating_date
def grant_user_subscription_bonus(caller_user, arguments, **kwargs):
    try:
        caller_user.grant_subscription_bonus()
    except UserException as err:
        raise ClientException(str(err)) from err
    return caller_user.serialize(caller_user.id)


@routes.register('Mutation.addAppStoreReceipt')
@validate_caller
@update_last_client
def add_app_store_receipt(caller_user, arguments, **kwargs):
    receipt_data = arguments['receiptData']
    try:
        appstore_manager.add_receipt(receipt_data, caller_user.id)
    except AppStoreException as err:
        raise ClientException(str(err)) from err
    return True


@routes.register('User.photo')
def user_photo(caller_user_id, arguments, source=None, **kwargs):
    user = user_manager.init_user(source)
    native_url = user.get_photo_url(image_size.NATIVE)
    if not native_url:
        return None
    return {
        'url': native_url,
        'url64p': user.get_photo_url(image_size.P64),
        'url480p': user.get_photo_url(image_size.P480),
        'url1080p': user.get_photo_url(image_size.P1080),
        'url4k': user.get_photo_url(image_size.K4),
    }


@routes.register('Mutation.followUser')
@validate_caller
@update_last_client
@update_last_disable_dating_date
def follow_user(caller_user, arguments, **kwargs):
    follower_user = caller_user
    followed_user_id = arguments['userId']

    if follower_user.id == followed_user_id:
        raise ClientException('User cannot follow themselves')

    followed_user = user_manager.get_user(followed_user_id)
    if not followed_user:
        raise ClientException(f'No user profile found for followed `{followed_user_id}`')

    try:
        follow = follower_manager.request_to_follow(follower_user, followed_user)
    except FollowerException as err:
        raise ClientException(str(err)) from err

    resp = followed_user.serialize(caller_user.id)
    resp['followedStatus'] = follow.status
    if follow.status == FollowStatus.FOLLOWING:
        resp['followerCount'] = followed_user.item.get('followerCount', 0) + 1
    return resp


@routes.register('Mutation.unfollowUser')
@validate_caller
@update_last_client
@update_last_disable_dating_date
def unfollow_user(caller_user, arguments, **kwargs):
    follower_user = caller_user
    followed_user_id = arguments['userId']

    follow = follower_manager.get_follow(follower_user.id, followed_user_id)
    if not follow:
        raise ClientException(f'User `{follower_user.id}` is not following `{followed_user_id}`')

    try:
        follow.unfollow()
    except FollowerException as err:
        raise ClientException(str(err)) from err

    resp = user_manager.get_user(followed_user_id, strongly_consistent=True).serialize(caller_user.id)
    resp['followedStatus'] = follow.status
    return resp


@routes.register('Mutation.acceptFollowerUser')
@validate_caller
@update_last_client
@update_last_disable_dating_date
def accept_follower_user(caller_user, arguments, **kwargs):
    followed_user = caller_user
    follower_user_id = arguments['userId']

    follow = follower_manager.get_follow(follower_user_id, followed_user.id)
    if not follow:
        raise ClientException(f'User `{follower_user_id}` has not requested to follow user `{followed_user.id}`')

    try:
        follow.accept()
    except FollowerException as err:
        raise ClientException(str(err)) from err

    resp = user_manager.get_user(follower_user_id, strongly_consistent=True).serialize(caller_user.id)
    resp['followerStatus'] = follow.status
    return resp


@routes.register('Mutation.denyFollowerUser')
@validate_caller
@update_last_client
@update_last_disable_dating_date
def deny_follower_user(caller_user, arguments, **kwargs):
    followed_user = caller_user
    follower_user_id = arguments['userId']

    follow = follower_manager.get_follow(follower_user_id, followed_user.id)
    if not follow:
        raise ClientException(f'User `{follower_user_id}` has not requested to follow user `{followed_user.id}`')

    try:
        follow.deny()
    except FollowerException as err:
        raise ClientException(str(err)) from err

    resp = user_manager.get_user(follower_user_id, strongly_consistent=True).serialize(caller_user.id)
    resp['followerStatus'] = follow.status
    return resp


@routes.register('Mutation.blockUser')
@validate_caller
@update_last_client
@update_last_disable_dating_date
def block_user(caller_user, arguments, **kwargs):
    blocker_user = caller_user
    blocked_user_id = arguments['userId']

    if blocker_user.id == blocked_user_id:
        raise ClientException('Cannot block yourself')

    blocked_user = user_manager.get_user(blocked_user_id)
    if not blocked_user:
        raise ClientException(f'User `{blocked_user_id}` does not exist')

    try:
        block_manager.block(blocker_user, blocked_user)
    except BlockException as err:
        raise ClientException(str(err)) from err

    resp = blocked_user.serialize(caller_user.id)
    resp['blockedStatus'] = BlockStatus.BLOCKING
    return resp


@routes.register('Mutation.unblockUser')
@validate_caller
@update_last_client
@update_last_disable_dating_date
def unblock_user(caller_user, arguments, **kwargs):
    blocker_user = caller_user
    blocked_user_id = arguments['userId']

    if blocker_user.id == blocked_user_id:
        raise ClientException('Cannot unblock yourself')

    blocked_user = user_manager.get_user(blocked_user_id)
    if not blocked_user:
        raise ClientException(f'User `{blocked_user_id}` does not exist')

    try:
        block_manager.unblock(blocker_user, blocked_user)
    except BlockException as err:
        raise ClientException(str(err)) from err

    resp = blocked_user.serialize(caller_user.id)
    resp['blockedStatus'] = BlockStatus.NOT_BLOCKING
    return resp


@routes.register('Mutation.addPost')
@validate_caller
@update_last_client
@update_last_disable_dating_date
def add_post(caller_user, arguments, **kwargs):
    post_id = arguments['postId']
    post_type = arguments.get('postType') or PostType.IMAGE
    text = arguments.get('text')
    image_input = arguments.get('imageInput')
    album_id = arguments.get('albumId')
    set_as_user_photo = arguments.get('setAsUserPhoto')
    comments_disabled = arguments.get('commentsDisabled')
    likes_disabled = arguments.get('likesDisabled')
    sharing_disabled = arguments.get('sharingDisabled')
    verification_hidden = arguments.get('verificationHidden')
    keywords = arguments.get('keywords')

    lifetime_iso = arguments.get('lifetime')
    if lifetime_iso:
        try:
            lifetime_duration = pendulum.parse(lifetime_iso)
        except pendulum.exceptions.ParserError as err:
            raise ClientException(f'Unable to parse lifetime `{lifetime_iso}`') from err
        if not isinstance(lifetime_duration, pendulum.Duration):
            raise ClientException(f'Unable to parse lifetime `{lifetime_iso}` as duration')
    else:
        lifetime_duration = None

    try:
        post = post_manager.add_post(
            caller_user,
            post_id,
            post_type,
            image_input=image_input,
            text=text,
            lifetime_duration=lifetime_duration,
            album_id=album_id,
            comments_disabled=comments_disabled,
            likes_disabled=likes_disabled,
            sharing_disabled=sharing_disabled,
            verification_hidden=verification_hidden,
            keywords=keywords,
            set_as_user_photo=set_as_user_photo,
        )
    except PostException as err:
        raise ClientException(str(err)) from err

    return post.serialize(caller_user.id)


@routes.register('Post.image')
def post_image(caller_user_id, arguments, source=None, **kwargs):
    post = post_manager.get_post(source['postId'])

    if not post or post.status == PostStatus.DELETING:
        return None

    if post.type == PostType.TEXT_ONLY:
        return None

    if post.status not in (PostStatus.COMPLETED, PostStatus.ARCHIVED):
        return None

    image_item = post.image_item.copy() if post.image_item else {}
    image_item.update(
        {
            'url': post.get_image_readonly_url(image_size.NATIVE),
            'url64p': post.get_image_readonly_url(image_size.P64),
            'url480p': post.get_image_readonly_url(image_size.P480),
            'url1080p': post.get_image_readonly_url(image_size.P1080),
            'url4k': post.get_image_readonly_url(image_size.K4),
        }
    )
    return image_item


@routes.register('Post.imageUploadUrl')
def post_image_upload_url(caller_user_id, arguments, source=None, **kwargs):
    post_id = source['postId']
    user_id = source['postedByUserId']

    if caller_user_id != user_id:
        return None

    post = post_manager.get_post(post_id)
    if not post or post.type != PostType.IMAGE or post.status != PostStatus.PENDING:
        return None

    return post.get_image_writeonly_url()


@routes.register('Post.video')
def post_video(caller_user_id, arguments, source=None, **kwargs):
    post = post_manager.get_post(source['postId'])

    statuses = (PostStatus.COMPLETED, PostStatus.ARCHIVED)
    if not post or post.type != PostType.VIDEO or post.status not in statuses:
        return None

    return {
        'urlMasterM3U8': post.get_hls_master_m3u8_url(),
        'accessCookies': post.get_hls_access_cookies(),
    }


@routes.register('Post.videoUploadUrl')
def post_video_upload_url(caller_user_id, arguments, source=None, **kwargs):
    post_id = source['postId']
    user_id = source['postedByUserId']

    if caller_user_id != user_id:
        return None

    post = post_manager.get_post(post_id)
    if not post or post.type != PostType.VIDEO or post.status != PostStatus.PENDING:
        return None

    return post.get_video_writeonly_url()


@routes.register('Mutation.editPost')
@validate_caller
@update_last_client
@update_last_disable_dating_date
def edit_post(caller_user, arguments, **kwargs):
    post_id = arguments['postId']
    edit_kwargs = {
        'text': arguments.get('text'),
        'comments_disabled': arguments.get('commentsDisabled'),
        'likes_disabled': arguments.get('likesDisabled'),
        'sharing_disabled': arguments.get('sharingDisabled'),
        'verification_hidden': arguments.get('verificationHidden'),
        'keywords': arguments.get('keywords'),
    }

    post = post_manager.get_post(post_id)
    if not post:
        raise ClientException(f'Post `{post_id}` does not exist')

    if caller_user.id != post.user_id:
        raise ClientException("Cannot edit another User's post")

    try:
        post.set(**edit_kwargs)
    except PostException as err:
        raise ClientException(str(err)) from err

    return post.serialize(caller_user.id)


@routes.register('Mutation.editPostAlbum')
@validate_caller
@update_last_client
@update_last_disable_dating_date
def edit_post_album(caller_user, arguments, **kwargs):
    post_id = arguments['postId']
    album_id = arguments.get('albumId') or None

    post = post_manager.get_post(post_id)
    if not post:
        raise ClientException(f'Post `{post_id}` does not exist')

    if caller_user.id != post.user_id:
        raise ClientException("Cannot edit another user's post")

    try:
        post.set_album(album_id)
    except PostException as err:
        raise ClientException(str(err)) from err

    return post.serialize(caller_user.id)


@routes.register('Mutation.editPostAlbumOrder')
@validate_caller
@update_last_client
@update_last_disable_dating_date
def edit_post_album_order(caller_user, arguments, **kwargs):
    post_id = arguments['postId']
    preceding_post_id = arguments.get('precedingPostId')

    post = post_manager.get_post(post_id)
    if not post:
        raise ClientException(f'Post `{post_id}` does not exist')

    if caller_user.id != post.user_id:
        raise ClientException("Cannot edit another user's post")

    try:
        post.set_album_order(preceding_post_id)
    except PostException as err:
        raise ClientException(str(err)) from err

    return post.serialize(caller_user.id)


@routes.register('Mutation.editPostExpiresAt')
@validate_caller
@update_last_client
@update_last_disable_dating_date
def edit_post_expires_at(caller_user, arguments, **kwargs):
    post_id = arguments['postId']
    expires_at_str = arguments.get('expiresAt')
    expires_at = pendulum.parse(expires_at_str) if expires_at_str else None

    post = post_manager.get_post(post_id)
    if not post:
        raise ClientException(f'Post `{post_id}` does not exist')

    if caller_user.id != post.user_id:
        raise ClientException("Cannot edit another User's post")

    if expires_at and expires_at < pendulum.now('utc'):
        raise ClientException("Cannot set expiresAt to date time in the past: `{expires_at}`")

    post.set_expires_at(expires_at)
    return post.serialize(caller_user.id)


@routes.register('Mutation.flagPost')
@validate_caller
@update_last_client
@update_last_disable_dating_date
def flag_post(caller_user, arguments, **kwargs):
    post_id = arguments['postId']

    post = post_manager.get_post(post_id)
    if not post:
        raise ClientException(f'Post `{post_id}` does not exist')

    try:
        post.flag(caller_user)
    except (PostException, FlagException) as err:
        raise ClientException(str(err)) from err

    resp = post.serialize(caller_user.id)
    resp['flagStatus'] = FlagStatus.FLAGGED
    return resp


@routes.register('Mutation.archivePost')
@validate_caller
@update_last_client
@update_last_disable_dating_date
def archive_post(caller_user, arguments, **kwargs):
    post_id = arguments['postId']

    post = post_manager.get_post(post_id)
    if not post:
        raise ClientException(f'Post `{post_id}` does not exist')

    if caller_user.id != post.user_id:
        raise ClientException("Cannot archive another User's post")

    try:
        post.archive()
    except PostException as err:
        raise ClientException(str(err)) from err

    return post.serialize(caller_user.id)


@routes.register('Mutation.deletePost')
@validate_caller
@update_last_client
@update_last_disable_dating_date
def delete_post(caller_user, arguments, **kwargs):
    post_id = arguments['postId']

    post = post_manager.get_post(post_id)
    if not post:
        raise ClientException(f'Post `{post_id}` does not exist')

    if caller_user.id != post.user_id:
        raise ClientException("Cannot delete another User's post")

    try:
        post = post.delete()
    except PostException as err:
        raise ClientException(str(err)) from err

    return post.serialize(caller_user.id)


@routes.register('Mutation.restoreArchivedPost')
@validate_caller
@update_last_client
@update_last_disable_dating_date
def restore_archived_post(caller_user, arguments, **kwargs):
    post_id = arguments['postId']

    post = post_manager.get_post(post_id)
    if not post:
        raise ClientException(f'Post `{post_id}` does not exist')

    if caller_user.id != post.user_id:
        raise ClientException("Cannot restore another User's post")

    try:
        post.restore()
    except PostException as err:
        raise ClientException(str(err)) from err

    return post.serialize(caller_user.id)


@routes.register('Mutation.onymouslyLikePost')
@validate_caller
@update_last_client
@update_last_disable_dating_date
def onymously_like_post(caller_user, arguments, **kwargs):
    post_id = arguments['postId']

    post = post_manager.get_post(post_id)
    if not post:
        raise ClientException(f'Post `{post_id}` does not exist')

    try:
        like_manager.like_post(caller_user, post, LikeStatus.ONYMOUSLY_LIKED)
    except LikeException as err:
        raise ClientException(str(err)) from err

    resp = post.serialize(caller_user.id)
    resp['likeStatus'] = LikeStatus.ONYMOUSLY_LIKED
    return resp


@routes.register('Mutation.anonymouslyLikePost')
@validate_caller
@update_last_client
@update_last_disable_dating_date
def anonymously_like_post(caller_user, arguments, **kwargs):
    post_id = arguments['postId']

    post = post_manager.get_post(post_id)
    if not post:
        raise ClientException(f'Post `{post_id}` does not exist')

    try:
        like_manager.like_post(caller_user, post, LikeStatus.ANONYMOUSLY_LIKED)
    except LikeException as err:
        raise ClientException(str(err)) from err

    resp = post.serialize(caller_user.id)
    resp['likeStatus'] = LikeStatus.ANONYMOUSLY_LIKED
    return resp


@routes.register('Mutation.dislikePost')
@validate_caller
@update_last_client
@update_last_disable_dating_date
def dislike_post(caller_user, arguments, **kwargs):
    post_id = arguments['postId']

    post = post_manager.dynamo.get_post(post_id)
    if not post:
        raise ClientException(f'Post `{post_id}` does not exist')

    like = like_manager.get_like(caller_user.id, post_id)
    if not like:
        raise ClientException(f'User has not liked post `{post_id}`, thus cannot dislike it')

    prev_status = like.item['likeStatus']
    like.dislike()

    resp = post_manager.init_post(post).serialize(caller_user.id)
    post_like_count = 'onymousLikeCount' if prev_status == LikeStatus.ONYMOUSLY_LIKED else 'anonymousLikeCount'
    if resp.get(post_like_count, 0) > 0:
        resp[post_like_count] -= 1
    resp['likeStatus'] = LikeStatus.NOT_LIKED
    return resp


@routes.register('Mutation.reportPostViews')
@validate_caller(allowed_statuses=(UserStatus.ACTIVE, UserStatus.ANONYMOUS))
@update_last_client
@update_last_disable_dating_date
def report_post_views(caller_user, arguments, **kwargs):
    post_ids = arguments['postIds']
    view_type = arguments.get('viewType', ViewType.THUMBNAIL)
    if len(post_ids) == 0:
        raise ClientException('A minimum of 1 post id must be reported')
    if len(post_ids) > 100:
        raise ClientException('A max of 100 post ids may be reported at a time')

    viewed_at = pendulum.now('utc')
    post_manager.record_views(post_ids, caller_user.id, viewed_at=viewed_at, view_type=view_type)
    return True


@routes.register('Mutation.addComment')
@validate_caller
@update_last_client
@update_last_disable_dating_date
def add_comment(caller_user, arguments, **kwargs):
    comment_id = arguments['commentId']
    post_id = arguments['postId']
    text = arguments['text']

    try:
        comment = comment_manager.add_comment(comment_id, post_id, caller_user.id, text)
    except CommentException as err:
        raise ClientException(str(err)) from err

    return comment.serialize(caller_user.id)


@routes.register('Mutation.deleteComment')
@validate_caller
@update_last_client
@update_last_disable_dating_date
def delete_comment(caller_user, arguments, **kwargs):
    comment_id = arguments['commentId']

    comment = comment_manager.get_comment(comment_id)
    if not comment:
        raise ClientException(f'No comment with id `{comment_id}` found')

    try:
        comment.delete(deleter_user_id=caller_user.id)
    except CommentException as err:
        raise ClientException(str(err)) from err

    return comment.serialize(caller_user.id)


@routes.register('Mutation.flagComment')
@validate_caller
@update_last_client
@update_last_disable_dating_date
def flag_comment(caller_user, arguments, **kwargs):
    comment_id = arguments['commentId']

    comment = comment_manager.get_comment(comment_id)
    if not comment:
        raise ClientException(f'Comment `{comment_id}` does not exist')

    try:
        comment.flag(caller_user)
    except (CommentException, FlagException) as err:
        raise ClientException(str(err)) from err

    resp = comment.serialize(caller_user.id)
    resp['flagStatus'] = FlagStatus.FLAGGED
    return resp


@routes.register('Mutation.deleteCard')
@validate_caller
@update_last_client
@update_last_disable_dating_date
def delete_card(caller_user, arguments, **kwargs):
    card_id = arguments['cardId']

    card = card_manager.get_card(card_id)
    if not card:
        raise ClientException(f'No card with id `{card_id}` found')

    if caller_user.id != card.user_id:
        raise ClientException(f'Caller `{caller_user.id}` does not own Card `{card_id}`')

    try:
        card.delete()
    except CardException as err:
        raise ClientException(str(err)) from err

    return card.serialize(caller_user.id)


@routes.register('Card.thumbnail')
def card_thumbnail(caller_user_id, arguments, source=None, **kwargs):
    card = card_manager.get_card(source['cardId'])
    if card and card.post and card.post.type != PostType.TEXT_ONLY:
        return {
            'url': card.post.get_image_readonly_url(image_size.NATIVE),
            'url64p': card.post.get_image_readonly_url(image_size.P64),
            'url480p': card.post.get_image_readonly_url(image_size.P480),
            'url1080p': card.post.get_image_readonly_url(image_size.P1080),
            'url4k': card.post.get_image_readonly_url(image_size.K4),
        }
    return None


@routes.register('Mutation.addAlbum')
@validate_caller
@update_last_client
@update_last_disable_dating_date
def add_album(caller_user, arguments, **kwargs):
    album_id = arguments['albumId']
    name = arguments['name']
    description = arguments.get('description')

    try:
        album = album_manager.add_album(caller_user.id, album_id, name, description=description)
    except AlbumException as err:
        raise ClientException(str(err)) from err

    return album.serialize(caller_user.id)


@routes.register('Mutation.editAlbum')
@validate_caller
@update_last_client
@update_last_disable_dating_date
def edit_album(caller_user, arguments, **kwargs):
    album_id = arguments['albumId']
    name = arguments.get('name')
    description = arguments.get('description')

    if name is None and description is None:
        raise ClientException('Called without any arguments... probably not what you intended?')

    album = album_manager.get_album(album_id)
    if not album:
        raise ClientException(f'Album `{album_id}` does not exist')

    if album.user_id != caller_user.id:
        raise ClientException(f'Caller `{caller_user.id}` does not own Album `{album_id}`')

    try:
        album.update(name=name, description=description)
    except AlbumException as err:
        raise ClientException(str(err)) from err

    return album.serialize(caller_user.id)


@routes.register('Mutation.deleteAlbum')
@validate_caller
@update_last_client
@update_last_disable_dating_date
def delete_album(caller_user, arguments, **kwargs):
    album_id = arguments['albumId']

    album = album_manager.get_album(album_id)
    if not album:
        raise ClientException(f'Album `{album_id}` does not exist')

    if album.user_id != caller_user.id:
        raise ClientException(f'Caller `{caller_user.id}` does not own Album `{album_id}`')

    try:
        album.delete()
    except AlbumException as err:
        raise ClientException(str(err)) from err

    return album.serialize(caller_user.id)


@routes.register('Album.art')
def album_art(caller_user_id, arguments, source=None, **kwargs):
    album = album_manager.init_album(source)
    return {
        'url': album.get_art_image_url(image_size.NATIVE),
        'url64p': album.get_art_image_url(image_size.P64),
        'url480p': album.get_art_image_url(image_size.P480),
        'url1080p': album.get_art_image_url(image_size.P1080),
        'url4k': album.get_art_image_url(image_size.K4),
    }


@routes.register('Mutation.createDirectChat')
@validate_caller
@update_last_client
@update_last_disable_dating_date
def create_direct_chat(caller_user, arguments, **kwargs):
    chat_id, user_id = arguments['chatId'], arguments['userId']
    message_id, message_text = arguments['messageId'], arguments['messageText']

    user = user_manager.get_user(user_id)
    if not user:
        raise ClientException(f'User `{user_id}` does not exist')

    now = pendulum.now('utc')
    try:
        chat = chat_manager.add_direct_chat(chat_id, caller_user.id, user_id, now=now)
        msg = chat_message_manager.add_chat_message(message_id, message_text, chat_id, caller_user.id, now=now)
    except ChatException as err:
        raise ClientException(str(err)) from err

    msg.trigger_notifications(ChatMessageNotificationType.ADDED, user_ids=[user_id])
    chat.refresh_item(strongly_consistent=True)
    return chat.item


@routes.register('Mutation.createGroupChat')
@validate_caller
@update_last_client
@update_last_disable_dating_date
def create_group_chat(caller_user, arguments, **kwargs):
    chat_id, user_ids, name = arguments['chatId'], arguments['userIds'], arguments.get('name')
    message_id, message_text = arguments['messageId'], arguments['messageText']

    try:
        chat = chat_manager.add_group_chat(chat_id, caller_user, name=name)
        chat.add(caller_user, user_ids)
        message = chat_message_manager.add_chat_message(message_id, message_text, chat_id, caller_user.id)
    except ChatException as err:
        raise ClientException(str(err)) from err

    message.trigger_notifications(ChatMessageNotificationType.ADDED, user_ids=user_ids)
    chat.refresh_item(strongly_consistent=True)
    return chat.item


@routes.register('Mutation.editGroupChat')
@validate_caller
@update_last_client
@update_last_disable_dating_date
def edit_group_chat(caller_user, arguments, **kwargs):
    chat_id = arguments['chatId']
    name = arguments.get('name')

    chat = chat_manager.get_chat(chat_id)
    if not chat or not chat.is_member(caller_user.id):
        raise ClientException(f'User `{caller_user.id}` is not a member of chat `{chat_id}`')

    try:
        chat.edit(caller_user, name=name)
    except ChatException as err:
        raise ClientException(str(err)) from err

    return chat.item


@routes.register('Mutation.addToGroupChat')
@validate_caller
@update_last_client
@update_last_disable_dating_date
def add_to_group_chat(caller_user, arguments, **kwargs):
    chat_id, user_ids = arguments['chatId'], arguments['userIds']

    chat = chat_manager.get_chat(chat_id)
    if not chat or not chat.is_member(caller_user.id):
        raise ClientException(f'User `{caller_user.id}` is not a member of chat `{chat_id}`')

    try:
        chat.add(caller_user, user_ids)
    except ChatException as err:
        raise ClientException(str(err)) from err

    return chat.item


@routes.register('Mutation.leaveGroupChat')
@validate_caller
@update_last_client
@update_last_disable_dating_date
def leave_group_chat(caller_user, arguments, **kwargs):
    chat_id = arguments['chatId']

    chat = chat_manager.get_chat(chat_id)
    if not chat or not chat.is_member(caller_user.id):
        raise ClientException(f'User `{caller_user.id}` is not a member of chat `{chat_id}`')

    try:
        chat.leave(caller_user)
    except ChatException as err:
        raise ClientException(str(err)) from err

    return chat.item


@routes.register('Mutation.reportChatViews')
@validate_caller
@update_last_client
@update_last_disable_dating_date
def report_chat_views(caller_user, arguments, **kwargs):
    chat_ids = arguments['chatIds']
    if len(chat_ids) == 0:
        raise ClientException('A minimum of 1 chat id must be reported')
    if len(chat_ids) > 100:
        raise ClientException('A max of 100 chat ids may be reported at a time')

    viewed_at = pendulum.now('utc')
    chat_manager.record_views(chat_ids, caller_user.id, viewed_at=viewed_at)
    return True


@routes.register('Mutation.flagChat')
@validate_caller
@update_last_client
@update_last_disable_dating_date
def flag_chat(caller_user, arguments, **kwargs):
    chat_id = arguments['chatId']

    chat = chat_manager.get_chat(chat_id)
    if not chat:
        raise ClientException(f'Chat `{chat_id}` does not exist')

    try:
        chat.flag(caller_user)
    except (ChatException, FlagException) as err:
        raise ClientException(str(err)) from err

    resp = chat.item.copy()
    resp['flagStatus'] = FlagStatus.FLAGGED
    return resp


@routes.register('Mutation.addChatMessage')
@validate_caller
@update_last_client
@update_last_disable_dating_date
def add_chat_message(caller_user, arguments, **kwargs):
    chat_id, message_id, text = arguments['chatId'], arguments['messageId'], arguments['text']

    chat = chat_manager.get_chat(chat_id)
    if not chat or not chat.is_member(caller_user.id):
        raise ClientException(f'User `{caller_user.id}` is not a member of chat `{chat_id}`')

    try:
        message = chat_message_manager.add_chat_message(message_id, text, chat_id, caller_user.id)
    except ChatException as err:
        raise ClientException(str(err)) from err

    message.trigger_notifications(ChatMessageNotificationType.ADDED)
    return message.serialize(caller_user.id)


@routes.register('Mutation.editChatMessage')
@validate_caller
@update_last_client
@update_last_disable_dating_date
def edit_chat_message(caller_user, arguments, **kwargs):
    message_id, text = arguments['messageId'], arguments['text']

    message = chat_message_manager.get_chat_message(message_id)
    if not message or message.user_id != caller_user.id:
        raise ClientException(f'User `{caller_user.id}` cannot edit message `{message_id}`')

    try:
        message.edit(text)
    except ChatException as err:
        raise ClientException(str(err)) from err

    message.trigger_notifications(ChatMessageNotificationType.EDITED)
    return message.serialize(caller_user.id)


@routes.register('Mutation.deleteChatMessage')
@validate_caller
@update_last_client
@update_last_disable_dating_date
def delete_chat_message(caller_user, arguments, **kwargs):
    message_id = arguments['messageId']

    message = chat_message_manager.get_chat_message(message_id)
    if not message or message.user_id != caller_user.id:
        raise ClientException(f'User `{caller_user.id}` cannot delete message `{message_id}`')

    try:
        message.delete()
    except ChatException as err:
        raise ClientException(str(err)) from err

    message.trigger_notifications(ChatMessageNotificationType.DELETED)
    return message.serialize(caller_user.id)


@routes.register('Mutation.flagChatMessage')
@validate_caller
@update_last_client
@update_last_disable_dating_date
def flag_chat_message(caller_user, arguments, **kwargs):
    message_id = arguments['messageId']

    message = chat_message_manager.get_chat_message(message_id)
    if not message:
        raise ClientException(f'ChatMessage `{message_id}` does not exist')

    try:
        message.flag(caller_user)
    except (ChatMessageException, FlagException) as err:
        raise ClientException(str(err)) from err

    resp = message.serialize(caller_user.id)
    resp['flagStatus'] = FlagStatus.FLAGGED
    return resp


@routes.register('Mutation.lambdaClientError')
def lambda_client_error(caller_user_id, arguments, context=None, event=None, **kwargs):
    request_id = getattr(context, 'aws_request_id', None)
    raise ClientException(f'Test of lambda client error, request `{request_id}`', info={'event': event})


@routes.register('Mutation.lambdaServerError')
def lambda_server_error(caller_user_id, arguments, context=None, **kwargs):
    request_id = getattr(context, 'aws_request_id', None)
    raise Exception(f'Test of lambda server error, request `{request_id}`')


@routes.register('Query.findContacts')
@validate_caller
@update_last_client
@update_last_disable_dating_date
def find_contacts(caller_user, arguments, **kwargs):
    contacts = arguments['contacts']

    if len(contacts) > 100:
        raise ClientException('Cannot submit more than 100 contact inputs')

    try:
        contact_id_to_user_id = user_manager.find_contacts(caller_user, contacts)
        caller_user.update_last_found_contacts_at(now=pendulum.now('utc'))
    except UserException as err:
        raise ClientException(str(err)) from err

    contact_ids = [contact['contactId'] for contact in contacts]
    return [
        {'contactId': contact_id, 'userId': contact_id_to_user_id.get(contact_id)} for contact_id in contact_ids
    ]


@routes.register('Query.findPosts')
@validate_caller
@update_last_client
@update_last_disable_dating_date
def find_posts(caller_user, arguments, **kwargs):
    keywords = arguments['keywords'].strip()
    limit = arguments.get('limit', 20)
    limit = 100 if limit <= 0 else limit
    next_token = arguments.get('nextToken', 0)

    if not keywords:
        raise ClientException('Empty keywords are not allowed')

    try:
        paginated_posts = post_manager.find_posts(keywords, limit, next_token)
    except UserException as err:
        raise ClientException(str(err)) from err

    return paginated_posts


@routes.register('Query.similarPosts')
@validate_caller
@update_last_client
@update_last_disable_dating_date
def similar_posts(caller_user, arguments, **kwargs):
    post_id = arguments['postId']
    limit = arguments.get('limit', 20)
    limit = 100 if limit <= 0 else limit
    next_token = arguments.get('nextToken', 0)

    keywords = post_manager.get_post(post_id).item.get('keywords', [])
    keywords = ' '.join(keywords)

    if not keywords:
        raise ClientException('Empty keywords are not allowed')

    try:
        paginated_posts = post_manager.find_posts(keywords, limit, next_token)
    except UserException as err:
        raise ClientException(str(err)) from err

    return paginated_posts


@routes.register('Query.searchKeywords')
@validate_caller
@update_last_client
@update_last_disable_dating_date
def search_keywords(caller_user, arguments, **kwargs):
    keyword = arguments['keyword']
    try:
        keywords = post_manager.search_keywords(keyword)
    except UserException as err:
        raise ClientException(str(err)) from err

    return keywords


@routes.register('Query.swipedRightUsers')
@validate_caller
@update_last_client
@update_last_disable_dating_date
def swiped_right_users(caller_user, arguments, **kwargs):
    try:
        user_ids = caller_user.get_swiped_right_users()
    except UserException as err:
        raise ClientException(str(err)) from err

    response = []
    for user_id in user_ids:
        user = user_manager.get_user(user_id)
        if user:
            response.append(user.serialize(caller_user.id))

    return response
