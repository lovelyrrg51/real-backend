import pendulum


class CardTemplate:

    title = None
    action = None
    notify_user_after = None
    sub_title = None
    target_item_id = None
    post_id = None
    comment_id = None
    only_usernames = ()

    def __init__(self, user_id):
        self.user_id = user_id


class ChatCardTemplate(CardTemplate):

    action = 'https://real.app/chat/'
    notify_user_after = pendulum.duration(minutes=5)

    @staticmethod
    def get_card_id(user_id):
        return f'{user_id}:CHAT_ACTIVITY'

    def __init__(self, user_id, chats_with_unviewed_messages_count):
        super().__init__(user_id)
        self.card_id = self.get_card_id(user_id)
        cnt = chats_with_unviewed_messages_count
        self.title = f'You have {cnt} chat{"s" if cnt > 1 else ""} with new messages'


class CommentCardTemplate(CardTemplate):

    notify_user_after = pendulum.duration(hours=24)

    @staticmethod
    def get_card_id(user_id, post_id):
        return f'{user_id}:COMMENT_ACTIVITY:{post_id}'

    def __init__(self, user_id, post_id, unviewed_comments_count):
        super().__init__(user_id)
        self.card_id = self.get_card_id(user_id, post_id)
        self.action = f'https://real.app/user/{user_id}/post/{post_id}/comments'
        cnt = unviewed_comments_count
        self.title = f'You have {cnt} new comment{"s" if cnt > 1 else ""}'
        self.post_id = post_id


class CommentMentionCardTemplate(CardTemplate):

    notify_user_after = pendulum.duration(hours=24)

    @staticmethod
    def get_card_id(user_id, comment_id):
        return f'{user_id}:COMMENT_MENTION:{comment_id}'

    def __init__(self, user_id, comment):
        super().__init__(user_id)
        self.card_id = self.get_card_id(user_id, comment.id)
        self.action = f'https://real.app/user/{comment.post.user_id}/post/{comment.post_id}/comments/{comment.id}'
        self.title = f'@{comment.user.username} mentioned you in a comment'
        self.post_id = comment.post_id
        self.comment_id = comment.id


class PostLikesCardTemplate(CardTemplate):

    title = 'You have new likes'
    notify_user_after = pendulum.duration(hours=24)

    @staticmethod
    def get_card_id(user_id, post_id):
        return f'{user_id}:POST_LIKES:{post_id}'

    def __init__(self, user_id, post_id):
        super().__init__(user_id)
        self.card_id = self.get_card_id(user_id, post_id)
        self.action = f'https://real.app/user/{user_id}/post/{post_id}/likes'
        self.post_id = post_id


class PostMentionCardTemplate(CardTemplate):

    notify_user_after = pendulum.duration(hours=24)

    @staticmethod
    def get_card_id(user_id, post_id):
        return f'{user_id}:POST_MENTION:{post_id}'

    def __init__(self, user_id, post):
        super().__init__(user_id)
        self.card_id = self.get_card_id(user_id, post.id)
        self.action = f'https://real.app/user/{post.user_id}/post/{post.id}'
        self.title = f'@{post.user.username} tagged you in a post'
        self.post_id = post.id


class PostRepostCardTemplate(CardTemplate):

    notify_user_after = pendulum.duration(hours=24)

    @staticmethod
    def get_card_id(user_id, post_id):
        return f'{user_id}:POST_REPOST:{post_id}'

    def __init__(self, user_id, post):
        super().__init__(user_id)
        self.card_id = self.get_card_id(user_id, post.id)
        self.action = f'https://real.app/user/{post.user_id}/post/{post.id}'
        self.title = f'@{post.user.username} reposted one of your posts'
        self.post_id = post.id


class PostViewsCardTemplate(CardTemplate):

    title = 'You have new views'
    notify_user_after = pendulum.duration(hours=24)

    @staticmethod
    def get_card_id(user_id, post_id):
        return f'{user_id}:POST_VIEWS:{post_id}'

    def __init__(self, user_id, post_id):
        super().__init__(user_id)
        self.card_id = self.get_card_id(user_id, post_id)
        self.action = f'https://real.app/user/{user_id}/post/{post_id}/views'
        self.post_id = post_id


class RequestedFollowersCardTemplate(CardTemplate):

    action = 'https://real.app/chat/'
    notify_user_after = pendulum.duration(hours=24)

    @staticmethod
    def get_card_id(user_id):
        return f'{user_id}:REQUESTED_FOLLOWERS'

    def __init__(self, user_id, requested_followers_count):
        super().__init__(user_id)
        self.card_id = self.get_card_id(user_id)
        cnt = requested_followers_count
        self.title = f'You have {cnt} pending follow request{"s" if cnt > 1 else ""}'


class ContactJoinedCardTemplate(CardTemplate):

    notify_user_after = pendulum.duration(hours=24)

    @staticmethod
    def get_card_id(user_id, user_id_joined):
        return f'{user_id}:CONTACT_JOINED:{user_id_joined}'

    def __init__(self, user_id, user_id_joined, username_joined):
        super().__init__(user_id)
        self.card_id = self.get_card_id(user_id, user_id_joined)
        self.action = f'https://real.app/user/{user_id_joined}'
        self.title = f'{username_joined} joined REAL'


class UserSubscriptionLevelTemplate(CardTemplate):

    action = 'https://real.app/diamond'
    notify_user_after = pendulum.duration(hours=24)

    @staticmethod
    def get_card_id(user_id):
        return f'{user_id}:USER_SUBSCRIPTION_LEVEL'

    def __init__(self, user_id):
        super().__init__(user_id)
        self.card_id = self.get_card_id(user_id)
        self.title = 'Welcome to Diamond'
        self.sub_title = 'Enjoy exclusive perks of being a subscriber'


class AddProfilePhotoCardTemplate(CardTemplate):
    @staticmethod
    def get_card_id(user_id):
        return f'{user_id}:ADD_PROFILE_PHOTO'

    def __init__(self, user_id):
        super().__init__(user_id)
        self.card_id = self.get_card_id(user_id)
        self.action = f'https://real.app/user/{user_id}/settings/photo'
        self.title = 'Add a profile photo'


class AnonymousUserUpsellCardTemplate(CardTemplate):

    notify_user_after = pendulum.duration(hours=24)

    @staticmethod
    def get_card_id(user_id):
        return f'{user_id}:ANONYMOUS_USER_UPSELL'

    def __init__(self, user_id):
        super().__init__(user_id)
        self.card_id = self.get_card_id(user_id)
        self.action = f'https://real.app/signup/{user_id}'
        self.title = 'Reserve your username & sign up!'


class UserNewDatingMatchesTemplate(CardTemplate):

    notify_user_after = pendulum.duration(hours=24)

    @staticmethod
    def get_card_id(user_id):
        return f'{user_id}:USER_DATING_NEW_MATCHES'

    def __init__(self, user_id):
        super().__init__(user_id)
        self.card_id = self.get_card_id(user_id)
        self.action = f'https://real.app/user/{user_id}/new_matches'
        self.title = 'You have new dating matches to review.'
