import collections
import functools
import logging

import pendulum
from boto3.dynamodb.conditions import Attr, Key

from app.models.post.enums import PostStatus

from .. import enums

logger = logging.getLogger()


class PostDynamo:
    def __init__(self, dynamo_client):
        self.client = dynamo_client

    def pk(self, post_id):
        return {
            'partitionKey': f'post/{post_id}',
            'sortKey': '-',
        }

    def get_post(self, post_id, strongly_consistent=False):
        return self.client.get_item(self.pk(post_id), ConsistentRead=strongly_consistent)

    def delete_post(self, post_id):
        return self.client.delete_item(self.pk(post_id))

    def get_next_completed_post_to_expire(self, user_id, exclude_post_id=None):
        query_kwargs = {
            'KeyConditionExpression': (
                Key('gsiA1PartitionKey').eq(f'post/{user_id}')
                & Key('gsiA1SortKey').begins_with(f'{PostStatus.COMPLETED}/')
            ),
            'IndexName': 'GSI-A1',
        }
        if exclude_post_id:
            query_kwargs['FilterExpression'] = Attr('postId').ne(exclude_post_id)
        return next(self.client.generate_all_query(query_kwargs), None)

    def generate_posts_by_user(self, user_id, completed=None):
        query_kwargs = {
            'KeyConditionExpression': Key('gsiA2PartitionKey').eq(f'post/{user_id}'),
            'IndexName': 'GSI-A2',
        }
        if completed is not None:
            filter_exp = Attr('postStatus')
            filter_exp = filter_exp.eq if completed else filter_exp.ne
            query_kwargs['FilterExpression'] = filter_exp(PostStatus.COMPLETED)
        return self.client.generate_all_query(query_kwargs)

    def generate_expired_post_pks_by_day(self, date, cut_off_time=None):
        key_conditions = [Key('gsiK1PartitionKey').eq(f'post/{date}')]
        if cut_off_time:
            key_conditions.append(Key('gsiK1SortKey').lt(str(cut_off_time)))
        query_kwargs = {
            'KeyConditionExpression': functools.reduce(lambda a, b: a & b, key_conditions),
            'IndexName': 'GSI-K1',
            'ProjectionExpression': 'partitionKey, sortKey',
        }
        return self.client.generate_all_query(query_kwargs)

    def generate_expired_post_pks_with_scan(self, cut_off_date):
        "Do a table **scan** to generate pks of expired posts. Does *not* include cut_off_date."
        query_kwargs = {
            'FilterExpression': (
                Attr('partitionKey').begins_with('post/') & Attr('expiresAt').lt(str(cut_off_date))
            ),
            'ProjectionExpression': 'partitionKey, sortKey',
        }
        return self.client.generate_all_scan(query_kwargs)

    def add_pending_post(
        self,
        posted_by_user_id,
        post_id,
        post_type,
        posted_at=None,
        expires_at=None,
        album_id=None,
        text=None,
        text_tags=None,
        comments_disabled=None,
        likes_disabled=None,
        sharing_disabled=None,
        verification_hidden=None,
        keywords=None,
        set_as_user_photo=None,
    ):
        posted_at = posted_at or pendulum.now('utc')
        posted_at_str = posted_at.to_iso8601_string()
        post_status = enums.PostStatus.PENDING
        item = {
            'schemaVersion': 3,
            'partitionKey': f'post/{post_id}',
            'sortKey': '-',
            'gsiA2PartitionKey': f'post/{posted_by_user_id}',
            'gsiA2SortKey': f'{post_status}/{posted_at_str}',
            'postId': post_id,
            'postedAt': posted_at_str,
            'postedByUserId': posted_by_user_id,
            'postType': post_type,
            'postStatus': post_status,
        }
        if expires_at:
            expires_at_str = expires_at.to_iso8601_string()
            item.update(
                {
                    'expiresAt': expires_at_str,
                    'gsiA1PartitionKey': f'post/{posted_by_user_id}',
                    'gsiA1SortKey': f'{post_status}/{expires_at_str}',
                    'gsiK1PartitionKey': f'post/{expires_at.date()}',
                    'gsiK1SortKey': str(expires_at.time()),
                }
            )
        if album_id:
            item.update(
                {
                    'albumId': album_id,
                    'gsiK3PartitionKey': f'post/{album_id}',
                    'gsiK3SortKey': -1,  # all non-completed posts have a rank of -1
                }
            )
        if text:
            item['text'] = text
        if text_tags is not None:
            item['textTags'] = [{'tag': tt['tag'], 'userId': tt['userId']} for tt in text_tags]
        if comments_disabled is not None:
            item['commentsDisabled'] = comments_disabled
        if likes_disabled is not None:
            item['likesDisabled'] = likes_disabled
        if sharing_disabled is not None:
            item['sharingDisabled'] = sharing_disabled
        if verification_hidden is not None:
            item['verificationHidden'] = verification_hidden
        if set_as_user_photo is not None:
            item['setAsUserPhoto'] = set_as_user_photo
        if keywords is not None:
            item['keywords'] = list(set(keywords))  # remove duplicates
        return self.client.add_item({'Item': item})

    def increment_flag_count(self, post_id):
        return self.client.increment_count(self.pk(post_id), 'flagCount')

    def decrement_flag_count(self, post_id):
        return self.client.decrement_count(self.pk(post_id), 'flagCount')

    def increment_viewed_by_count(self, post_id):
        return self.client.increment_count(self.pk(post_id), 'viewedByCount')

    def decrement_viewed_by_count(self, post_id):
        return self.client.decrement_count(self.pk(post_id), 'viewedByCount')

    def set_post_status(self, post_item, status, status_reason=None, original_post_id=None, album_rank=None):
        album_id = post_item.get('albumId')

        assert (album_rank is not None) is bool(
            album_id and status == PostStatus.COMPLETED
        ), 'album_rank must be specified only when completing a post in an album'
        album_rank = album_rank if album_rank is not None else -1

        exp_sets = ['postStatus = :postStatus', 'gsiA2SortKey = :gsia2sk']
        exp_removes = []
        exp_values = {
            ':postStatus': status,
            ':gsia2sk': f'{status}/{post_item["postedAt"]}',
        }

        if original_post_id:
            exp_sets.append('originalPostId = :opi')
            exp_values[':opi'] = original_post_id

        if album_id:
            exp_sets.append('gsiK3SortKey = :ar')
            exp_values[':ar'] = album_rank

        if status_reason:
            exp_sets.append('postStatusReason = :psr')
            exp_values[':psr'] = status_reason
        else:
            exp_removes.append('postStatusReason')

        if 'expiresAt' in post_item:
            exp_sets.append('gsiA1SortKey = :gsiA1SortKey')
            exp_values[':gsiA1SortKey'] = f'{status}/{post_item["expiresAt"]}'

        # the setAsUserPhoto attr is not needed after reaching COMPLETED, so delete it if it exists
        if status == PostStatus.COMPLETED:
            exp_removes.append('setAsUserPhoto')

        query_kwargs = {
            'Key': self.pk(post_item['postId']),
            'UpdateExpression': (
                'SET ' + ', '.join(exp_sets) + (' REMOVE ' + ', '.join(exp_removes) if exp_removes else '')
            ),
            'ExpressionAttributeValues': exp_values,
        }

        return self.client.update_item(query_kwargs)

    def set(
        self,
        post_id,
        text=None,
        text_tags=None,
        comments_disabled=None,
        likes_disabled=None,
        sharing_disabled=None,
        verification_hidden=None,
        keywords=None,
    ):
        assert any(
            k is not None
            for k in (text, comments_disabled, likes_disabled, sharing_disabled, verification_hidden, keywords)
        ), 'Action-less post edit requested'

        exp_actions = collections.defaultdict(list)
        exp_names = {}
        exp_values = {}

        if text is not None:
            # empty string deletes
            if text == '':
                exp_actions['REMOVE'].append('#text')
                exp_actions['REMOVE'].append('textTags')
                exp_names['#text'] = 'text'
            else:
                exp_actions['SET'].append('#text = :text')
                exp_names['#text'] = 'text'
                exp_values[':text'] = text

                if text_tags is not None:
                    exp_actions['SET'].append('textTags = :tu')
                    exp_values[':tu'] = text_tags

        if comments_disabled is not None:
            exp_actions['SET'].append('commentsDisabled = :cd')
            exp_values[':cd'] = comments_disabled

        if likes_disabled is not None:
            exp_actions['SET'].append('likesDisabled = :ld')
            exp_values[':ld'] = likes_disabled

        if sharing_disabled is not None:
            exp_actions['SET'].append('sharingDisabled = :sd')
            exp_values[':sd'] = sharing_disabled

        if verification_hidden is not None:
            exp_actions['SET'].append('verificationHidden = :vd')
            exp_values[':vd'] = verification_hidden

        if keywords is not None:
            exp_actions['SET'].append('keywords = :kw')
            exp_values[':kw'] = list(set(keywords))  # remove duplicates

        update_query_kwargs = {
            'Key': self.pk(post_id),
            'UpdateExpression': ' '.join([f'{k} {", ".join(v)}' for k, v in exp_actions.items()]),
        }

        if exp_names:
            update_query_kwargs['ExpressionAttributeNames'] = exp_names
        if exp_values:
            update_query_kwargs['ExpressionAttributeValues'] = exp_values

        return self.client.update_item(update_query_kwargs)

    def set_checksum(self, post_id, posted_at_str, checksum):
        assert checksum  # no deletes
        query_kwargs = {
            'Key': self.pk(post_id),
            'UpdateExpression': 'SET checksum = :checksum, gsiK2PartitionKey = :pk, gsiK2SortKey = :sk',
            'ExpressionAttributeValues': {
                ':checksum': checksum,
                ':pk': f'postChecksum/{checksum}',
                ':sk': posted_at_str,
            },
        }
        return self.client.update_item(query_kwargs)

    def set_is_verified(self, post_id, is_verified, hidden=False):
        query_kwargs = {
            'Key': self.pk(post_id),
            'UpdateExpression': 'SET isVerified = :visibleValue',
            'ExpressionAttributeValues': {},
        }
        if hidden:
            query_kwargs['UpdateExpression'] += ', isVerifiedHiddenValue = :hiddenValue'
            query_kwargs['ExpressionAttributeValues'][':visibleValue'] = True
            query_kwargs['ExpressionAttributeValues'][':hiddenValue'] = is_verified
        else:
            query_kwargs['UpdateExpression'] += ' REMOVE isVerifiedHiddenValue'
            query_kwargs['ExpressionAttributeValues'][':visibleValue'] = is_verified
        return self.client.update_item(query_kwargs)

    def get_first_with_checksum(self, checksum):
        query_kwargs = {
            'KeyConditionExpression': Key('gsiK2PartitionKey').eq(f'postChecksum/{checksum}'),
            'IndexName': 'GSI-K2',
        }
        keys = self.client.query_head(query_kwargs)
        post_id = keys['partitionKey'].split('/')[1] if keys else None
        return post_id

    def set_last_unviewed_comment_at(self, post_item, at):
        "Use `new_value = None` to delete"
        post_id = post_item['postId']
        user_id = post_item['postedByUserId']
        kwargs = {
            'Key': self.pk(post_id),
        }
        if at:
            kwargs['UpdateExpression'] = 'SET gsiA3PartitionKey = :pk, gsiA3SortKey = :sk'
            kwargs['ExpressionAttributeValues'] = {
                ':pk': f'post/{user_id}',
                ':sk': at.to_iso8601_string(),
            }
        else:
            kwargs['UpdateExpression'] = 'REMOVE gsiA3PartitionKey, gsiA3SortKey'
        return self.client.update_item(kwargs)

    def set_expires_at(self, post_item, expires_at):
        expires_at_str = expires_at.to_iso8601_string()
        update_query_kwargs = {
            'Key': self.pk(post_item['postId']),
            'UpdateExpression': 'SET '
            + ', '.join(
                [
                    'expiresAt = :ea',
                    'gsiA1PartitionKey = :ga1pk',
                    'gsiA1SortKey = :ga1sk',
                    'gsiK1PartitionKey = :gk1pk',
                    'gsiK1SortKey = :gk1sk',
                ]
            ),
            'ExpressionAttributeValues': {
                ':ea': expires_at_str,
                ':ga1pk': 'post/' + post_item['postedByUserId'],
                ':ga1sk': post_item['postStatus'] + '/' + expires_at_str,
                ':gk1pk': f'post/{expires_at.date()}',
                ':gk1sk': str(expires_at.time()),
                ':ps': post_item['postStatus'],
            },
            'ConditionExpression': 'postStatus = :ps',
        }
        return self.client.update_item(update_query_kwargs)

    def remove_expires_at(self, post_id):
        update_query_kwargs = {
            'Key': self.pk(post_id),
            'UpdateExpression': 'REMOVE expiresAt, gsiA1PartitionKey, gsiA1SortKey, gsiK1PartitionKey, gsiK1SortKey',
        }
        return self.client.update_item(update_query_kwargs)

    def increment_onymous_like_count(self, post_id):
        return self.client.increment_count(self.pk(post_id), 'onymousLikeCount')

    def decrement_onymous_like_count(self, post_id):
        return self.client.decrement_count(self.pk(post_id), 'onymousLikeCount')

    def increment_anonymous_like_count(self, post_id):
        return self.client.increment_count(self.pk(post_id), 'anonymousLikeCount')

    def decrement_anonymous_like_count(self, post_id):
        return self.client.decrement_count(self.pk(post_id), 'anonymousLikeCount')

    def increment_comment_count(self, post_id, viewed=False):
        query_kwargs = {
            'Key': self.pk(post_id),
            'UpdateExpression': 'ADD commentCount :one',
            'ExpressionAttributeValues': {':one': 1},
            'ConditionExpression': 'attribute_exists(partitionKey)',  # only updates, no creates
        }
        attrs = ['commentCount']
        if not viewed:
            query_kwargs['UpdateExpression'] += ', commentsUnviewedCount :one'
            attrs.append('commentsUnviewedCount')
        msg = f'Failed to increment {", ".join(attrs)} for post `{post_id}`'
        return self.client.update_item(query_kwargs, failure_warning=msg)

    def decrement_comment_count(self, post_id):
        return self.client.decrement_count(self.pk(post_id), 'commentCount')

    def decrement_comments_unviewed_count(self, post_id):
        return self.client.decrement_count(self.pk(post_id), 'commentsUnviewedCount')

    def clear_comments_unviewed_count(self, post_id):
        query_kwargs = {
            'Key': self.pk(post_id),
            'UpdateExpression': 'REMOVE commentsUnviewedCount',
            'ConditionExpression': 'attribute_exists(partitionKey)',
        }
        msg = f'Failed to clear commentsUnviewedCount for post `{post_id}`'
        return self.client.update_item(query_kwargs, failure_warning=msg)

    def set_album_id(self, post_item, album_id, album_rank=None):
        post_id = post_item['postId']
        post_status = post_item['postStatus']

        assert (album_rank is not None) is bool(
            album_id and post_status == PostStatus.COMPLETED
        ), 'album_rank must be specified only when setting album_id for a completed post'
        album_rank = album_rank if album_rank is not None else -1

        query_kwargs = {'Key': self.pk(post_id)}
        if album_id:
            query_kwargs['UpdateExpression'] = 'SET albumId = :aid, gsiK3PartitionKey = :pk, gsiK3SortKey = :ar'
            query_kwargs['ConditionExpression'] = 'postStatus = :ps'
            query_kwargs['ExpressionAttributeValues'] = {
                ':aid': album_id,
                ':pk': f'post/{album_id}',
                ':ar': album_rank,
                ':ps': post_status,
            }
        else:
            query_kwargs['UpdateExpression'] = 'REMOVE albumId, gsiK3PartitionKey, gsiK3SortKey'
        return self.client.update_item(query_kwargs)

    def set_album_rank(self, post_id, album_rank):
        query_kwargs = {
            'Key': self.pk(post_id),
            'UpdateExpression': 'SET gsiK3SortKey = :ar',
            'ExpressionAttributeValues': {':ar': album_rank},
        }
        return self.client.update_item(query_kwargs)

    def generate_post_ids_in_album(self, album_id, completed=None, after_rank=None):
        assert completed is None or after_rank is None, 'Cant specify both completed and after_rank kwargs'

        key_exps = [Key('gsiK3PartitionKey').eq(f'post/{album_id}')]
        if completed is True:
            key_exps.append(Key('gsiK3SortKey').gt(-1))
        if completed is False:
            key_exps.append(Key('gsiK3SortKey').eq(-1))
        if after_rank is not None:
            key_exps.append(Key('gsiK3SortKey').gt(after_rank))

        query_kwargs = {
            'KeyConditionExpression': functools.reduce(lambda a, b: a & b, key_exps),
            'IndexName': 'GSI-K3',
            'ProjectionExpression': 'partitionKey',
        }
        return map(lambda item: item['partitionKey'].split('/')[1], self.client.generate_all_query(query_kwargs))
