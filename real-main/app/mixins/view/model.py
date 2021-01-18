import logging

import pendulum

from .enums import ViewedStatus
from .exceptions import ViewAlreadyExists

logger = logging.getLogger()


class ViewModelMixin:
    def __init__(self, view_dynamo=None, **kwargs):
        super().__init__(**kwargs)
        if view_dynamo:
            self.view_dynamo = view_dynamo

    def get_viewed_status(self, user_id):
        """
        ViewedStatus based only on view records for this model.

        Note that for some of our models the ViewedStatus rendered on
        the GQL api is dependent on view records for the parent model.
        IE, Comment.viewedStatus depends on the parent Post's view records, and
        ChatMessage.viewedStatus depends on the parent Chat's view records.
        That parent dependency is not reflected in the viewed status returned
        by this method.
        """
        if hasattr(self, 'user_id') and self.user_id == user_id:  # owner of the item
            return ViewedStatus.VIEWED
        elif self.view_dynamo.get_view(self.id, user_id):
            return ViewedStatus.VIEWED
        else:
            return ViewedStatus.NOT_VIEWED

    def record_view_count(self, user_id, view_count, viewed_at=None, view_type=None):
        viewed_at = viewed_at or pendulum.now('utc')
        is_first_view_for_user = False
        view_item = self.view_dynamo.get_view(self.id, user_id)
        if view_item:
            self.view_dynamo.increment_view_count(self.id, user_id, view_count, viewed_at, view_type=view_type)
        else:
            try:
                self.view_dynamo.add_view(self.id, user_id, view_count, viewed_at, view_type=view_type)
            except ViewAlreadyExists:
                # we lost a race condition to add the view, so still need to record our data
                self.view_dynamo.increment_view_count(
                    self.id, user_id, view_count, viewed_at, view_type=view_type
                )
            else:
                is_first_view_for_user = True
        return is_first_view_for_user
