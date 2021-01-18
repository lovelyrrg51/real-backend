import collections
import logging

from app.mixins.base import ManagerBase
from app.mixins.view.manager import ViewManagerMixin

from .model import Screen

logger = logging.getLogger()


class ScreenManager(ViewManagerMixin, ManagerBase):

    item_type = 'screen'

    def __init__(self, clients, managers=None):
        super().__init__(clients, managers=managers)
        managers = managers or {}
        managers['screen'] = self

    def init_screen(self, screen_name):
        view_dynamo = getattr(self, 'view_dynamo', None)
        return Screen(screen_name, view_dynamo=view_dynamo)

    def record_views(self, screens, user_id, viewed_at=None):
        for screen_name, view_count in dict(collections.Counter(screens)).items():
            self.init_screen(screen_name).record_view_count(user_id, view_count, viewed_at=viewed_at)
