import logging

from app.mixins.flag.model import FlagModelMixin
from app.mixins.view.model import ViewModelMixin

logger = logging.getLogger()


class Screen(ViewModelMixin, FlagModelMixin):

    item_type = 'screen'

    def __init__(self, screen_name, **kwargs):
        self.id = screen_name
        super().__init__(**kwargs)
