class UserStatus:
    ACTIVE = 'ACTIVE'
    ANONYMOUS = 'ANONYMOUS'
    DISABLED = 'DISABLED'
    DELETING = 'DELETING'
    RESETTING = 'RESETTING'

    _ALL = (ACTIVE, ANONYMOUS, DISABLED, DELETING, RESETTING)


class UserPrivacyStatus:
    PRIVATE = 'PRIVATE'
    PUBLIC = 'PUBLIC'

    _ALL = (PRIVATE, PUBLIC)


class UserSubscriptionLevel:
    BASIC = 'BASIC'
    DIAMOND = 'DIAMOND'

    _ALL = (BASIC, DIAMOND)
    _PAID = (DIAMOND,)


class UserGender:
    MALE = 'MALE'
    FEMALE = 'FEMALE'

    _ALL = (MALE, FEMALE)


class UserDatingStatus:
    ENABLED = 'ENABLED'
    DISABLED = 'DISABLED'

    _ALL = (ENABLED, DISABLED)
