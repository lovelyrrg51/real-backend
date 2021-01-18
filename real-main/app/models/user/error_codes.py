from enum import Enum


class UserDatingMissingError(Enum):
    fullName = 'MISSING_FULL_NAME'
    displayName = 'MISSING_DISPLAY_NAME'
    photoPostId = 'MISSING_PHOTO_POST_ID'
    age = 'MISSING_AGE'
    gender = 'MISSING_GENDER'
    location = 'MISSING_LOCATION'
    height = 'MISSING_HEIGHT'
    matchAgeRange = 'MISSING_MATCH_AGE_RANGE'
    matchGenders = 'MISSING_MATCH_GENDERS'
    matchHeightRange = 'MISSING_MATCH_HEIGHT_RANGE'
    matchLocationRadius = 'MISSING_MATCH_LOCATION_RADIUS'


class UserDatingWrongError:
    MIN_AGE = 'WRONG_AGE_MIN'
    MAX_AGE = 'WRONG_AGE_MAX'
    THREE_HOUR_PERIOD = 'WRONG_THREE_HOUR_PERIOD'
