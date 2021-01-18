import pendulum

from app.models.user.enums import UserSubscriptionLevel

from .exceptions import ClientException


def validate_match_location_radius(match_location_radius, subscription_level):
    if match_location_radius < 5:
        raise ClientException('matchLocationRadius should be greater than or equal to 5')
    if subscription_level == UserSubscriptionLevel.BASIC and match_location_radius > 100:
        raise ClientException('matchLocationRadius should be less than or equal to 100')
    return True


def validate_age_range(match_age_range):
    minAge = match_age_range.get('min')
    maxAge = match_age_range.get('max')

    if minAge > maxAge or minAge < 18 or maxAge > 100:
        raise ClientException('Invalid matchAgeRange')
    return True


def validate_location(location):
    latitude = location['latitude']
    longitude = location['longitude']
    accuracy = location.get('accuracy')

    if latitude > 90 or latitude < -90:
        raise ClientException('latitude should be in [-90, 90]')
    if longitude > 180 or longitude < -180:
        raise ClientException('longitude should be in [-180, 180]')
    if accuracy is not None and accuracy < 0:
        raise ClientException('accuracy should be greater than or equal to zero')
    return True


def validate_match_genders(match_genders):
    if not match_genders:
        raise ClientException('matchGenders cannot be empty')
    return True


def validate_date_of_birth(date_of_birth):
    try:
        pendulum.parse(date_of_birth)
    except pendulum.parsing.exceptions.ParserError as err:
        raise ClientException('dateOfBirth contains timezone information') from err
    return True


def validate_height(height):
    if height < 0 or height > 117:
        raise ClientException('Invalid height')
    return True


def validate_height_range(match_height_range):
    minHeight = match_height_range.get('min')
    maxHeight = match_height_range.get('max')

    if minHeight > maxHeight or minHeight < 0 or maxHeight > 117:
        raise ClientException('Invalid matchHeightRange')
    return True
