import logging
from redash import models, settings
import httplib2
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

SERVICE_ACCOUNT_KEY_FILE_PATH = 'redash/authentication/service_account_key.json'

logger = logging.getLogger('google_apps_group_membership_verification')

def create_directory_service():
    credentials = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_KEY_FILE_PATH, 'https://www.googleapis.com/auth/admin.directory.group.readonly')
    http = httplib2.Http()
    http = credentials.authorize(http)

    return build('admin', 'directory_v1', http=http)


def get_user_group_memberships(domain, email):
    directory_service = create_directory_service()
    response = directory_service.groups().list(domain=domain, userKey=email).execute()

    if response.status_code == 401:
        logger.warning("Failed getting user group memberships (response code 401).")
        return None

    return response.json()


def verify_group_membership(org, domain, email):
    whitelisted_groups_for_domain = org.google_apps_domain_groups_whitelist(domain)
    if whitelisted_groups_for_domain:
        group_memberships = get_user_group_memberships(domain, email)
        # TODO: parse the json into a list
        return bool(set(group_memberships) & set(whitelisted_groups))
    else:
        return True

