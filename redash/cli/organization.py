from flask_script import Manager
from redash import models

manager = Manager(help="Organization management commands.")


@manager.option('domains', help="comma separated list of domains to allow")
def set_google_apps_domains(domains):
    organization = models.Organization.select().first()

    organization.settings[models.Organization.SETTING_GOOGLE_APPS_DOMAINS] = domains.split(',')
    organization.save()

    print "Updated list of allowed domains to: {}".format(organization.google_apps_domains)


@manager.option('groups', help='comma separated list of groups to allow for the domain')
@manager.option('domain', help='domain to which the group whitelist should apply')
def set_google_apps_groups_for_domain(domain, groups):
    organization = models.Organization.select().first()

    organization.settings.setdefault(models.Organization.SETTING_GOOGLE_APPS_DOMAIN_GROUPS_WHITELIST], {})
    organization.settings[models.Organization.SETTING_GOOGLE_APPS_DOMAIN_GROUPS_WHITELIST][domain] = groups.split(',')
    organization.save()

    print "Updated list of allowed domains to: {}".format(organization.google_apps_domain_groups_whitelist(domain))


@manager.command
def show_google_apps_domains():
    organization = models.Organization.select().first()
    print "Current list of Google Apps domains: {}".format(organization.google_apps_domains)


@manager.command
def list():
    """List all organizations"""
    orgs = models.Organization.select()
    for i, org in enumerate(orgs):
        if i > 0:
            print "-" * 20

        print "Id: {}\nName: {}\nSlug: {}".format(org.id, org.name, org.slug)
