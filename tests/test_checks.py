# tests/test_checks.py

import django
from django.conf import settings
from django.core import mail
from django.test.utils import override_settings

from daphne.checks import check_daphne_installed


def test_check_daphne_installed():
    settings.configure(
        INSTALLED_APPS=["daphne.apps.DaphneConfig", "django.contrib.staticfiles"],
        EMAIL_BACKEND="django.core.mail.backends.locmem.EmailBackend",
    )
    django.setup()
    mail.outbox = []
    errors = check_daphne_installed(None)
    assert len(errors) == 0
    with override_settings(INSTALLED_APPS=["django.contrib.staticfiles", "daphne"]):
        errors = check_daphne_installed(None)
        assert len(errors) == 1
        assert errors[0].id == "daphne.E001"
