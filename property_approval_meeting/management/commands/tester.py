import os
from django.core.management.base import BaseCommand, CommandError


class Command(BaseCommand):
    help = 'Testing purpose'

    def handle(self, *args, **options):
        pass