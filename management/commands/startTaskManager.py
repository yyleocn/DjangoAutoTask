from django.core.management.base import BaseCommand, no_translations


class Command(BaseCommand):
    help = "Starts auto task manager."

    def add_arguments(self, parser):
        pass
        # parser.add_argument(
        #     "--run-once",
        #     action="store_true",
        #     dest="run_once",
        #     default=False,
        #     help="Run once and then stop.",
        # )

    @no_translations
    def handle(self, *args, **options):
        pass
