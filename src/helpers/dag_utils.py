import os, pkgutil, importlib, inspect
from src.configs.settings import settings
from datetime import datetime, timedelta, date
from src.helpers.exceptions import InputValidationError

import src.scripts

def import_if_subclass_of(base_cls, package):
    imported_modules = []
    for _, modname, _ in pkgutil.walk_packages(
        package.__path__, package.__name__ + "."
    ):
        module = importlib.import_module(modname)
        # lấy tất cả class định nghĩa trong module đó
        for _, obj in inspect.getmembers(module, inspect.isclass):
            # kiểm tra class có kế thừa base không (trừ base class)
            if issubclass(obj, base_cls) and obj is not base_cls:
                imported_modules.append(modname)
                break  # chỉ cần 1 class hợp lệ là đủ
    return imported_modules

def get_start_time(from_time_str) -> datetime:
    try:
        from_time = None

        if (
            from_time_str is not None
            and from_time_str.strip() != ""
            and from_time_str.strip() != "None"
        ):
            from_time = datetime.strptime(
                from_time_str, settings.datetime_format
            ).replace(tzinfo=settings.timezone)

        if from_time is not None and from_time > datetime.now(tz=settings.timezone):
            raise InputValidationError(
                messages=f"LAST_PROCESS {from_time} must be less than the current time"
            )

        return from_time

    except Exception:
        raise InputValidationError(
            messages=f"LAST_PROCESS invalid format {settings.datetime_format}"
        )


def datetime_from_str(datetime_str):
    if datetime_str is None or datetime_str == '':
        return None
    else:
        return datetime.strptime(datetime_str, settings.datetime_format).replace(
            tzinfo=settings.timezone
        )


def str_from_datetime(datetime):
    if datetime is None:
        return None
    else:
        return datetime.strftime(settings.datetime_format)


def str_to_etl_processors(processors_str: str, etlBaseClass: type) -> list:
    if not processors_str:
        return []

    processor_tables = list(
        set(item.strip() for item in processors_str.replace(",", ";").split(";"))
    )
    processors = [
        p
        for p in (processor_from_table(table, etlBaseClass) for table in processor_tables)
        if p is not None
    ]

    return processors

def processor_from_table(table: str, etlBaseClass: type):
    if not table:
        return None

    table = table.strip().lower()
    processors = all_processors(etlBaseClass)
    for processor in processors:
        if (
            table == f"{processor.__name__}_Etl".lower()
            or table == processor.__name__.lower()
        ):
            return processor

    return None


def all_processors(etlBaseClass):
    # force import all modules
    all_modules = import_if_subclass_of(etlBaseClass, src.scripts)
    return etlBaseClass.__subclasses__()


def table_names_collector(etlBaseClass):
    list_processor_names=[]
    processors = all_processors(etlBaseClass)
    for processor in processors:
        list_processor_names.append(processor.__name__.lower())
    return list_processor_names


# Find all snapshot files in a bucket
def file_matching(objs):
    snap_paths = [f"s3a://{settings.s3_bucket_name}/{obj}" for obj in objs if obj.split('/')[-1].startswith(settings.snapshot_prefix)]
    cdc_paths = [f"s3a://{settings.s3_bucket_name}/{obj}" for obj in objs if obj.split('/')[-1].startswith(settings.cdc_prefix)]
    return snap_paths, cdc_paths