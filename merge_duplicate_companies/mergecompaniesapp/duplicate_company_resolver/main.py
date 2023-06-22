import logging

from django.conf import settings
from .. import bitrix24
from . import services
from .fields_merge import FieldsMergeUpdate


# Логгер - ОШИБКА ОБЪЕДИНЕНИЯ КОМПАНИЙ
logger_1 = logging.getLogger('log-1')
logger_1.setLevel(logging.INFO)
fh_1 = logging.handlers.TimedRotatingFileHandler('./logs/v2/access.log', when='D', interval=1)
formatter_1 = logging.Formatter(fmt='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')
fh_1.setFormatter(formatter_1)
logger_1.addHandler(fh_1)


# объединение компаний - ДЛЯ ВНЕШНЕГО ВЫЗОВА
def find_and_merge_duplicates(id_company):
    bx24 = bitrix24.Bitrix24()
    # # результат объединения компаний
    # result_update = []

    # список реквизитов компаний с одинаковым ИНН: {"ENTITY_ID": ..., "RQ_INN": ..., "RQ_KPP": ...}
    # batch-запрос с двумя подзапросами
    companies = services.get_list_company_ids_with_same_inn(bx24, id_company)
    # удаление компаний из списка дубликатов, которые нельзя объединять
    companies_filter = services.filter_companies(companies)
    # список сгруппированных ID дубликатов компаний [[id_1, id_2, ...], [id_10, id_20, ...], ...]
    duplicates_ids = services.group_companies_for_merging(companies_filter)

    logger_1.info({
        "id_company": id_company,
        "duplicates_ids": duplicates_ids,
    })

    for companies_ids in duplicates_ids:
        # получение данных сделок
        result = services.get_companies_data(companies_ids)
        fields_merge = FieldsMergeUpdate(bx24, [{key: val} for key, val in result.items() if key in companies_ids], result["fields"])
        # данные для объединения
        fields_date_new = fields_merge.get_data()
        logger_1.info({
            "id_company": id_company,
            "duplicates_ids": duplicates_ids,
            "fields_date_new": fields_date_new
        })

    # # удаление из списка игнорируемых компаний
    # for company in companies:
    #     if company["ENTITY_ID"] in LIST_COMPANY_IDS_IGNORED:
    #         continue
    #     if company["RQ_INN"] in LIST_COMPANY_INN_IGNORED:
    #         continue
    #     companies_filter.append(company)

    # # список компаний с одинаковым ИНН и отсутствующим КПП [..., ]
    # companies_kpp_no = get_company__without_kpp(companies_filter)
    # # список компаний с одинаковым ИНН и КПП: [[...,], [...,], ...]
    # companies_kpp_yes = get_company__with_kpp(companies_filter)
    #
    # # Список ID компаний дубликатов
    # result_merge =  ([company["ENTITY_ID"] for company in companies_kpp_no])
    # result_update.append(result_merge)
    # for companies_ in companies_kpp_yes:
    #     # Список ID компаний дубликатов
    #     result_merge = merge_fields_companies([company["ENTITY_ID"] for company in companies_])
    #     result_update.append(result_merge)


def merge_fields_companies(bx24, fields, companies):
    # if 2 <= len(companies) <= 4:
    #     result = merge(bx24, companies)
    #     return result

    data = {}  # {"field_1": "val_1", "field_2": "val_2", ...}
    for field, field_data in fields.items():
        if field_data['isReadOnly'] is True:
            continue
        elif field_data['type'] == 'crm_multifield':
            field_content = contacts_update.get_field_type_crm_multifield(field)
            if field_content:
                data[field] = field_content
        elif field_data['type'] == 'file':
            field_content = contacts_update.get_field_type_file(field)
            if field_content:
                data[field] = field_content
        else:
            field_content = contacts_update.get_field_non_empty(field)
            if field_content:
                data[field] = field_content
    return 1

