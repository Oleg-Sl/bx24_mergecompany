import requests
import time
import os
import json
import datetime
import logging
import re

from django.conf import settings
from . import bitrix24
# from .views import LIST_COMPANY_IDS_IGNORED, LIST_COMPANY_INN_IGNORED, FIELDS_COMPANIES_LISST

# Список ID компаний объединение которых запрещено
LIST_COMPANY_IDS_IGNORED = []
# Список ИНН компаний объединение которых запрещено
LIST_COMPANY_INN_IGNORED = ["5407207664", "5407473338", ]

# Список полей объединяемых при слиянии компаний:
#   "multiple_value" - PHONE: [{"VALUE": ...}, {"VALUE": ...}]
#   "multiple" - IM: []
#   "single" - одиночное значение
FIELDS_COMPANIES_LISST = {
    "multiple_value": ["PHONE", "EMAIL", ],
    "multiple": ["WEB", "IM", ],
    "single": ["TITLE", "ASSIGNED_BY_ID", "CREATED_BY_ID", "ADDRESS", "UF_CRM_1639121341", "INDUSTRY",
               "UF_CRM_1639121988", "UF_CRM_1617767435", "UF_CRM_1639121225", "UF_CRM_1639121303", "REVENUE",
               "UF_CRM_1639121262", "UF_CRM_1640828035", "UF_CRM_1640828023", "UF_CRM_1639121612", "UF_CRM_1639121999",]
}

# , encoding="cp1251"
# Логгер - ERROR
logger_error = logging.getLogger('errors')
logger_error.setLevel(logging.ERROR)
fh_error = logging.handlers.TimedRotatingFileHandler('./logs/error/error.log', when='D', interval=1)
formatter_error = logging.Formatter(fmt='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')
fh_error.setFormatter(formatter_error)
logger_error.addHandler(fh_error)

# Логгер - УСПЕШНОЕ ОБЪЕДИНЕНИЕ КОМПАНИЙ
logger_success_merge = logging.getLogger('success_merge')
logger_success_merge.setLevel(logging.INFO)
fh_success_merge = logging.handlers.TimedRotatingFileHandler('./logs/success/success_merge.log', when='D', interval=1)
formatter_success_merge = logging.Formatter(fmt='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')
fh_success_merge.setFormatter(formatter_success_merge)
logger_success_merge.addHandler(fh_success_merge)

# Логгер - ОШИБКА ОБЪЕДИНЕНИЯ КОМПАНИЙ
logger_error_merge = logging.getLogger('error_merge')
logger_error_merge.setLevel(logging.INFO)
fh_error_merge = logging.handlers.TimedRotatingFileHandler('./logs/success/error_merge.log', when='D', interval=1)
formatter_error_merge = logging.Formatter(fmt='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')
fh_error_merge.setFormatter(formatter_error_merge)
logger_error_merge.addHandler(fh_error_merge)

# Логгер - ОШИБКА ОБЪЕДИНЕНИЯ КОМПАНИЙ
logger_test = logging.getLogger('testlog')
logger_test.setLevel(logging.INFO)
fh_test = logging.handlers.TimedRotatingFileHandler('./logs/test.log', when='D', interval=1)
formatter_test = logging.Formatter(fmt='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')
fh_test.setFormatter(formatter_test)
logger_test.addHandler(fh_test)

# Логгер -
logger_access_v2 = logging.getLogger('testlog')
logger_access_v2.setLevel(logging.INFO)
fh_access_v2 = logging.handlers.TimedRotatingFileHandler('./logs/get_list_company_ids_with_same_inn/access.log', when='D', interval=1)
formatter_access_v2 = logging.Formatter(fmt='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')
fh_access_v2.setFormatter(formatter_access_v2)
logger_access_v2.addHandler(fh_access_v2)

# Ключи и секреты для авторизации в Битрикс
path_secret_file = os.path.join(settings.BASE_DIR, 'secrets.json')
path_settings_file = os.path.join(settings.BASE_DIR, 'settings_app_bx24.json')


class MyException(Exception):
    def __init__(self, message):
        super().__init__(message)


# объединение компаний - ДЛЯ ВНЕШНЕГО ВЫЗОВА
def merge_companies(id_company):
    bx24 = bitrix24.Bitrix24()
    logger_test.info({"company_id": id_company, "row": "140", })
    # результат объединения компаний
    result_update = []
    # список компаний с одинаковым ИНН
    companies = get_list_company_ids_with_same_inn(bx24, id_company)
    # список компаний без наличия их в списке игнорирования
    companies_filter = []
    logger_test.info({
        "company_id": id_company,
        "список компаний с одинаковым ИНН": companies,
        "row": "147",
    })

    # удаление из списка игнорируемых компаний
    for company in companies:
        if company["ENTITY_ID"] in LIST_COMPANY_IDS_IGNORED:
            continue
        if company["RQ_INN"] in LIST_COMPANY_INN_IGNORED:
            continue
        companies_filter.append(company)

    logger_test.info({
        "company_id": id_company,
        "список компаний без наличия их в списке игнорирования": companies_filter,
        "row": "161",
    })

    # объединение компаний без КПП
    companies_kpp_no = [company for company in companies_filter if not company["RQ_KPP"]]
    if 2 <= len(companies_kpp_no) <= 4:
        logger_test.info({
            "company_id": id_company,
            "список объединение компаний без КПП": companies_kpp_no,
            "row": "170",
        })
        # объединение компаний
        res = merge(bx24, companies_kpp_no)
        result_update.append(res)

    # группировка компаний с КПП
    companies_kpp_yes = [company for company in companies_filter if company["RQ_KPP"]]
    # компании сгруппированнные по КПП
    companies_kpp_group = {}
    for company in companies_kpp_yes:
        key = company["RQ_KPP"]
        if key in companies_kpp_group:
            companies_kpp_group[key].append(company)
        else:
            companies_kpp_group[key] = [company, ]

    # объединение компаний с КПП
    for _, _companies in companies_kpp_group.items():
        if 2 <= len(_companies) <= 4:
            logger_test.info({
                "company_id": id_company,
                "список объединение компаний с КПП": _companies,
                "row": "170",
            })
            # объединение компаний
            res = merge(bx24, _companies)
            result_update.append(res)

    return result_update


# возвращает список компаний с одинаковым ИНН
def get_list_company_ids_with_same_inn(bx24, id_company):
    response = bx24.batch_2({
        "halt": 0,
        "cmd": {
            "INN": f"crm.requisite.list?FILTER[ENTITY_ID]={id_company}&FILTER[ENTITY_TYPE_ID]=4",
            "REQUISITES": f"crm.requisite.list?select[]=ENTITY_ID&select[]=RQ_INN&select[]=RQ_KPP&FILTER[RQ_INN]=$result[INN][0][RQ_INN]&FILTER[ENTITY_TYPE_ID]=4"
        }
    })
    logger_access_v2.info({
        "id_company": id_company,
        "response": response,
    })

    if not response or not response.get("result", None) or not response["result"].get("result", None):
        logger_error.error({
                "company_id": id_company,
                "function": "get_list_company_ids_with_same_inn",
                "msg": "Ответ от Битрикс не верный",
                "response": response,
        })
        return []

    result = response["result"]["result"]
    if "REQUISITES" not in result or "INN" not in result:
        logger_error.error({
            "company_id": id_company,
            "function": "get_list_company_ids_with_same_inn",
            "msg": 'Не полный ответ от Битрикс (отсутствует INN или REQUISITES)',
            "response": result,
        })
        return []

    inn = ""
    companies_inn = result.get("INN", [])
    requisites = result.get("REQUISITES", [])

    if companies_inn:
        inn = companies_inn[0]["RQ_INN"]

    # если ИНН отсутствует или слишком короткий, то выход
    if not inn or len(inn) < 5:
        return []

    ids_list = []                   # спииок id уникальных компаний
    response_unique = []            # список реквизитов уникальных компаний

    # выбор только уникальных компаний
    for requisite in requisites:
        if requisite["ENTITY_ID"] not in ids_list:
            ids_list.append(requisite["ENTITY_ID"])
            response_unique.append(requisite)

    return response_unique


# принимет список компаний и выполняет их объединение
def merge(bx24, companies):
    # Срисок ID объединяемых компаний
    _companies_ids = [company["ENTITY_ID"] for company in companies]

    # получение данных компаний из Битрикс
    updated_company_data = get_data_companies(bx24, _companies_ids)
    if not updated_company_data:
        return

    companies_ids = updated_company_data["companies_ids"]

    # ID настоящей компании
    id_real = min(companies_ids, key=int)

    # объединение компаний
    result_merge = _merge_company(bx24, id_real, updated_company_data)
    if not result_merge:
        return

    # проверка объединения компаний
    verification_merge, data_update = verification_merge_company(bx24, id_real, companies_ids, updated_company_data)

    # если объединение компаний прошло успешно
    if verification_merge:
        deleteCompanies(bx24, id_real, companies_ids, updated_company_data)
        logger_success_merge.info(data_update)
    else:
        logger_error_merge.info(data_update)

    return data_update


# получение данных компаний из Битрикс
def get_data_companies(bx24, _companies_ids):
    cmd = formation_command_to_receive_company_data(_companies_ids)
    result = bx24.batch_2({
        "halt": 0,
        "cmd": cmd
    })

    if not result or not result.get("result", None) or not result["result"].get("result", None):
        logger_error.error({
            "companies_ids": _companies_ids,
            "function": "get_data_companies",
            "msg": "Ответ от Битрикс не верный",
            "response": result,
        })
        return None

    companies_ids = [companiy_id for companiy_id in _companies_ids if companiy_id in result["result"]["result"].keys()]
    if len(companies_ids) < 2:
        return
    
    id_real = min(companies_ids, key=int)

    data_real = {}
    data_duplicate = {}

    # Создание объекта для хранения значений объединяемых полей
    for field in FIELDS_COMPANIES_LISST["multiple_value"]:
        data_duplicate[field] = []
        data_real[field] = []
    for field in FIELDS_COMPANIES_LISST["multiple"]:
        data_duplicate[field] = []
        data_real[field] = []
    for field in FIELDS_COMPANIES_LISST["single"]:
        data_duplicate[field] = []
        data_real[field] = None

    data_deals_real = []
    data_deals_duplicate = []
    data_contacts_real = []
    data_contacts_duplicate = []

    # Объединение полей
    for ident in companies_ids:
        key_cont = f"contact{ident}"
        key_deal = f"deals{ident}"
        company = result["result"]["result"].get(ident, {})                 # данные компании
        response_cont = result["result"]["result"].get(key_cont, {})        # контакты компании
        response_deal = result["result"]["result"].get(key_deal, {})        # сделки компании

        for field in FIELDS_COMPANIES_LISST["multiple_value"]:
            lst = [elem["VALUE"] for elem in company.get(field, [])]
            if id_real != ident:
                data_duplicate[field] += data_duplicate[field] + lst
            if id_real == ident:
                data_real[field] += data_real[field] + lst

        for field in FIELDS_COMPANIES_LISST["multiple"]:
            lst = [elem for elem in company.get(field, [])]
            if id_real != ident:
                data_duplicate[field] += data_duplicate[field] + lst
            if id_real == ident:
                data_real[field] += data_real[field] + lst

        for field in FIELDS_COMPANIES_LISST["single"]:
            value = company.get(field, None)
            if id_real != ident and value:
                data_duplicate[field].append(value)
            if id_real == ident:
                data_real[field] = value

        deals = [deal["ID"] for deal in response_deal]                      # список ID сделоек компании
        contacts = [contact["CONTACT_ID"] for contact in response_cont]     # список ID контактов компании

        # данные дубликатов компаний
        if id_real != ident:
            data_deals_duplicate = data_deals_duplicate + deals
            data_contacts_duplicate = data_contacts_duplicate + contacts

        # данные настоящей компании
        if id_real == ident:
            data_deals_real = data_deals_real + deals
            data_contacts_real = data_contacts_real + contacts

    data_company = {}

    for field in FIELDS_COMPANIES_LISST["multiple_value"]:
        data_company[field] = data_duplicate[field] + data_real[field]

    for field in FIELDS_COMPANIES_LISST["multiple"]:
        data_company[field] = data_duplicate[field] + data_real[field]

    for field in FIELDS_COMPANIES_LISST["single"]:
        if not data_real[field] and len(data_duplicate[field]) > 0:
            data_company[field] = data_duplicate[field][0]

    return {
        "companies_ids": companies_ids,
        "deals": data_deals_duplicate,                                  # crm.deal.update
        "contacts": data_contacts_duplicate + data_contacts_real,       # crm.company.contact.items.set
        "data": data_company,                                           # crm.company.update
        "responsible": data_duplicate["ASSIGNED_BY_ID"] + data_duplicate["CREATED_BY_ID"] + [data_real["ASSIGNED_BY_ID"], data_real["CREATED_BY_ID"]],
        "title_real": data_real["TITLE"],
        "title_dupl": data_duplicate["TITLE"],
    }


# формирование комманд на получение данных компаний
def formation_command_to_receive_company_data(ids):
    cmd = {}
    for ident in ids:
        key_cont = f"contact{ident}"
        key_deal = f"deals{ident}"
        cmd[ident] = f"crm.company.get?ID={ident}"
        cmd[key_cont] = f"crm.company.contact.items.get?ID={ident}"
        cmd[key_deal] = f"crm.deal.list?select[0]=ID&filter[COMPANY_ID]={ident}"

    return cmd


# получает ID компании и поля с обновляемыми данными, возвращает сформированный запрос обновления компании
def formation_req_update_data_company(id_real, data):
    command = f"crm.company.update?ID={id_real}"
    for field in data:
        if isinstance(data[field], list):
            for index, element in enumerate(data[field]):
                if isinstance(element, dict):
                    command += f'&fields[{field}][{index}][VALUE]={element["VALUE"]}'
                    command += f'&fields[{field}][{index}][VALUE_TYPE]={element["VALUE_TYPE"]}'
                else:
                    command += f'&fields[{field}][{index}][VALUE]={element}'
        else:
            command += f'&fields[{field}]={data[field]}'

    return command


# получает ID компании и поля с обновляемыми контактами, возвращает сформированный запрос обновления контактов компании
def formation_req_update_date_contacts(id_real, contacts):
    if isinstance(contacts, list):
        cmd = f'crm.company.contact.items.set?id={id_real}'
        for index, contact in enumerate(contacts):
            cmd += f'&items[{index}][CONTACT_ID]={contact}'

        return cmd;


# получает ID компании и поля с добавляемыми сделками, возвращает сформированный запрос на обновление привязки компаний к сделке
def formation_req_get_date_deals(id_real, deals):
    commands = {};
    for deal in deals:
        key = f'deal{deal}'
        commands[key] = f'crm.deal.update?id={deal}&fields[COMPANY_ID]={id_real}'

    return commands;


# слияние дублей компаний
def _merge_company(bx24, id_real, data):
    cmd = {}
    # формирование запроса на обновление данных о компании - crm.company.update
    if data["data"]:
        cmd["data"] = formation_req_update_data_company(id_real, data["data"])

    # формирование запроса на перезапись всех контактов - crm.company.contact.items.set
    if data["contacts"]:
        cmd["contacts"] = formation_req_update_date_contacts(id_real, data["contacts"])

    # формирование запроса на перенос всех сделок - crm.deal.update
    if data["deals"]:
        commands = formation_req_get_date_deals(id_real, data["deals"])
        for key in commands:
            cmd[key] = commands[key]

    response = bx24.batch_2({
        "halt": 0,
        "cmd": cmd
    })

    if not response or not response.get("result", None) or not response["result"].get("result", None):
        logger_error.error({
            "company_id": id_real,
            "function": "_merge_company",
            "msg": "Ошибка объединения компаний",
            "response": response,
        })
        return None

    return response["result"]["result"]


# проверка вхождения элементов старого массива в новый
def verification_elements_in_list(lst_real, lst_dupl):
    for elem in lst_dupl:
        if elem not in lst_real:
            return False

    return True


# проверка вхождения элементов старого массива в новый
def verification_objects_in_list(lst_obj_real, lst_obj_dupl):
    lst_real = [elem["VALUE"] for elem in lst_obj_real]
    lst_dupl = [elem["VALUE"] for elem in lst_obj_dupl]
    for elem in lst_dupl:
        if elem not in lst_real:
            return False

    return True


# проверка вхождения элементов старого массива в новый
def verification_objects_number_in_list(lst_obj_real, lst_obj_dupl):
    lst_real = ["".join(re.findall(r'\d+', elem["VALUE"])) for elem in lst_obj_real]
    lst_dupl = ["".join(re.findall(r'\d+', elem["VALUE"])) for elem in lst_obj_dupl]
    for elem in lst_dupl:
        if elem not in lst_real:
            return False

    return True


# проверка вхождения элементов старого массива в новый
def verification_contacts_in_list(lst_obj_real, lst_obj_dupl):
    lst_real = [elem["CONTACT_ID"] for elem in lst_obj_real]
    lst_dupl = [elem["CONTACT_ID"] for elem in lst_obj_dupl]
    for elem in lst_dupl:
        if elem not in lst_real:
            return False

    return True


# проверка верности объединения компаний
def verification_merge_company(bx24, id_real, companies_ids, company_data_old):
    cmd = formation_command_to_receive_company_data(companies_ids)

    result = bx24.batch_2({
        "halt": 0,
        "cmd": cmd
    })

    if not result or not result.get("result", None) or not result["result"].get("result", None):
        logger_error.error({
            "companies_ids": companies_ids,
            "function": "verification_merge_company",
            "msg": "Ошибка проверки объединения компаний",
            "response": result,
        })
        return False, {}

    contacts_real = []                  # список контактов - настоящей компании
    contacts_duplicate = []             # список контактов - дубликатов компаний
    deals_real = []                     # список сделок - настоящей компании
    deals_duplicate = []                # список сделок - дубликатов компаний

    data_duplicate = {}                 # данные - дубликатов компаний
    data_real = {}                      # данные - настоящей компании

    for field in FIELDS_COMPANIES_LISST["multiple_value"]:
        data_duplicate[field] = []
        data_real[field] = []

    for field in FIELDS_COMPANIES_LISST["multiple"]:
        data_duplicate[field] = []
        data_real[field] = []

    for ident in companies_ids:
        key_cont = f"contact{ident}"
        key_deal = f"deals{ident}"

        company = result["result"]["result"].get(ident, {})
        response_cont = result["result"]["result"].get(key_cont, {})
        response_deal = result["result"]["result"].get(key_deal, {})

        if id_real != ident:
            contacts_duplicate = contacts_duplicate + response_cont
            deals_duplicate = deals_duplicate + response_deal

        if id_real == ident:
            contacts_real = contacts_real + response_cont
            deals_real = deals_real + response_deal

        for field in FIELDS_COMPANIES_LISST["multiple_value"]:
            if id_real != ident:
                data_duplicate[field] = data_duplicate[field] + company.get(field, [])
            if id_real == ident:
                data_real[field] = data_real[field] + company.get(field, [])

        for field in FIELDS_COMPANIES_LISST["multiple"]:
            if id_real != ident:
                data_duplicate[field] = data_duplicate[field] + company.get(field, [])
            if id_real == ident:
                data_real[field] = data_real[field] + company.get(field, [])

    # проверка валидности - КОНТАКТЫ
    status_verif_contacts = verification_contacts_in_list(contacts_real, contacts_duplicate)
    # проверка валидности - СДЕЛКИ
    status_verif_deals = True if len(deals_duplicate) == 0 else False

    # проверка валидности - ПОЛЯ КОМПАНИИ
    for field in FIELDS_COMPANIES_LISST["multiple_value"]:
        if field == "PHONE":
            verification = verification_objects_number_in_list(data_real[field], data_duplicate[field])
            # verification = verification_objects_in_list(data_real[field], data_duplicate[field])
            if not verification:
                return False, {
                    "merge": "False",
                    "message": f"Не удалось объединить поле компании: {field}",
                    "companies_ids": companies_ids,
                    "real": data_real[field],
                    "duplicate": data_duplicate[field],
                }

    # for field in FIELDS_COMPANIES_LISST["multiple"]:
    #     if not verification_objects_in_list(data_real[field], data_duplicate[field]):
    #         logger_error.error({
    #             "companies_ids": companies_ids,
    #             "function": "verification_merge_company",
    #             "msg": f"Объединение компаний не прошло проверку по полю: {field}",
    #             "data_real": data_real[field],
    #             "data_duplicate": data_duplicate[field],
    #         })
    #         return False, {}

    # проверка валидности - КОНТАКТЫ
    if not status_verif_contacts:
        return False, {
            "merge": "False",
            "message": "Не удалось объединить контакты компании",
            "companies_ids": companies_ids,
            "contacts_real": contacts_real,
            "contacts_duplicate": contacts_duplicate,
        }

    # проверка валидности - СДЕЛОК
    if not status_verif_deals:
        return False, {
            "merge": "False",
            "message": "Не удалось объединить сделки компании",
            "companies_ids": companies_ids,
            "deals_real": deals_real,
            "deals_duplicate": deals_duplicate,
        }

    return True, {
        "merge": "True",
        "message": "Компании успешно объединены",
        "companies_ids": companies_ids,
        "contacts_real": contacts_real,
        "data_real": data_real,
        "deals_real": deals_real,
    }


# Удаление списка компаний
def deleteCompanies(bx24, id_real, companies_ids, update_data_company):
    cmd = {}
    for ident in companies_ids:
        if ident != id_real:
            cmd[ident] = f"crm.company.delete?ID={ident}"

    users = list(set(update_data_company['responsible']))
    domain = get_token().get("domain", "atonlab.bitrix24.ru")
    url_real_company = f"https://{domain}/crm/company/details/{id_real}/"
    for user in users:
        key = f"MSG{user}"
        message = f"Компании {', '.join(update_data_company['title_dupl'])} объединились в компанию {update_data_company['title_real']}"
        cmd[key] = f"im.notify.personal.add?USER_ID={user}&MESSAGE={message}&ATTACH[0][LINK][NAME]={update_data_company['title_real']}&ATTACH[0][LINK][DESC]=&ATTACH[0][LINK][LINK]={url_real_company}"

    # print("cmd = ", cmd)
    response = bx24.batch_2({
        "halt": 0,
        "cmd": cmd
    })

    if not response or not response.get("result", None) or not response["result"].get("result", None):
        logger_error.error({
            "companies_ids": companies_ids,
            "function": "deleteCompanies",
            "msg": f"Удалить дубликаты компаний не удалось",
            "response": response,
        })
        return None

    return response["result"]["result"]



# запись настроек приложения в фаил
def write_app_data_to_file(data):
    lifetime_token = data.get("expires_in", 3600)    # время жизни access токена
    data["expires_in"] = time.time() + float(lifetime_token) - 5 * 60   # время по истечении которого обновляется токен

    # сохранение данных авторизации в файл
    with open(path_secret_file, 'w') as secrets_file:
        json.dump(data, secrets_file)


# обновление токенов в файле
def update_tokens_in_file(auth_token, expires_in, refresh_token):
    # expires_in = time.time() + float(lifetime_token) - 5 * 60  # время по истечении которого обновляется токен

    with open(path_secret_file) as secrets_file:
        data = json.load(secrets_file)

    data["auth_token"] = auth_token
    data["expires_in"] = expires_in
    data["refresh_token"] = refresh_token

    with open(path_secret_file, 'w') as secrets_file:
        json.dump(data, secrets_file)


# возвращает токен приложения
def get_app_sid():
    if not os.path.exists(path_secret_file):
        return

    with open(path_secret_file) as secrets_file:
        token_app = json.load(secrets_file)

    return token_app.get("application_token", "")


# возвращает все токены из файла
def get_token():
    if not os.path.exists(path_secret_file):
        return {}

    with open(path_secret_file) as secrets_file:
        token_app = json.load(secrets_file)

    return token_app


# возвращает все токены из файла
def get_settings_app():
    if not os.path.exists(path_settings_file):
        return {}

    with open(path_settings_file) as secrets_file:
        settings_app = json.load(secrets_file)

    return settings_app


# преобразование даты к виду для сохранения в БД
def convert_date_to_obj(date):
    if date:
        return datetime.datetime.strptime(date, "%Y-%m-%dT%H:%M:%S%z")


# if __name__ == "__main__":
#     merge_companies(2895271)

