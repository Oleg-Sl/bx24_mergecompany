import logging


# Список ID компаний объединение которых запрещено
LIST_COMPANY_IDS_IGNORED = []
# Список ИНН компаний объединение которых запрещено
LIST_COMPANY_INN_IGNORED = ["5407207664", "5407473338", ]

# Логгер - ОШИБКА ОБЪЕДИНЕНИЯ КОМПАНИЙ
logger_1 = logging.getLogger('log-1')
logger_1.setLevel(logging.INFO)
fh_1 = logging.handlers.TimedRotatingFileHandler('./logs/v2/access.log', when='D', interval=1)
formatter_1 = logging.Formatter(fmt='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')
fh_1.setFormatter(formatter_1)
logger_1.addHandler(fh_1)


# возвращает список компаний с одинаковым ИНН
def get_list_company_ids_with_same_inn(bx24, id_company):
    response = bx24.batch_2({
        "halt": 0,
        "cmd": {
            "INN": f"crm.requisite.list?FILTER[ENTITY_ID]={id_company}&FILTER[ENTITY_TYPE_ID]=4",
            "REQUISITES": f"crm.requisite.list?select[]=ENTITY_ID&select[]=RQ_INN&select[]=RQ_KPP&FILTER[RQ_INN]=$result[INN][0][RQ_INN]&FILTER[ENTITY_TYPE_ID]=4"
        }
    })

    # ошибка при получении данных из Битрикс
    if not response or not response.get("result", None) or not response["result"].get("result", None):
        return []

    result = response["result"]["result"]

    # у переданной компании нет ИНН
    if "REQUISITES" not in result or "INN" not in result:
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


def filter_companies(companies):
    # companies = [{"ENTITY_ID": ..., "RQ_INN": ..., "RQ_KPP": ...}, {"ENTITY_ID": ..., "RQ_INN": ..., "RQ_KPP": ...}, ...]
    # список компаний без наличия их в списке игнорирования
    companies_filter = []

    # удаление из списка игнорируемых компаний
    for company in companies:
        if company["ENTITY_ID"] in LIST_COMPANY_IDS_IGNORED:
            continue
        if company["RQ_INN"] in LIST_COMPANY_INN_IGNORED:
            continue
        companies_filter.append(company)
    return companies_filter


def group_companies_for_merging(companies):
    # companies = [{"ENTITY_ID": ..., "RQ_INN": ..., "RQ_KPP": ...}, {"ENTITY_ID": ..., "RQ_INN": ..., "RQ_KPP": ...}, ...]
    # Список ID компаний дубликатов [[id_1, id_2, ...], [id_10, id_20, ...], ...]
    duplicates = []
    # список компаний с одинаковым ИНН и отсутствующим КПП [..., ]
    companies_kpp_no = get_company__without_kpp(companies)
    # список компаний с одинаковым ИНН и КПП: [[...,], [...,], ...]
    companies_kpp_yes = get_company__with_kpp(companies)

    duplicates.append([company_["ENTITY_ID"] for company_ in companies_kpp_no])
    for companies_data in companies_kpp_yes:
        duplicates.append([company_["ENTITY_ID"] for company_ in companies_data])

    return duplicates


def get_company__without_kpp(companies):
    companies_kpp_no = [company for company in companies if not company["RQ_KPP"]]
    return companies_kpp_no


def get_company__with_kpp(companies):
    # группировка компаний с КПП
    companies_kpp_yes = [company for company in companies if company["RQ_KPP"]]
    # компании сгруппированнные по КПП
    companies_kpp_group = {}
    for company in companies_kpp_yes:
        key = company["RQ_KPP"]
        if key in companies_kpp_group:
            companies_kpp_group[key].append(company)
        else:
            companies_kpp_group[key] = [company, ]

    companies_list_group_by_kpp = []
    # объединение компаний с КПП
    for _, _companies in companies_kpp_group.items():
        if 2 <= len(_companies) <= 4:
            companies_list_group_by_kpp.append(_companies)

    return companies_list_group_by_kpp


def get_companies_data(bx24, companies_ids):
    cmd = { "fields": "crm.company.fields" }
    for company_id in companies_ids:
        cmd[company_id] = f"crm.company.get?ID={company_id}"

    result = bx24.batch_2({
        "halt": 0,
        "cmd": cmd
    })

    if not result or not result.get("result", None) or not result["result"].get("result", None):
        return None

    return result.get("result", {}).get("result", {})


def update_duplicates(bx24, companies_ids, fields_date_new):
    cmd = {}
    for company_id in companies_ids:
        cmd[company_id] = formation_request_update_data_company(company_id, fields_date_new)

    logger_1.info({
        "companies_ids": companies_ids,
        "result_update": result_update,
    })
    result = bx24.batch_2({
        "halt": 0,
        "cmd": cmd
    })


    # if not result or not result.get("result", None) or not result["result"].get("result", None):
    #     return None
    #
    return result.get("result", {}).get("result", {})


# получает ID компании и поля с обновляемыми данными, возвращает сформированный запрос обновления компании
def formation_request_update_data_company(company_id, data):
    command = f"crm.company.update?ID={company_id}"
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


























