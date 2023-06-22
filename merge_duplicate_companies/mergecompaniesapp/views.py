from rest_framework import views, viewsets, filters, status
from rest_framework.response import Response
from django.shortcuts import render
from django.views.decorators.clickjacking import xframe_options_exempt

# import redis
# import pprint
import logging
import re

# from django.conf import settings

from .service import write_app_data_to_file
# from . import service, bitrix24
# from .service import MyException
from .tasks import merge_run_task, merge_run_task_2


# Логгер всех входящих запросов
# access_handler = logging.handlers.TimedRotatingFileHandler('./logs/access/access.log', when='D', interval=1)
# formatter_handler = logging.Formatter(fmt='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')
# access_handler.setFormatter(formatter_handler)
# logger = logging.getLogger(__name__)
# logger.setLevel(logging.INFO)
# logger.addHandler(access_handler)
logger_access = logging.getLogger('access')
logger_access.setLevel(logging.INFO)
fh_access = logging.handlers.TimedRotatingFileHandler('./logs/access/access.log', when='D', interval=1)
formatter_access = logging.Formatter(fmt='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')
fh_access.setFormatter(formatter_access)
logger_access.addHandler(fh_access)

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

# redis_instance = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, db=0)


class InstallApiView(views.APIView):
    @xframe_options_exempt
    def post(self, request):
        data = {
            "domain": request.query_params.get("DOMAIN", "atonlab.bitrix24.ru"),
            "auth_token": request.data.get("AUTH_ID", ""),
            "expires_in": request.data.get("AUTH_EXPIRES", 3600),
            "refresh_token": request.data.get("REFRESH_ID", ""),
            "application_token": request.query_params.get("APP_SID", ""),   # используется для проверки достоверности событий Битрикс24
            'client_endpoint': f'https://{request.query_params.get("DOMAIN", "atonlab.bitrix24.ru")}/rest/',
        }
        write_app_data_to_file(data)
        return render(request, 'install.html')


# Обработчик установленного приложения
class IndexApiView(views.APIView):
    @xframe_options_exempt
    def post(self, request):
        return render(request, 'index.html')


class MergeDuplicateCompaniesApiView(views.APIView):

    def post(self, request):
        event = request.data.get("event", "")
        id_company = request.data.get("data[FIELDS][ID]", None)
        logger_access.info({
            "event": event,
            "id_company": id_company
        })

        # не передано id компании
        if not id_company:
            return Response("Not transferred ID company", status=status.HTTP_400_BAD_REQUEST)

        # id компании входит в список игнорирования
        if id_company in LIST_COMPANY_IDS_IGNORED:
            return Response(f"The company with the id={id_company} is on the ignore list", status=status.HTTP_200_OK)

        # добавление ID компании в очередь на объединение
        merge_run_task_2.delay(id_company)

        return Response("OK", status=status.HTTP_200_OK)





# # возвращает список id компаний с одинаковым ИНН
# def get_list_company_ids_with_same_inn(bx24, id_company):
#     result = bx24.batch_2({
#         "halt": 0,
#         "cmd": {
#             "INN": f"crm.requisite.list?FILTER[ENTITY_ID]={id_company}&FILTER[ENTITY_TYPE_ID]=4",
#             "COMPANIES": f"crm.requisite.list?select[0]=ENTITY_ID&FILTER[RQ_INN]=$result[INN][0][RQ_INN]&FILTER[ENTITY_TYPE_ID]=4"
#         }
#     })
#
#     if not result or not result.get("result", None) or not result["result"].get("result", None) or "COMPANIES" not in result["result"]["result"] or "INN" not in result["result"]["result"]:
#         logging.info({
#             "company_id": id_company,
#             "msg": f'Could not get company data by INN',
#             "response": result,
#         })
#         return None
#
#     inn = ""
#     companies_inn = result["result"]["result"].get("INN", [])
#     if companies_inn:
#         inn = companies_inn[0]["RQ_INN"]
#
#     companies = result["result"]["result"].get("COMPANIES", [])
#
#     return inn, [company["ENTITY_ID"] for company in companies]


# # формирование комманд на получение данных компаний
# def formation_command_to_receive_company_data(ids):
#     cmd = {}
#     for ident in ids:
#         key_cont = f"contact{ident}"
#         key_deal = f"deals{ident}"
#         cmd[ident] = f"crm.company.get?ID={ident}"
#         cmd[key_cont] = f"crm.company.contact.items.get?ID={ident}"
#         cmd[key_deal] = f"crm.deal.list?select[0]=ID&filter[COMPANY_ID]={ident}"
#
#     return cmd
#
#
# # получает ID компании и поля с обновляемыми данными, возвращает сформированный запрос обновления компании
# def formation_req_update_data_company(id_real, data):
#     command = f"crm.company.update?ID={id_real}"
#     for field in data:
#         if isinstance(data[field], list):
#             for index, element in enumerate(data[field]):
#                 if isinstance(element, dict):
#                     command += f'&fields[{field}][{index}][VALUE]={element["VALUE"]}'
#                     command += f'&fields[{field}][{index}][VALUE_TYPE]={element["VALUE_TYPE"]}'
#                 else:
#                     command += f'&fields[{field}][{index}][VALUE]={element}'
#         else:
#             command += f'&fields[{field}]={data[field]}'
#
#     return command
#
#
# # получает ID компании и поля с обновляемыми контактами, возвращает сформированный запрос обновления контактов компании
# def formation_req_update_date_contacts(id_real, contacts):
#     if isinstance(contacts, list):
#         cmd = f'crm.company.contact.items.set?id={id_real}'
#         for index, contact in enumerate(contacts):
#             cmd += f'&items[{index}][CONTACT_ID]={contact}'
#
#         return cmd;
#
#
# # получает ID компании и поля с добавляемыми сделками, возвращает сформированный запрос на обновление привязки компаний к сделке
# def formation_req_get_date_deals(id_real, deals):
#     commands = {};
#     for deal in deals:
#         key = f'deal{deal}'
#         commands[key] = f'crm.deal.update?id={deal}&fields[COMPANY_ID]={id_real}'
#
#     return commands;
#
#
# # слияние дублей компаний
# def merge_company(bx24, id_real, data):
#     cmd = {};
#     # формирование запроса на обновление данных о комании - crm.company.update
#     if data["data"]:
#         cmd["data"] = formation_req_update_data_company(id_real, data["data"])
#
#     # формирование запроса на перезапись всех контактов - crm.company.contact.items.set
#     if data["contacts"]:
#         cmd["contacts"] = formation_req_update_date_contacts(id_real, data["contacts"])
#
#     # формирование запроса на перенос всех сделок - crm.deal.update
#     if data["deals"]:
#         commands = formation_req_get_date_deals(id_real, data["deals"])
#         for key in commands:
#             cmd[key] = commands[key]
#
#     # return cmd
#     response = bx24.batch_2({
#         "halt": 0,
#         "cmd": cmd
#     })
#
#     if not response or not response.get("result", None) or not response["result"].get("result", None):
#         logging.info({
#             "company_id": id_real,
#             "msg": f'Failed to merge companies',
#             "response": response,
#         })
#         return None
#
#     return response["result"]["result"]
#
#
# # проверка вхождения элементов старого массива в новый
# def verification_elements_in_list(lst_real, lst_dupl):
#     for elem in lst_dupl:
#         if elem not in lst_real:
#             return False
#
#     return True
#
#
# # проверка вхождения элементов старого массива в новый
# def verification_objects_in_list(lst_obj_real, lst_obj_dupl):
#     lst_real = [elem["VALUE"] for elem in lst_obj_real]
#     lst_dupl = [elem["VALUE"] for elem in lst_obj_dupl]
#     for elem in lst_dupl:
#         if elem not in lst_real:
#             return False
#
#     return True
#
#
# # проверка вхождения элементов старого массива в новый
# def verification_objects_number_in_list(lst_obj_real, lst_obj_dupl):
#     lst_real = ["".join(re.findall(r'\d+', elem["VALUE"])) for elem in lst_obj_real]
#     lst_dupl = ["".join(re.findall(r'\d+', elem["VALUE"])) for elem in lst_obj_dupl]
#     for elem in lst_dupl:
#         if elem not in lst_real:
#             return False
#
#     return True
#
#
# # проверка вхождения элементов старого массива в новый
# def verification_contacts_in_list(lst_obj_real, lst_obj_dupl):
#     lst_real = [elem["CONTACT_ID"] for elem in lst_obj_real]
#     lst_dupl = [elem["CONTACT_ID"] for elem in lst_obj_dupl]
#     for elem in lst_dupl:
#         if elem not in lst_real:
#             return False
#
#     return True
#
#
# # проверка верности объединения компаний
# def verification_merge_company(bx24, id_real, companies_ids, company_data_old):
#
#     cmd = formation_command_to_receive_company_data(companies_ids)
#
#     result = bx24.batch_2({
#         "halt": 0,
#         "cmd": cmd
#     })
#
#     if not result or not result.get("result", None) or not result["result"].get("result", None):
#         logging.info({
#             "company_id": id_real,
#             "msg": f'The verification of the merger of companies has not been passed',
#             "response": result,
#         })
#         return False, {}
#
#     contacts_real = []
#     contacts_duplicate = []
#     deals_real = []
#     deals_duplicate = []
#
#     data_duplicate = {}
#     data_real = {}
#
#     for field in FIELDS_COMPANIES_LISST["multiple_value"]:
#         data_duplicate[field] = []
#         data_real[field] = []
#
#     for field in FIELDS_COMPANIES_LISST["multiple"]:
#         data_duplicate[field] = []
#         data_real[field] = []
#
#     for ident in companies_ids:
#         key_cont = f"contact{ident}"
#         key_deal = f"deals{ident}"
#
#         company = result["result"]["result"].get(ident, {})
#         response_cont = result["result"]["result"].get(key_cont, {})
#         response_deal = result["result"]["result"].get(key_deal, {})
#
#         if id_real != ident:
#             contacts_duplicate = contacts_duplicate + response_cont
#             deals_duplicate = deals_duplicate + response_deal
#
#         if id_real == ident:
#             contacts_real = contacts_real + response_cont
#             deals_real = deals_real + response_deal
#
#         for field in FIELDS_COMPANIES_LISST["multiple_value"]:
#             if id_real != ident:
#                 data_duplicate[field] = data_duplicate[field] + company.get(field, [])
#             if id_real == ident:
#                 data_real[field] = data_real[field] + company.get(field, [])
#
#         for field in FIELDS_COMPANIES_LISST["multiple"]:
#             if id_real != ident:
#                 data_duplicate[field] = data_duplicate[field] + company.get(field, [])
#             if id_real == ident:
#                 data_real[field] = data_real[field] + company.get(field, [])
#
#     status_verif_contacts = verification_contacts_in_list(contacts_real, contacts_duplicate)
#     status_verif_deals = True if len(deals_duplicate) == 0 else False
#
#     # print("status_verif_contacts = ", status_verif_contacts)
#     # print("status_verif_deals = ", status_verif_deals)
#     for field in FIELDS_COMPANIES_LISST["multiple_value"]:
#         if field == "PHONE":
#             verification = verification_objects_number_in_list(data_real[field], data_duplicate[field])
#         else:
#             verification = verification_objects_in_list(data_real[field], data_duplicate[field])
#
#         if not verification:
#             logging.error({
#                 "company_id": id_real,
#                 "msg": f'Dont transfer the data of the field "{field}"',
#                 "data_real": data_real[field],
#                 "data_duplicate": data_duplicate[field],
#             })
#             return False, {}
#
#     for field in FIELDS_COMPANIES_LISST["multiple"]:
#         if not verification_objects_in_list(data_real[field], data_duplicate[field]):
#             logging.error({
#                 "company_id": id_real,
#                 "msg": f'Dont transfer the data of the field "{field}"',
#                 "data_real": data_real[field],
#                 "data_duplicate": data_duplicate[field],
#             })
#             return False, {}
#
#     if not status_verif_contacts:
#         logging.error({
#             "company_id": id_real,
#             "msg": f'Dont transfer contact data',
#             "contacts_real": contacts_real,
#             "contacts_duplicate": contacts_duplicate,
#
#         })
#
#     if not status_verif_deals:
#         logging.error({
#             "company_id": id_real,
#             "msg": f'Deals not transferred',
#             "deals_duplicate": "deals_duplicate"
#         })
#
#     if status_verif_contacts and status_verif_deals:
#         return True, result
#
#     return False, {}
#
#
# # Удаление списка компаний
# def deleteCompanies(bx24, id_real, companies_ids, update_data_company):
#     cmd = {};
#     for ident in companies_ids:
#         if ident != id_real:
#             cmd[ident] = f"crm.company.delete?ID={ident}"
#
#     users = list(set(update_data_company['responsible']))
#     domain = service.get_token().get("domain", "atonlab.bitrix24.ru")
#     url_real_company = f"https://{domain}/crm/company/details/{id_real}/"
#     for user in users:
#         key = f"MSG{user}"
#         message = f"Компании {', '.join(update_data_company['title_dupl'])} объединились в компанию {update_data_company['title_real']}"
#         cmd[key] = f"im.notify.personal.add?USER_ID={user}&MESSAGE={message}&ATTACH[0][LINK][NAME]={update_data_company['title_real']}&ATTACH[0][LINK][DESC]=&ATTACH[0][LINK][LINK]={url_real_company}"
#
#     print("cmd = ", cmd)
#     response = bx24.batch_2({
#         "halt": 0,
#         "cmd": cmd
#     })
#     print("response = ", response)
#     if not response or not response.get("result", None) or not response["result"].get("result", None):
#         logging.info({
#             "company_id": id_real,
#             "msg": f'Unable to remove company duplicates',
#             "response": response,
#         })
#         return None
#
#     return response["result"]["result"]
#
#
# # проверка существования элемента в хранилище
# def storage_exist_elem(elem):
#     return redis_instance.exists(elem)
#
#
# # сохранение списка элементов в хранилище
# def storage_entry_list(lst):
#     for el in lst:
#         redis_instance.set(el, el)
#
#
# # удаление списка элементов из хранилища
# def storage_deleting_list(lst):
#     for el in lst:
#         redis_instance.delete(el)



    # def post(self, request):
    #     # logging.info(request.data)
    #     event = request.data.get("event", "")
    #     id_company = request.data.get("data[FIELDS][ID]", None)
    #     # storage_deleting_list([id_company])
    #     logging.info({
    #         "event": event,
    #         "id_company": id_company
    #     })
    #
    #     # не передано id компании
    #     if not id_company:
    #         return Response("Not transferred ID company", status=status.HTTP_400_BAD_REQUEST)
    #
    #     # id компании входит в список игнорирования
    #     if id_company in LIST_COMPANY_IDS_IGNORED:
    #         return Response(f"The company with the id={id_company} is on the ignore list", status=status.HTTP_200_OK)
    #
    #     merge_run_task.delay(id_company)
    #
    #     # # проверка - компания уже редактируется
    #     # if storage_exist_elem(id_company):
    #     #     # raise MyException(f"The company with the id={id_company} is already being edited")
    #     #     return Response(f"The company with the id={id_company} is already being edited", status=status.HTTP_200_OK)
    #     #
    #     # try:
    #     #     inn, companies_ids = get_list_company_ids_with_same_inn(self.bx24, id_company)
    #     #     companies_ids = list(set(companies_ids))
    #     #
    #     #     id_real = min(companies_ids, key=int)
    #     #     # storage_entry_list(companies_ids)
    #     #
    #     #     # у копании отсутствует ИНН
    #     #     if not inn or len(inn) < 5:
    #     #         raise MyException(f"The company with the id={id_company} does not have a INN")
    #     #         # return Response(f"The company with the id={id_company} does not have a INN", status=status.HTTP_400_BAD_REQUEST)
    #     #
    #     #     # id компании входит в список игнорирования
    #     #     if inn in LIST_COMPANY_INN_IGNORED:
    #     #         raise MyException(f"The company with the id={id_company} and inn={inn} is on the ignore list")
    #     #         # return Response(f"The company with the id={id_company} and inn={inn} is on the ignore list", status=status.HTTP_200_OK)
    #     #
    #     #     # !!!!!!!!!!ВРЕМЕННО ЗАКОММЕНТИРУЕМ
    #     #     # отсутствуют дубли компании
    #     #     if len(companies_ids) < 2:
    #     #         raise MyException(f"Company duplicates with id={id_company} missing")
    #     #         # return Response(f"Company duplicates with id={id_company} missing", status=status.HTTP_400_BAD_REQUEST)
    #     #
    #     #     # количество дубликатов компании более 4
    #     #     if len(companies_ids) > 4:
    #     #         raise MyException(f"Number of companies with the same INN more than four")
    #     #         # return Response(f"Number of companies with the same INN more than four", status=status.HTTP_400_BAD_REQUEST)
    #     #
    #     #     # сохранение списка компаний в хранилище на время объединения
    #     #     storage_entry_list(companies_ids)
    #     #
    #     #     # получение данных компаний из Битрикс
    #     #     updated_company_data = get_data_companies(self.bx24, companies_ids)
    #     #
    #     #     # id_real = min(companies_ids, key=int)
    #     #
    #     #     # объединение компаний
    #     #     result_merge = merge_company(self.bx24, id_real, updated_company_data)
    #     #     if not result_merge:
    #     #         raise MyException(f"Merger company failed")
    #     #
    #     #     # pprint.pprint(result_merge)
    #     #
    #     #     # проверка объединения компаний
    #     #     verification_merge, data_update = verification_merge_company(self.bx24, id_real, companies_ids, updated_company_data)
    #     #
    #     #     # verification_merge = True
    #     #     if verification_merge:
    #     #         deleteCompanies(self.bx24, id_real, companies_ids, updated_company_data)
    #     #
    #     #     companies_ids.append(id_real)
    #     #     storage_deleting_list(companies_ids)
    #     #
    #     #     logging.info({
    #     #         "event": event,
    #     #         "id_company": id_company,
    #     #         "data_update": data_update,
    #     #     })
    #     #     return Response("OK", status=status.HTTP_200_OK)
    #     #
    #     # except MyException as err:
    #     #
    #     #     companies_ids.append(id_real)
    #     #     storage_deleting_list(companies_ids)
    #     #     return Response(err.args[0], status=status.HTTP_400_BAD_REQUEST)
    #     #
    #     # except Exception as err:
    #     #
    #     #     companies_ids.append(id_real)
    #     #     storage_deleting_list(companies_ids)
    #     #     print("Error = ", err)
    #     #     return Response(err.args[0], status=status.HTTP_400_BAD_REQUEST)
    #     #
    #     #
    #     return Response("OK", status=status.HTTP_200_OK)


# # получение данных компаний
# def get_data_companies(bx24, companies_ids):
#     id_real = min(companies_ids, key=int)
#     cmd = formation_command_to_receive_company_data(companies_ids)
#     result = bx24.batch_2({
#         "halt": 0,
#         "cmd": cmd
#     })
#
#     if not result or not result.get("result", None) or not result["result"].get("result", None):
#         logging.info({
#             "company_id": id_real,
#             "msg": f'Failed to get duplicate companies',
#             "response": result,
#         })
#         return None
#
#     data_real = {}
#     data_duplicate = {}
#
#     for field in FIELDS_COMPANIES_LISST["multiple_value"]:
#         data_duplicate[field] = []
#         data_real[field] = []
#
#     for field in FIELDS_COMPANIES_LISST["multiple"]:
#         data_duplicate[field] = []
#         data_real[field] = []
#
#     for field in FIELDS_COMPANIES_LISST["single"]:
#         data_duplicate[field] = []
#         data_real[field] = None
#
#     data_deals_real = []
#     data_deals_duplicate = []
#     data_contacts_real = []
#     data_contacts_duplicate = []
#
#     for ident in companies_ids:
#         key_cont = f"contact{ident}"
#         key_deal = f"deals{ident}"
#         company = result["result"]["result"].get(ident, {})
#         response_cont = result["result"]["result"].get(key_cont, {})
#         response_deal = result["result"]["result"].get(key_deal, {})
#
#         for field in FIELDS_COMPANIES_LISST["multiple_value"]:
#             lst = [elem["VALUE"] for elem in company.get(field, [])]
#             if id_real != ident:
#                 data_duplicate[field] += data_duplicate[field] + lst
#             if id_real == ident:
#                 data_real[field] += data_real[field] + lst
#
#         for field in FIELDS_COMPANIES_LISST["multiple"]:
#             lst = [elem for elem in company.get(field, [])]
#             if id_real != ident:
#                 data_duplicate[field] += data_duplicate[field] + lst
#             if id_real == ident:
#                 data_real[field] += data_real[field] + lst
#
#         for field in FIELDS_COMPANIES_LISST["single"]:
#             value = company.get(field, None)
#             if id_real != ident and value:
#                 data_duplicate[field].append(value)
#             if id_real == ident:
#                 data_real[field] = value
#
#         deals = [deal["ID"] for deal in response_deal]
#         contacts = [contact["CONTACT_ID"] for contact in response_cont]
#
#         if id_real != ident:
#             data_deals_duplicate = data_deals_duplicate + deals
#             data_contacts_duplicate = data_contacts_duplicate + contacts
#
#         if id_real == ident:
#             data_deals_real = data_deals_real + deals
#             data_contacts_real = data_contacts_real + contacts
#
#     data_company = {}
#
#     for field in FIELDS_COMPANIES_LISST["multiple_value"]:
#         data_company[field] = data_duplicate[field] + data_real[field]
#
#     for field in FIELDS_COMPANIES_LISST["multiple"]:
#         data_company[field] = data_duplicate[field] + data_real[field]
#
#     for field in FIELDS_COMPANIES_LISST["single"]:
#         if not data_real[field] and len(data_duplicate[field]) > 0:
#             data_company[field] = data_duplicate[field][0]
#
#     return {
#         "deals": data_deals_duplicate,                                  # crm.deal.update
#         "contacts": data_contacts_duplicate + data_contacts_real,       # crm.company.contact.items.set
#         "data": data_company,                                           # crm.company.update
#         "responsible": data_duplicate["ASSIGNED_BY_ID"] + data_duplicate["CREATED_BY_ID"] + [data_real["ASSIGNED_BY_ID"], data_real["CREATED_BY_ID"]],
#         "title_real": data_real["TITLE"],
#         "title_dupl": data_duplicate["TITLE"],
#     }
#
#
