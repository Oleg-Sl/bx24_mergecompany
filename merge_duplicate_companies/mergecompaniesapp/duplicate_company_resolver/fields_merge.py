import os
import base64


class FieldsFirstNonEmpty:
    def crm_non_empty(self, field):
        values = []
        for obj_id_ in self.ids_sort:
            obj_ = self.data.get(obj_id_, {})
            values.append(obj_.get(field))

        # если в списке не одинаковые элементы
        if not all(element == values[0] for element in values):
            for val_ in values:
                if val_ is not None and val_ != "":
                    return  val_

        # # если поле в настоящей компании заполнено,
        # if self.data.get(self.id_origin):
        #     pass
        # if self.contacts[self.ids_sort_date[-1]].get(field):
        #     return



        # for id_contact in self.ids_sort_date[::-1]:
        #     value = self.contacts[id_contact].get(field)
        #     if value:
        #         return value


class FieldsTypeCrmMultifield:
    def crm_multifield(self, field):
        multifield = []

        for obj_id_ in self.ids_sort:
            obj_ = self.data.get(obj_id_, {})
            if not obj_.get(field):
                continue

            for item in obj_[field]:
                if item['VALUE'] not in [d['VALUE'] for d in multifield]:
                    multifield.append({
                        'TYPE_ID': item['TYPE_ID'],
                        'VALUE': item['VALUE'],
                        'VALUE_TYPE': item['VALUE_TYPE']
                    })

        return multifield


class FieldsMergeUpdate(FieldsTypeCrmMultifield, FieldsFirstNonEmpty):
    def __init__(self, bx24, ids, data, fields):
        self.bx24 = bx24
        self.ids = ids
        self.data = data
        self.fields = fields
        self.ids_sort = None
        self.id_origin = None

        self.sort_ids()

    def sort_ids(self):
        self.ids_sort = sorted(self.ids, key=int)
        self.id_origin = self.ids_sort[0]

    def get_data(self):
        data = {}
        for field, field_data in self.fields.items():
            if field_data['isReadOnly'] is True:
                continue
            elif field_data['type'] == 'crm_multifield':
                field_content = self.crm_multifield(field)
                if field_content:
                    data[field] = field_content
            elif field_data['type'] == 'file':
                continue
                # field_content = contacts_update.get_field_type_file(field)
                # if field_content:
                #     data[field] = field_content
            else:
                field_content = self.crm_non_empty(field)
                if field_content:
                    data[field] = field_content

