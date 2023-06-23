from django.core.cache import cache
from django.http import HttpResponseForbidden

class RequestFilterMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        # id_company = request.data.get("data[FIELDS][ID]", None)
        if 'data[FIELDS][ID]' in request.data:
            id_company = request.data.get('data[FIELDS][ID]')
            cache_key = f'request_{id_company}'

            if cache.get(cache_key):
                return HttpResponseForbidden('Duplicate request')

            cache.set(cache_key, True, timeout=60)  # Хранить данные в кэше на 60 секунд

        return self.get_response(request)
