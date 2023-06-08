from django.urls import include, path
from rest_framework import routers


from .views import (MergeDuplicateCompaniesApiView, InstallApiView, IndexApiView)


app_name = 'mergecompaniesapp'


router = routers.DefaultRouter()


urlpatterns = [
    path('', include(router.urls)),
    path('install/', InstallApiView.as_view()),
    path('index/', IndexApiView.as_view()),
    path('merge-duplicate-companies/', MergeDuplicateCompaniesApiView.as_view()),

]


urlpatterns += router.urls