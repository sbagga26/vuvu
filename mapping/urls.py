from django.urls import path
from .views import Portal_Mapping

urlpatterns = [
    path('portal_mapping/', Portal_Mapping.as_view()),
]