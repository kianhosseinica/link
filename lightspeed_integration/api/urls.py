from django.urls import path
from .views import *

urlpatterns = [
    path('zoho-items/', get_all_zoho_items, name='get_all_zoho_items'),
    path('compare-items/', compare_items, name='compare_items'),
    path('update-create-items/', update_or_create_specific_items, name='update_or_create_specific_items'),
]
