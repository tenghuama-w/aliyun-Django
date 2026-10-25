/root/HelloWorld/game_Cchess/urls.py

# HTTP 路由

from django.urls import path
from . import views

urlpatterns = [
    path('Cchess/', views.lobby, name='cchess_lobby'),
    path('Cchess/create/', views.create_room_view, name='cchess_create'),
    path('Cchess/room/<str:room_id>/', views.room_page, name='cchess_room'),
    path('Cchess/api_rooms/', views.api_rooms, name='cchess_api_rooms'),
    path('Cchess/check_nickname/', views.check_nickname, name='cchess_check_nickname'),
    path('Cchess/admin_delete_user/', views.admin_delete_user, name='cchess_admin_delete'),
    path('Cchess/force_leave/', views.force_leave_view, name='cchess_force_leave'),
]