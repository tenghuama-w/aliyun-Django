/root/HelloWorld/game_wuziqi/urls.py

# HTTP路由

from django.urls import path
from . import views

app_name = 'game_wuziqi'

urlpatterns = [
    path('', views.entertainment_hall, name='entertainment_hall'),
    path('gobang/', views.gobang_lobby, name='gobang_lobby'),
    path('gobang/create/', views.create_room_view, name='create_room'),
    path('gobang/room/<str:room_id>/', views.room_page, name='room_page'),
    path('check_nickname/', views.check_nickname, name='check_nickname'),
    path('admin/delete_user/', views.admin_delete_user, name='admin_delete_user'),

    path('gobang/force_leave/', views.force_leave_view, name='force_leave'),
    path('gobang/api/rooms/', views.api_rooms, name='api_rooms'),

]