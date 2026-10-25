/root/HelloWorld/HelloWorld/urls.py

#总路由，把请求分发到首页/留言/象棋/五子棋

from django.urls import include
from django.urls import path
from . import views

urlpatterns = [
    path('', views.index, name='index'),
    path('about/', views.about, name='about'),
    path('admin-panel/', views.admin_panel, name='admin_panel'),
    path('api/hot/', views.hot_api, name='hot_api'),
    path('api/check-status/', views.check_status, name='check_status'),
    path('api/messages/', views.get_messages, name='get_messages'),
    path('game_wzq/', include('game_wuziqi.urls')),
    path('game_Cchess/', include('game_Cchess.urls')),

]