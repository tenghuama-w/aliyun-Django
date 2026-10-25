/root/HelloWorld/game_wuziqi/routing.py

# WebSocket 路由

from django.urls import path
from . import consumers

websocket_urlpatterns = [
    path('ws/game_wzq/gobang/room/<room_id>/', consumers.GobangConsumer),
]