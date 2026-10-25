/root/HelloWorld/game_Cchess/routing.py

# WebSocket 路由

from django.urls import path
from . import consumers

websocket_urlpatterns = [
    path('ws/game_Cchess/Cchess/room/<room_id>/', consumers.CchessConsumer),
]