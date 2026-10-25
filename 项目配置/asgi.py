/root/HelloWorld/HelloWorld/asgi.py

#ASGI 入口，daphne 靠它启动 WebSocket 服务

import os
import django
from channels.routing import ProtocolTypeRouter, URLRouter
from channels.sessions import SessionMiddlewareStack
from django.core.asgi import get_asgi_application

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'HelloWorld.settings')
django.setup()

from game_wuziqi.routing import websocket_urlpatterns as wzq_ws
from game_Cchess.routing import websocket_urlpatterns as cchess_ws

websocket_urlpatterns = wzq_ws + cchess_ws

application = ProtocolTypeRouter({
    "http": get_asgi_application(),
    "websocket": SessionMiddlewareStack(
        URLRouter(
            websocket_urlpatterns
        )
    ),
})