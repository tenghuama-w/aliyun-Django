/root/HelloWorld/game_Cchess/views.py

# 大厅、建房、api_rooms 接口、昵称校验

import json
from django.shortcuts import render, redirect
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from .consumers import (create_room, get_active_rooms, get_room,
                        admin_delete_nickname, is_room_expired, r,
                        FIRST_OWNER_RED)
from channels.layers import get_channel_layer
from asgiref.sync import async_to_sync
from HelloWorld.models import SiteUser


def _nickname_valid(nickname):
    """昵称是否仍有效：未被管理员删除"""
    if not nickname:
        return False
    return SiteUser.objects.filter(username=nickname).exists()


def lobby(request):
    """中国象棋匹配大厅"""
    nickname = request.session.get('nickname', '')
    if not nickname or not _nickname_valid(nickname):
        return redirect('/')
    return render(request, 'game_Cchess/Cchess_lobby.html', {
        'nickname': nickname,
        'rooms': get_active_rooms(),
    })


def api_rooms(request):
    """房间列表接口（供大厅实时刷新用）"""
    nickname = request.session.get('nickname', '')
    if not nickname or not _nickname_valid(nickname):
        return JsonResponse({'ok': False, 'rooms': []})
    return JsonResponse({'ok': True, 'rooms': get_active_rooms()})


@csrf_exempt
def create_room_view(request):
    """创建房间（AJAX），前端可选先手模式"""
    if request.method != 'POST':
        return JsonResponse({'success': False})

    nickname = request.session.get('nickname', '')
    if not nickname:
        return JsonResponse({'success': False, 'message': '请先留言设置昵称'})
    if not _nickname_valid(nickname):
        return JsonResponse({'success': False, 'message': '昵称状态异常，请重新设置昵称'})

    first_mode = FIRST_OWNER_RED
    try:
        body = json.loads(request.body.decode('utf-8') or '{}')
        first_mode = body.get('first_mode', FIRST_OWNER_RED)
    except Exception:
        first_mode = request.POST.get('first_mode', FIRST_OWNER_RED)

    room_id = create_room(nickname, first_mode)
    return JsonResponse({'success': True, 'room_id': room_id})


def room_page(request, room_id):
    """房间页"""
    nickname = request.session.get('nickname', '')
    if not nickname or not _nickname_valid(nickname):
        return redirect('/')

    if is_room_expired(room_id):
        return redirect('/game_Cchess/Cchess/')

    room = get_room(room_id)
    is_owner = room and room.get('owner') == nickname

    return render(request, 'game_Cchess/room.html', {
        'nickname': nickname,
        'room_id': room_id,
        'is_owner': is_owner,
    })


@csrf_exempt
def check_nickname(request):
    """检查昵称（AJAX，供入口拦截用）"""
    nickname = request.session.get('nickname', '')
    return JsonResponse({'has_nickname': bool(nickname)})


def admin_delete_user(request):
    """管理员删除用户昵称（调用此接口中断对局）"""
    if request.method == 'POST':
        nickname = request.POST.get('nickname', '')
        if nickname:
            admin_delete_nickname(nickname)
            return JsonResponse({'success': True})
    return JsonResponse({'success': False})


@csrf_exempt
def force_leave_view(request):
    """被管理员删除后强制退出房间，并通知对手"""
    if request.method != 'POST':
        return JsonResponse({'success': False})

    nickname = request.session.get('nickname', '')
    if not nickname:
        return JsonResponse({'success': True})

    room_id = r.get(f'cc:user_room:{nickname}')
    if room_id:
        room_data = r.hgetall(f'cc:room:{room_id}')
        group_name = f'cc_room_{room_id}'
        if room_data:
            if room_data.get('owner') == nickname:
                player2 = room_data.get('player2', '')
                if player2:
                    r.delete(f'cc:user_room:{player2}')
                r.delete(f'cc:room:{room_id}')
                r.srem('cc:active_rooms', room_id)
                r.delete(f'cc:user_room:{nickname}')
                r.delete(f'cc:owner_conns:{room_id}')
                r.delete(f'cc:player2_conns:{room_id}')
                try:
                    async_to_sync(get_channel_layer().group_send)(
                        group_name,
                        {'type': 'ws.room.closed', 'message': '房主已退出，房间已解散'}
                    )
                except Exception:
                    pass
            elif room_data.get('player2') == nickname:
                r.hset(f'cc:room:{room_id}', 'player2', '')
                r.hset(f'cc:room:{room_id}', 'status', 'waiting')
                r.hset(f'cc:room:{room_id}', 'owner_ready', '0')
                r.hset(f'cc:room:{room_id}', 'player2_ready', '0')
                r.delete(f'cc:user_room:{nickname}')
                r.delete(f'cc:player2_conns:{room_id}')

                # 座位空过 → 轮换记录作废
                try:
                    r.hdel(f'cc:room:{room_id}', 'last_first')
                except Exception:
                    pass

                ttl_now = 0
                try:
                    t = r.ttl(f'cc:room:{room_id}')
                    if t and t > 0:
                        ttl_now = t
                except Exception:
                    ttl_now = 0

                try:
                    async_to_sync(get_channel_layer().group_send)(
                        group_name,
                        {'type': 'ws.opponent.left', 'nickname': nickname, 'ttl': ttl_now}
                    )
                except Exception:
                    pass

    request.session['nickname'] = None
    request.session['banned_name'] = nickname
    return JsonResponse({'success': True})