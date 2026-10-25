/root/HelloWorld/HelloWorld/views.py

#首页、留言板、昵称注册、check-status 踢人检测

from django.shortcuts import render, redirect
from django.http import JsonResponse, HttpResponse
from django.views.decorators.csrf import csrf_exempt
from django.utils import timezone
from django.utils.html import escape
import requests
import json
import urllib.parse
import requests
from .models import Message, SiteUser, DeletedName



from django.conf import settings
from django.contrib.auth.hashers import check_password

ADMIN_KEY_HASH = getattr(settings, 'ADMIN_KEY_HASH', '')

def index(request):
    # 名字锁定逻辑
    nickname = request.session.get('nickname')
    error_msg = ''

    # 检查是否被禁言
    is_banned = False
    if nickname:
        site_user = SiteUser.objects.filter(username=nickname).first()
        if site_user and site_user.is_banned:
            is_banned = True

    if request.method == 'POST':
        username = request.POST.get('username', '').strip()
        message = request.POST.get('message', '').strip()
        # 检查是否使用了被删除的名字
        banned_name = request.session.get('banned_name')
        if username and username == banned_name:
            error_msg = '⚠️ 不能使用和上次相同的名字：' + banned_name

        elif username and nickname and username == nickname:
            # 已登录的老用户继续留言：跳过名字占用校验
            if is_banned:
                error_msg = '⚠️ 你已被禁言，无法留言'
            elif message:
                Message.objects.create(username=username, content=message)
                return redirect('/')

        elif username and SiteUser.objects.filter(username=username).exists():
            error_msg = '⚠️ 该名字已被占用，请换一个'

        elif username and message:
            SiteUser.objects.get_or_create(username=username)
            Message.objects.create(username=username, content=message)
            request.session['nickname'] = username
            request.session['banned_name'] = None
            return redirect('/')

    # 查询留言（置顶排前面）
    messages = Message.objects.all()[:50]

    return render(request, 'index.html', {
        'title': '马化腾的Django网站',
        'message': '欢迎来到我的网站！',
        'nickname': nickname,
        'messages': messages,
        'error_msg': error_msg,
        'is_banned': is_banned,
    })

def about(request):
    return render(request, 'about.html', {
        'title': '关于网站',
    })

def admin_panel(request):
    # 检查登录状态
    authenticated = request.session.get('admin_authenticated', False)

    if request.method == 'POST':
        action = request.POST.get('action')

        if action == 'login':
            password = request.POST.get('password', '')
            if ADMIN_KEY_HASH and check_password(password, ADMIN_KEY_HASH):
                request.session['admin_authenticated'] = True
                return redirect('/admin-panel/')
            else:
                return render(request, 'admin.html', {
                    'authenticated': False,
                    'error': '密码错误'
                })

        elif action == 'logout':
            request.session['admin_authenticated'] = False
            return redirect('/admin-panel/')

        elif not authenticated:
            return redirect('/admin-panel/')

        elif action == 'delete_msg':
            msg_id = request.POST.get('msg_id')
            try:
                Message.objects.get(id=msg_id).delete()
            except:
                pass
            return redirect('/admin-panel/')

        elif action == 'delete_user':
            del_name = request.POST.get('del_name', '').strip()
            if del_name:
                Message.objects.filter(username=del_name).delete()
                SiteUser.objects.filter(username=del_name).delete()
                DeletedName.objects.get_or_create(username=del_name)
            return redirect('/admin-panel/')

        elif action == 'ban_user':
            ban_name = request.POST.get('ban_name', '').strip()
            if ban_name:
                SiteUser.objects.filter(username=ban_name).update(is_banned=True)
            return redirect('/admin-panel/')

        elif action == 'unban_user':
            unban_name = request.POST.get('unban_name', '').strip()
            if unban_name:
                SiteUser.objects.filter(username=unban_name).update(is_banned=False)
            return redirect('/admin-panel/')

        elif action == 'pin_msg':
            msg_id = request.POST.get('msg_id')
            try:
                msg = Message.objects.get(id=msg_id)
                if msg.is_pinned:
                    msg.is_pinned = False
                    msg.pinned_at = None
                else:
                    msg.is_pinned = True
                    msg.pinned_at = timezone.now()
                msg.save()
            except:
                pass
            return redirect('/admin-panel/')

    # 用户列表来自独立的 SiteUser 表（删留言不会让消失）
    all_users = SiteUser.objects.all()

    user_stats = []
    for u in all_users:
        user_stats.append({
            'username': u.username,
            'total': Message.objects.filter(username=u.username).count(),
            'messages': Message.objects.filter(username=u.username).order_by('-created_at'),
        })
    user_stats.sort(key=lambda x: x['total'], reverse=True)

    messages = Message.objects.all()[:50]

    banned_users = set(
        SiteUser.objects.filter(is_banned=True).values_list('username', flat=True)
    )

    return render(request, 'admin.html', {
        'authenticated': authenticated,
        'user_stats': user_stats,
        'all_users': all_users,
        'messages': messages,
        'error': None,
        'banned_users': banned_users,
    })

def hot_api(request):
    try:
        url = 'https://top.baidu.com/api/board?platform=wise&tab=realtime'
        headers = {
            'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1',
            'Referer': 'https://top.baidu.com/',
        }
        resp = requests.get(url, headers=headers, timeout=6)
        resp.raise_for_status()
        data = resp.json()

        # 兼容多种返回结构：data.cards / cards / 多层content
        cards = []
        if isinstance(data.get('data'), dict) and data['data'].get('cards'):
            cards = data['data']['cards']
        elif isinstance(data.get('cards'), list):
            cards = data['cards']

        items = []
        for card in cards:
            content = card.get('content') if isinstance(card, dict) else None
            if isinstance(content, list) and content:
                for sub in content:
                    if isinstance(sub, dict) and isinstance(sub.get('content'), list):
                        items.extend(sub['content'])
                    elif isinstance(sub, dict):
                        items.append(sub)
            elif isinstance(content, dict) and isinstance(content.get('content'), list):
                items.extend(content['content'])

        hot_list = []
        rank = 0
        for item in items:
            word = (item.get('word') or item.get('query') or '').strip()
            if not word:
                continue

            # 百度返回的置顶项：isTop=true 且不占排名序号
            is_top = bool(item.get('isTop'))
            if is_top:
                rank_display = '🔝'
            else:
                rank += 1
                if rank > 20:
                    break
                rank_display = item.get('index') or rank

            hot_value = (item.get('desc') or item.get('hotScore') or '')
            raw = item.get('rawUrl') or item.get('url') or ''
            if not raw:
                raw = 'https://m.baidu.com/s?word=' + urllib.parse.quote(word)
            hot_list.append({
                'rank': rank_display,
                'title': word,
                'hot_value': str(hot_value),
                'url': raw,
                'is_top': is_top,
            })

        if not hot_list:
            return JsonResponse({'hot': [], 'error': 'parse-empty'})

        return JsonResponse({'hot': hot_list})

    except Exception as e:
        return JsonResponse({'hot': [], 'error': str(e)})

def check_status(request):
    nickname = request.session.get('nickname')
    if not nickname:
        return JsonResponse({'status': 'ok'})

    site_user = SiteUser.objects.filter(username=nickname).first()
    if not site_user:
        request.session['banned_name'] = nickname
        request.session['nickname'] = None
        return JsonResponse({'status': 'kicked'})

    if site_user.is_banned:
        return JsonResponse({'status': 'banned'})

    return JsonResponse({'status': 'ok'})

def get_messages(request):
    messages = Message.objects.all()[:50]
    if messages:
        html = ''
        for msg in messages:
            safe_name = escape(msg.username)
            safe_content = escape(msg.content)
            if msg.is_pinned:
                html += f'''<div class="message-item pinned">
                    <span class="pin-icon">📌</span>
                    <strong>{safe_name}</strong>
                    <p>{safe_content}</p>
                </div>'''
            else:
                html += f'''<div class="message-item">
                    <strong>{safe_name}</strong>
                    <p>{safe_content}</p>
                </div>'''
    else:
        html = '<div class="no-messages">暂无留言，来抢沙发吧~</div>'
    return HttpResponse(html)