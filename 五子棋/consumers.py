/root/HelloWorld/game_wuziqi/consumers.py

# 后端全部逻辑：落子、胜负判定、房间管理

import json
import redis
import random
import threading
import time
from channels.generic.websocket import WebsocketConsumer
from channels.layers import get_channel_layer
from asgiref.sync import async_to_sync

r = redis.Redis(host='127.0.0.1', port=6379, db=0, decode_responses=True)

# 在线心跳有效期（秒）
HEARTBEAT_TTL = 90
# 断线后的"回归权"保留时间（秒）——仅用于重连提示，不再用于踢人
GRACE_TTL = 20
# 未开局时，对手断线后的回归权（秒）：10 秒内可回原位，超时踢出
FAST_CONFIRM = 10
# 未开局时，房主断线后多久解散房间（秒）
OWNER_WAITING_CONFIRM = 10
# 被踢出后的"痕迹"有效期（秒）——用于区分"超时回来"和"新来的第三人"
EXITED_TRACE_TTL = 30
# 胜负已分后，若一直没人点确定，服务端兜底强制重置的等待时间（秒）
POST_OVER_GRACE = 5
# 兜底扫描间隔（秒）
SWEEP_INTERVAL = 10
# 棋盘尺寸
BOARD_SIZE = 15
# 悔棋应答超时（秒，服务端兜底）
UNDO_TIMEOUT = 8
# 每步限时（秒），超时判负（断线时间一并计入，不暂停）
MOVE_TIME_LIMIT = 45
# 重置后重新挂的房间有效期（秒）
ROOM_TTL_AFTER_RESET = 600


# ========== 在线心跳（ZSET，O(log N)，不用 KEYS 全库扫描）==========

def _hb_key(room_id, role):
    return f'hb:{room_id}:{role}'


def _hb_touch(room_id, role, channel, ttl=HEARTBEAT_TTL):
    """记录/续期心跳：score = 该心跳的过期时间戳"""
    try:
        k = _hb_key(room_id, role)
        now = time.time()
        r.zadd(k, {channel: now + ttl})
        r.expire(k, int(ttl) + 300)
        r.zremrangebyscore(k, '-inf', now)      # 顺手清掉已过期的成员
    except Exception:
        pass


def _hb_leave(room_id, role, channel, grace=GRACE_TTL):
    """断开连接：不立即移除，把过期时间提前到 grace 秒后。
    其他连接的心跳不受影响（保留了原 per-channel TTL 的语义）"""
    try:
        r.zadd(_hb_key(room_id, role), {channel: time.time() + grace})
    except Exception:
        pass


def _hb_drop(room_id, role, channel):
    """彻底移除某条连接（主动离开 / 被踢出）"""
    try:
        r.zrem(_hb_key(room_id, role), channel)
    except Exception:
        pass


def _hb_clear(room_id, role):
    """清空某角色的所有心跳"""
    try:
        r.delete(_hb_key(room_id, role))
    except Exception:
        pass


def _hb_count(room_id, role):
    """该角色当前在线连接数（O(log N)）"""
    try:
        k = _hb_key(room_id, role)
        r.zremrangebyscore(k, '-inf', time.time())
        return r.zcard(k)
    except Exception:
        return 0


# ========== 在线状态判断 ==========

def _is_online(room_id, role):
    """该角色当前是否在线：有活跃心跳，且没有"待重连"标记"""
    if not role:
        return False
    try:
        if r.get(f'pending:{room_id}:{role}') is not None:
            return False
    except Exception:
        pass
    return _hb_count(room_id, role) > 0


# ========== 离线处理：未开局 ==========

def _mark_offline_player2(room_id, nickname):
    """
    未开局阶段 player2 离线：
    1) 立刻清掉位子（房主看到没人，房间列表开放，别人可加入）
    2) 给 10 秒回归权：10 秒内回来且位子空着 → 回原位；位子被占 → 提示"位置已有人"
    3) 超过 10 秒 → 广播 opponent_left，之后回来提示"你已不在房间内"
    """
    rd = r.hgetall(f'room:{room_id}')
    if not rd or rd.get('player2') != nickname:
        return

    try:
        r.set(f'lost_ready:{room_id}', rd.get('player2_ready', '0'), ex=FAST_CONFIRM + 30)
    except Exception:
        pass

    r.hset(f'room:{room_id}', 'player2', '')
    r.hset(f'room:{room_id}', 'status', 'waiting')
    r.hset(f'room:{room_id}', 'player2_ready', '0')
    _clear_board_fields(room_id)
    r.delete(f'user_room:{nickname}')
    r.delete(f'player2_conns:{room_id}')
    _hb_clear(room_id, 'player2')

    try:
        r.set(f'lost_player:{room_id}', nickname, ex=FAST_CONFIRM + 10)
        r.set(f'exited:{room_id}:{nickname}', '1', ex=EXITED_TRACE_TTL)
    except Exception:
        pass

    try:
        cur_ttl = r.ttl(f'room:{room_id}')
        if cur_ttl is not None and cur_ttl < 0:
            r.expire(f'room:{room_id}', ROOM_TTL_AFTER_RESET)
    except Exception:
        pass

    def confirm_left():
        time.sleep(FAST_CONFIRM)
        try:
            if r.get(f'lost_player:{room_id}') != nickname:
                return
            r.delete(f'lost_player:{room_id}')
            left_ttl = 0
            try:
                t = r.ttl(f'room:{room_id}')
                if t and t > 0:
                    left_ttl = t
            except Exception:
                pass
            try:
                async_to_sync(get_channel_layer().group_send)(
                    f'room_{room_id}',
                    {'type': 'ws.opponent.left', 'nickname': nickname, 'ttl': left_ttl}
                )
            except Exception:
                pass
        except Exception:
            pass

    t = threading.Thread(target=confirm_left, daemon=True)
    t.start()


def _mark_offline_owner(room_id, nickname):
    """未开局房主离线：10 秒后解散房间"""
    try:
        r.srem('active_rooms', room_id)
    except Exception:
        pass

    def worker():
        time.sleep(OWNER_WAITING_CONFIRM)
        try:
            if r.get(f'pending:{room_id}:owner') is None:
                return
            cur = r.hgetall(f'room:{room_id}')
            if not cur or cur.get('owner') != nickname:
                return
            if cur.get('status') == 'playing':
                return
            p2 = cur.get('player2', '')
            if p2:
                r.delete(f'user_room:{p2}')
            r.delete(f'room:{room_id}')
            r.srem('active_rooms', room_id)
            r.delete(f'user_room:{nickname}')
            r.delete(f'owner_conns:{room_id}')
            r.delete(f'player2_conns:{room_id}')
            _hb_clear(room_id, 'owner')
            _hb_clear(room_id, 'player2')
            try:
                r.delete(f'lost_player:{room_id}')
                r.delete(f'lost_ready:{room_id}')
                r.delete(f'pending:{room_id}:owner')
            except Exception:
                pass
            try:
                async_to_sync(get_channel_layer().group_send)(
                    f'room_{room_id}',
                    {'type': 'ws.room.closed', 'message': '房主已退出，房间已解散'}
                )
            except Exception:
                pass
        except Exception:
            pass

    t = threading.Thread(target=worker, daemon=True)
    t.start()


# ========== 重置 / 兜底接管 ==========

def _clear_board_fields(room_id):
    """清空棋盘相关字段"""
    r.hset(f'room:{room_id}', 'board', '')
    r.hset(f'room:{room_id}', 'winner', '')
    r.hset(f'room:{room_id}', 'move_count', '0')
    r.hset(f'room:{room_id}', 'last_x', '')
    r.hset(f'room:{room_id}', 'last_y', '')
    r.hset(f'room:{room_id}', 'moves', '')
    r.hset(f'room:{room_id}', 'over_reason', '')
    r.hset(f'room:{room_id}', 'undo_pending', '')
    r.hset(f'room:{room_id}', 'undo_lock_owner', '0')
    r.hset(f'room:{room_id}', 'undo_lock_player2', '0')
    r.hset(f'room:{room_id}', 'move_deadline', '')


def _clear_undo_pending(room_id):
    r.hset(f'room:{room_id}', 'undo_pending', '')


def _room_ttl(room_id):
    """取房间剩余秒数；若被 persist 取消了有效期，重新挂上"""
    try:
        cur_ttl = r.ttl(f'room:{room_id}')
        if cur_ttl is None or cur_ttl < 0:
            r.expire(f'room:{room_id}', ROOM_TTL_AFTER_RESET)
            return ROOM_TTL_AFTER_RESET
        if cur_ttl > 0:
            return cur_ttl
    except Exception:
        pass
    return 0


def _do_reset(room_id):
    """把已分胜负的房间重置回未开局，返回 (owner, p2, next_first)"""
    rd = r.hgetall(f'room:{room_id}')
    if not rd:
        return None
    owner = rd.get('owner', '')
    p2 = rd.get('player2', '')
    first = rd.get('game_first', '') or rd.get('current_turn', '') or owner
    if p2:
        next_first = p2 if first == owner else owner
    else:
        next_first = owner

    _clear_board_fields(room_id)
    r.hset(f'room:{room_id}', 'game_first', next_first)
    r.hset(f'room:{room_id}', 'current_turn', next_first)
    r.hset(f'room:{room_id}', 'status', 'full' if p2 else 'waiting')
    r.hset(f'room:{room_id}', 'owner_ready', '0')
    r.hset(f'room:{room_id}', 'player2_ready', '0')
    return (owner, p2, next_first)


def _reset_and_takeover(room_id):
    """
    重置房间 + 立刻把离线方交给未开局离线策略。
    先清离线方再广播，避免房主看到"人还在"。
    返回 True 表示真的重置了；False 表示已经被重置过（幂等）
    """
    cur = r.hgetall(f'room:{room_id}')
    if not cur or not cur.get('winner', ''):
        return False

    res = _do_reset(room_id)
    if not res:
        return False
    owner, p2, next_first = res

    try:
        if p2 and not _is_online(room_id, 'player2'):
            _mark_offline_player2(room_id, p2)
        if not _is_online(room_id, 'owner'):
            _mark_offline_owner(room_id, owner)
    except Exception:
        pass

    ttl_now = _room_ttl(room_id)

    cur2 = r.hgetall(f'room:{room_id}')
    if not cur2:
        return True

    try:
        async_to_sync(get_channel_layer().group_send)(
            f'room_{room_id}',
            {
                'type': 'ws.game.reset',
                'owner': cur2.get('owner', ''),
                'player2': cur2.get('player2', ''),
                'current_turn': next_first,
                'status': cur2.get('status', 'waiting'),
                'ttl': ttl_now,
            }
        )
    except Exception:
        pass
    return True


def _start_post_over_watchdog(room_id):
    """
    胜负已分后的兜底（关键：不依赖任何人点确定）
    - 已重置过 → 什么都不做
    - 双方都在线 → 正常等他们点确定，不干预
    - 有人离线（离线方永远不会点确定）→ 强制重置 + 交给未开局离线策略
    """
    def worker():
        time.sleep(POST_OVER_GRACE)
        try:
            cur = r.hgetall(f'room:{room_id}')
            if not cur:
                return
            if not cur.get('winner', ''):
                return          # 已经点过确定了
            if _is_online(room_id, 'owner') and _is_online(room_id, 'player2'):
                return          # 双方都在线，不干预
            _reset_and_takeover(room_id)
        except Exception:
            pass

    t = threading.Thread(target=worker, daemon=True)
    t.start()


# ========== 棋盘工具 ==========

def _moves_from_str(s):
    out = []
    if not s:
        return out
    for c in s.split(','):
        if not c:
            continue
        try:
            x, y = c.split(':')
            out.append((int(x), int(y)))
        except Exception:
            pass
    return out


def _moves_to_str(moves):
    return ','.join(f'{a}:{b}' for a, b in moves)


def _board_from_str(s):
    b = [[0] * BOARD_SIZE for _ in range(BOARD_SIZE)]
    if not s:
        return b
    try:
        for c in s.split(','):
            if not c:
                continue
            x, y, v = c.split(':')
            b[int(x)][int(y)] = int(v)
    except Exception:
        pass
    return b


def _board_to_str(b):
    out = []
    for x in range(BOARD_SIZE):
        for y in range(BOARD_SIZE):
            if b[x][y]:
                out.append(f'{x}:{y}:{b[x][y]}')
    return ','.join(out)


def _check_win(b, x, y):
    v = b[x][y]
    if not v:
        return False
    dirs = [(1, 0), (0, 1), (1, 1), (1, -1)]
    for dx, dy in dirs:
        count = 1
        for step in (1, -1):
            nx, ny = x + dx * step, y + dy * step
            while 0 <= nx < BOARD_SIZE and 0 <= ny < BOARD_SIZE and b[nx][ny] == v:
                count += 1
                nx += dx * step
                ny += dy * step
        if count >= 5:
            return True
    return False


# ---------- 步时（超时判负） ----------

def _move_left(rd):
    try:
        dl = float(rd.get('move_deadline', '') or 0)
    except Exception:
        return 0
    if not dl:
        return 0
    left = dl - time.time()
    if left <= 0:
        return 0
    return int(left + 0.5)


def _arm_move_timer(room_id, nickname, seconds=MOVE_TIME_LIMIT):
    try:
        cur = r.hgetall(f'room:{room_id}')
        if not cur:
            return
        if cur.get('status') != 'playing':
            return
        if cur.get('winner', ''):
            return
        if cur.get('current_turn', '') != nickname:
            return
        deadline = time.time() + seconds
        r.hset(f'room:{room_id}', 'move_deadline', str(deadline))
        t = threading.Thread(
            target=_move_timeout_watcher, args=(room_id, nickname, deadline), daemon=True
        )
        t.start()
    except Exception:
        pass


def _move_timeout_watcher(room_id, nickname, deadline):
    time.sleep(MOVE_TIME_LIMIT + 2)
    try:
        cur = r.hgetall(f'room:{room_id}')
        if not cur:
            return
        if cur.get('status') != 'playing':
            return
        if cur.get('winner', ''):
            return
        if cur.get('current_turn', '') != nickname:
            return
        try:
            dl = float(cur.get('move_deadline', '') or 0)
        except Exception:
            return
        if not dl:
            return
        if time.time() < dl:
            return
        _timeout_lose(room_id, nickname)
    except Exception:
        pass


def _timeout_lose(room_id, loser):
    """超时判负：与投降共用同一套结束流程"""
    cur = r.hgetall(f'room:{room_id}')
    if not cur:
        return
    if cur.get('status') != 'playing':
        return
    if cur.get('winner', ''):
        return

    owner = cur.get('owner', '')
    p2 = cur.get('player2', '')
    if loser == owner:
        winner = p2
    elif loser == p2:
        winner = owner
    else:
        return
    if not winner:
        return

    r.hset(f'room:{room_id}', 'winner', winner)
    r.hset(f'room:{room_id}', 'over_reason', 'timeout')
    r.hset(f'room:{room_id}', 'current_turn', '')
    r.hset(f'room:{room_id}', 'move_deadline', '')
    r.hset(f'room:{room_id}', 'undo_pending', '')

    try:
        async_to_sync(get_channel_layer().group_send)(
            f'room_{room_id}',
            {'type': 'ws.game.over', 'winner': winner, 'loser': loser, 'reason': 'timeout'}
        )
    except Exception:
        pass

    try:
        _start_post_over_watchdog(room_id)
    except Exception:
        pass


# ========== 兜底清扫 ==========

def _room_sweeper(room_id):
    """兜底扫描。对局进行中一律不踢人（交给步时判负）"""
    lock_key = f'sweeper:{room_id}'
    try:
        if not r.set(lock_key, '1', ex=30, nx=True):
            return
    except Exception:
        return

    while True:
        try:
            room = r.hgetall(f'room:{room_id}')
            if not room:
                break

            owner = room.get('owner', '')
            p2 = room.get('player2', '')
            status = room.get('status', 'waiting')
            has_winner = bool(room.get('winner', ''))

            owner_alive = bool(owner) and _hb_count(room_id, 'owner') > 0
            p2_alive = bool(p2) and _hb_count(room_id, 'player2') > 0

            # 对局进行中：不踢人，由步时判负
            if status == 'playing' and not has_winner:
                # 自愈：服务重启后步时线程全部丢失，这里兜底补判负 / 补挂计时
                turn = room.get('current_turn', '')
                if turn:
                    try:
                        dl = float(room.get('move_deadline', '') or 0)
                    except Exception:
                        dl = 0
                    if dl and time.time() >= dl:
                        _timeout_lose(room_id, turn)
                    elif not dl:
                        _arm_move_timer(room_id, turn)

                try:
                    r.set(lock_key, '1', ex=30)
                except Exception:
                    pass
                time.sleep(SWEEP_INTERVAL)
                continue

            if p2 and not p2_alive:
                ttl_now = 0
                try:
                    cur_ttl = r.ttl(f'room:{room_id}')
                    if cur_ttl is not None and cur_ttl < 0:
                        r.expire(f'room:{room_id}', ROOM_TTL_AFTER_RESET)
                    t = r.ttl(f'room:{room_id}')
                    if t and t > 0:
                        ttl_now = t
                except Exception:
                    pass

                r.hset(f'room:{room_id}', 'player2', '')
                r.hset(f'room:{room_id}', 'status', 'waiting')
                r.delete(f'user_room:{p2}')
                r.delete(f'player2_conns:{room_id}')
                _hb_clear(room_id, 'player2')

                if has_winner:
                    pass   # 已分胜负：只踢人，保留结果让赢家能重置
                else:
                    r.hset(f'room:{room_id}', 'owner_ready', '0')
                    r.hset(f'room:{room_id}', 'player2_ready', '0')
                    _clear_board_fields(room_id)

                try:
                    async_to_sync(get_channel_layer().group_send)(
                        f'room_{room_id}',
                        {'type': 'ws.opponent.left', 'nickname': p2, 'ttl': ttl_now}
                    )
                except Exception:
                    pass

            if owner and not owner_alive:
                p2_now = r.hget(f'room:{room_id}', 'player2') or ''
                r.delete(f'room:{room_id}')
                r.srem('active_rooms', room_id)
                r.delete(f'user_room:{owner}')
                if p2_now:
                    r.delete(f'user_room:{p2_now}')
                r.delete(f'owner_conns:{room_id}')
                r.delete(f'player2_conns:{room_id}')
                _hb_clear(room_id, 'owner')
                _hb_clear(room_id, 'player2')
                try:
                    async_to_sync(get_channel_layer().group_send)(
                        f'room_{room_id}',
                        {'type': 'ws.room.closed', 'message': '房主已退出，房间已解散'}
                    )
                except Exception:
                    pass
                break

            try:
                r.set(lock_key, '1', ex=30)
            except Exception:
                pass
            time.sleep(SWEEP_INTERVAL)
        except Exception:
            time.sleep(SWEEP_INTERVAL)


# ========== 房间管理函数（供 views.py 调用） ==========

def create_room(nickname):
    old_room = r.get(f'user_room:{nickname}')
    if old_room:
        old_data = r.hgetall(f'room:{old_room}')
        if old_data and old_data.get('player2'):
            r.delete(f'user_room:{old_data["player2"]}')
        r.delete(f'room:{old_room}')
        r.srem('active_rooms', old_room)
        r.delete(f'owner_conns:{old_room}')
        r.delete(f'player2_conns:{old_room}')
        _hb_clear(old_room, 'owner')
        _hb_clear(old_room, 'player2')

    for _ in range(100):
        room_id = str(random.randint(1000, 9999))
        if not r.exists(f'room:{room_id}'):
            break
    else:
        room_id = str(random.randint(1000, 9999))

    room_data = {
        'room_id': room_id,
        'owner': nickname,
        'player2': '',
        'status': 'waiting',
        'board': '',
        'current_turn': nickname,
        'owner_ready': '0',
        'player2_ready': '0',
        'winner': '',
        'move_count': '0',
        'last_x': '',
        'last_y': '',
        'game_first': '',
        'moves': '',
        'over_reason': '',
        'undo_pending': '',
        'undo_lock_owner': '0',
        'undo_lock_player2': '0',
        'move_deadline': '',
    }
    r.hmset(f'room:{room_id}', room_data)
    r.expire(f'room:{room_id}', ROOM_TTL_AFTER_RESET)

    r.sadd('active_rooms', room_id)
    r.set(f'user_room:{nickname}', room_id, ex=ROOM_TTL_AFTER_RESET)

    return room_id


def get_active_rooms():
    room_ids = r.smembers('active_rooms')
    rooms = []
    for rid in room_ids:
        data = r.hgetall(f'room:{rid}')
        if data:
            rooms.append({
                'room_id': rid,
                'owner': data.get('owner', ''),
                'status': data.get('status', 'waiting'),
                'player2': data.get('player2', ''),
            })
        else:
            r.srem('active_rooms', rid)
    return rooms


def get_room(room_id):
    return r.hgetall(f'room:{room_id}')


def is_room_expired(room_id):
    return not r.exists(f'room:{room_id}')


def admin_delete_nickname(nickname):
    room_id = r.get(f'user_room:{nickname}')
    if room_id:
        r.publish(f'room:{room_id}:notify', json.dumps({
            'type': 'force_close',
            'message': '对方昵称已被管理员删除，对局异常结束',
            'deleted_user': nickname,
        }))
        r.delete(f'room:{room_id}')
        r.srem('active_rooms', room_id)
        r.delete(f'user_room:{nickname}')
        room_data = r.hgetall(f'room:{room_id}')
        if room_data:
            player2 = room_data.get('player2', '')
            if player2:
                r.delete(f'user_room:{player2}')
    r.delete(f'user_room:{nickname}')
    if room_id:
        r.delete(f'owner_conns:{room_id}')
        r.delete(f'player2_conns:{room_id}')
        _hb_clear(room_id, 'owner')
        _hb_clear(room_id, 'player2')


# ========== WebSocket Consumer ==========

class TestConsumer(WebsocketConsumer):
    def connect(self):
        self.accept()
        self.send(text_data=json.dumps({'message': 'test ok'}))

    def disconnect(self, close_code):
        pass

    def receive(self, text_data):
        data = json.loads(text_data)
        self.send(text_data=json.dumps({'echo': data.get('message', '')}))


class GobangConsumer(WebsocketConsumer):
    def connect(self):
        self.room_id = self.scope['url_route']['kwargs']['room_id']
        self.nickname = self.scope['session'].get('nickname', '')

        if not self.nickname:
            self.close(code=4001)
            return

        self.room_group_name = f'room_{self.room_id}'
        self._handoff = False
        self.accept()

        room_data = r.hgetall(f'room:{self.room_id}')
        self.role = None

        # 房间已不存在（被解散）
        if not room_data:
            self.send(text_data=json.dumps({
                'action': 'error',
                'message': '房间已解散',
            }))
            self.close(code=4003)
            return

        if room_data.get('owner') == self.nickname:
            self.role = 'owner'

        elif room_data.get('player2') == self.nickname:
            self.role = 'player2'

        else:
            lost = None
            try:
                lost = r.get(f'lost_player:{self.room_id}')
            except Exception:
                lost = None

            if lost == self.nickname:
                # 回归权有效期内回来了
                if room_data.get('player2'):
                    # 位子已被别人占了
                    self.send(text_data=json.dumps({
                        'action': 'error',
                        'message': '你的位置已被其他玩家占用'
                    }))
                    self.close(code=4003)
                    return

                # 位子还空着 → 回原位
                lost_ready = '0'
                try:
                    lost_ready = r.get(f'lost_ready:{self.room_id}') or '0'
                except Exception:
                    lost_ready = '0'
                r.hset(f'room:{self.room_id}', 'player2', self.nickname)
                r.hset(f'room:{self.room_id}', 'status', 'full')
                r.hset(f'room:{self.room_id}', 'player2_ready', lost_ready)
                r.set(f'user_room:{self.nickname}', self.room_id, ex=ROOM_TTL_AFTER_RESET)
                r.delete(f'lost_player:{self.room_id}')
                try:
                    r.delete(f'exited:{self.room_id}:{self.nickname}')
                except Exception:
                    pass
                self.role = 'player2'

                ttl = 0
                try:
                    t = r.ttl(f'room:{self.room_id}')
                    if t and t > 0:
                        ttl = t
                except Exception:
                    pass
                try:
                    async_to_sync(self.channel_layer.group_send)(
                        self.room_group_name,
                        {
                            'type': 'player.joined',
                            'nickname': self.nickname,
                            'owner': room_data.get('owner', ''),
                            'player2': self.nickname,
                            'current_turn': room_data.get('current_turn', '') or room_data.get('owner', ''),
                            'owner_ready': room_data.get('owner_ready', '0'),
                            'player2_ready': lost_ready,
                            'ttl': ttl,
                        }
                    )
                except Exception:
                    pass

            else:
                # 不是本房间的人：检查是否刚被踢出（超时回来）
                exited = False
                try:
                    exited = r.get(f'exited:{self.room_id}:{self.nickname}') is not None
                except Exception:
                    exited = False

                if exited:
                    # 一次性：用掉就删，避免"点掉弹窗后想立刻回来还得干等 30 秒"
                    try:
                        r.delete(f'exited:{self.room_id}:{self.nickname}')
                    except Exception:
                        pass
                    self.send(text_data=json.dumps({
                        'action': 'error',
                        'message': '你已不在本房间内'
                    }))
                    self.close(code=4003)
                    return

                # 对局已结束、自己却不是成员（已被踢出）→ 直接提示，避免"幽灵"状态
                if room_data.get('winner', ''):
                    try:
                        r.set(f'exited:{self.room_id}:{self.nickname}', '1', ex=EXITED_TRACE_TTL)
                    except Exception:
                        pass
                    self.send(text_data=json.dumps({
                        'action': 'error',
                        'message': '你已不在本房间内'
                    }))
                    self.close(code=4003)
                    return

                # 新来的第三人：允许连接，等待 join 加入

        if self.role == 'owner':
            r.sadd(f'owner_conns:{self.room_id}', self.channel_name)
            try:
                if r.exists(f'room:{self.room_id}'):
                    r.sadd('active_rooms', self.room_id)
            except Exception:
                pass
        elif self.role == 'player2':
            r.sadd(f'player2_conns:{self.room_id}', self.channel_name)

        if self.role:
            _hb_touch(self.room_id, self.role, self.channel_name)
            was_pending = False
            try:
                was_pending = r.get(f'pending:{self.room_id}:{self.role}') is not None
                r.delete(f'pending:{self.room_id}:{self.role}')
            except Exception:
                pass
            if was_pending:
                try:
                    async_to_sync(self.channel_layer.group_send)(
                        self.room_group_name,
                        {'type': 'ws.opponent.reconnected', 'role': self.role, 'nickname': self.nickname}
                    )
                except Exception:
                    pass

        async_to_sync(self.channel_layer.group_add)(
            self.room_group_name,
            self.channel_name
        )

        try:
            t = threading.Thread(target=_room_sweeper, args=(self.room_id,), daemon=True)
            t.start()
        except Exception:
            pass

        self.send(text_data=json.dumps({
            'type': 'system',
            'message': f'已连接到房间 {self.room_id}'
        }))

        cur = r.hgetall(f'room:{self.room_id}')
        ttl = 0
        try:
            t = r.ttl(f'room:{self.room_id}')
            if t and t > 0:
                ttl = t
        except Exception:
            pass

        self.send(text_data=json.dumps({
            'action': 'room_state',
            'owner': cur.get('owner', '') if cur else '',
            'player2': cur.get('player2', '') if cur else '',
            'status': cur.get('status', 'waiting') if cur else 'waiting',
            'current_turn': cur.get('current_turn', '') if cur else '',
            'owner_ready': cur.get('owner_ready', '0') if cur else '0',
            'player2_ready': cur.get('player2_ready', '0') if cur else '0',
            'board': cur.get('board', '') if cur else '',
            'winner': cur.get('winner', '') if cur else '',
            'last_x': cur.get('last_x', '') if cur else '',
            'last_y': cur.get('last_y', '') if cur else '',
            'over_reason': cur.get('over_reason', '') if cur else '',
            'undo_pending': cur.get('undo_pending', '') if cur else '',
            'undo_lock_owner': cur.get('undo_lock_owner', '0') if cur else '0',
            'undo_lock_player2': cur.get('undo_lock_player2', '0') if cur else '0',
            'move_left': _move_left(cur) if cur else 0,
            'ttl': ttl,
        }))

        try:
            if ttl > 0:
                def expire_notify(secs=ttl):
                    time.sleep(secs + 1)
                    try:
                        if not r.exists(f'room:{self.room_id}'):
                            self.send(text_data=json.dumps({
                                'action': 'room_expired',
                                'message': '等待超时，房间已解散',
                            }))
                    except Exception:
                        pass
                t = threading.Thread(target=expire_notify, daemon=True)
                t.start()
        except Exception:
            pass

    def disconnect(self, close_code):
        if not hasattr(self, 'room_id') or not hasattr(self, 'nickname'):
            return

        role = getattr(self, 'role', None)
        if role:
            # 该连接的心跳提前到 GRACE_TTL 后过期（其他连接不受影响）
            _hb_leave(self.room_id, role, self.channel_name)
            try:
                r.set(f'pending:{self.room_id}:{role}', '1', ex=GRACE_TTL + 15)
            except Exception:
                pass

        if getattr(self, '_handoff', False):
            return

        try:
            async_to_sync(self.channel_layer.group_discard)(
                self.room_group_name,
                self.channel_name
            )
        except Exception:
            pass

        if not role:
            return

        if role == 'owner':
            r.srem(f'owner_conns:{self.room_id}', self.channel_name)
        else:
            r.srem(f'player2_conns:{self.room_id}', self.channel_name)

        # 广播"对手已断线"提示（对局中也照样提示）
        try:
            async_to_sync(self.channel_layer.group_send)(
                self.room_group_name,
                {'type': 'ws.opponent.lost', 'role': role, 'nickname': self.nickname}
            )
        except Exception:
            pass

        rd = r.hgetall(f'room:{self.room_id}')

        # ---------- 对局中：不单独清场，统一由步时判负 ----------
        if rd.get('status') == 'playing':
            return

        # ---------- 未开局 ----------
        if role == 'player2':
            _mark_offline_player2(self.room_id, self.nickname)
            return

        if role == 'owner':
            _mark_offline_owner(self.room_id, self.nickname)
            return

    # ---------- 统一发送房间状态 ----------
    def _send_state(self, rd):
        ttl = 0
        try:
            t = r.ttl(f'room:{self.room_id}')
            if t and t > 0:
                ttl = t
        except Exception:
            pass
        self.send(text_data=json.dumps({
            'action': 'room_state',
            'owner': rd.get('owner', ''),
            'player2': rd.get('player2', ''),
            'status': rd.get('status', 'waiting'),
            'current_turn': rd.get('current_turn', ''),
            'owner_ready': rd.get('owner_ready', '0'),
            'player2_ready': rd.get('player2_ready', '0'),
            'board': rd.get('board', ''),
            'winner': rd.get('winner', ''),
            'last_x': rd.get('last_x', ''),
            'last_y': rd.get('last_y', ''),
            'over_reason': rd.get('over_reason', ''),
            'undo_pending': rd.get('undo_pending', ''),
            'undo_lock_owner': rd.get('undo_lock_owner', '0'),
            'undo_lock_player2': rd.get('undo_lock_player2', '0'),
            'move_left': _move_left(rd),
            'ttl': ttl,
        }))

    def receive(self, text_data):
        data = json.loads(text_data)
        action = data.get('action', '')

        if action == 'ping':
            role = getattr(self, 'role', None)
            if role:
                _hb_touch(self.room_id, role, self.channel_name)
            return

        if action == 'leave':
            rd = r.hgetall(f'room:{self.room_id}')
            if rd:
                if rd.get('player2') == self.nickname:
                    r.hset(f'room:{self.room_id}', 'player2', '')
                    r.hset(f'room:{self.room_id}', 'status', 'waiting')
                    r.hset(f'room:{self.room_id}', 'owner_ready', '0')
                    r.hset(f'room:{self.room_id}', 'player2_ready', '0')
                    _clear_board_fields(self.room_id)
                    r.delete(f'user_room:{self.nickname}')
                    r.delete(f'player2_conns:{self.room_id}')
                    _hb_drop(self.room_id, 'player2', self.channel_name)
                    try:
                        r.delete(f'lost_player:{self.room_id}')
                        r.delete(f'lost_ready:{self.room_id}')
                        r.delete(f'pending:{self.room_id}:player2')
                    except Exception:
                        pass
                    ttl_now = 0
                    try:
                        cu = r.ttl(f'room:{self.room_id}')
                        if cu is not None and cu < 0:
                            r.expire(f'room:{self.room_id}', ROOM_TTL_AFTER_RESET)
                        t = r.ttl(f'room:{self.room_id}')
                        if t and t > 0:
                            ttl_now = t
                    except Exception:
                        pass
                    try:
                        async_to_sync(self.channel_layer.group_send)(
                            self.room_group_name,
                            {'type': 'ws.opponent.left', 'nickname': self.nickname, 'ttl': ttl_now}
                        )
                    except Exception:
                        pass
                elif rd.get('owner') == self.nickname:
                    player2 = rd.get('player2', '')
                    if player2:
                        r.delete(f'user_room:{player2}')
                    r.delete(f'room:{self.room_id}')
                    r.srem('active_rooms', self.room_id)
                    r.delete(f'user_room:{self.nickname}')
                    r.delete(f'owner_conns:{self.room_id}')
                    r.delete(f'player2_conns:{self.room_id}')
                    _hb_drop(self.room_id, 'owner', self.channel_name)
                    try:
                        r.delete(f'pending:{self.room_id}:owner')
                    except Exception:
                        pass
                    try:
                        async_to_sync(self.channel_layer.group_send)(
                            self.room_group_name,
                            {'type': 'ws.room.closed', 'message': '房主已退出，房间已解散'}
                        )
                    except Exception:
                        pass
            self._handoff = True
            return

        if action == 'enter':
            rd = r.hgetall(f'room:{self.room_id}')
            if not rd:
                self.send(text_data=json.dumps({
                    'action': 'room_expired',
                    'message': '房间已解散',
                }))
                self.close(code=4003)
                return

            if rd.get('owner') == self.nickname or rd.get('player2') == self.nickname:
                return

            # 不是成员，但位子空着 → 直接补位
            # （被踢出者点掉弹窗后回来能立刻坐下，不用等痕迹过期）
            if not rd.get('player2') and rd.get('status') != 'playing':
                owner_name = rd.get('owner', '')
                r.hset(f'room:{self.room_id}', 'player2', self.nickname)
                r.hset(f'room:{self.room_id}', 'status', 'full')
                r.set(f'user_room:{self.nickname}', self.room_id, ex=ROOM_TTL_AFTER_RESET)
                r.hset(f'room:{self.room_id}', 'owner_ready', '0')
                r.hset(f'room:{self.room_id}', 'player2_ready', '0')
                _clear_board_fields(self.room_id)
                r.hset(f'room:{self.room_id}', 'current_turn', owner_name)
                r.hset(f'room:{self.room_id}', 'game_first', owner_name)
                try:
                    r.delete(f'exited:{self.room_id}:{self.nickname}')
                    if r.get(f'lost_player:{self.room_id}') == self.nickname:
                        r.delete(f'lost_player:{self.room_id}')
                except Exception:
                    pass

                self.role = 'player2'
                r.sadd(f'player2_conns:{self.room_id}', self.channel_name)
                _hb_touch(self.room_id, 'player2', self.channel_name)
                try:
                    r.delete(f'pending:{self.room_id}:player2')
                except Exception:
                    pass

                ttl = 0
                try:
                    t = r.ttl(f'room:{self.room_id}')
                    if t and t > 0:
                        ttl = t
                except Exception:
                    pass

                self.send(text_data=json.dumps({
                    'action': 'joined', 'room_id': self.room_id
                }))
                try:
                    async_to_sync(self.channel_layer.group_send)(
                        self.room_group_name,
                        {
                            'type': 'player.joined',
                            'nickname': self.nickname,
                            'owner': owner_name,
                            'player2': self.nickname,
                            'current_turn': owner_name,
                            'owner_ready': '0',
                            'player2_ready': '0',
                            'ttl': ttl,
                        }
                    )
                except Exception:
                    pass
                return

            # 位子确实有人 → 进不去
            self.send(text_data=json.dumps({
                'action': 'error',
                'message': '你已不在本房间内',
            }))
            self.close(code=4003)
            return

        # ---------- 落子 ----------
        if action == 'move':
            rd = r.hgetall(f'room:{self.room_id}')
            if not rd:
                return

            if rd.get('owner') != self.nickname and rd.get('player2') != self.nickname:
                return
            if rd.get('status') != 'playing':
                return
            if rd.get('undo_pending', ''):
                return
            if rd.get('current_turn', '') != self.nickname:
                return
            if rd.get('winner', ''):
                return

            try:
                x = int(data.get('x'))
                y = int(data.get('y'))
            except Exception:
                return
            if not (0 <= x < BOARD_SIZE and 0 <= y < BOARD_SIZE):
                return

            board = _board_from_str(rd.get('board', ''))
            if board[x][y] != 0:
                return

            if rd.get('owner') == self.nickname:
                color = 1
                next_turn = rd.get('player2', '')
            else:
                color = 2
                next_turn = rd.get('owner', '')

            board[x][y] = color

            try:
                move_count = int(rd.get('move_count', '0')) + 1
            except Exception:
                move_count = 1

            winner = ''
            if _check_win(board, x, y):
                winner = self.nickname
                next_turn = ''
            elif move_count >= BOARD_SIZE * BOARD_SIZE:
                winner = 'draw'
                next_turn = ''

            moves = _moves_from_str(rd.get('moves', ''))
            moves.append((x, y))

            r.hset(f'room:{self.room_id}', 'board', _board_to_str(board))
            r.hset(f'room:{self.room_id}', 'moves', _moves_to_str(moves))
            r.hset(f'room:{self.room_id}', 'current_turn', next_turn)
            r.hset(f'room:{self.room_id}', 'move_count', str(move_count))
            r.hset(f'room:{self.room_id}', 'winner', winner)
            r.hset(f'room:{self.room_id}', 'last_x', str(x))
            r.hset(f'room:{self.room_id}', 'last_y', str(y))
            r.hset(f'room:{self.room_id}', 'undo_lock_owner', '0')
            r.hset(f'room:{self.room_id}', 'undo_lock_player2', '0')

            if winner:
                r.hset(f'room:{self.room_id}', 'move_deadline', '')
            else:
                _arm_move_timer(self.room_id, next_turn)

            new_rd = r.hgetall(f'room:{self.room_id}')

            try:
                async_to_sync(self.channel_layer.group_send)(
                    self.room_group_name,
                    {
                        'type': 'ws.move.made',
                        'x': x, 'y': y, 'color': color,
                        'next_turn': next_turn,
                        'winner': winner,
                        'by': self.nickname,
                        'move_count': move_count,
                        'last_x': x, 'last_y': y,
                        'move_left': _move_left(new_rd),
                    }
                )
            except Exception:
                pass

            if winner:
                try:
                    _start_post_over_watchdog(self.room_id)
                except Exception:
                    pass
            return

        # ---------- 请求悔棋 ----------
        if action == 'undo_request':
            rd = r.hgetall(f'room:{self.room_id}')
            if not rd:
                return
            if rd.get('owner') != self.nickname and rd.get('player2') != self.nickname:
                return
            if rd.get('status') != 'playing':
                return
            if rd.get('winner', ''):
                return
            if rd.get('current_turn', '') == self.nickname:
                return
            if rd.get('undo_pending', ''):
                return
            is_owner = (rd.get('owner') == self.nickname)
            lock_key = 'undo_lock_owner' if is_owner else 'undo_lock_player2'
            if rd.get(lock_key, '0') == '1':
                return
            moves = _moves_from_str(rd.get('moves', ''))
            if not moves:
                return

            r.hset(f'room:{self.room_id}', 'undo_pending', self.nickname)
            r.hset(f'room:{self.room_id}', 'move_deadline', '')

            try:
                async_to_sync(self.channel_layer.group_send)(
                    self.room_group_name,
                    {'type': 'ws.undo.request', 'by': self.nickname, 'seconds': UNDO_TIMEOUT}
                )
            except Exception:
                pass

            def undo_timeout():
                time.sleep(UNDO_TIMEOUT)
                try:
                    cur = r.hgetall(f'room:{self.room_id}')
                    if not cur:
                        return
                    if cur.get('undo_pending', '') != self.nickname:
                        return
                    _clear_undo_pending(self.room_id)
                    lk = 'undo_lock_owner' if is_owner else 'undo_lock_player2'
                    r.hset(f'room:{self.room_id}', lk, '1')
                    _arm_move_timer(self.room_id, cur.get('current_turn', ''))
                    try:
                        async_to_sync(get_channel_layer().group_send)(
                            self.room_group_name,
                            {'type': 'ws.undo.rejected', 'by': self.nickname, 'reason': 'timeout'}
                        )
                    except Exception:
                        pass
                except Exception:
                    pass

            t = threading.Thread(target=undo_timeout, daemon=True)
            t.start()
            return

        # ---------- 悔棋应答 ----------
        if action == 'undo_response':
            rd = r.hgetall(f'room:{self.room_id}')
            if not rd:
                return
            pending = rd.get('undo_pending', '')
            if not pending:
                return
            if pending == self.nickname:
                return
            if rd.get('owner') != self.nickname and rd.get('player2') != self.nickname:
                return

            accept = bool(data.get('accept'))
            req_is_owner = (rd.get('owner') == pending)

            if not accept:
                _clear_undo_pending(self.room_id)
                r.hset(f'room:{self.room_id}',
                       'undo_lock_owner' if req_is_owner else 'undo_lock_player2', '1')
                _arm_move_timer(self.room_id, rd.get('current_turn', ''))
                new_rd = r.hgetall(f'room:{self.room_id}')
                try:
                    async_to_sync(self.channel_layer.group_send)(
                        self.room_group_name,
                        {
                            'type': 'ws.undo.rejected',
                            'by': pending, 'reason': 'rejected',
                            'move_left': _move_left(new_rd),
                        }
                    )
                except Exception:
                    pass
                return

            moves = _moves_from_str(rd.get('moves', ''))
            if not moves:
                _clear_undo_pending(self.room_id)
                _arm_move_timer(self.room_id, rd.get('current_turn', ''))
                return

            lx, ly = moves.pop()
            board = _board_from_str(rd.get('board', ''))
            board[lx][ly] = 0

            try:
                move_count = max(0, int(rd.get('move_count', '0')) - 1)
            except Exception:
                move_count = 0

            new_last = moves[-1] if moves else None

            r.hset(f'room:{self.room_id}', 'board', _board_to_str(board))
            r.hset(f'room:{self.room_id}', 'moves', _moves_to_str(moves))
            r.hset(f'room:{self.room_id}', 'move_count', str(move_count))
            r.hset(f'room:{self.room_id}', 'winner', '')
            r.hset(f'room:{self.room_id}', 'over_reason', '')
            r.hset(f'room:{self.room_id}', 'current_turn', pending)
            r.hset(f'room:{self.room_id}', 'last_x', str(new_last[0]) if new_last else '')
            r.hset(f'room:{self.room_id}', 'last_y', str(new_last[1]) if new_last else '')
            r.hset(f'room:{self.room_id}', 'undo_pending', '')
            r.hset(f'room:{self.room_id}', 'undo_lock_owner', '0')
            r.hset(f'room:{self.room_id}', 'undo_lock_player2', '0')

            _arm_move_timer(self.room_id, pending)

            new_rd = r.hgetall(f'room:{self.room_id}')

            try:
                async_to_sync(self.channel_layer.group_send)(
                    self.room_group_name,
                    {
                        'type': 'ws.undo.done',
                        'board': _board_to_str(board),
                        'current_turn': pending,
                        'by': pending,
                        'last_x': new_last[0] if new_last else '',
                        'last_y': new_last[1] if new_last else '',
                        'move_count': move_count,
                        'move_left': _move_left(new_rd),
                    }
                )
            except Exception:
                pass
            return

        # ---------- 投降 ----------
        if action == 'surrender':
            rd = r.hgetall(f'room:{self.room_id}')
            if not rd:
                return
            if rd.get('status') != 'playing':
                return
            if rd.get('winner', ''):
                return

            if rd.get('owner') == self.nickname:
                winner = rd.get('player2', '')
            elif rd.get('player2') == self.nickname:
                winner = rd.get('owner', '')
            else:
                return
            if not winner:
                return

            r.hset(f'room:{self.room_id}', 'winner', winner)
            r.hset(f'room:{self.room_id}', 'over_reason', 'surrender')
            r.hset(f'room:{self.room_id}', 'current_turn', '')
            r.hset(f'room:{self.room_id}', 'undo_pending', '')
            r.hset(f'room:{self.room_id}', 'move_deadline', '')

            try:
                async_to_sync(self.channel_layer.group_send)(
                    self.room_group_name,
                    {
                        'type': 'ws.game.over',
                        'winner': winner,
                        'loser': self.nickname,
                        'reason': 'surrender',
                    }
                )
            except Exception:
                pass

            try:
                _start_post_over_watchdog(self.room_id)
            except Exception:
                pass
            return

        # ---------- 游戏结束后重置 ----------
        if action == 'reset_game':
            rd = r.hgetall(f'room:{self.room_id}')
            if not rd:
                self.send(text_data=json.dumps({
                    'action': 'room_expired',
                    'message': '房间已解散',
                }))
                return

            # 已不是本房间成员（中途被踢出）→ 直接提示回大厅，避免卡"幽灵"状态
            if rd.get('owner') != self.nickname and rd.get('player2') != self.nickname:
                self.send(text_data=json.dumps({
                    'action': 'error',
                    'message': '你已不在本房间内'
                }))
                self.close(code=4003)
                return

            # 已被重置过（或兜底已重置）→ 回吐当前状态即可（幂等）
            if not _reset_and_takeover(self.room_id):
                self._send_state(r.hgetall(f'room:{self.room_id}') or rd)
            return

        # ---------- 状态同步 ----------
        if action == 'sync_state':
            rd = r.hgetall(f'room:{self.room_id}')
            if not rd:
                self.send(text_data=json.dumps({
                    'action': 'room_expired',
                    'message': '房间已解散',
                }))
                return

            # 已不是成员 → 主动踢回大厅（5 秒轮询内自动清掉"幽灵"，无需刷新）
            if rd.get('owner') != self.nickname and rd.get('player2') != self.nickname:
                try:
                    r.set(f'exited:{self.room_id}:{self.nickname}', '1', ex=EXITED_TRACE_TTL)
                except Exception:
                    pass
                self.send(text_data=json.dumps({
                    'action': 'error',
                    'message': '你已不在本房间内'
                }))
                self.close(code=4003)
                return

            self._send_state(rd)
            return

        if action == 'join':
            room_data = r.hgetall(f'room:{self.room_id}')
            if not room_data:
                self.send(text_data=json.dumps({
                    'action': 'error',
                    'message': '房间不存在'
                }))
                return

            if room_data.get('owner') == self.nickname:
                self.send(text_data=json.dumps({
                    'action': 'joined', 'room_id': self.room_id
                }))
                return

            if room_data.get('player2') == self.nickname:
                self.send(text_data=json.dumps({
                    'action': 'joined', 'room_id': self.room_id
                }))
                self._handoff = True
                return

            if room_data.get('status') == 'full' or room_data.get('player2'):
                self.send(text_data=json.dumps({
                    'action': 'error', 'message': '房间已满'
                }))
                return

            r.hset(f'room:{self.room_id}', 'player2', self.nickname)
            r.hset(f'room:{self.room_id}', 'status', 'full')
            r.set(f'user_room:{self.nickname}', self.room_id, ex=ROOM_TTL_AFTER_RESET)
            r.hset(f'room:{self.room_id}', 'owner_ready', '0')
            r.hset(f'room:{self.room_id}', 'player2_ready', '0')

            _clear_board_fields(self.room_id)
            r.hset(f'room:{self.room_id}', 'current_turn', room_data.get('owner', ''))
            r.hset(f'room:{self.room_id}', 'game_first', room_data.get('owner', ''))

            # 注意：这里不删 lost_player，保留 10 秒回归权
            # 原主回来时若位子已被占，会收到"你的位置已被其他玩家占用"

            current_turn = room_data.get('owner', '')

            self.send(text_data=json.dumps({
                'action': 'joined', 'room_id': self.room_id
            }))

            self._handoff = True

            ttl = 0
            try:
                t = r.ttl(f'room:{self.room_id}')
                if t and t > 0:
                    ttl = t
            except Exception:
                pass

            async_to_sync(self.channel_layer.group_send)(
                self.room_group_name,
                {
                    'type': 'player.joined',
                    'nickname': self.nickname,
                    'owner': room_data.get('owner', ''),
                    'player2': self.nickname,
                    'current_turn': current_turn,
                    'owner_ready': '0',
                    'player2_ready': '0',
                    'ttl': ttl,
                }
            )

        elif action == 'ready':
            room_data = r.hgetall(f'room:{self.room_id}')
            if not room_data:
                return

            if room_data.get('status') == 'playing':
                return

            is_ready = '1' if data.get('ready') else '0'
            if room_data.get('owner') == self.nickname:
                r.hset(f'room:{self.room_id}', 'owner_ready', is_ready)
            elif room_data.get('player2') == self.nickname:
                r.hset(f'room:{self.room_id}', 'player2_ready', is_ready)
            else:
                return

            rd = r.hgetall(f'room:{self.room_id}')
            owner_ready = rd.get('owner_ready', '0')
            player2_ready = rd.get('player2_ready', '0')

            if rd.get('player2') and owner_ready == '1' and player2_ready == '1':
                r.hset(f'room:{self.room_id}', 'status', 'playing')
                _clear_board_fields(self.room_id)
                first = rd.get('game_first', '') or rd.get('current_turn', '') or rd.get('owner', '')
                r.hset(f'room:{self.room_id}', 'current_turn', first)
                r.hset(f'room:{self.room_id}', 'game_first', first)
                try:
                    r.persist(f'room:{self.room_id}')
                except Exception:
                    pass

                _arm_move_timer(self.room_id, first)

                new_rd = r.hgetall(f'room:{self.room_id}')

                async_to_sync(self.channel_layer.group_send)(
                    self.room_group_name,
                    {
                        'type': 'ws.game.start',
                        'owner': rd.get('owner', ''),
                        'player2': rd.get('player2', ''),
                        'current_turn': first,
                        'move_left': _move_left(new_rd),
                    }
                )
            else:
                async_to_sync(self.channel_layer.group_send)(
                    self.room_group_name,
                    {
                        'type': 'ws.ready.update',
                        'owner_ready': owner_ready,
                        'player2_ready': player2_ready,
                    }
                )

        else:
            self.send(text_data=json.dumps({
                'type': 'echo',
                'message': data.get('message', '')
            }))

    def player_joined(self, event):
        self.send(text_data=json.dumps({
            'action': 'player_joined',
            'nickname': event['nickname'],
            'owner': event['owner'],
            'player2': event['player2'],
            'current_turn': event.get('current_turn', ''),
            'owner_ready': event.get('owner_ready', '0'),
            'player2_ready': event.get('player2_ready', '0'),
            'ttl': event.get('ttl', 0),
        }))

    def ws_ready_update(self, event):
        self.send(text_data=json.dumps({
            'action': 'ready_update',
            'owner_ready': event['owner_ready'],
            'player2_ready': event['player2_ready'],
        }))

    def ws_game_start(self, event):
        self.send(text_data=json.dumps({
            'action': 'game_start',
            'owner': event['owner'],
            'player2': event['player2'],
            'current_turn': event.get('current_turn', ''),
            'move_left': event.get('move_left', 0),
        }))

    def ws_move_made(self, event):
        self.send(text_data=json.dumps({
            'action': 'move_made',
            'x': event['x'], 'y': event['y'], 'color': event['color'],
            'next_turn': event.get('next_turn', ''),
            'winner': event.get('winner', ''),
            'by': event.get('by', ''),
            'last_x': event.get('last_x', ''),
            'last_y': event.get('last_y', ''),
            'move_left': event.get('move_left', 0),
        }))

    def ws_undo_request(self, event):
        self.send(text_data=json.dumps({
            'action': 'undo_request',
            'by': event.get('by', ''),
            'seconds': event.get('seconds', UNDO_TIMEOUT),
        }))

    def ws_undo_done(self, event):
        self.send(text_data=json.dumps({
            'action': 'undo_done',
            'board': event.get('board', ''),
            'current_turn': event.get('current_turn', ''),
            'by': event.get('by', ''),
            'last_x': event.get('last_x', ''),
            'last_y': event.get('last_y', ''),
            'move_left': event.get('move_left', 0),
        }))

    def ws_undo_rejected(self, event):
        self.send(text_data=json.dumps({
            'action': 'undo_rejected',
            'by': event.get('by', ''),
            'reason': event.get('reason', ''),
            'move_left': event.get('move_left', 0),
        }))

    def ws_game_over(self, event):
        self.send(text_data=json.dumps({
            'action': 'game_over',
            'winner': event.get('winner', ''),
            'loser': event.get('loser', ''),
            'reason': event.get('reason', ''),
        }))

    def ws_game_reset(self, event):
        self.send(text_data=json.dumps({
            'action': 'game_reset',
            'owner': event['owner'],
            'player2': event['player2'],
            'current_turn': event.get('current_turn', ''),
            'status': event.get('status', 'waiting'),
            'ttl': event.get('ttl', 0),
        }))

    def ws_room_closed(self, event):
        self.send(text_data=json.dumps({
            'action': 'room_closed',
            'message': event.get('message', '房间已解散'),
        }))

    def ws_opponent_left(self, event):
        self.send(text_data=json.dumps({
            'action': 'opponent_left',
            'nickname': event.get('nickname', ''),
            'ttl': event.get('ttl', 0),
        }))

    def ws_opponent_lost(self, event):
        self.send(text_data=json.dumps({
            'action': 'opponent_lost',
            'role': event.get('role', ''),
            'nickname': event.get('nickname', ''),
        }))

    def ws_opponent_reconnected(self, event):
        self.send(text_data=json.dumps({
            'action': 'opponent_reconnected',
            'role': event.get('role', ''),
            'nickname': event.get('nickname', ''),
        }))