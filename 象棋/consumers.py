/root/HelloWorld/game_Cchess/consumers.py

# 核心后端：走棋校验、将军/绝杀判定、悔棋、议和、超时、胜负结算

import json
import redis
import random
import threading
import time
from channels.generic.websocket import WebsocketConsumer
from channels.layers import get_channel_layer
from asgiref.sync import async_to_sync

r = redis.Redis(host='127.0.0.1', port=6379, db=0, decode_responses=True)

# ---- 在线 ----
HEARTBEAT_TTL = 90
GRACE_TTL = 20
FAST_CONFIRM = 10
OWNER_WAITING_CONFIRM = 10
EXITED_TRACE_TTL = 30
POST_OVER_GRACE = 5
SWEEP_INTERVAL = 10

# ---- 棋盘 9 列 × 10 行 ----
BOARD_W = 9
BOARD_H = 10

# ---- 悔棋 ----
UNDO_TIMEOUT = 8
DRAW_MAX = 2
DRAW_TIMEOUT = 5
UNDO_LIMIT = 5

# ---- 步时：每人前 3 步 = 开局前 6 手 ----
MOVE_FAST_SECONDS = 30
MOVE_FAST_PLIES = 6
MOVE_NORMAL_SECONDS = 75
MOVE_TIME_LIMIT = MOVE_NORMAL_SECONDS

ROOM_TTL_AFTER_RESET = 600

# ---- 先手模式 ----
FIRST_OWNER_RED = 'owner_red'        # 我方执红（房主先）
FIRST_OPPONENT_RED = 'opponent_red'  # 对方执红（对手先）
FIRST_ALTERNATE = 'alternate'        # 轮换先手

# 棋子编码：红正黑负  1將帥 2士仕 3象相 4車 5馬 6砲炮 7兵卒


# ========== 在线心跳（ZSET，O(log N)）==========

def _hb_key(room_id, role):
    return f'cc:hb:{room_id}:{role}'


def _hb_touch(room_id, role, channel, ttl=HEARTBEAT_TTL):
    try:
        k = _hb_key(room_id, role)
        now = time.time()
        r.zadd(k, {channel: now + ttl})
        r.expire(k, int(ttl) + 300)
        r.zremrangebyscore(k, '-inf', now)
    except Exception:
        pass


def _hb_leave(room_id, role, channel, grace=GRACE_TTL):
    try:
        r.zadd(_hb_key(room_id, role), {channel: time.time() + grace})
    except Exception:
        pass


def _hb_drop(room_id, role, channel):
    try:
        r.zrem(_hb_key(room_id, role), channel)
    except Exception:
        pass


def _hb_clear(room_id, role):
    try:
        r.delete(_hb_key(room_id, role))
    except Exception:
        pass


def _hb_count(room_id, role):
    try:
        k = _hb_key(room_id, role)
        r.zremrangebyscore(k, '-inf', time.time())
        return r.zcard(k)
    except Exception:
        return 0


def _is_online(room_id, role):
    if not role:
        return False
    try:
        if r.get(f'cc:pending:{room_id}:{role}') is not None:
            return False
    except Exception:
        pass
    return _hb_count(room_id, role) > 0


# ========== 棋盘 ==========

def _initial_board():
    b = [[0] * BOARD_H for _ in range(BOARD_W)]
    back = [4, 5, 3, 2, 1, 2, 3, 5, 4]
    for x, v in enumerate(back):
        b[x][0] = -v
        b[x][9] = v
    b[1][2] = -6
    b[7][2] = -6
    b[1][7] = 6
    b[7][7] = 6
    for x in (0, 2, 4, 6, 8):
        b[x][3] = -7
        b[x][6] = 7
    return b


def _board_to_str(b):
    out = []
    for x in range(BOARD_W):
        for y in range(BOARD_H):
            if b[x][y]:
                out.append(f'{x}:{y}:{b[x][y]}')
    return ','.join(out)


def _board_from_str(s):
    b = [[0] * BOARD_H for _ in range(BOARD_W)]
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


def _initial_board_str():
    return _board_to_str(_initial_board())



def _in_board(x, y):
    return 0 <= x < BOARD_W and 0 <= y < BOARD_H


def _gen_moves(b, x, y):
    """生成 (x,y) 处棋子的所有合法落点（7 种走法）。
    将死/困毙/自杀过滤在第 4 步做，这里不含。"""
    v = b[x][y]
    if not v:
        return []
    is_red = (v > 0)
    av = abs(v)
    out = []

    def ok(nx, ny):
        t = b[nx][ny]
        return (t == 0) or ((t > 0) != is_red)

    if av == 1 or av == 2:                       # 將帥 / 仕士：九宫内
        py0, py1 = (7, 9) if is_red else (0, 2)
        dirs = ((1, 0), (-1, 0), (0, 1), (0, -1)) if av == 1 \
            else ((1, 1), (1, -1), (-1, 1), (-1, -1))
        for dx, dy in dirs:
            nx, ny = x + dx, y + dy
            if 3 <= nx <= 5 and py0 <= ny <= py1 and ok(nx, ny):
                out.append((nx, ny))

    elif av == 3:                                # 相象：田字、塞象眼、不过河
        for dx, dy in ((2, 2), (2, -2), (-2, 2), (-2, -2)):
            nx, ny = x + dx, y + dy
            if not _in_board(nx, ny):
                continue
            if is_red and ny < 5:
                continue
            if (not is_red) and ny > 4:
                continue
            if b[x + dx // 2][y + dy // 2] != 0:
                continue
            if ok(nx, ny):
                out.append((nx, ny))

    elif av == 4:                                # 車：直线滑动
        for dx, dy in ((1, 0), (-1, 0), (0, 1), (0, -1)):
            nx, ny = x + dx, y + dy
            while _in_board(nx, ny):
                t = b[nx][ny]
                if t == 0:
                    out.append((nx, ny))
                else:
                    if (t > 0) != is_red:
                        out.append((nx, ny))
                    break
                nx += dx
                ny += dy

    elif av == 5:                                # 馬：蹩马腿
        for dx, dy in ((1, 2), (1, -2), (-1, 2), (-1, -2),
                       (2, 1), (2, -1), (-2, 1), (-2, -1)):
            nx, ny = x + dx, y + dy
            if not _in_board(nx, ny):
                continue
            if abs(dx) == 2:
                lx, ly = x + dx // 2, y
            else:
                lx, ly = x, y + dy // 2
            if b[lx][ly] != 0:
                continue
            if ok(nx, ny):
                out.append((nx, ny))

    elif av == 6:                                # 砲炮：移动不隔子，吃子须隔且只隔一个
        for dx, dy in ((1, 0), (-1, 0), (0, 1), (0, -1)):
            nx, ny = x + dx, y + dy
            screen = 0
            while _in_board(nx, ny):
                t = b[nx][ny]
                if screen == 0:
                    if t == 0:
                        out.append((nx, ny))
                    else:
                        screen = 1
                else:
                    if t != 0:
                        if (t > 0) != is_red:
                            out.append((nx, ny))
                        break
                nx += dx
                ny += dy

    elif av == 7:                                # 兵卒：只进不退，过河可横走
        if is_red:
            steps = [(0, -1)]
            if y <= 4:
                steps += [(1, 0), (-1, 0)]
        else:
            steps = [(0, 1)]
            if y >= 5:
                steps += [(1, 0), (-1, 0)]
        for dx, dy in steps:
            nx, ny = x + dx, y + dy
            if _in_board(nx, ny) and ok(nx, ny):
                out.append((nx, ny))

    return out

def _board_copy(b):
    return [row[:] for row in b]


def _find_king(b, is_red):
    """找將/帥位置，返回 (x, y)，找不到返回 (-1, -1)"""
    target = 1 if is_red else -1
    for x in range(BOARD_W):
        for y in range(BOARD_H):
            if b[x][y] == target:
                return (x, y)
    return (-1, -1)


def _kings_facing(b):
    """白脸將：两將同列且中间无子"""
    rk = _find_king(b, True)
    bk = _find_king(b, False)
    if rk[0] < 0 or bk[0] < 0:
        return False
    if rk[0] != bk[0]:
        return False
    x = rk[0]
    y1, y2 = min(rk[1], bk[1]), max(rk[1], bk[1])
    for y in range(y1 + 1, y2):
        if b[x][y] != 0:
            return False
    return True


def _is_checked(b, is_red):
    """is_red 方的帥是否被将军（含白脸將）"""
    kx, ky = _find_king(b, is_red)
    if kx < 0:
        return True
    if _kings_facing(b):
        return True
    for x in range(BOARD_W):
        for y in range(BOARD_H):
            v = b[x][y]
            if not v:
                continue
            if (v > 0) == is_red:
                continue
            if (kx, ky) in _gen_moves(b, x, y):
                return True
    return False


def _legal_moves(b, x, y):
    """(x,y) 处棋子的合法落点：剔除走完后自己被将军的着法（含送將、白脸將）"""
    v = b[x][y]
    if not v:
        return []
    is_red = (v > 0)
    out = []
    for (tx, ty) in _gen_moves(b, x, y):
        nb = _board_copy(b)
        nb[tx][ty] = v
        nb[x][y] = 0
        if not _is_checked(nb, is_red):
            out.append((tx, ty))
    return out


def _has_legal_move(b, is_red):
    """is_red 方是否还有合法着法（将死 / 困毙判定）"""
    for x in range(BOARD_W):
        for y in range(BOARD_H):
            v = b[x][y]
            if not v:
                continue
            if (v > 0) != is_red:
                continue
            if _legal_moves(b, x, y):
                return True
    return False


def _moves_from_str_c(s):
    """历史：fx:fy:tx:ty:cap，用 ; 分隔（cap = 被吃子编码，0 = 没吃子）"""
    out = []
    if not s:
        return out
    for c in s.split(';'):
        if not c:
            continue
        try:
            a, bb, cc, d, e = c.split(':')
            out.append((int(a), int(bb), int(cc), int(d), int(e)))
        except Exception:
            pass
    return out


def _moves_to_str_c(moves):
    return ';'.join('%d:%d:%d:%d:%d' % m for m in moves)

def _undo_need_steps(rd, requester):
    """请求者要回到自己的决策点需退几步：1 / 2；不可悔返回 0"""
    moves = _moves_from_str_c(rd.get('moves', ''))
    if not moves:
        return 0
    if rd.get('current_turn', '') == requester:
        # 轮到自己 → 最后一手是对手走的，需退 2 步（对手应招 + 自己的招）
        return 2 if len(moves) >= 2 else 0
    # 轮到对手 → 最后一手是自己走的，退 1 步
    return 1

def _arm_undo_timeout(room_id, requester):
    """5 秒无人回应悔棋请求 → 自动拒绝并锁定请求者本回合"""
    def fire():
        try:
            rd = r.hgetall(f'cc:room:{room_id}')
            if not rd:
                return
            if rd.get('undo_pending', '') != requester:
                return
            owner = rd.get('owner', '')
            lkey = 'undo_lock_owner' if requester == owner else 'undo_lock_player2'
            pipe = r.pipeline()
            pipe.hset(f'cc:room:{room_id}', 'undo_pending', '')
            pipe.hset(f'cc:room:{room_id}', lkey, '1')
            pipe.execute()
            try:
                async_to_sync(get_channel_layer().group_send)(
                    f'cc_room_{room_id}',
                    {
                        'type': 'ws.undo.rejected',
                        'by': requester,
                        'timeout': True,
                        'undo_lock_owner': rd.get('undo_lock_owner', '0'),
                        'undo_lock_player2': rd.get('undo_lock_player2', '0'),
                    }
                )
            except Exception:
                pass
        except Exception:
            pass
    t = threading.Timer(5.0, fire)
    t.daemon = True
    t.start()
def _arm_draw_timeout(room_id, requester):
    """5 秒无人回应和棋请求 → 自动拒绝"""
    def fire():
        try:
            rd = r.hgetall(f'cc:room:{room_id}')
            if not rd:
                return
            if rd.get('draw_pending', '') != requester:
                return
            r.hset(f'cc:room:{room_id}', 'draw_pending', '')
            try:
                async_to_sync(get_channel_layer().group_send)(
                    f'cc_room_{room_id}',
                    {
                        'type': 'ws.draw.rejected',
                        'by': requester,
                        'draw_used_owner': rd.get('draw_used_owner', '0'),
                        'draw_used_player2': rd.get('draw_used_player2', '0'),
                    }
                )
            except Exception:
                pass
        except Exception:
            pass
    t = threading.Timer(5.0, fire)
    t.daemon = True
    t.start()

def _do_undo(room_id, requester):
    """悔棋：回退到请求者的决策点（1 或 2 步），还原棋盘/轮次/高亮，扣 1 次额度"""
    rd = r.hgetall(f'cc:room:{room_id}')
    if not rd:
        return None
    steps = _undo_need_steps(rd, requester)
    if steps <= 0:
        return None

    moves = _moves_from_str_c(rd.get('moves', ''))
    board = _board_from_str(rd.get('board', ''))
    if len(moves) < steps:
        return None

    for _ in range(steps):
        fx, fy, tx, ty, cap = moves.pop()
        piece = board[tx][ty]
        board[tx][ty] = cap      # 被吃的子放回原位
        board[fx][fy] = piece    # 走的子退回起点

    # 退完后轮次回到请求者
    mover = requester

    if moves:
        lf = moves[-1]
        lfx, lfy, ltx, lty = lf[0], lf[1], lf[2], lf[3]
    else:
        lfx = lfy = ltx = lty = None

    try:
        mc = max(0, int(rd.get('move_count', '0')) - steps)
    except Exception:
        mc = 0

    ukey = 'undo_used_owner' if requester == rd.get('owner', '') else 'undo_used_player2'
    try:
        used = int(rd.get(ukey, '0') or 0) + 1
    except Exception:
        used = 1

    pipe = r.pipeline()
    pipe.hset(f'cc:room:{room_id}', 'board', _board_to_str(board))
    pipe.hset(f'cc:room:{room_id}', 'moves', _moves_to_str_c(moves))
    pipe.hset(f'cc:room:{room_id}', 'current_turn', mover)
    pipe.hset(f'cc:room:{room_id}', 'move_count', str(mc))
    pipe.hset(f'cc:room:{room_id}', 'winner', '')
    pipe.hset(f'cc:room:{room_id}', 'over_reason', '')
    pipe.hset(f'cc:room:{room_id}', 'undo_pending', '')
    pipe.hset(f'cc:room:{room_id}', ukey, str(used))
    pipe.hset(f'cc:room:{room_id}', 'last_x', '' if ltx is None else str(ltx))
    pipe.hset(f'cc:room:{room_id}', 'last_y', '' if lty is None else str(lty))
    pipe.hset(f'cc:room:{room_id}', 'last_from_x', '' if lfx is None else str(lfx))
    pipe.hset(f'cc:room:{room_id}', 'last_from_y', '' if lfy is None else str(lfy))
    pipe.execute()

    return {
        'board': _board_to_str(board),
        'current_turn': mover,
        'last_x': '' if ltx is None else str(ltx),
        'last_y': '' if lty is None else str(lty),
        'last_from_x': '' if lfx is None else str(lfx),
        'last_from_y': '' if lfy is None else str(lfy),
        'mover': mover,
    }

def _clear_board_fields(room_id):
    """清空棋盘字段。
    注意：这里【绝不能】清 last_first —— 本函数开局时也会被调用，
    清了会导致轮换模式每局都从房主先，轮换彻底失效。"""
    r.hset(f'cc:room:{room_id}', 'board', '')
    r.hset(f'cc:room:{room_id}', 'winner', '')
    r.hset(f'cc:room:{room_id}', 'move_count', '0')
    r.hset(f'cc:room:{room_id}', 'last_x', '')
    r.hset(f'cc:room:{room_id}', 'last_y', '')
    r.hset(f'cc:room:{room_id}', 'over_reason', '')
    r.hset(f'cc:room:{room_id}', 'last_from_x', '')
    r.hset(f'cc:room:{room_id}', 'last_from_y', '')
    r.hset(f'cc:room:{room_id}', 'moves', '')
    r.hset(f'cc:room:{room_id}', 'undo_pending', '')
    r.hset(f'cc:room:{room_id}', 'undo_lock_owner', '0')
    r.hset(f'cc:room:{room_id}', 'undo_lock_player2', '0')
    r.hset(f'cc:room:{room_id}', 'move_deadline', '')


# ========== 先手模式 ==========

def _compute_red(rd):
    """本局执红者（=先手方）"""
    mode = rd.get('first_mode', FIRST_OWNER_RED)
    owner = rd.get('owner', '')
    p2 = rd.get('player2', '')
    if mode == FIRST_OPPONENT_RED:
        return p2 or ''
    if mode == FIRST_ALTERNATE:
        last = rd.get('last_first', '')
        if last == owner and p2:
            return p2
        if last == p2 and p2:
            return owner
        return owner
    return owner


def _apply_first_mode(room_id):
    rd = r.hgetall(f'cc:room:{room_id}')
    if not rd:
        return
    r.hset(f'cc:room:{room_id}', 'red_player', _compute_red(rd))


def _clear_alternate(room_id):
    """座位空过 → 轮换重置为房主先（不管再来的是不是同一个人）"""
    try:
        r.hdel(f'cc:room:{room_id}', 'last_first')
    except Exception:
        pass
    _apply_first_mode(room_id)


def _move_seconds(move_count):
    """开局前 6 手（每人各 3 步）30 秒，之后 75 秒"""
    return MOVE_FAST_SECONDS if move_count < MOVE_FAST_PLIES else MOVE_NORMAL_SECONDS


# ========== 离线处理：未开局 ==========

def _mark_offline_player2(room_id, nickname):
    rd = r.hgetall(f'cc:room:{room_id}')
    if not rd or rd.get('player2') != nickname:
        return

    try:
        r.set(f'cc:lost_ready:{room_id}', rd.get('player2_ready', '0'), ex=FAST_CONFIRM + 30)
    except Exception:
        pass

    r.hset(f'cc:room:{room_id}', 'player2', '')
    r.hset(f'cc:room:{room_id}', 'status', 'waiting')
    r.hset(f'cc:room:{room_id}', 'player2_ready', '0')
    _clear_board_fields(room_id)
    r.delete(f'cc:user_room:{nickname}')
    r.delete(f'cc:player2_conns:{room_id}')
    _hb_clear(room_id, 'player2')

    # 座位空过 → 轮换记录作废
    _clear_alternate(room_id)

    try:
        r.set(f'cc:lost_player:{room_id}', nickname, ex=FAST_CONFIRM + 10)
        r.set(f'cc:exited:{room_id}:{nickname}', '1', ex=EXITED_TRACE_TTL)
    except Exception:
        pass

    try:
        cur_ttl = r.ttl(f'cc:room:{room_id}')
        if cur_ttl is not None and cur_ttl < 0:
            r.expire(f'cc:room:{room_id}', ROOM_TTL_AFTER_RESET)
    except Exception:
        pass

    def confirm_left():
        time.sleep(FAST_CONFIRM)
        try:
            if r.get(f'cc:lost_player:{room_id}') != nickname:
                return
            r.delete(f'cc:lost_player:{room_id}')
            left_ttl = 0
            try:
                t = r.ttl(f'cc:room:{room_id}')
                if t and t > 0:
                    left_ttl = t
            except Exception:
                pass
            try:
                async_to_sync(get_channel_layer().group_send)(
                    f'cc_room_{room_id}',
                    {'type': 'ws.opponent.left', 'nickname': nickname, 'ttl': left_ttl}
                )
            except Exception:
                pass
        except Exception:
            pass

    t = threading.Thread(target=confirm_left, daemon=True)
    t.start()


def _mark_offline_owner(room_id, nickname):
    try:
        r.srem('cc:active_rooms', room_id)
    except Exception:
        pass

    def worker():
        time.sleep(OWNER_WAITING_CONFIRM)
        try:
            if r.get(f'cc:pending:{room_id}:owner') is None:
                return
            cur = r.hgetall(f'cc:room:{room_id}')
            if not cur or cur.get('owner') != nickname:
                return
            if cur.get('status') == 'playing':
                return
            p2 = cur.get('player2', '')
            if p2:
                r.delete(f'cc:user_room:{p2}')
            r.delete(f'cc:room:{room_id}')
            r.srem('cc:active_rooms', room_id)
            r.delete(f'cc:user_room:{nickname}')
            r.delete(f'cc:owner_conns:{room_id}')
            r.delete(f'cc:player2_conns:{room_id}')
            _hb_clear(room_id, 'owner')
            _hb_clear(room_id, 'player2')
            try:
                r.delete(f'cc:lost_player:{room_id}')
                r.delete(f'cc:lost_ready:{room_id}')
                r.delete(f'cc:pending:{room_id}:owner')
            except Exception:
                pass
            try:
                async_to_sync(get_channel_layer().group_send)(
                    f'cc_room_{room_id}',
                    {'type': 'ws.room.closed', 'message': '房主已退出，房间已解散'}
                )
            except Exception:
                pass
        except Exception:
            pass

    t = threading.Thread(target=worker, daemon=True)
    t.start()


# ========== 重置 / 兜底接管 ==========

def _room_ttl(room_id):
    try:
        cur_ttl = r.ttl(f'cc:room:{room_id}')
        if cur_ttl is None or cur_ttl < 0:
            r.expire(f'cc:room:{room_id}', ROOM_TTL_AFTER_RESET)
            return ROOM_TTL_AFTER_RESET
        if cur_ttl > 0:
            return cur_ttl
    except Exception:
        pass
    return 0


def _do_reset(room_id):
    rd = r.hgetall(f'cc:room:{room_id}')
    if not rd:
        return None
    owner = rd.get('owner', '')
    p2 = rd.get('player2', '')
    _clear_board_fields(room_id)
    r.hset(f'cc:room:{room_id}', 'owner_ready', '0')
    r.hset(f'cc:room:{room_id}', 'player2_ready', '0')
    r.hset(f'cc:room:{room_id}', 'undo_used_owner', '0')
    r.hset(f'cc:room:{room_id}', 'undo_used_player2', '0')
    r.hset(f'cc:room:{room_id}', 'status', 'full' if p2 else 'waiting')
    next_red = _compute_red(rd)
    r.hset(f'cc:room:{room_id}', 'red_player', next_red)
    r.hset(f'cc:room:{room_id}', 'game_first', next_red)
    r.hset(f'cc:room:{room_id}', 'current_turn', next_red)
    return (owner, p2, next_red)


def _reset_and_takeover(room_id):
    cur = r.hgetall(f'cc:room:{room_id}')
    if not cur or not cur.get('winner', ''):
        return False

    res = _do_reset(room_id)
    if not res:
        return False
    owner, p2, next_red = res

    try:
        if p2 and not _is_online(room_id, 'player2'):
            _mark_offline_player2(room_id, p2)
        if not _is_online(room_id, 'owner'):
            _mark_offline_owner(room_id, owner)
    except Exception:
        pass

    ttl_now = _room_ttl(room_id)
    cur2 = r.hgetall(f'cc:room:{room_id}')
    if not cur2:
        return True

    try:
        async_to_sync(get_channel_layer().group_send)(
            f'cc_room_{room_id}',
            {
                'type': 'ws.game.reset',
                'owner': cur2.get('owner', ''),
                'player2': cur2.get('player2', ''),
                'current_turn': cur2.get('current_turn', ''),
                'red_player': cur2.get('red_player', ''),
                'status': cur2.get('status', 'waiting'),
                'ttl': ttl_now,
            }
        )
    except Exception:
        pass
    return True


def _start_post_over_watchdog(room_id):
    def worker():
        time.sleep(POST_OVER_GRACE)
        try:
            cur = r.hgetall(f'cc:room:{room_id}')
            if not cur:
                return
            if not cur.get('winner', ''):
                return
            if _is_online(room_id, 'owner') and _is_online(room_id, 'player2'):
                return
            _reset_and_takeover(room_id)
        except Exception:
            pass

    t = threading.Thread(target=worker, daemon=True)
    t.start()


# ---------- 步时 ----------

def _move_left(rd):
    try:
        dl = float(rd.get('move_deadline', '') or 0)
    except Exception:
        return 0
    if not dl:
        return 0
    left = dl - time.time()
    return int(left + 0.5) if left > 0 else 0


def _arm_move_timer(room_id, nickname):
    try:
        cur = r.hgetall(f'cc:room:{room_id}')
        if not cur:
            return
        if cur.get('status') != 'playing':
            return
        if cur.get('winner', ''):
            return
        if cur.get('current_turn', '') != nickname:
            return
        try:
            mc = int(cur.get('move_count', '0'))
        except Exception:
            mc = 0
        seconds = _move_seconds(mc)
        deadline = time.time() + seconds
        r.hset(f'cc:room:{room_id}', 'move_deadline', str(deadline))
        t = threading.Thread(
            target=_move_timeout_watcher, args=(room_id, nickname, deadline, seconds), daemon=True
        )
        t.start()
    except Exception:
        pass


def _move_timeout_watcher(room_id, nickname, deadline, seconds):
    time.sleep(seconds + 2)
    try:
        cur = r.hgetall(f'cc:room:{room_id}')
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
        if not dl or time.time() < dl:
            return
        _timeout_lose(room_id, nickname)
    except Exception:
        pass


def _timeout_lose(room_id, loser):
    cur = r.hgetall(f'cc:room:{room_id}')
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

    r.hset(f'cc:room:{room_id}', 'winner', winner)
    r.hset(f'cc:room:{room_id}', 'over_reason', 'timeout')
    r.hset(f'cc:room:{room_id}', 'current_turn', '')
    r.hset(f'cc:room:{room_id}', 'move_deadline', '')
    r.hset(f'cc:room:{room_id}', 'undo_pending', '')

    try:
        async_to_sync(get_channel_layer().group_send)(
            f'cc_room_{room_id}',
            {'type': 'ws.game.over', 'winner': winner, 'loser': loser, 'reason': 'timeout'}
        )
    except Exception:
        pass
    _start_post_over_watchdog(room_id)


# ========== 兜底清扫 ==========

def _room_sweeper(room_id):
    lock_key = f'cc:sweeper:{room_id}'
    try:
        if not r.set(lock_key, '1', ex=30, nx=True):
            return
    except Exception:
        return

    while True:
        try:
            room = r.hgetall(f'cc:room:{room_id}')
            if not room:
                break

            owner = room.get('owner', '')
            p2 = room.get('player2', '')
            status = room.get('status', 'waiting')
            has_winner = bool(room.get('winner', ''))

            owner_alive = bool(owner) and _hb_count(room_id, 'owner') > 0
            p2_alive = bool(p2) and _hb_count(room_id, 'player2') > 0

            if status == 'playing' and not has_winner:
                # 自愈：重启后步时线程丢失，这里补判负 / 补挂计时
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
                    cur_ttl = r.ttl(f'cc:room:{room_id}')
                    if cur_ttl is not None and cur_ttl < 0:
                        r.expire(f'cc:room:{room_id}', ROOM_TTL_AFTER_RESET)
                    t = r.ttl(f'cc:room:{room_id}')
                    if t and t > 0:
                        ttl_now = t
                except Exception:
                    pass

                r.hset(f'cc:room:{room_id}', 'player2', '')
                r.hset(f'cc:room:{room_id}', 'status', 'waiting')
                r.delete(f'cc:user_room:{p2}')
                r.delete(f'cc:player2_conns:{room_id}')
                _hb_clear(room_id, 'player2')
                _clear_alternate(room_id)

                if not has_winner:
                    r.hset(f'cc:room:{room_id}', 'owner_ready', '0')
                    r.hset(f'cc:room:{room_id}', 'player2_ready', '0')
                    _clear_board_fields(room_id)

                try:
                    async_to_sync(get_channel_layer().group_send)(
                        f'cc_room_{room_id}',
                        {'type': 'ws.opponent.left', 'nickname': p2, 'ttl': ttl_now}
                    )
                except Exception:
                    pass

            if owner and not owner_alive:
                p2_now = r.hget(f'cc:room:{room_id}', 'player2') or ''
                r.delete(f'cc:room:{room_id}')
                r.srem('cc:active_rooms', room_id)
                r.delete(f'cc:user_room:{owner}')
                if p2_now:
                    r.delete(f'cc:user_room:{p2_now}')
                r.delete(f'cc:owner_conns:{room_id}')
                r.delete(f'cc:player2_conns:{room_id}')
                _hb_clear(room_id, 'owner')
                _hb_clear(room_id, 'player2')
                try:
                    async_to_sync(get_channel_layer().group_send)(
                        f'cc_room_{room_id}',
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


# ========== 房间管理（供 views 调用）==========

def create_room(nickname, first_mode=FIRST_OWNER_RED):
    if first_mode not in (FIRST_OWNER_RED, FIRST_OPPONENT_RED, FIRST_ALTERNATE):
        first_mode = FIRST_OWNER_RED

    old_room = r.get(f'cc:user_room:{nickname}')
    if old_room:
        old_data = r.hgetall(f'cc:room:{old_room}')
        if old_data and old_data.get('player2'):
            r.delete(f'cc:user_room:{old_data["player2"]}')
        r.delete(f'cc:room:{old_room}')
        r.srem('cc:active_rooms', old_room)
        r.delete(f'cc:owner_conns:{old_room}')
        r.delete(f'cc:player2_conns:{old_room}')
        _hb_clear(old_room, 'owner')
        _hb_clear(old_room, 'player2')

    for _ in range(100):
        room_id = str(random.randint(1000, 9999))
        if not r.exists(f'cc:room:{room_id}'):
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
        'last_from_x': '',
        'last_from_y': '',
        'moves': '',
        'over_reason': '',
        'undo_pending': '',
        'undo_lock_owner': '0',
        'undo_lock_player2': '0',
        'undo_used_owner': '0',
        'draw_used_owner': '0',
        'draw_used_player2': '0',
        'undo_used_player2': '0',
        'move_deadline': '',
        'first_mode': first_mode,
        'last_first': '',
        'red_player': '' if first_mode == FIRST_OPPONENT_RED else nickname,

    }
    r.hmset(f'cc:room:{room_id}', room_data)
    r.expire(f'cc:room:{room_id}', ROOM_TTL_AFTER_RESET)
    r.sadd('cc:active_rooms', room_id)
    r.set(f'cc:user_room:{nickname}', room_id, ex=ROOM_TTL_AFTER_RESET)
    return room_id


def get_active_rooms():
    room_ids = r.smembers('cc:active_rooms')
    rooms = []
    for rid in room_ids:
        data = r.hgetall(f'cc:room:{rid}')
        if data:
            rooms.append({
                'room_id': rid,
                'owner': data.get('owner', ''),
                'status': data.get('status', 'waiting'),
                'player2': data.get('player2', ''),
                'first_mode': data.get('first_mode', FIRST_OWNER_RED),
            })
        else:
            r.srem('cc:active_rooms', rid)
    return rooms


def get_room(room_id):
    return r.hgetall(f'cc:room:{room_id}')


def is_room_expired(room_id):
    return not r.exists(f'cc:room:{room_id}')


def admin_delete_nickname(nickname):
    room_id = r.get(f'cc:user_room:{nickname}')
    if room_id:
        room_data = r.hgetall(f'cc:room:{room_id}')
        if room_data:
            p2 = room_data.get('player2', '')
            if p2:
                r.delete(f'cc:user_room:{p2}')
        r.delete(f'cc:room:{room_id}')
        r.srem('cc:active_rooms', room_id)
        r.delete(f'cc:owner_conns:{room_id}')
        r.delete(f'cc:player2_conns:{room_id}')
        _hb_clear(room_id, 'owner')
        _hb_clear(room_id, 'player2')
    r.delete(f'cc:user_room:{nickname}')


# ========== Consumer ==========

class CchessConsumer(WebsocketConsumer):
    def connect(self):
        self.room_id = self.scope['url_route']['kwargs']['room_id']
        self.nickname = self.scope['session'].get('nickname', '')

        if not self.nickname:
            self.close(code=4001)
            return

        self.room_group_name = f'cc_room_{self.room_id}'
        self._handoff = False
        self.accept()

        room_data = r.hgetall(f'cc:room:{self.room_id}')
        self.role = None

        if not room_data:
            self.send(text_data=json.dumps({'action': 'error', 'message': '房间已解散'}))
            self.close(code=4003)
            return

        if room_data.get('owner') == self.nickname:
            self.role = 'owner'
        elif room_data.get('player2') == self.nickname:
            self.role = 'player2'
        else:
            lost = None
            try:
                lost = r.get(f'cc:lost_player:{self.room_id}')
            except Exception:
                lost = None

            if lost == self.nickname:
                if room_data.get('player2'):
                    self.send(text_data=json.dumps({
                        'action': 'error', 'message': '你的位置已被其他玩家占用'}))
                    self.close(code=4003)
                    return

                lost_ready = '0'
                try:
                    lost_ready = r.get(f'cc:lost_ready:{self.room_id}') or '0'
                except Exception:
                    lost_ready = '0'
                r.hset(f'cc:room:{self.room_id}', 'player2', self.nickname)
                r.hset(f'cc:room:{self.room_id}', 'status', 'full')
                r.hset(f'cc:room:{self.room_id}', 'player2_ready', lost_ready)
                r.set(f'cc:user_room:{self.nickname}', self.room_id, ex=ROOM_TTL_AFTER_RESET)
                r.delete(f'cc:lost_player:{self.room_id}')
                try:
                    r.delete(f'cc:exited:{self.room_id}:{self.nickname}')
                except Exception:
                    pass
                _apply_first_mode(self.room_id)
                self.role = 'player2'

                ttl = 0
                try:
                    t = r.ttl(f'cc:room:{self.room_id}')
                    if t and t > 0:
                        ttl = t
                except Exception:
                    pass
                cur = r.hgetall(f'cc:room:{self.room_id}')
                try:
                    async_to_sync(self.channel_layer.group_send)(
                        self.room_group_name,
                        {
                            'type': 'player.joined',
                            'nickname': self.nickname,
                            'owner': cur.get('owner', ''),
                            'player2': self.nickname,
                            'current_turn': cur.get('current_turn', ''),
                            'red_player': cur.get('red_player', ''),
                            'owner_ready': cur.get('owner_ready', '0'),
                            'player2_ready': lost_ready,
                            'ttl': ttl,
                        }
                    )
                except Exception:
                    pass

            else:
                exited = False
                try:
                    exited = r.get(f'cc:exited:{self.room_id}:{self.nickname}') is not None
                except Exception:
                    exited = False
                if exited:
                    try:
                        r.delete(f'cc:exited:{self.room_id}:{self.nickname}')
                    except Exception:
                        pass
                    self.send(text_data=json.dumps({'action': 'error', 'message': '你已不在本房间内'}))
                    self.close(code=4003)
                    return

                if room_data.get('winner', ''):
                    try:
                        r.set(f'cc:exited:{self.room_id}:{self.nickname}', '1', ex=EXITED_TRACE_TTL)
                    except Exception:
                        pass
                    self.send(text_data=json.dumps({'action': 'error', 'message': '你已不在本房间内'}))
                    self.close(code=4003)
                    return

        if self.role == 'owner':
            r.sadd(f'cc:owner_conns:{self.room_id}', self.channel_name)
            try:
                if r.exists(f'cc:room:{self.room_id}'):
                    r.sadd('cc:active_rooms', self.room_id)
            except Exception:
                pass
        elif self.role == 'player2':
            r.sadd(f'cc:player2_conns:{self.room_id}', self.channel_name)

        if self.role:
            _hb_touch(self.room_id, self.role, self.channel_name)
            was_pending = False
            try:
                was_pending = r.get(f'cc:pending:{self.room_id}:{self.role}') is not None
                r.delete(f'cc:pending:{self.room_id}:{self.role}')
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

        async_to_sync(self.channel_layer.group_add)(self.room_group_name, self.channel_name)

        try:
            threading.Thread(target=_room_sweeper, args=(self.room_id,), daemon=True).start()
        except Exception:
            pass

        self.send(text_data=json.dumps({'type': 'system', 'message': f'已连接到房间 {self.room_id}'}))
        self._send_state(r.hgetall(f'cc:room:{self.room_id}') or {})

        try:
            ttl = r.ttl(f'cc:room:{self.room_id}')
            if ttl and ttl > 0:
                def expire_notify(secs=ttl):
                    time.sleep(secs + 1)
                    try:
                        if not r.exists(f'cc:room:{self.room_id}'):
                            self.send(text_data=json.dumps({
                                'action': 'room_expired', 'message': '等待超时，房间已解散'}))
                    except Exception:
                        pass
                threading.Thread(target=expire_notify, daemon=True).start()
        except Exception:
            pass

    def disconnect(self, close_code):
        if not hasattr(self, 'room_id') or not hasattr(self, 'nickname'):
            return
        role = getattr(self, 'role', None)
        if role:
            _hb_leave(self.room_id, role, self.channel_name)
            try:
                r.set(f'cc:pending:{self.room_id}:{role}', '1', ex=GRACE_TTL + 15)
            except Exception:
                pass

        if getattr(self, '_handoff', False):
            return

        try:
            async_to_sync(self.channel_layer.group_discard)(self.room_group_name, self.channel_name)
        except Exception:
            pass

        if not role:
            return

        if role == 'owner':
            r.srem(f'cc:owner_conns:{self.room_id}', self.channel_name)
        else:
            r.srem(f'cc:player2_conns:{self.room_id}', self.channel_name)

        try:
            async_to_sync(self.channel_layer.group_send)(
                self.room_group_name,
                {'type': 'ws.opponent.lost', 'role': role, 'nickname': self.nickname}
            )
        except Exception:
            pass

        rd = r.hgetall(f'cc:room:{self.room_id}')
        if rd.get('status') == 'playing':
            return
        if role == 'player2':
            _mark_offline_player2(self.room_id, self.nickname)
        elif role == 'owner':
            _mark_offline_owner(self.room_id, self.nickname)

    def _send_state(self, rd):
        ttl = 0
        try:
            t = r.ttl(f'cc:room:{self.room_id}')
            if t and t > 0:
                ttl = t
        except Exception:
            pass
        try:
            ul_o = UNDO_LIMIT - int(rd.get('undo_used_owner', '0') or 0)
        except Exception:
            ul_o = UNDO_LIMIT
        try:
            ul_p = UNDO_LIMIT - int(rd.get('undo_used_player2', '0') or 0)
        except Exception:
            ul_p = UNDO_LIMIT
        try:
            mc = int(rd.get('move_count', '0') or 0)
        except Exception:
            mc = 0

        self.send(text_data=json.dumps({
            'action': 'room_state',
            'owner': rd.get('owner', ''),
            'player2': rd.get('player2', ''),
            'status': rd.get('status', 'waiting'),
            'current_turn': rd.get('current_turn', ''),
            'red_player': rd.get('red_player', ''),
            'first_mode': rd.get('first_mode', FIRST_OWNER_RED),
            'owner_ready': rd.get('owner_ready', '0'),
            'player2_ready': rd.get('player2_ready', '0'),
            'board': rd.get('board', ''),
            'winner': rd.get('winner', ''),
            'last_x': rd.get('last_x', ''),
            'last_y': rd.get('last_y', ''),
            'last_from_x': rd.get('last_from_x', ''),
            'last_from_y': rd.get('last_from_y', ''),
            'over_reason': rd.get('over_reason', ''),
            'undo_pending': rd.get('undo_pending', ''),
            'undo_lock_owner': rd.get('undo_lock_owner', '0'),
            'undo_lock_player2': rd.get('undo_lock_player2', '0'),
            'undo_left_owner': ul_o,
            'undo_left_player2': ul_p,
            'move_left': _move_left(rd),
            'move_total': _move_seconds(mc),
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
            rd = r.hgetall(f'cc:room:{self.room_id}')
            if rd:
                if rd.get('player2') == self.nickname:
                    r.hset(f'cc:room:{self.room_id}', 'player2', '')
                    r.hset(f'cc:room:{self.room_id}', 'status', 'waiting')
                    r.hset(f'cc:room:{self.room_id}', 'owner_ready', '0')
                    r.hset(f'cc:room:{self.room_id}', 'player2_ready', '0')
                    _clear_board_fields(self.room_id)
                    r.delete(f'cc:user_room:{self.nickname}')
                    r.delete(f'cc:player2_conns:{self.room_id}')
                    _hb_drop(self.room_id, 'player2', self.channel_name)
                    _clear_alternate(self.room_id)
                    try:
                        r.delete(f'cc:lost_player:{self.room_id}')
                        r.delete(f'cc:lost_ready:{self.room_id}')
                        r.delete(f'cc:pending:{self.room_id}:player2')
                    except Exception:
                        pass
                    ttl_now = _room_ttl(self.room_id)
                    try:
                        async_to_sync(self.channel_layer.group_send)(
                            self.room_group_name,
                            {'type': 'ws.opponent.left', 'nickname': self.nickname, 'ttl': ttl_now}
                        )
                    except Exception:
                        pass
                elif rd.get('owner') == self.nickname:
                    p2 = rd.get('player2', '')
                    if p2:
                        r.delete(f'cc:user_room:{p2}')
                    r.delete(f'cc:room:{self.room_id}')
                    r.srem('cc:active_rooms', self.room_id)
                    r.delete(f'cc:user_room:{self.nickname}')
                    r.delete(f'cc:owner_conns:{self.room_id}')
                    r.delete(f'cc:player2_conns:{self.room_id}')
                    _hb_drop(self.room_id, 'owner', self.channel_name)
                    try:
                        r.delete(f'cc:pending:{self.room_id}:owner')
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
            rd = r.hgetall(f'cc:room:{self.room_id}')
            if not rd:
                self.send(text_data=json.dumps({'action': 'room_expired', 'message': '房间已解散'}))
                self.close(code=4003)
                return
            if rd.get('owner') == self.nickname or rd.get('player2') == self.nickname:
                return
            if not rd.get('player2') and rd.get('status') != 'playing':
                owner_name = rd.get('owner', '')
                r.hset(f'cc:room:{self.room_id}', 'player2', self.nickname)
                r.hset(f'cc:room:{self.room_id}', 'status', 'full')
                r.set(f'cc:user_room:{self.nickname}', self.room_id, ex=ROOM_TTL_AFTER_RESET)
                r.hset(f'cc:room:{self.room_id}', 'owner_ready', '0')
                r.hset(f'cc:room:{self.room_id}', 'player2_ready', '0')
                _clear_board_fields(self.room_id)
                _apply_first_mode(self.room_id)
                cur = r.hgetall(f'cc:room:{self.room_id}')
                r.hset(f'cc:room:{self.room_id}', 'current_turn', cur.get('red_player', '') or owner_name)
                r.hset(f'cc:room:{self.room_id}', 'game_first', cur.get('red_player', '') or owner_name)
                try:
                    r.delete(f'cc:exited:{self.room_id}:{self.nickname}')
                    if r.get(f'cc:lost_player:{self.room_id}') == self.nickname:
                self.role = 'player2'
                r.sadd(f'cc:player2_conns:{self.room_id}', self.channel_name)
                _hb_touch(self.room_id, 'player2', self.channel_name)
                try:
                    r.delete(f'cc:pending:{self.room_id}:player2')
                except Exception:
                    pass

                ttl = _room_ttl(self.room_id)
                self.send(text_data=json.dumps({'action': 'joined', 'room_id': self.room_id}))
                try:
                    async_to_sync(self.channel_layer.group_send)(
                        self.room_group_name,
                        {
                            'type': 'player.joined',
                            'nickname': self.nickname,
                            'owner': owner_name,
                            'player2': self.nickname,
                            'current_turn': cur.get('current_turn', ''),
                            'red_player': cur.get('red_player', ''),
                            'owner_ready': '0',
                            'player2_ready': '0',
                            'ttl': ttl,
                        }
                    )
                except Exception:
                    pass
                return

            self.send(text_data=json.dumps({'action': 'error', 'message': '你已不在本房间内'}))
            self.close(code=4003)
            return

        # ---------- 走子 ----------
        if action == 'move':
            rd = r.hgetall(f'cc:room:{self.room_id}')
            if not rd:
                return
            if rd.get('status') != 'playing':
                return
            if rd.get('winner', ''):
                return
            if rd.get('undo_pending', ''):
                return
            if rd.get('current_turn', '') != self.nickname:
                return
            if rd.get('owner') != self.nickname and rd.get('player2') != self.nickname:
                return

            try:
                fx = int(data.get('from_x'))
                fy = int(data.get('from_y'))
                tx = int(data.get('x'))
                ty = int(data.get('y'))
            except Exception:
                return
            if not (_in_board(fx, fy) and _in_board(tx, ty)):
                return

            board = _board_from_str(rd.get('board', ''))
            v = board[fx][fy]
            if not v:
                return

            # 只能动自己的子
            i_am_red = (rd.get('red_player', '') == self.nickname)
            if (v > 0) != i_am_red:
                return

            if (tx, ty) not in _legal_moves(board, fx, fy):
                return

            captured = board[tx][ty]
            board[tx][ty] = v
            board[fx][fy] = 0

            try:
                move_count = int(rd.get('move_count', '0')) + 1
            except Exception:
                move_count = 1

            if self.nickname == rd.get('owner', ''):
                next_turn = rd.get('player2', '')
            else:
                next_turn = rd.get('owner', '')

            winner = ''
            over_reason = ''
            if captured and abs(captured) == 1:
                winner = self.nickname            # 吃掉將帥
                over_reason = 'checkmate'
            elif next_turn:
                next_is_red = (next_turn == rd.get('red_player', ''))
                if not _has_legal_move(board, next_is_red):
                    winner = self.nickname        # 对手无棋可走
                    over_reason = 'checkmate' if _is_checked(board, next_is_red) else 'stalemate'

            if winner:
                next_turn = ''

            moves = _moves_from_str_c(rd.get('moves', ''))
            moves.append((fx, fy, tx, ty, captured))

            r.hset(f'cc:room:{self.room_id}', 'board', _board_to_str(board))
            r.hset(f'cc:room:{self.room_id}', 'moves', _moves_to_str_c(moves))
            r.hset(f'cc:room:{self.room_id}', 'current_turn', next_turn)
            r.hset(f'cc:room:{self.room_id}', 'move_count', str(move_count))
            r.hset(f'cc:room:{self.room_id}', 'winner', winner)
            r.hset(f'cc:room:{self.room_id}', 'last_x', str(tx))
            r.hset(f'cc:room:{self.room_id}', 'last_y', str(ty))
            r.hset(f'cc:room:{self.room_id}', 'last_from_x', str(fx))
            r.hset(f'cc:room:{self.room_id}', 'last_from_y', str(fy))
            r.hset(f'cc:room:{self.room_id}', 'undo_lock_owner', '0')
            r.hset(f'cc:room:{self.room_id}', 'undo_lock_player2', '0')
            r.hset(f'cc:room:{self.room_id}', 'over_reason', over_reason)

            if winner:
                r.hset(f'cc:room:{self.room_id}', 'move_deadline', '')
            else:
                _arm_move_timer(self.room_id, next_turn)

            new_rd = r.hgetall(f'cc:room:{self.room_id}')

            try:
                async_to_sync(self.channel_layer.group_send)(
                    self.room_group_name,
                    {
                        'type': 'ws.move.made',
                        'from_x': fx, 'from_y': fy,
                        'x': tx, 'y': ty,
                        'v': v,
                        'captured': captured,
                        'next_turn': next_turn,
                        'winner': winner,
                        'reason': over_reason,
                        'by': self.nickname,
                        'last_x': tx, 'last_y': ty,
                        'last_from_x': fx, 'last_from_y': fy,
                        'move_left': _move_left(new_rd),
                        'move_total': _move_seconds(move_count),
                    }
                )
            except Exception:
                pass

            if winner:
                _start_post_over_watchdog(self.room_id)
            return

        # ---------- 悔棋：请求 ----------
        if action == 'undo_request':
            rd = r.hgetall(f'cc:room:{self.room_id}')
            if not rd:
                return
            if rd.get('status') != 'playing':
                return
            if rd.get('winner', ''):
                return
            if rd.get('undo_pending', ''):
                return
            owner = rd.get('owner', '')
            p2 = rd.get('player2', '')
            if self.nickname != owner and self.nickname != p2:
                return
            # 本回合已被拒 → 锁定
            if self.nickname == owner:
                if rd.get('undo_lock_owner', '0') == '1':
                    return
            else:
                if rd.get('undo_lock_player2', '0') == '1':
                    return
            # 额度
            ukey = 'undo_used_owner' if self.nickname == owner else 'undo_used_player2'
            try:
                used = int(rd.get(ukey, '0') or 0)
            except Exception:
                used = 0
            if used >= UNDO_LIMIT:
                return

            # 必须能回到自己的决策点（开局对手先走时，己方不可悔）
            if _undo_need_steps(rd, self.nickname) <= 0:
                return


            r.hset(f'cc:room:{self.room_id}', 'undo_pending', self.nickname)
            _arm_undo_timeout(self.room_id, self.nickname)
            try:
                async_to_sync(self.channel_layer.group_send)(
                    self.room_group_name,
                    {
                        'type': 'ws.undo.request',
                        'by': self.nickname,
                        'undo_left_owner': UNDO_LIMIT - int(rd.get('undo_used_owner', '0') or 0),
                        'undo_left_player2': UNDO_LIMIT - int(rd.get('undo_used_player2', '0') or 0),
                    }
                )
            except Exception:
                pass
            return

        # ---------- 悔棋：回应 ----------
        if action == 'draw_request':
            rd = r.hgetall(f'cc:room:{self.room_id}')
            if not rd:
                return
            if rd.get('status') != 'playing':
                return
            if rd.get('winner', ''):
                return
            if rd.get('draw_pending', ''):
                return
            owner = rd.get('owner', '')
            p2 = rd.get('player2', '')
            if self.nickname != owner and self.nickname != p2:
                return
            # 仅自己回合可发起
            if rd.get('current_turn', '') != self.nickname:
                return
            ukey = 'draw_used_owner' if self.nickname == owner else 'draw_used_player2'
            used = int(rd.get(ukey, '0') or 0)
            if used >= DRAW_MAX:
                return
            r.hset(f'cc:room:{self.room_id}', 'draw_pending', self.nickname)
            r.hset(f'cc:room:{self.room_id}', ukey, str(used + 1))
            _arm_draw_timeout(self.room_id, self.nickname)
            try:
                async_to_sync(self.channel_layer.group_send)(
                    self.room_group_name,
                    {
                        'type': 'ws.draw.request',
                        'by': self.nickname,
                        'seconds': DRAW_TIMEOUT,
                        'draw_left_owner': rd.get('draw_used_owner', '0'),
                        'draw_left_player2': rd.get('draw_used_player2', '0'),
                    }
                )
            except Exception:
                pass
            return

        if action == 'draw_response':
            rd = r.hgetall(f'cc:room:{self.room_id}')
            if not rd:
                return
            pend = rd.get('draw_pending', '')
            if not pend:
                return
            owner = rd.get('owner', '')
            p2 = rd.get('player2', '')
            if self.nickname == owner and pend == p2:
                pass
            elif self.nickname == p2 and pend == owner:
                pass
            else:
                return

            if not data.get('agree'):
                r.hset(f'cc:room:{self.room_id}', 'draw_pending', '')
                try:
                    async_to_sync(self.channel_layer.group_send)(
                        self.room_group_name,
                        {
                            'type': 'ws.draw.rejected',
                            'by': self.nickname,
                            'draw_used_owner': rd.get('draw_used_owner', '0'),
                            'draw_used_player2': rd.get('draw_used_player2', '0'),
                        }
                    )
                except Exception:
                    pass
                return

            # 同意 → 和棋
            pipe = r.pipeline()
            pipe.hset(f'cc:room:{self.room_id}', 'draw_pending', '')
            pipe.hset(f'cc:room:{self.room_id}', 'winner', 'draw')
            pipe.hset(f'cc:room:{self.room_id}', 'over_reason', 'draw')
            pipe.hset(f'cc:room:{self.room_id}', 'status', 'playing')
            pipe.execute()
            try:
                async_to_sync(self.channel_layer.group_send)(
                    self.room_group_name,
                    {'type': 'ws.game.over', 'winner': 'draw', 'loser': '', 'reason': 'draw'}
                )
            except Exception:
                pass
            return

        if action == 'undo_response':
            rd = r.hgetall(f'cc:room:{self.room_id}')
            if not rd:
                return
            pend = rd.get('undo_pending', '')
            if not pend:
                return
            owner = rd.get('owner', '')
            p2 = rd.get('player2', '')
            if self.nickname == owner and pend == p2:
                pass
            elif self.nickname == p2 and pend == owner:
                pass
            else:
                return

            if not data.get('agree'):
                # 拒绝 → 请求者本回合锁定
                lkey = 'undo_lock_owner' if pend == owner else 'undo_lock_player2'
                r.hset(f'cc:room:{self.room_id}', 'undo_pending', '')
                r.hset(f'cc:room:{self.room_id}', lkey, '1')
                try:
                    async_to_sync(self.channel_layer.group_send)(
                        self.room_group_name,
                        {
                            'type': 'ws.undo.rejected',
                            'by': self.nickname,
                            'undo_lock_owner': rd.get('undo_lock_owner', '0'),
                            'undo_lock_player2': rd.get('undo_lock_player2', '0'),
                        }
                    )
                except Exception:
                    pass
                return

            # 同意 → 执行还原
            res = _do_undo(self.room_id, pend)
            if not res:
                return
            new_rd = r.hgetall(f'cc:room:{self.room_id}')
            try:
                ul_o = UNDO_LIMIT - int(new_rd.get('undo_used_owner', '0') or 0)
                ul_p = UNDO_LIMIT - int(new_rd.get('undo_used_player2', '0') or 0)
            except Exception:
                ul_o = ul_p = UNDO_LIMIT

            res.update({
                'by': pend,
                'undo_left_owner': ul_o,
                'undo_left_player2': ul_p,
                'move_left': _move_left(new_rd),
                'move_total': _move_seconds(int(new_rd.get('move_count', '0') or 0)),
            })
            _arm_move_timer(self.room_id, res['current_turn'])
            try:
                async_to_sync(self.channel_layer.group_send)(
                    self.room_group_name,
                    dict(res, type='ws.undo.done')
                )
            except Exception:
                pass
            return

        # ---------- 投降 ----------
        if action == 'surrender':
            rd = r.hgetall(f'cc:room:{self.room_id}')
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

            r.hset(f'cc:room:{self.room_id}', 'winner', winner)
            r.hset(f'cc:room:{self.room_id}', 'over_reason', 'surrender')
            r.hset(f'cc:room:{self.room_id}', 'current_turn', '')
            r.hset(f'cc:room:{self.room_id}', 'undo_pending', '')
            r.hset(f'cc:room:{self.room_id}', 'move_deadline', '')

            try:
                async_to_sync(self.channel_layer.group_send)(
                    self.room_group_name,
                    {'type': 'ws.game.over', 'winner': winner, 'loser': self.nickname, 'reason': 'surrender'}
                )
            except Exception:
                pass
            _start_post_over_watchdog(self.room_id)
            return

        # ---------- 重置 ----------
        if action == 'reset_game':
            rd = r.hgetall(f'cc:room:{self.room_id}')
            if not rd:
                self.send(text_data=json.dumps({'action': 'room_expired', 'message': '房间已解散'}))
                return
            if rd.get('owner') != self.nickname and rd.get('player2') != self.nickname:
                self.send(text_data=json.dumps({'action': 'error', 'message': '你已不在本房间内'}))
                self.close(code=4003)
                return
            if not _reset_and_takeover(self.room_id):
                self._send_state(r.hgetall(f'cc:room:{self.room_id}') or rd)
            return

        # ---------- 同步 ----------
        if action == 'sync_state':
            rd = r.hgetall(f'cc:room:{self.room_id}')
            if not rd:
                self.send(text_data=json.dumps({'action': 'room_expired', 'message': '房间已解散'}))
                return
            if rd.get('owner') != self.nickname and rd.get('player2') != self.nickname:
                try:
                    r.set(f'cc:exited:{self.room_id}:{self.nickname}', '1', ex=EXITED_TRACE_TTL)
                except Exception:
                    pass
                self.send(text_data=json.dumps({'action': 'error', 'message': '你已不在本房间内'}))
                self.close(code=4003)
                return
            self._send_state(rd)
            return

        if action == 'join':
            room_data = r.hgetall(f'cc:room:{self.room_id}')
            if not room_data:
                self.send(text_data=json.dumps({'action': 'error', 'message': '房间不存在'}))
                return
            if room_data.get('owner') == self.nickname:
                self.send(text_data=json.dumps({'action': 'joined', 'room_id': self.room_id}))
                return
            if room_data.get('player2') == self.nickname:
                self.send(text_data=json.dumps({'action': 'joined', 'room_id': self.room_id}))
                self._handoff = True
                return
            if room_data.get('status') == 'full' or room_data.get('player2'):
                self.send(text_data=json.dumps({'action': 'error', 'message': '房间已满'}))
                return

            r.hset(f'cc:room:{self.room_id}', 'player2', self.nickname)
            r.hset(f'cc:room:{self.room_id}', 'status', 'full')
            r.set(f'cc:user_room:{self.nickname}', self.room_id, ex=ROOM_TTL_AFTER_RESET)
            r.hset(f'cc:room:{self.room_id}', 'owner_ready', '0')
            r.hset(f'cc:room:{self.room_id}', 'player2_ready', '0')
            _clear_board_fields(self.room_id)
            _apply_first_mode(self.room_id)
            cur = r.hgetall(f'cc:room:{self.room_id}')
            r.hset(f'cc:room:{self.room_id}', 'current_turn', cur.get('red_player', ''))
            r.hset(f'cc:room:{self.room_id}', 'game_first', cur.get('red_player', ''))

            self.send(text_data=json.dumps({'action': 'joined', 'room_id': self.room_id}))
            self._handoff = True

            ttl = _room_ttl(self.room_id)
            try:
                async_to_sync(self.channel_layer.group_send)(
                    self.room_group_name,
                    {
                        'type': 'player.joined',
                        'nickname': self.nickname,
                        'owner': room_data.get('owner', ''),
                        'player2': self.nickname,
                        'current_turn': cur.get('current_turn', ''),
                        'red_player': cur.get('red_player', ''),
                        'owner_ready': '0',
                        'player2_ready': '0',
                        'ttl': ttl,
                    }
                )
            except Exception:
                pass

        elif action == 'ready':
            room_data = r.hgetall(f'cc:room:{self.room_id}')
            if not room_data:
                return
            if room_data.get('status') == 'playing':
                return
            is_ready = '1' if data.get('ready') else '0'
            if room_data.get('owner') == self.nickname:
                r.hset(f'cc:room:{self.room_id}', 'owner_ready', is_ready)
            elif room_data.get('player2') == self.nickname:
                r.hset(f'cc:room:{self.room_id}', 'player2_ready', is_ready)
            else:
                return

            rd = r.hgetall(f'cc:room:{self.room_id}')
            owner_ready = rd.get('owner_ready', '0')
            player2_ready = rd.get('player2_ready', '0')

            if rd.get('player2') and owner_ready == '1' and player2_ready == '1':
                _apply_first_mode(self.room_id)
                red = r.hget(f'cc:room:{self.room_id}', 'red_player') or rd.get('owner', '')
                r.hset(f'cc:room:{self.room_id}', 'status', 'playing')
                r.hset(f'cc:room:{self.room_id}', 'board', _initial_board_str())
                r.hset(f'cc:room:{self.room_id}', 'winner', '')
                r.hset(f'cc:room:{self.room_id}', 'over_reason', '')
                r.hset(f'cc:room:{self.room_id}', 'move_count', '0')
                r.hset(f'cc:room:{self.room_id}', 'last_x', '')
                r.hset(f'cc:room:{self.room_id}', 'last_y', '')
                r.hset(f'cc:room:{self.room_id}', 'undo_pending', '')
                r.hset(f'cc:room:{self.room_id}', 'undo_lock_owner', '0')
                r.hset(f'cc:room:{self.room_id}', 'undo_lock_player2', '0')
                r.hset(f'cc:room:{self.room_id}', 'undo_used_owner', '0')
                r.hset(f'cc:room:{self.room_id}', 'undo_used_player2', '0')
                r.hset(f'cc:room:{self.room_id}', 'current_turn', red)
                r.hset(f'cc:room:{self.room_id}', 'game_first', red)
                # 轮换模式：记录本局先手，供下一局取反
                r.hset(f'cc:room:{self.room_id}', 'last_first', red)
                try:
                    r.persist(f'cc:room:{self.room_id}')
                except Exception:
                    pass

                _arm_move_timer(self.room_id, red)
                new_rd = r.hgetall(f'cc:room:{self.room_id}')

                try:
                    async_to_sync(self.channel_layer.group_send)(
                        self.room_group_name,
                        {
                            'type': 'ws.game.start',
                            'owner': rd.get('owner', ''),
                            'player2': rd.get('player2', ''),
                            'current_turn': red,
                            'red_player': red,
                            'move_left': _move_left(new_rd),
                            'move_total': MOVE_FAST_SECONDS,
                        }
                    )
                except Exception:
                    pass
            else:
                try:
                    async_to_sync(self.channel_layer.group_send)(
                        self.room_group_name,
                        {
                            'type': 'ws.ready.update',
                            'owner_ready': owner_ready,
                            'player2_ready': player2_ready,
                            'red_player': rd.get('red_player', ''),
                        }
                    )
                except Exception:
                    pass

        else:
            self.send(text_data=json.dumps({'type': 'echo', 'message': data.get('message', '')}))

    # ---------- 事件转发 ----------
    def player_joined(self, event):
        self.send(text_data=json.dumps({
            'action': 'player_joined',
            'nickname': event['nickname'],
            'owner': event['owner'],
            'player2': event['player2'],
            'current_turn': event.get('current_turn', ''),
            'red_player': event.get('red_player', ''),
            'owner_ready': event.get('owner_ready', '0'),
            'player2_ready': event.get('player2_ready', '0'),
            'ttl': event.get('ttl', 0),
        }))

    def ws_ready_update(self, event):
        self.send(text_data=json.dumps({
            'action': 'ready_update',
            'owner_ready': event['owner_ready'],
            'player2_ready': event['player2_ready'],
            'red_player': event.get('red_player', ''),
        }))

    def ws_game_start(self, event):
        self.send(text_data=json.dumps({
            'action': 'game_start',
            'owner': event['owner'],
            'player2': event['player2'],
            'current_turn': event.get('current_turn', ''),
            'red_player': event.get('red_player', ''),
            'move_left': event.get('move_left', 0),
            'move_total': event.get('move_total', MOVE_FAST_SECONDS),
        }))

    def ws_move_made(self, event):
        self.send(text_data=json.dumps({
            'action': 'move_made',
            'from_x': event.get('from_x'),
            'from_y': event.get('from_y'),
            'x': event.get('x'),
            'y': event.get('y'),
            'v': event.get('v'),
            'captured': event.get('captured', 0),
            'next_turn': event.get('next_turn', ''),
            'winner': event.get('winner', ''),
            'reason': event.get('reason', ''),
            'by': event.get('by', ''),
            'last_x': event.get('last_x', ''),
            'last_y': event.get('last_y', ''),
            'last_from_x': event.get('last_from_x', ''),
            'last_from_y': event.get('last_from_y', ''),
            'move_left': event.get('move_left', 0),
            'move_total': event.get('move_total', MOVE_NORMAL_SECONDS),
        }))

    def ws_draw_request(self, event):
        self.send(text_data=json.dumps({
            'action': 'draw_request',
            'by': event.get('by', ''),
            'seconds': event.get('seconds', DRAW_TIMEOUT),
            'draw_used_owner': event.get('draw_left_owner', '0'),
            'draw_used_player2': event.get('draw_left_player2', '0'),
        }))

    def ws_draw_rejected(self, event):
        self.send(text_data=json.dumps({
            'action': 'draw_rejected',
            'by': event.get('by', ''),
            'draw_used_owner': event.get('draw_used_owner', '0'),
            'draw_used_player2': event.get('draw_used_player2', '0'),
        }))

    def ws_undo_request(self, event):
        self.send(text_data=json.dumps({
            'action': 'undo_request',
            'by': event.get('by', ''),
            'steps': event.get('steps', 1),
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
            'last_from_x': event.get('last_from_x', ''),
            'last_from_y': event.get('last_from_y', ''),
            'move_left': event.get('move_left', 0),
            'move_total': event.get('move_total', MOVE_NORMAL_SECONDS),
            'undo_left_owner': event.get('undo_left_owner', UNDO_LIMIT),
            'undo_left_player2': event.get('undo_left_player2', UNDO_LIMIT),
        }))

    def ws_undo_rejected(self, event):
        self.send(text_data=json.dumps({
            'action': 'undo_rejected',
            'by': event.get('by', ''),
            'reason': event.get('reason', ''),
            'undo_lock_owner': event.get('undo_lock_owner', '0'),
            'undo_lock_player2': event.get('undo_lock_player2', '0'),
            'move_left': event.get('move_left', 0),
            'move_total': event.get('move_total', MOVE_NORMAL_SECONDS),
            'timeout': event.get('timeout', False),
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
            'red_player': event.get('red_player', ''),
            'status': event.get('status', 'waiting'),
            'ttl': event.get('ttl', 0),
        }))

    def ws_room_closed(self, event):
        self.send(text_data=json.dumps({
            'action': 'room_closed', 'message': event.get('message', '房间已解散')}))

    def ws_opponent_left(self, event):
        self.send(text_data=json.dumps({
            'action': 'opponent_left', 'nickname': event.get('nickname', ''), 'ttl': event.get('ttl', 0)}))

    def ws_opponent_lost(self, event):
        self.send(text_data=json.dumps({
            'action': 'opponent_lost', 'role': event.get('role', ''), 'nickname': event.get('nickname', '')}))

    def ws_opponent_reconnected(self, event):
        self.send(text_data=json.dumps({
            'action': 'opponent_reconnected', 'role': event.get('role', ''), 'nickname': event.get('nickname', '')}))