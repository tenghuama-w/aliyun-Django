/root/HelloWorld/views.py

#首页与留言相关视图 

from django.shortcuts import render
from datetime import datetime

def index(request):
        return render(request,
'index.html', {
                'title': '马化腾的Django网站',
                'message': 'Hello Django',
                'time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                })