/root/HelloWorld/HelloWorld/models.py

#数据模型（用户、留言、DeletedName），留它才能重建表

from django.db import models

class SiteUser(models.Model):
    username = models.CharField(max_length=50, unique=True)
    is_banned = models.BooleanField(default=False)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['-created_at']

    def __str__(self):
        return self.username

class DeletedName(models.Model):
    """被管理员删除的名字，下次不可再次使用"""
    username = models.CharField(max_length=50, unique=True)
    deleted_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['-deleted_at']

    def __str__(self):
        return self.username

class Message(models.Model):
    username = models.CharField(max_length=50)
    content = models.TextField()
    created_at = models.DateTimeField(auto_now_add=True)
    is_pinned = models.BooleanField(default=False)
    pinned_at = models.DateTimeField(null=True, blank=True)
    is_banned = models.BooleanField(default=False)

    class Meta:
        ordering = ['-is_pinned', '-pinned_at', '-created_at']

    def __str__(self):
        return f'{self.username}: {self.content[:20]}'