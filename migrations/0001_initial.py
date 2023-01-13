# Generated by Django 4.1.5 on 2023-01-13 05:44

import AutoTask.models
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='TaskPackage',
            fields=[
                ('createTime', models.BigIntegerField(default=AutoTask.models.getNowTimeStamp)),
                ('createUser', models.CharField(max_length=20)),
                ('name', models.CharField(max_length=50)),
                ('tag', models.JSONField(max_length=50, null=True)),
                ('planTime', models.BigIntegerField(default=0)),
                ('priority', models.SmallIntegerField(choices=[(10, 'Max'), (50, 'Scheme'), (100, 'Normal'), (200, 'Idle')], default=100)),
                ('pause', models.BooleanField(default=False)),
                ('cancel', models.BooleanField(default=False)),
                ('config', models.TextField()),
                ('taskPackageSn', models.BigAutoField(primary_key=True, serialize=False)),
                ('taskCount', models.PositiveIntegerField(default=0)),
                ('successCount', models.PositiveIntegerField(default=0)),
                ('failCount', models.PositiveIntegerField(default=0)),
                ('runningCount', models.PositiveIntegerField(default=0)),
                ('finished', models.BooleanField(default=False)),
            ],
            options={
                'abstract': False,
            },
        ),
        migrations.CreateModel(
            name='TaskRec',
            fields=[
                ('createTime', models.BigIntegerField(default=AutoTask.models.getNowTimeStamp)),
                ('createUser', models.CharField(max_length=20)),
                ('name', models.CharField(max_length=50)),
                ('tag', models.JSONField(max_length=50, null=True)),
                ('planTime', models.BigIntegerField(default=0)),
                ('executeTimeLimit', models.SmallIntegerField(default=AutoTask.models.getTaskTimeLimit)),
                ('retryDelay', models.SmallIntegerField(default=30)),
                ('retryLimit', models.SmallIntegerField(default=0)),
                ('priority', models.SmallIntegerField(choices=[(10, 'Max'), (50, 'Scheme'), (100, 'Normal'), (200, 'Idle')], default=100)),
                ('pause', models.BooleanField(default=False)),
                ('cancel', models.BooleanField(default=False)),
                ('config', models.TextField()),
                ('combine', models.BigIntegerField(null=True)),
                ('taskSn', models.BigAutoField(primary_key=True, serialize=False)),
                ('taskPackageSn', models.BigIntegerField(null=True)),
                ('taskSchemeSn', models.BigIntegerField(null=True)),
                ('taskState', models.SmallIntegerField(choices=[(-100, 'Fail'), (-10, 'Crash'), (0, 'Init'), (10, 'Running'), (100, 'Success')], default=0)),
                ('taskStateTime', models.BigIntegerField(default=0)),
                ('result', models.TextField(default=None, null=True)),
                ('detail', models.TextField(default=None, null=True)),
                ('execWarn', models.TextField(default=None, null=True)),
                ('errorCode', models.SmallIntegerField(choices=[(100001, 'Crash'), (200001, 'Timeout'), (300001, 'Invalidconfig')], null=True)),
                ('errorMessage', models.CharField(max_length=20, null=True)),
                ('retryTime', models.BigIntegerField(default=0)),
                ('timeout', models.BigIntegerField(null=True)),
                ('startTime', models.BigIntegerField(null=True)),
                ('endTime', models.BigIntegerField(null=True)),
                ('workerName', models.CharField(max_length=30, null=True)),
                ('execute', models.SmallIntegerField(default=0)),
                ('previousTask', models.ForeignKey(null=True, on_delete=django.db.models.deletion.PROTECT, related_name='followTask', to='AutoTask.taskrec')),
            ],
            options={
                'ordering': ('priority', 'taskSn'),
                'index_together': {('taskSn', 'priority', 'previousTask', 'planTime', 'retryTime', 'taskState', 'pause', 'cancel')},
            },
        ),
        migrations.CreateModel(
            name='TaskScheme',
            fields=[
                ('createTime', models.BigIntegerField(default=AutoTask.models.getNowTimeStamp)),
                ('createUser', models.CharField(max_length=20)),
                ('name', models.CharField(max_length=50)),
                ('tag', models.JSONField(max_length=50, null=True)),
                ('planTime', models.BigIntegerField(default=0)),
                ('executeTimeLimit', models.SmallIntegerField(default=AutoTask.models.getTaskTimeLimit)),
                ('retryDelay', models.SmallIntegerField(default=30)),
                ('retryLimit', models.SmallIntegerField(default=0)),
                ('pause', models.BooleanField(default=False)),
                ('cancel', models.BooleanField(default=False)),
                ('config', models.TextField()),
                ('combine', models.BigIntegerField(null=True)),
                ('taskSchemeSn', models.AutoField(primary_key=True, serialize=False)),
                ('cronStr', models.CharField(max_length=20)),
                ('interval', models.PositiveIntegerField(default=86400)),
                ('retainTime', models.PositiveIntegerField(default=604800)),
                ('message', models.CharField(blank=True, max_length=30, null=True)),
                ('currentTask', models.ForeignKey(null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='currentTaskScheme', to='AutoTask.taskrec')),
            ],
            options={
                'abstract': False,
            },
        ),
    ]
