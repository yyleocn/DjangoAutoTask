# Generated by Django 4.1.5 on 2023-01-18 08:26

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('AutoTask', '0003_alter_taskrec_executetimelimit_and_more'),
    ]

    operations = [
        migrations.AlterField(
            model_name='taskpackage',
            name='priority',
            field=models.SmallIntegerField(choices=[(10, 'Max'), (20, 'Scheme'), (50, 'Normal'), (100, 'Idle')], default=50),
        ),
        migrations.AlterField(
            model_name='taskrec',
            name='errorCode',
            field=models.IntegerField(choices=[(1001, 'Crash'), (2001, 'Timeout'), (3001, 'Invalidconfig')], null=True),
        ),
        migrations.AlterField(
            model_name='taskrec',
            name='priority',
            field=models.SmallIntegerField(choices=[(10, 'Max'), (20, 'Scheme'), (50, 'Normal'), (100, 'Idle')], default=50),
        ),
    ]
