# Generated by Django 4.1.5 on 2023-01-18 05:53

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('AutoTask', '0001_initial'),
    ]

    operations = [
        migrations.RenameField(
            model_name='taskrec',
            old_name='config',
            new_name='configJson',
        ),
        migrations.RenameField(
            model_name='taskscheme',
            old_name='config',
            new_name='configJson',
        ),
        migrations.RemoveField(
            model_name='taskpackage',
            name='config',
        ),
        migrations.RemoveField(
            model_name='taskrec',
            name='combine',
        ),
        migrations.RemoveField(
            model_name='taskscheme',
            name='combine',
        ),
        migrations.AddField(
            model_name='taskpackage',
            name='note',
            field=models.CharField(default=None, max_length=50, null=True),
        ),
        migrations.AddField(
            model_name='taskrec',
            name='blockKey',
            field=models.CharField(default=None, max_length=20, null=True),
        ),
        migrations.AddField(
            model_name='taskrec',
            name='note',
            field=models.CharField(default=None, max_length=50, null=True),
        ),
        migrations.AddField(
            model_name='taskscheme',
            name='blockKey',
            field=models.CharField(default=None, max_length=20, null=True),
        ),
        migrations.AddField(
            model_name='taskscheme',
            name='note',
            field=models.CharField(default=None, max_length=50, null=True),
        ),
        migrations.AlterField(
            model_name='taskpackage',
            name='tag',
            field=models.CharField(max_length=50, null=True),
        ),
        migrations.AlterField(
            model_name='taskrec',
            name='tag',
            field=models.CharField(max_length=50, null=True),
        ),
        migrations.AlterField(
            model_name='taskscheme',
            name='tag',
            field=models.CharField(max_length=50, null=True),
        ),
    ]