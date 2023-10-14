from django.db import models

# Create your models here.

class Activecode(models.Model):
    id = models.IntegerField(primary_key=True)
    code = models.IntegerField()
    isactive = models.CharField(max_length=20)

    class Meta:
        managed = False
        db_table = 'activecode'


class Closetypecode(models.Model):
    id = models.IntegerField(primary_key=True)
    code = models.IntegerField()
    closetype = models.CharField(max_length=20)

    class Meta:
        managed = False
        db_table = 'closetypecode'


class Educationlevelcode(models.Model):
    id = models.IntegerField(primary_key=True)
    code = models.IntegerField()
    education = models.CharField(max_length=40)

    class Meta:
        managed = False
        db_table = 'educationlevelcode'


class Experiencelevelcode(models.Model):
    id = models.IntegerField(primary_key=True)
    code = models.IntegerField()
    experience = models.CharField(max_length=20)

    class Meta:
        managed = False
        db_table = 'experiencelevelcode'


class Jobcode(models.Model):
    id = models.BigIntegerField(primary_key=True)
    code = models.BigIntegerField()
    keyword = models.CharField(max_length=40)

    class Meta:
        managed = False
        db_table = 'jobcode'


class Jobsearch(models.Model):
    id = models.BigIntegerField(primary_key=True)
    url = models.TextField(blank=True, null=True)
    active = models.BigIntegerField(blank=True, null=True)
    keyword = models.TextField(blank=True, null=True)
    read_cnt = models.TextField(blank=True, null=True)
    # apply-cnt = models.TextField(blank=True, null=True)
    company_detail_href = models.TextField(blank=True, null=True)
    company_detail_name = models.TextField(blank=True, null=True)
    title = models.TextField(blank=True, null=True)
    location_code = models.TextField(blank=True, null=True)
    location_name = models.TextField(blank=True, null=True)
    jobtype_code = models.TextField(blank=True, null=True)
    jobtype_name = models.TextField(blank=True, null=True)
    keyword_code = models.TextField(blank=True, null=True)
    keyword_name = models.TextField(blank=True, null=True)
    experiencelevel_code = models.BigIntegerField(blank=True, null=True)
    experiencelevel_min = models.BigIntegerField(blank=True, null=True)
    experiencelevel_max = models.BigIntegerField(blank=True, null=True)
    experiencelevel_name = models.TextField(blank=True, null=True)
    educationlevel_code = models.TextField(blank=True, null=True)
    educationlevel_name = models.TextField(blank=True, null=True)
    salary_code = models.TextField(blank=True, null=True)
    salary_name = models.TextField(blank=True, null=True)
    closetype_code = models.TextField(blank=True, null=True)
    closetype_name = models.TextField(blank=True, null=True)
    posting_date = models.TextField(blank=True, null=True)
    modification_timestamp = models.TextField(blank=True, null=True)
    opening_timestamp = models.TextField(blank=True, null=True)
    expiration_timestamp = models.TextField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'jobsearch'


class Jobtypecode(models.Model):
    id = models.IntegerField(primary_key=True)
    code = models.IntegerField()
    type = models.CharField(max_length=20)

    class Meta:
        managed = False
        db_table = 'jobtypecode'


class Locationcode(models.Model):
    id = models.IntegerField(primary_key=True)
    code = models.IntegerField()
    location = models.CharField(max_length=20)

    class Meta:
        managed = False
        db_table = 'locationcode'


class Salarycode(models.Model):
    id = models.IntegerField(primary_key=True)
    code = models.IntegerField()
    salary = models.CharField(max_length=20)

    class Meta:
        managed = False
        db_table = 'salarycode'
