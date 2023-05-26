from rest_framework import serializers
from .models import Jobsearch, Jobcode

# 직무 매칭 파라미터
class ParameterSerializer(serializers.Serializer):
    location = serializers.CharField(max_length=20, allow_blank=True, required=False)
    salary = serializers.CharField(allow_blank=True, required=False)
    career = serializers.CharField(max_length=10, allow_blank=True, required=False)
    education = serializers.CharField(max_length=20, allow_blank=True, required=False)
    skills = serializers.ListField(child=serializers.CharField(max_length=100), allow_empty=True, required=False)
    work_type = serializers.CharField(max_length=10, allow_blank=True, required=False)

    def create(self, validated_data):
        location = validated_data.get('location')
        salary = validated_data.get('salary')
        career = validated_data.get('career')
        education = validated_data.get('education')
        skills = validated_data.get('skills')
        work_type = validated_data.get('work_type')

        # Perform desired operations using the input data, including skills
        # ...

        return validated_data
    
    
# 현재 채용 공고
class JobsearchSerializer(serializers.ModelSerializer):
    class Meta:
        model = Jobsearch
        fields = ['id','keyword_name', 'company_detail_name', 'title', 'location_name', 'educationlevel_name']



# ai 결과
class SelectedJobsearchSerializer(serializers.ModelSerializer):
    class Meta:
        model = Jobsearch
        fields = ['id', 'url', 'company_detail_name', 'title', 'location_name', 'jobtype_name', 'keyword_name', 'experiencelevel_name', 'educationlevel_name', 'salary_name', 'expiration_timestamp']


# 스택 가져오기
class StackSerializer(serializers.ModelSerializer):
    class Meta:
        model = Jobcode
        fields = ['id', 'code', 'keyword']
