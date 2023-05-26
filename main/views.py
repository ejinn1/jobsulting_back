from django.shortcuts import render

# Create your views here.

from rest_framework.views import APIView
from rest_framework.response import Response
from .serializers import ParameterSerializer, JobsearchSerializer, SelectedJobsearchSerializer, StackSerializer
from rest_framework.generics import ListAPIView
from .models import Jobsearch, Jobcode
from .multipleai import run_argoritm



class ParameterView(APIView):
    def post(self, request):
        serializer = ParameterSerializer(data=request.data)
        if serializer.is_valid():
            # Access and use the validated data as needed
            data = serializer.validated_data
            location = data.get('location')
            salary = data.get('salary')
            career = data.get('career')
            education = data.get('education')
            work_type = data.get('work_type')
            skills = data.get('skills')
            
            ids = run_argoritm(location, salary, career, education, work_type, skills)

            jobsearch_data = Jobsearch.objects.filter(id__in=ids)
            serializer = SelectedJobsearchSerializer(jobsearch_data, many=True)
            jobsearch_data_json = serializer.data

            
            # print(jobsearch_data_json)


            response_data = {
                'message': 'Success',
                # 'location': location,
                # 'salary': salary,
                # 'career': career,
                # 'education': education,
                # 'work_type': work_type,
                # 'skills': skills,
                'ids': ids,
                'jobsearch_data_json': jobsearch_data_json
            }
        

            return Response(response_data)
        else:
            return Response(serializer.errors, status=400)



from rest_framework.pagination import LimitOffsetPagination

class JobsearchListView(ListAPIView):
    serializer_class = JobsearchSerializer
    pagination_class = LimitOffsetPagination

    def get_queryset(self):
        queryset = Jobsearch.objects.all()
        return queryset



class JobcodeListView(ListAPIView):
    serializer_class = StackSerializer
    
    def get_queryset(self):
        queryset = Jobcode.objects.all()
        return queryset
