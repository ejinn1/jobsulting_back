from django.shortcuts import render

# Create your views here.

from rest_framework.views import APIView
from rest_framework.response import Response
from .serializers import ParameterSerializer, JobsearchSerializer, SelectedJobsearchSerializer, StackSerializer, WorkTypeSerializer, LocationSerializer, TopPostSerializer
from rest_framework.generics import ListAPIView
from .models import Jobsearch, Jobcode, Jobtypecode, Locationcode
from .multipleai import run_argoritm
from .trend import trend, optimize_trend
from .user_search import search_company


class TrendView(APIView):
    def get(self, request):
        stacks = request.GET.getlist('stack[]')  # Get the 'stack' parameter as a list from the query string
        # monthly_counts = trend(stacks)  # Execute the trend function with the provided stack
        monthly_counts = optimize_trend(stacks)
        
        # Prepare the response
        response_data = {
            'stack': stacks,
            'monthly_counts': monthly_counts
        }

        return Response(response_data)
    
    

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
            
            keyword ,ids, stars = run_argoritm(location, salary, career, education, work_type, skills)
            # print(ids)
            
            jobsearch_data = []
            for id in ids:
                try:
                    jobsearch_data.append(Jobsearch.objects.get(id=id))
                except Jobsearch.DoesNotExist:
                    # 예외처리
                    pass
    
            serializer = SelectedJobsearchSerializer(jobsearch_data, many=True)
            jobsearch_data_json = serializer.data
            
            # jobsearch_data = Jobsearch.objects.filter(id__in=ids)
            # serializer = SelectedJobsearchSerializer(jobsearch_data, many=True)
            # jobsearch_data_json = serializer.data

            response_data = {
                'message': 'Success',
                'keyword': keyword,
                'ids': ids,
                'stars': stars,
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


class JobTypecodeListView(ListAPIView):
    serializer_class = WorkTypeSerializer
    
    def get_queryset(self):
        queryset = Jobtypecode.objects.all()
        return queryset
    
    
class LocationcodeListView(ListAPIView):
    serializer_class = LocationSerializer
    
    def get_queryset(self):
        queryset = Locationcode.objects.all()
        return queryset
    
    
from django.db import models

    
class TopPostListView(APIView):
    serializer_class = TopPostSerializer
    model_fields = [field.name for field in Jobsearch._meta.get_fields() if isinstance(field, models.Field)]
    # print(model_fields)
    
    def get(self, request):
        queryset = Jobsearch.objects.all()
        sorted_queryset = sorted(queryset, key=lambda x: int(x.read_cnt), reverse=True)[:12]
        serializer = self.serializer_class(sorted_queryset, many=True)
        return Response(serializer.data)
    
    
    
from django.http import JsonResponse
import json

class CompanySearchView(APIView):
    def post(self, request):
        # Ajax 요청에서 검색어 추출
        body_data = json.loads(request.body)
        company_name = body_data.get('company_name')
        # print(company_name)
        
        # 검색 결과 실행
        filtered_data = search_company(company_name)
        
        # 검색 결과가 있는 경우, JSON 형식으로 데이터 전달
        if len(filtered_data) > 0:
             # JSON 형식으로 변환 후 전달
            return JsonResponse({'data': filtered_data})

        # 검색 결과가 없는 경우, 에러 메시지 전달
        error_message = f"No data found for {company_name}"
        return JsonResponse({'error': error_message}, status=404)

    def get(self, request):
        # GET 요청이 아닌 경우, "Method not allowed" 반환
        return JsonResponse({'status': 405, 'error': 'Method not allowed'})
        
        
    
    
from django.http import JsonResponse
from .mini_jobsulting import mini_jobsulting  # mini_jobsulting 함수를 import 합니다.

class MiniConsultView(APIView):
    def post(self, request, *args, **kwargs):
        body_data = json.loads(request.body)
        company_id = body_data.get('company_id')
        user_data = body_data.get('data')
        skills = user_data.get('skills')
        jobtype = user_data.get('work_type')
        education = user_data.get('education')
        location = user_data.get('location')
        career = user_data.get('career')
        salary = user_data.get('salary')
        print(company_id)


        if company_id is not None:
            result = mini_jobsulting(company_id, skills, jobtype, education, location, career, salary)
            return JsonResponse(result)  # 함수로부터 반환된 데이터를 JsonResponse 형식으로 반환
        else:
            return JsonResponse({"error": "Invalid input data"}, status=400)

    def get(self, request, *args, **kwargs):
        return JsonResponse({"error": "Invalid request method"}, status=400)
    
   
   
from django.db.models import F, Sum, Count

class KeywordPostsView(APIView):
    serializer_class = TopPostSerializer

    def get(self, request):
        keyword = request.GET.get('keyword', '').lower()
        
        queryset = Jobsearch.objects.filter(keyword=keyword).annotate(read_cnt_int=Sum(F('read_cnt')))
        sorted_queryset = queryset.order_by('-read_cnt_int')[:12]
        serializer = self.serializer_class(sorted_queryset, many=True)

        return Response(serializer.data)
