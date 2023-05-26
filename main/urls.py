from django.urls import path
from .views import ParameterView, JobsearchListView, JobcodeListView

urlpatterns = [
    path('parameter-api/', ParameterView.as_view(), name='parameter'),
    path('jobsearch-api/', JobsearchListView.as_view(), name='jobsearch-list'),
    path('stack-api/',JobcodeListView.as_view(), name='stack-api')
]
