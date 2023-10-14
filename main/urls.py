from django.urls import path
from .views import ParameterView, JobsearchListView, JobcodeListView
from .views import JobTypecodeListView, TrendView, LocationcodeListView
from .views import TopPostListView, CompanySearchView, MiniConsultView, KeywordPostsView


urlpatterns = [
    path('parameter-api/', ParameterView.as_view(), name='parameter'),
    path('jobsearch-api/', JobsearchListView.as_view(), name='jobsearch-list'),
    path('stack-api/',JobcodeListView.as_view(), name='stack-api'),
    path('jobtype-api/',JobTypecodeListView.as_view(), name='jobtype-api'),
    path('trend-api/',TrendView.as_view(), name='trend-api'),
    path('location-api/', LocationcodeListView.as_view(), name='location-api'),
    path('toppost-api/', TopPostListView.as_view(), name='toppost'),
    path('company-search/', CompanySearchView.as_view(), name='company_search'),
    path('mini-consulting/', MiniConsultView.as_view(), name='mini-consulting'),
    path('keyword/', KeywordPostsView.as_view(), name='keyword'),
]
