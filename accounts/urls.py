from django.urls import path
from . import views

urlpatterns = [
    path('kakao-login/', views.KakaoLogInView.as_view(), name='kakao-login'),
]
