from django.urls import path
from .views import UpdateOrCreateUserProfileView, KakaoLogInView, UserProfileView

urlpatterns = [
    path('kakao-login/', KakaoLogInView.as_view(), name='kakao-login'),
    path('update-profile/', UpdateOrCreateUserProfileView.as_view(), name='update-profile'),
    path('get-user-profile/', UserProfileView.as_view(), name='get-profile')
]
