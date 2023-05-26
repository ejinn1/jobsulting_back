from django.shortcuts import render
from django.contrib.auth import login
from rest_framework.views import APIView
from rest_framework import status
from rest_framework.response import Response
from django.contrib.auth import get_user_model
import requests

User = get_user_model()

class KakaoLogInView(APIView):
    def post(self, request):
        try:
            code = request.data.get("code")
            print("CODE", code)
            access_token = requests.post(
                "https://kauth.kakao.com/oauth/token",
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                data={
                    "grant_type": "authorization_code",
                    "client_id": "3520f65988c6ec5c88bc150693095916",
                    "redirect_uri": "http://localhost:3000/kakao",
                    "code": code
                },
            )
            print("ACCESS TOKEN RESPONSE:", access_token.json())
            access_token = access_token.json().get("access_token")
            
            user_data = requests.get(
                "https://kapi.kakao.com/v2/user/me",
                headers={
                    "Authorization": f"Bearer {access_token}",
                    "Content-type": "application/x-www-form-urlencoded;charset=utf-8",
                },
            )
            user_data = user_data.json()
            kakao_account = user_data.get("kakao_account")
            profile = kakao_account.get("profile")

            if 'email' not in kakao_account:
                raise ValueError('Email is required')

            email = kakao_account.get("email")
            user, created = User.objects.get_or_create(email=email)

            if created:
                user.username = profile.get("nickname")
                user.name = profile.get("nickname")
                user.set_unusable_password()
                user.save()

            login(request, user)
            return Response(status=status.HTTP_200_OK, data={'name': user.name, 'email': user.email, 'access_token':access_token})

        except ValueError as e:
            return Response(status=status.HTTP_400_BAD_REQUEST, data={'message': str(e)})

        except Exception as e:
            return Response(status=status.HTTP_400_BAD_REQUEST, data={'message': f'An unknown error occurred: {str(e)}'})



