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

import json
from django.http import JsonResponse, HttpResponseBadRequest
from django.views import View
from accounts.models import User
from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator

from rest_framework import serializers
from .models import UserProfile, Skill

@method_decorator(csrf_exempt, name='dispatch')
class UpdateOrCreateUserProfileView(View):
    def post(self, request):
        try:
            data = json.loads(request.body)
            email = data.get('email')
            user = User.objects.get(email=email)

            # Try to get existing profile or create a new one
            profile, created = UserProfile.objects.get_or_create(author=user)

            profile.location = data.get('location', profile.location)
            profile.salary = data.get('salary', profile.salary)
            profile.career = data.get('career', profile.career)
            profile.education = data.get('education', profile.education)
            profile.work_type = data.get('work_type', profile.work_type)

            # handle skills
            skill_data = data.get('skills', [])
            if isinstance(skill_data, list):
                skills = [Skill.objects.get_or_create(skill_name=skill)[0] for skill in skill_data]
                profile.skills.clear()
                profile.skills.set(skills)
            else:
                return JsonResponse({'error': 'Invalid skill data provided, please provide a list of skills'},
                                    status=400)
            # skills = [Skill.objects.get_or_create(skill_name=skill)[0] for skill in skill_data]
            # profile.skills.clear()
            # profile.skills.set(skills)

            profile.save()

            if created:
                return JsonResponse("Profile Created", safe=False, status=201)
            else:
                return JsonResponse("Profile Updated", safe=False, status=200)

        except KeyError as e:
            return HttpResponseBadRequest(f"Field '{str(e)}' is missing in the payload")
        except User.DoesNotExist:
            return JsonResponse({'error': 'User not found'}, status=404)


@method_decorator(csrf_exempt, name='dispatch')
class UserProfileView(View):
    def post(self, request):
        try:
            data = json.loads(request.body)
            email = data.get('email')
            profile = UserProfile.objects.get(author__email=email)

            # Serialize profile data as needed
            serialized_profile = {
                'location': profile.location,
                'salary': profile.salary,
                'career': profile.career,
                'education': profile.education,
                'work_type': profile.work_type,
                # Include other fields as needed
                 'skills': list(profile.skills.values_list('skill_name', flat=True))
            }

            return JsonResponse(serialized_profile, safe=False)

        except KeyError as e:
            return JsonResponse({'error': f"Field '{str(e)}' is missing in the payload"}, status=400)
        except UserProfile.DoesNotExist:
            return JsonResponse({'error': 'User profile not found'}, status=404)
