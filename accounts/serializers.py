from rest_framework import serializers
from .models import UserProfile, Skill

class SkillSerializer(serializers.ModelSerializer):
    class Meta:
        model = Skill
        fields = ['skill_name']

class UserProfileSerializer(serializers.ModelSerializer):
    skills = SkillSerializer(many=True)

    class Meta:
        model = UserProfile
        fields = ['location', 'salary', 'career', 'education', 'skills', 'work_type']

