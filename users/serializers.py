from rest_framework import serializers
from django.contrib.auth.models import User

class ChangePasswordSerializer(serializers.ModelSerializer):
    
    password = serializers.CharField(write_only=True,required=True)
    confirm_password = serializers.CharField(write_only=True,required=True)
    old_password = serializers.CharField(write_only=True,required=True)

    class Meta:
        model = User
        fields = ["old_password","password","confirm_password"]
    
    def validate(self, attrs):
        user = self.context["request"].user
        if not user.check_password(attrs['old_password']):
            raise serializers.ValidationError({"detail":"Incorrect Current Password"})
        if attrs['password'] != attrs['confirm_password']:
            raise serializers.ValidationError({"detail":"Passwords do not match"})
        
        return super().validate(attrs)

    
    
    def update(self, instance, validated_data):
        instance.set_password(validated_data['password'])
        instance.save()
        return instance
    
