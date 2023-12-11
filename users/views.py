from rest_framework.permissions import IsAuthenticated,AllowAny
from rest_framework import generics
from django.contrib.auth.models import User
from .serializers import ChangePasswordSerializer

class ChangePasswordView(generics.UpdateAPIView):
    queryset = User.objects.all()
    permission_classes = (AllowAny,)
    serializer_class = ChangePasswordSerializer
