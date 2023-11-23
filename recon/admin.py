from django.contrib import admin

# Register your models here.
from django.contrib import admin
from .models import Bank,UserBankMapping,Recon

# Register your models here.
class BankAdmin(admin.ModelAdmin):
    list_display = ["name","bank_code","swift_code"]

class MappedUserAdmin(admin.ModelAdmin):
    list_display = ["bank","user"]

class ReconciliationAdmin(admin.ModelAdmin):
    pass

class UploadedFilesAdmin(admin.ModelAdmin):
    list_display = ["file","time"]

admin.site.register(UserBankMapping,MappedUserAdmin)
admin.site.register(Bank,BankAdmin)
admin.site.register(Recon,ReconciliationAdmin)



