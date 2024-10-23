from django import forms

class SystemSkuForm(forms.Form):
    systemSku = forms.CharField(widget=forms.Textarea, help_text="Enter SKUs separated by commas.")
